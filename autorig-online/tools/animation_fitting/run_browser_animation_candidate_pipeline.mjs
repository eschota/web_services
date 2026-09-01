#!/usr/bin/env node
/**
 * Content-addressed, browser-only single-candidate animation fitting pipeline.
 *
 * This module is deliberately an inspector/command author.  It never executes
 * a subprocess, touches a database, dispatches GPU work, or grants approval.
 * Every invocation validates an externally SHA-pinned spec and all artifacts
 * already present, then create-exclusively writes one immutable state containing
 * at most one exact `shell:false` command for the next stage.
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
    ANIMATION_FITTING_ACTION_CONTRACTS,
    resolveAnimationFittingAction,
} from '../../static/js/animation-fitting-action-contract.js';
import {
    HOOF_CONTACT_INFERENCE_CONTRACT,
    deriveSam2GroundEvidence,
    diagnoseHoofContacts,
    validatePinnedHoofContactSchedule,
} from '../../static/js/animation-fitting-hoof-contact-inference.js';
import {
    BROWSER_FIT_CANARY_DEFAULTS,
    deriveFloat32LoopVelocityInvariantGate,
    measureLoopVelocitySeam,
} from './browser_fit_canary.mjs';
import {
    loadMaskFrames,
    prepareBridgeObservations,
} from './diagnose_browser_hoof_contacts.mjs';
import { inspectPublishedSemanticVisualQa } from './browser_animation_visual_phase_qa.mjs';

export const BROWSER_CANDIDATE_PIPELINE_SPEC_SCHEMA =
    'autorig.browser-animation-candidate-pipeline-spec.v2';
export const BROWSER_CANDIDATE_PIPELINE_STATE_SCHEMA =
    'autorig.browser-animation-candidate-pipeline-state.v2';
export const BROWSER_CANDIDATE_PIPELINE_TRUST_SCHEMA =
    'autorig.browser-animation-candidate-pipeline-trust.v2';
export const PROTECTED_STAGE_EXECUTION_RECEIPT_SCHEMA =
    'autorig.protected-animation-fitting-stage-execution-receipt.v1';
export const RAW_SEMANTIC_PASS_RECEIPT_SCHEMA =
    'autorig.raw-semantic-pass-receipt.v1';
export const RAW_SEMANTIC_EVIDENCE_SCHEMA =
    'autorig.server-raw-semantic-video-qa.v1';

const TOOLS_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
const BACKEND_DIRECTORY = path.resolve(TOOLS_DIRECTORY, '../../backend');
const STATIC_JS_DIRECTORY = path.resolve(TOOLS_DIRECTORY, '../../static/js');
const PYTHON_WORKING_DIRECTORY = path.dirname(TOOLS_DIRECTORY);
const SHA256_RE = /^[0-9a-f]{64}$/;
const GIT_COMMIT_RE = /^[0-9a-f]{40}$/;
const FRAME_COUNTS = new Set([33, 49, 65, 97]);
const OUTPUT_FPS = 30;
const TRUST_SECRET_ENV = 'AUTORIG_ANIMATION_FITTING_TRUST_HMAC_SECRET';
const TRUST_MAX_TTL_MS = 15 * 60 * 1000;
const TRUST_CLOCK_SKEW_MS = 30 * 1000;
const SOURCE_DISCOVERY_ALGORITHM = 'relative-static-import-closure.v1';
const PROTECTED_STAGE_PIN_FIELDS = Object.freeze({
    officialTracking: 'observationsArtifactSetSha256',
    initialFit: 'initialFitArtifactSetSha256',
    contactRefit: 'contactRefitArtifactSetSha256',
    visualQa: 'visualQaArtifactSetSha256',
});

export const BROWSER_CANDIDATE_AUTHORITATIVE_PATHS = Object.freeze({
    taxonomy: path.join(BACKEND_DIRECTORY, 'animal_animation_taxonomy.v1.json'),
    actionPrompts: path.join(
        BACKEND_DIRECTORY,
        'animation_fitting',
        'specs',
        'action_prompts.v1.json',
    ),
    browserActionContract: path.join(STATIC_JS_DIRECTORY, 'animation-fitting-action-contract.js'),
});

export const BROWSER_CANDIDATE_TOOL_SOURCE_PATHS = Object.freeze({
    browserFit: path.join(TOOLS_DIRECTORY, 'browser_fit_canary.mjs'),
    visualQa: path.join(TOOLS_DIRECTORY, 'browser_horse_visual_phase_qa.mjs'),
    semanticVisualQa: path.join(TOOLS_DIRECTORY, 'browser_animation_visual_phase_qa.mjs'),
    hoofDiagnostic: path.join(TOOLS_DIRECTORY, 'diagnose_browser_hoof_contacts.mjs'),
    contactManifestAuthor: path.join(TOOLS_DIRECTORY, 'author_browser_contact_refit_manifest.mjs'),
    contactRefit: path.join(TOOLS_DIRECTORY, 'browser_contact_refit.mjs'),
    trackingInit: path.join(TOOLS_DIRECTORY, 'tracking_runtime', '__init__.py'),
    trackingMain: path.join(TOOLS_DIRECTORY, 'tracking_runtime', '__main__.py'),
    trackingCli: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'cli.py'),
    trackingCore: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'core.py'),
    trackingModels: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'models.py'),
    trackingOfficialBackends: path.join(
        TOOLS_DIRECTORY,
        'tracking_runtime',
        'official_backends.py',
    ),
    trackingRuntimeLockCode: path.join(
        TOOLS_DIRECTORY,
        'tracking_runtime',
        'runtime_lock.py',
    ),
    trackingRuntimeLockContract: path.join(
        TOOLS_DIRECTORY,
        'tracking_runtime',
        'runtime-lock.v1.json',
    ),
    trackingContactIntegration: path.join(
        TOOLS_DIRECTORY,
        'tracking_runtime',
        'contact_integration.py',
    ),
    trackingContactSolver: path.join(
        TOOLS_DIRECTORY,
        'tracking_runtime',
        'contact_solver.py',
    ),
});

/**
 * This table is deliberately exhaustive.  A newly added taxonomy action cannot
 * silently fall through to the unconstrained fitter.  Locomotion clips that
 * need planted feet either pass the declared gait/contact stages or stop.
 */
export const BROWSER_CANDIDATE_ACTION_POLICY = Object.freeze({
    idle_neutral: 'direct',
    idle_alert: 'direct',
    idle_relaxed: 'direct',
    idle_look_around: 'direct',
    idle_fidget: 'direct',
    walk_forward: 'contact_refit',
    walk_backward: 'contact_refit',
    // The only current contact refitter is deliberately WALK-specific.  A
    // diagonal-pair trot must remain closed until a dedicated trot schedule and
    // refitter exist; routing it through WALK_FOOT_ORDER would be unsound.
    trot_jog: 'unsupported_gait_contact',
    run: 'unsupported_gait_contact',
    sprint: 'unsupported_gait_contact',
    turn_left_90: 'direct',
    turn_right_90: 'direct',
    turn_around_180: 'direct',
    stop_brake: 'direct',
    jump_air: 'direct',
    fall: 'direct',
    jump_start: 'direct',
    jump_land: 'direct',
    jump_full: 'direct',
    attack_primary: 'direct',
    attack_secondary: 'direct',
    attack_heavy: 'direct',
    hit_front: 'direct',
    hit_left: 'direct',
    hit_right: 'direct',
    death: 'direct',
    get_up: 'direct',
    eat_interact: 'direct',
    sleep_rest: 'direct',
    vocalize_emote: 'direct',
});

const CONTACT_REFIT_POLICIES = new Set(['contact_refit']);

function fail(message) {
    throw new Error(message);
}

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        fail(`${field} must be an object`);
    }
    return value;
}

function string(value, field) {
    if (typeof value !== 'string' || !value.trim()) fail(`${field} must be a non-empty string`);
    return value;
}

function integer(value, field, minimum = 0) {
    if (!Number.isInteger(value) || value < minimum) {
        fail(`${field} must be an integer >= ${minimum}`);
    }
    return value;
}

function finite(value, field) {
    const result = Number(value);
    if (!Number.isFinite(result)) fail(`${field} must be finite`);
    return result;
}

function sha256(value, field) {
    if (typeof value !== 'string' || !SHA256_RE.test(value)) {
        fail(`${field} must be a lowercase SHA-256`);
    }
    return value;
}

function sha256Buffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function pathKey(value) {
    const resolved = path.resolve(value);
    return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function samePath(left, right) {
    return pathKey(left) === pathKey(right);
}

function isInside(parent, child) {
    const relative = path.relative(path.resolve(parent), path.resolve(child));
    return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function pathsOverlap(left, right) {
    return isInside(left, right) || isInside(right, left);
}

function requireExactKeys(value, keys, field) {
    const actual = Object.keys(object(value, field)).sort();
    const expected = [...keys].sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        fail(`${field} must contain exactly: ${expected.join(', ')}`);
    }
}

function statIdentity(stat) {
    return {
        dev: String(stat.dev),
        ino: String(stat.ino),
        nlink: Number(stat.nlink),
        size: Number(stat.size),
        mtimeNs: String(stat.mtimeNs ?? BigInt(Math.trunc(Number(stat.mtimeMs) * 1e6))),
        ctimeNs: String(stat.ctimeNs ?? BigInt(Math.trunc(Number(stat.ctimeMs) * 1e6))),
    };
}

function sameIdentity(left, right) {
    return left.dev === right.dev && left.ino === right.ino && left.nlink === right.nlink
        && left.size === right.size && left.mtimeNs === right.mtimeNs && left.ctimeNs === right.ctimeNs;
}

function sameDirectoryObject(left, right) {
    return left.dev === right.dev && left.ino === right.ino;
}

function resolveUnlinkedPath(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, field));
    let realpath;
    let lstat;
    try {
        lstat = fs.lstatSync(filename, { bigint: true });
        realpath = fs.realpathSync.native(filename);
    } catch (error) {
        fail(`${field} cannot be resolved safely: ${error.message}`);
    }
    if (lstat.isSymbolicLink() || !samePath(filename, realpath)) {
        fail(`${field} must not traverse a symlink, junction, or reparse alias: ${filename}`);
    }
    return { filename, realpath: path.resolve(realpath), lstat };
}

function secureDirectory(filenameValue, field) {
    const resolved = resolveUnlinkedPath(filenameValue, field);
    if (!resolved.lstat.isDirectory()) fail(`${field} must be a directory: ${resolved.filename}`);
    const identity = statIdentity(resolved.lstat);
    const after = resolveUnlinkedPath(resolved.filename, field);
    if (!after.lstat.isDirectory() || !sameIdentity(identity, statIdentity(after.lstat))) {
        fail(`${field} changed while it was inspected`);
    }
    return { path: resolved.filename, realpath: resolved.realpath, identity };
}

function readSnapshot(filenameValue, field) {
    const resolved = resolveUnlinkedPath(filenameValue, field);
    if (!resolved.lstat.isFile()) fail(`${field} must be a file: ${resolved.filename}`);
    const beforePath = statIdentity(resolved.lstat);
    if (beforePath.nlink !== 1) fail(`${field} must not be hard-linked`);
    const flags = fs.constants.O_RDONLY | (process.platform === 'win32' ? 0 : fs.constants.O_NOFOLLOW);
    let descriptor;
    try {
        descriptor = fs.openSync(resolved.filename, flags);
        const beforeHandle = statIdentity(fs.fstatSync(descriptor, { bigint: true }));
        if (!sameIdentity(beforePath, beforeHandle)) fail(`${field} changed before it was opened`);
        const buffer = fs.readFileSync(descriptor);
        const afterHandle = statIdentity(fs.fstatSync(descriptor, { bigint: true }));
        const afterPath = resolveUnlinkedPath(resolved.filename, field);
        const afterPathIdentity = statIdentity(afterPath.lstat);
        if (!sameIdentity(beforeHandle, afterHandle) || !sameIdentity(afterHandle, afterPathIdentity)
            || buffer.length !== afterHandle.size || afterHandle.nlink !== 1
            || !samePath(resolved.realpath, afterPath.realpath)) {
            fail(`${field} changed while it was read`);
        }
        return {
            path: resolved.filename,
            realpath: resolved.realpath,
            buffer,
            bytes: buffer.length,
            sha256: sha256Buffer(buffer),
            identity: afterHandle,
        };
    } finally {
        if (descriptor !== undefined) fs.closeSync(descriptor);
    }
}

function parseJson(snapshot, field) {
    try {
        return object(JSON.parse(snapshot.buffer.toString('utf8')), field);
    } catch (error) {
        fail(`${field} is invalid JSON: ${error.message}`);
    }
}

function evidence(snapshot) {
    return {
        path: snapshot.path,
        realpath: snapshot.realpath,
        bytes: snapshot.bytes,
        sha256: snapshot.sha256,
    };
}

function resolveFrom(base, value, field) {
    const raw = string(value, field);
    return path.resolve(base, raw);
}

function pinnedSnapshot({ base, descriptor, field, requiredPath = null }) {
    requireExactKeys(descriptor, ['path', 'realpath', 'bytes', 'sha256'], field);
    const filename = resolveFrom(base, descriptor.path, `${field}.path`);
    if (requiredPath && !samePath(filename, requiredPath)) fail(`${field}.path is not authoritative`);
    const snapshot = readSnapshot(filename, field);
    if (snapshot.bytes !== integer(descriptor.bytes, `${field}.bytes`, 1)
        || snapshot.sha256 !== sha256(descriptor.sha256, `${field}.sha256`)
        || !samePath(snapshot.realpath, string(descriptor.realpath, `${field}.realpath`))) {
        fail(`${field} does not match its immutable pin`);
    }
    return snapshot;
}

function safeBundleFilename(value, field) {
    const name = string(value, field).replaceAll('\\', '/');
    const parts = name.split('/');
    if (name.startsWith('/') || parts.some((part) => !part || part === '.' || part === '..'
        || /[:\u0000-\u001f]/.test(part)) || path.isAbsolute(name)) {
        fail(`${field} must be a safe bundle-relative filename`);
    }
    return name;
}

function descriptorMatchesSnapshot(descriptor, snapshot, field) {
    requireExactKeys(descriptor, ['path', 'realpath', 'bytes', 'sha256'], field);
    if (!samePath(descriptor.path, snapshot.path)
        || !samePath(descriptor.realpath, snapshot.realpath)
        || integer(descriptor.bytes, `${field}.bytes`, 1) !== snapshot.bytes
        || sha256(descriptor.sha256, `${field}.sha256`) !== snapshot.sha256) {
        fail(`${field} does not match its immutable snapshot`);
    }
}

function stageDescriptorMatchesSnapshot(descriptor, snapshot, field) {
    requireExactKeys(descriptor, ['path', 'bytes', 'sha256'], field);
    if (!samePath(descriptor.path, snapshot.path)
        || descriptor.bytes !== snapshot.bytes || descriptor.sha256 !== snapshot.sha256) {
        fail(`${field} does not match the exact admitted stage artifact`);
    }
}

function secureExists(filenameValue, field) {
    try {
        resolveUnlinkedPath(filenameValue, field);
        return true;
    } catch (error) {
        if (/ENOENT|cannot be resolved safely/.test(error.message)
            && !fs.existsSync(path.resolve(filenameValue))) return false;
        throw error;
    }
}

function secureDirectoryInventory(directoryValue, field) {
    const directory = secureDirectory(directoryValue, field);
    const result = [];
    const visit = (current, prefix) => {
        const before = secureDirectory(current, `${field} directory ${prefix || '.'}`);
        const entries = fs.readdirSync(current, { withFileTypes: true })
            .sort((left, right) => left.name.localeCompare(right.name));
        for (const entry of entries) {
            const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
            const absolute = path.join(current, entry.name);
            const resolved = resolveUnlinkedPath(absolute, `${field} entry ${relative}`);
            if (entry.isSymbolicLink() || resolved.lstat.isSymbolicLink()) {
                fail(`${field} contains link/reparse entry ${relative}`);
            }
            if (entry.isDirectory() && resolved.lstat.isDirectory()) visit(absolute, relative);
            else if (entry.isFile() && resolved.lstat.isFile()) result.push(relative.replaceAll('\\', '/'));
            else fail(`${field} contains unsupported entry ${relative}`);
        }
        const after = secureDirectory(current, `${field} directory ${prefix || '.'}`);
        if (!sameIdentity(before.identity, after.identity)) fail(`${field} directory changed while listed`);
    };
    visit(directory.path, '');
    return { directory, files: result.sort() };
}

function validateAuthoritativeContracts(specBase, descriptors) {
    requireExactKeys(descriptors, ['taxonomy', 'actionPrompts', 'browserActionContract'], 'spec.contracts');
    const snapshots = Object.fromEntries(Object.entries(BROWSER_CANDIDATE_AUTHORITATIVE_PATHS)
        .map(([name, authoritativePath]) => [name, pinnedSnapshot({
            base: specBase,
            descriptor: descriptors[name],
            field: `spec.contracts.${name}`,
            requiredPath: authoritativePath,
        })]));
    const taxonomy = parseJson(snapshots.taxonomy, 'animal animation taxonomy');
    const prompts = parseJson(snapshots.actionPrompts, 'action prompts');
    if (taxonomy.schema !== 'animal-animation-taxonomy.v1'
        || taxonomy.output_fps !== OUTPUT_FPS
        || !Array.isArray(taxonomy.clips)
        || taxonomy.clips.length !== 30) {
        fail('authoritative animal taxonomy inventory changed');
    }
    if (prompts.schema !== 'autorig.animation-fitting-prompts.v1'
        || prompts.taxonomy_schema !== taxonomy.schema
        || prompts.output_fps_int !== OUTPUT_FPS
        || prompts.frame_rule_string !== '8n+1'
        || !Array.isArray(prompts.actions_array)
        || prompts.actions_array.length !== 30) {
        fail('authoritative action prompt inventory changed');
    }
    const taxonomyById = new Map();
    taxonomy.clips.forEach((row, index) => {
        const id = string(row?.id, `taxonomy.clips[${index}].id`);
        if (taxonomyById.has(id) || row.order !== index + 1) fail('taxonomy clip IDs/order are invalid');
        const frameCount = integer(row.frame_profile, `${id}.frame_profile`, 2);
        if (!FRAME_COUNTS.has(frameCount) || (frameCount - 1) % 8 !== 0) {
            fail(`${id} has an unsupported frame contract`);
        }
        taxonomyById.set(id, row);
    });
    const promptById = new Map();
    prompts.actions_array.forEach((row, index) => {
        const id = string(row?.action_id_string, `prompts.actions_array[${index}].action_id_string`);
        if (promptById.has(id)) fail(`action prompts repeat ${id}`);
        promptById.set(id, row);
    });
    const browserIds = Object.keys(ANIMATION_FITTING_ACTION_CONTRACTS);
    if (browserIds.length !== 30
        || JSON.stringify(browserIds) !== JSON.stringify([...taxonomyById.keys()])) {
        fail('browser action contract does not preserve exact taxonomy order/inventory');
    }
    for (const [id, taxonomyRow] of taxonomyById) {
        const prompt = promptById.get(id);
        const browser = ANIMATION_FITTING_ACTION_CONTRACTS[id];
        const mode = taxonomyRow.loop === true ? 'loop' : 'one_shot';
        if (!prompt || prompt.generation_mode_string !== mode
            || prompt.frame_count_int !== taxonomyRow.frame_profile
            || browser?.generationMode !== mode
            || browser?.frameCount !== taxonomyRow.frame_profile) {
            fail(`authoritative contracts disagree for ${id}`);
        }
    }
    return { snapshots, taxonomy, prompts, taxonomyById, promptById };
}

function validateCanonicalBundle(specBase, canonicalValue) {
    requireExactKeys(canonicalValue, [
        'directory', 'immutableManifestSha256', 'fittingBundleSha256', 'sourceModelSha256',
    ], 'spec.canonicalBundle');
    const directory = resolveFrom(specBase, canonicalValue.directory, 'spec.canonicalBundle.directory');
    const directorySnapshot = secureDirectory(directory, 'canonical bundle directory');
    const immutableSnapshot = readSnapshot(path.join(directory, 'immutable_manifest.json'), 'canonical immutable manifest');
    const fittingSnapshot = readSnapshot(path.join(directory, 'fitting_bundle.json'), 'canonical fitting bundle');
    if (immutableSnapshot.sha256 !== sha256(
        canonicalValue.immutableManifestSha256,
        'spec.canonicalBundle.immutableManifestSha256',
    ) || fittingSnapshot.sha256 !== sha256(
        canonicalValue.fittingBundleSha256,
        'spec.canonicalBundle.fittingBundleSha256',
    )) fail('canonical bundle manifest pins changed');
    const immutable = parseJson(immutableSnapshot, 'canonical immutable manifest');
    const fitting = parseJson(fittingSnapshot, 'canonical fitting bundle');
    const sourceModelSha256 = sha256(
        canonicalValue.sourceModelSha256,
        'spec.canonicalBundle.sourceModelSha256',
    );
    if (immutable.schema !== 'autorig-fitting-immutable-copy.v1'
        || fitting.schema !== 'autorig-actionless-fitting-bundle.v1'
        || fitting.source?.rig_type !== 'HORSE_2'
        || fitting.source?.sha256 !== sourceModelSha256
        || fitting.actionless?.actionless !== true
        || fitting.counts?.vertices !== 344 || fitting.counts?.faces !== 258
        || fitting.counts?.armatures !== 1 || fitting.counts?.meshes !== 1) {
        fail('canonical bundle is not the exact actionless HORSE_2 contract');
    }
    if (immutable.bundle_manifest?.sha256 !== fittingSnapshot.sha256
        || immutable.source_model?.sha256 !== sourceModelSha256
        || !Array.isArray(immutable.files)
        || !immutable.files.length) {
        fail('canonical immutable manifest does not bind fitting_bundle/source model');
    }
    const listed = new Set();
    const listedRows = new Map();
    const files = [];
    immutable.files.forEach((row, index) => {
        const relative = safeBundleFilename(row?.filename, `immutable.files[${index}].filename`);
        if (listed.has(relative)) fail(`canonical immutable manifest repeats ${relative}`);
        listed.add(relative);
        const snapshot = readSnapshot(path.join(directory, ...relative.split('/')), `canonical artifact ${relative}`);
        if (snapshot.bytes !== integer(row.bytes, `immutable.files[${index}].bytes`, 1)
            || snapshot.sha256 !== sha256(row.sha256, `immutable.files[${index}].sha256`)) {
            fail(`canonical artifact ${relative} changed`);
        }
        files.push(snapshot);
        listedRows.set(relative, snapshot);
    });
    const immutableTotalBytes = files.reduce((total, snapshot) => total + snapshot.bytes, 0);
    if (!listed.has('fitting_bundle.json')
        || immutable.bundle_file_count !== files.length
        || immutable.bundle_total_bytes !== immutableTotalBytes) {
        fail('immutable manifest must pin the exact fitting-bundle byte inventory');
    }
    const rgbDescriptor = object(fitting.artifacts?.rgb, 'fitting.artifacts.rgb');
    const referenceName = safeBundleFilename(rgbDescriptor.filename, 'fitting.artifacts.rgb.filename');
    const referenceSnapshot = readSnapshot(path.join(directory, ...referenceName.split('/')), 'canonical RGB reference');
    if (referenceSnapshot.bytes !== integer(rgbDescriptor.bytes, 'fitting.artifacts.rgb.bytes', 1)
        || referenceSnapshot.sha256 !== sha256(rgbDescriptor.sha256, 'fitting.artifacts.rgb.sha256')
        || !listed.has(referenceName)) {
        fail('canonical RGB reference is not immutable-manifest pinned');
    }
    const canonicalArtifact = (name, expectedFilename) => {
        const descriptor = object(fitting.artifacts?.[name], `fitting.artifacts.${name}`);
        const filename = safeBundleFilename(descriptor.filename, `fitting.artifacts.${name}.filename`);
        if (filename !== expectedFilename) fail(`canonical ${name} filename changed`);
        const snapshot = listedRows.get(filename);
        if (!snapshot
            || snapshot.bytes !== integer(descriptor.bytes, `fitting.artifacts.${name}.bytes`, 1)
            || snapshot.sha256 !== sha256(descriptor.sha256, `fitting.artifacts.${name}.sha256`)) {
            fail(`canonical ${name} is not immutable-manifest pinned`);
        }
        return snapshot;
    };
    const skeletonSnapshot = canonicalArtifact('skeleton', 'skeleton.json');
    const skinWeightsSnapshot = canonicalArtifact('skin_weights', 'skin_weights.json.gz');
    const surfaceTopologySnapshot = canonicalArtifact('surface_topology', 'surface_topology.json.gz');
    const skeleton = parseJson(skeletonSnapshot, 'canonical skeleton');
    if (!Array.isArray(skeleton.armatures) || skeleton.armatures.length !== 1
        || !Array.isArray(skeleton.armatures[0]?.bones) || skeleton.armatures[0].bones.length !== 304) {
        fail('canonical Horse_2 skeleton must contain exactly one 304-bone armature');
    }
    const boneNames = new Set();
    const rootBoneNames = new Set();
    skeleton.armatures[0].bones.forEach((bone, index) => {
        const name = string(bone?.name, `canonical skeleton bone ${index}.name`);
        if (boneNames.has(name)) fail(`canonical skeleton repeats bone ${name}`);
        boneNames.add(name);
        if (bone.parent == null) rootBoneNames.add(name);
    });
    skeleton.armatures[0].bones.forEach((bone) => {
        if (bone.parent != null && !boneNames.has(bone.parent)) {
            fail(`canonical skeleton bone ${bone.name} has a missing parent`);
        }
    });
    if (!rootBoneNames.size) fail('canonical skeleton has no root bone');
    const inventory = secureDirectoryInventory(directory, 'canonical bundle');
    const expectedInventory = [...listed, 'immutable_manifest.json'].sort();
    if (JSON.stringify(inventory.files) !== JSON.stringify(expectedInventory)) {
        fail('canonical bundle contains unpinned or missing files');
    }
    return {
        directory,
        directorySnapshot,
        immutableSnapshot,
        fittingSnapshot,
        referenceSnapshot,
        skeletonSnapshot,
        skinWeightsSnapshot,
        surfaceTopologySnapshot,
        skeleton,
        boneNames,
        rootBoneNames,
        sourceModelSha256,
        files,
        fitting,
    };
}

function validateRawSemanticReceipt({ specBase, descriptor, candidate, action, contracts, canonical }) {
    const snapshot = pinnedSnapshot({
        base: specBase,
        descriptor,
        field: 'spec.rawSemanticPassReceipt',
    });
    const receipt = parseJson(snapshot, 'raw semantic PASS receipt');
    requireExactKeys(receipt, [
        'schema', 'decision', 'semanticId', 'generationMode', 'frameCount', 'outputFps',
        'candidate', 'contracts', 'canonical', 'authority', 'evidence',
    ], 'raw semantic PASS receipt');
    if (receipt.schema !== RAW_SEMANTIC_PASS_RECEIPT_SCHEMA || receipt.decision !== 'PASS'
        || receipt.semanticId !== action.actionId
        || receipt.generationMode !== action.generationMode
        || receipt.frameCount !== action.frameCount || receipt.outputFps !== OUTPUT_FPS) {
        fail('raw semantic receipt does not match the authoritative action contract');
    }
    const receiptCandidate = object(receipt.candidate, 'raw semantic receipt.candidate');
    requireExactKeys(receiptCandidate, [
        'path', 'realpath', 'bytes', 'sha256', 'fps', 'frameCount',
    ], 'raw semantic receipt.candidate');
    if (!samePath(receiptCandidate.path, candidate.path)
        || !samePath(receiptCandidate.realpath, candidate.realpath)
        || receiptCandidate.bytes !== candidate.bytes || receiptCandidate.sha256 !== candidate.sha256
        || finite(receiptCandidate.fps, 'raw semantic receipt candidate fps') !== OUTPUT_FPS
        || receiptCandidate.frameCount !== action.frameCount) {
        fail('raw semantic receipt candidate video pin/timing changed');
    }
    const contractPins = object(receipt.contracts, 'raw semantic receipt.contracts');
    requireExactKeys(contractPins, ['taxonomy', 'actionPrompts', 'browserActionContract'], 'raw semantic receipt.contracts');
    for (const [name, contractSnapshot] of Object.entries(contracts.snapshots)) {
        descriptorMatchesSnapshot(contractPins[name], contractSnapshot, `raw semantic receipt.contracts.${name}`);
    }
    const canonicalPin = object(receipt.canonical, 'raw semantic receipt.canonical');
    requireExactKeys(canonicalPin, [
        'immutableManifestSha256', 'fittingBundleSha256', 'sourceModelSha256', 'reference',
    ], 'raw semantic receipt.canonical');
    if (canonicalPin.immutableManifestSha256 !== canonical.immutableSnapshot.sha256
        || canonicalPin.fittingBundleSha256 !== canonical.fittingSnapshot.sha256
        || canonicalPin.sourceModelSha256 !== canonical.sourceModelSha256) {
        fail('raw semantic receipt canonical bundle pins changed');
    }
    descriptorMatchesSnapshot(canonicalPin.reference, canonical.referenceSnapshot, 'raw semantic receipt.canonical.reference');
    const authority = object(receipt.authority, 'raw semantic receipt.authority');
    requireExactKeys(authority, [
        'kind', 'issuer', 'reviewId', 'clientAssertionAccepted',
    ], 'raw semantic receipt.authority');
    if (!['server_machine_gate', 'named_human_review'].includes(authority.kind)
        || !string(authority.issuer, 'raw semantic receipt.authority.issuer')
        || !string(authority.reviewId, 'raw semantic receipt.authority.reviewId')
        || authority.clientAssertionAccepted !== false) {
        fail('raw semantic PASS must come from a server/named-review authority; client QA assertions are forbidden');
    }
    const evidenceSnapshot = pinnedSnapshot({
        base: path.dirname(snapshot.path),
        descriptor: receipt.evidence,
        field: 'raw semantic receipt.evidence',
    });
    const qa = parseJson(evidenceSnapshot, 'server raw semantic QA evidence');
    requireExactKeys(qa, [
        'schema', 'decision', 'approvedForFitting', 'semanticId', 'generationMode',
        'frameCount', 'outputFps', 'candidate', 'contracts', 'canonical', 'review', 'checks',
    ], 'server raw semantic QA evidence');
    if (qa.schema !== RAW_SEMANTIC_EVIDENCE_SCHEMA || qa.decision !== 'PASS'
        || qa.approvedForFitting !== true || qa.semanticId !== action.actionId
        || qa.generationMode !== action.generationMode || qa.frameCount !== action.frameCount
        || qa.outputFps !== OUTPUT_FPS) {
        fail('server raw semantic QA evidence is not an exact PASS for this action');
    }
    if (qa.candidate?.sha256 !== candidate.sha256 || qa.candidate?.bytes !== candidate.bytes
        || qa.candidate?.frameCount !== action.frameCount || qa.candidate?.fps !== OUTPUT_FPS
        || qa.canonical?.immutableManifestSha256 !== canonical.immutableSnapshot.sha256
        || qa.canonical?.fittingBundleSha256 !== canonical.fittingSnapshot.sha256
        || qa.canonical?.sourceModelSha256 !== canonical.sourceModelSha256) {
        fail('server raw semantic QA evidence does not bind candidate/canonical inputs');
    }
    for (const [name, contractSnapshot] of Object.entries(contracts.snapshots)) {
        if (qa.contracts?.[name]?.sha256 !== contractSnapshot.sha256
            || qa.contracts?.[name]?.bytes !== contractSnapshot.bytes) {
            fail(`server raw semantic QA evidence lost ${name} pin`);
        }
    }
    if (qa.review?.authorityKind !== authority.kind
        || qa.review?.reviewId !== authority.reviewId
        || qa.review?.clientSuppliedDecisionUsed !== false) {
        fail('server raw semantic QA review authority is inconsistent or trusts client QA');
    }
    const requiredChecks = [
        'identityPreserved', 'fullBodyVisible', 'semanticActionReadable',
        'temporalModeCorrect', 'frameContractCorrect', 'fixedCamera',
    ];
    requireExactKeys(qa.checks, requiredChecks, 'server raw semantic QA evidence.checks');
    if (requiredChecks.some((name) => qa.checks[name] !== true)) {
        fail('server raw semantic QA evidence contains a failed required check');
    }
    return { snapshot, evidenceSnapshot, receipt, qa };
}

function validateTrackingRuntimeLock(snapshotValue) {
    const runtimeLock = parseJson(snapshotValue, 'tracking runtime lock');
    requireExactKeys(runtimeLock, ['schema', 'repos', 'checkpoints', 'python'], 'tracking runtime lock');
    if (runtimeLock.schema !== 'autorig-tracking-runtime-lock.v1') {
        fail('tracking runtime lock schema is not authoritative');
    }
    const repositoryNames = ['tapnet', 'sam2', 'video_depth_anything'];
    const checkpointNames = ['tapnextpp', 'sam2_hiera_tiny', 'video_depth_anything_small'];
    const pythonNames = [
        'python', 'torch', 'torchvision', 'numpy', 'scipy', 'PIL', 'cv2',
        'einops', 'hydra', 'iopath', 'tqdm',
    ];
    requireExactKeys(runtimeLock.repos, repositoryNames, 'tracking runtime lock.repos');
    requireExactKeys(runtimeLock.checkpoints, checkpointNames, 'tracking runtime lock.checkpoints');
    requireExactKeys(runtimeLock.python, pythonNames, 'tracking runtime lock.python');
    for (const name of repositoryNames) {
        const row = object(runtimeLock.repos[name], `tracking runtime lock.repos.${name}`);
        requireExactKeys(row, ['url', 'commit', 'license', 'license_sha256'], `tracking runtime lock.repos.${name}`);
        if (!/^https:\/\//.test(string(row.url, `tracking runtime lock.repos.${name}.url`))
            || !GIT_COMMIT_RE.test(row.commit)
            || safeBundleFilename(row.license, `tracking runtime lock.repos.${name}.license`) !== row.license
            || !SHA256_RE.test(row.license_sha256)) {
            fail(`tracking runtime repository pin ${name} is invalid`);
        }
    }
    for (const name of checkpointNames) {
        const row = object(runtimeLock.checkpoints[name], `tracking runtime lock.checkpoints.${name}`);
        requireExactKeys(
            row,
            ['url', 'bytes', 'sha256', 'license_source_repo'],
            `tracking runtime lock.checkpoints.${name}`,
        );
        if (!/^https:\/\//.test(string(row.url, `tracking runtime lock.checkpoints.${name}.url`))
            || integer(row.bytes, `tracking runtime lock.checkpoints.${name}.bytes`, 1) !== row.bytes
            || !SHA256_RE.test(row.sha256)
            || !repositoryNames.includes(row.license_source_repo)) {
            fail(`tracking runtime checkpoint pin ${name} is invalid`);
        }
    }
    for (const name of pythonNames) string(runtimeLock.python[name], `tracking runtime lock.python.${name}`);
    if (runtimeLock.python.python !== '3.10') fail('tracking runtime Python contract must remain exactly 3.10');
    return runtimeLock;
}

function validateRuntime(specBase, runtimeValue) {
    requireExactKeys(runtimeValue, [
        'executables', 'threeModule', 'trackingRuntimeRoot', 'trackingRuntimeLock',
    ], 'spec.runtime');
    const executableNames = ['python', 'node', 'chrome', 'ffmpeg', 'ffprobe', 'git'];
    requireExactKeys(runtimeValue.executables, executableNames, 'spec.runtime.executables');
    const executables = Object.fromEntries(executableNames.map((name) => [name, pinnedSnapshot({
        base: specBase,
        descriptor: runtimeValue.executables[name],
        field: `spec.runtime.executables.${name}`,
    })]));
    const executableBasenames = {
        python: /^(?:python|python3(?:\.\d+)?)(?:\.exe)?$/i,
        node: /^node(?:\.exe)?$/i,
        chrome: /^(?:chrome|chrome-headless-shell|chromium|chromium-browser)(?:\.exe)?$/i,
        ffmpeg: /^ffmpeg(?:\.exe)?$/i,
        ffprobe: /^ffprobe(?:\.exe)?$/i,
        git: /^git(?:\.exe)?$/i,
    };
    for (const [name, snapshot] of Object.entries(executables)) {
        if (!executableBasenames[name].test(path.basename(snapshot.realpath))) {
            fail(`spec.runtime.executables.${name} is not the expected runtime executable`);
        }
        if (process.platform !== 'win32') {
            try { fs.accessSync(snapshot.realpath, fs.constants.X_OK); } catch {
                fail(`spec.runtime.executables.${name} is not executable`);
            }
        }
    }
    requireExactKeys(runtimeValue.threeModule, ['path', 'realpath', 'bytes', 'sha256', 'revision'], 'spec.runtime.threeModule');
    if (String(runtimeValue.threeModule.revision) !== '160') fail('Three module revision must be exactly 160');
    const threeModule = pinnedSnapshot({
        base: specBase,
        descriptor: {
            path: runtimeValue.threeModule.path,
            realpath: runtimeValue.threeModule.realpath,
            bytes: runtimeValue.threeModule.bytes,
            sha256: runtimeValue.threeModule.sha256,
        },
        field: 'spec.runtime.threeModule',
    });
    requireExactKeys(runtimeValue.trackingRuntimeRoot, ['path', 'realpath'], 'spec.runtime.trackingRuntimeRoot');
    const trackingRuntimeRoot = resolveFrom(specBase, runtimeValue.trackingRuntimeRoot.path, 'spec.runtime.trackingRuntimeRoot.path');
    const trackingRuntimeRootSnapshot = secureDirectory(trackingRuntimeRoot, 'tracking runtime root');
    if (!samePath(trackingRuntimeRootSnapshot.realpath, runtimeValue.trackingRuntimeRoot.realpath)) {
        fail('tracking runtime root realpath changed');
    }
    const trackingRuntimeLock = pinnedSnapshot({
        base: specBase,
        descriptor: runtimeValue.trackingRuntimeLock,
        field: 'spec.runtime.trackingRuntimeLock',
        requiredPath: BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.trackingRuntimeLockContract,
    });
    const trackingRuntimeLockContract = validateTrackingRuntimeLock(trackingRuntimeLock);
    return {
        executables,
        threeModule,
        trackingRuntimeRoot,
        trackingRuntimeRootSnapshot,
        trackingRuntimeLock,
        trackingRuntimeLockContract,
    };
}

function resolveModuleFile(baseFile, specifier, extensions) {
    if (!(specifier.startsWith('./') || specifier.startsWith('../'))) return null;
    const withoutQuery = specifier.split(/[?#]/, 1)[0];
    const raw = path.resolve(path.dirname(baseFile), withoutQuery);
    const candidates = path.extname(raw)
        ? [raw]
        : [raw, ...extensions.map((extension) => `${raw}${extension}`),
            ...extensions.map((extension) => path.join(raw, `index${extension}`))];
    const matches = candidates.filter((candidate) => {
        try { return fs.lstatSync(candidate).isFile(); } catch { return false; }
    });
    if (matches.length !== 1) fail(`cannot resolve unique local import ${specifier} from ${baseFile}`);
    return path.resolve(matches[0]);
}

export function dynamicJavaScriptImportExpressions(source, filename = '<source>') {
    const expressions = [];
    const importCall = /\bimport(?:\s|\/\*[\s\S]*?\*\/|\/\/[^\r\n\u2028\u2029]*(?:\r?\n|[\u2028\u2029]|$))*\(/g;
    for (const match of source.matchAll(importCall)) {
        const open = match.index + match[0].lastIndexOf('(');
        let depth = 1;
        let quote = null;
        let escaped = false;
        let index = open + 1;
        for (; index < source.length && depth > 0; index += 1) {
            const character = source[index];
            if (quote != null) {
                if (escaped) escaped = false;
                else if (character === '\\') escaped = true;
                else if (character === quote) quote = null;
                continue;
            }
            if (character === '\'' || character === '"' || character === '`') {
                quote = character;
            } else if (character === '(') depth += 1;
            else if (character === ')') depth -= 1;
        }
        if (depth !== 0) fail(`unterminated dynamic JavaScript import in ${filename}`);
        expressions.push(source.slice(open + 1, index - 1).trim());
    }
    return expressions;
}

const JAVASCRIPT_TOKEN_GAP = String.raw`(?:\s|\/\*[\s\S]*?\*\/|\/\/[^\r\n\u2028\u2029]*(?:\r?\n|[\u2028\u2029]|$))*`;

export function javascriptStaticModuleSpecifiers(source) {
    if (typeof source !== 'string') fail('JavaScript dependency source must be text');
    const expression = new RegExp(
        String.raw`(?:^|[;\r\n}\u2028\u2029])${JAVASCRIPT_TOKEN_GAP}(?:import|export)${JAVASCRIPT_TOKEN_GAP}`
            + String.raw`(?:[^;]*?\bfrom${JAVASCRIPT_TOKEN_GAP})?['"]([^'"]+)['"]\s*;?`,
        'g',
    );
    return [...source.matchAll(expression)].map((match) => match[1]);
}

export function hasUnresolvedCommonJsLoader(source) {
    if (typeof source !== 'string') fail('JavaScript dependency source must be text');
    return new RegExp(String.raw`\brequire${JAVASCRIPT_TOKEN_GAP}\(`).test(source);
}

function jsRelativeImports(snapshot) {
    const source = snapshot.buffer.toString('utf8');
    const found = new Set();
    for (const expression of dynamicJavaScriptImportExpressions(source, snapshot.path)) {
        const literal = expression.match(/^(['"])([^'"\\]+)\1$/);
        if (literal) {
            const resolved = resolveModuleFile(snapshot.path, literal[2], ['.mjs', '.js', '.cjs', '.json']);
            if (resolved) found.add(resolved);
            continue;
        }
        const normalized = expression.replace(/\s+/g, '');
        if (samePath(snapshot.path, BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.browserFit)
            && normalized === 'resolveThreeModule(config.threeModule)') continue;
        fail(`unresolved dynamic JavaScript import in ${snapshot.path}: ${expression}`);
    }
    if (hasUnresolvedCommonJsLoader(source)) fail(`unresolved CommonJS loader in ${snapshot.path}`);
    for (const specifier of javascriptStaticModuleSpecifiers(source)) {
        const resolved = resolveModuleFile(snapshot.path, specifier, ['.mjs', '.js', '.cjs', '.json']);
        if (resolved) found.add(resolved);
    }
    return [...found];
}

function pythonModulePath(moduleName) {
    const raw = path.join(PYTHON_WORKING_DIRECTORY, ...moduleName.split('.'));
    const candidates = [`${raw}.py`, path.join(raw, '__init__.py')];
    const matches = candidates.filter((candidate) => {
        try { return fs.lstatSync(candidate).isFile(); } catch { return false; }
    });
    if (matches.length > 1) fail(`ambiguous Python module ${moduleName}`);
    return matches[0] ? path.resolve(matches[0]) : null;
}

export function pythonRelativeImportClauses(source) {
    if (typeof source !== 'string') fail('Python dependency source must be text');
    const continued = source.replace(/\\\r?\n/g, ' ');
    return [...continued.matchAll(
        /(?:^|[;\r\n])\s*from\s+((?:\.\s*)*)([\p{ID_Start}_][\p{ID_Continue}_]*(?:\s*\.\s*[\p{ID_Start}_][\p{ID_Continue}_]*)*)?\s+import\s+(\([^)]*\)|[^#;\n]+)/gu,
    )].map((match) => ({
        dots: (match[1].match(/\./g) || []).length,
        named: (match[2] || '').replace(/\s*\.\s*/g, '.'),
        members: match[3].replace(/#[^\r\n]*/g, '').replace(/[()]/g, '').split(',')
            .map((value) => value.trim().split(/\s+as\s+/)[0])
            .filter((value) => value && value !== '*'),
    }));
}

export function pythonAbsoluteImportClauses(source) {
    if (typeof source !== 'string') fail('Python dependency source must be text');
    const continued = source.replace(/\\\r?\n/g, ' ');
    return [...continued.matchAll(/(?:^|[;\r\n])\s*import\s+([^#;\n]+)/g)]
        .flatMap((match) => match[1].split(',').map((item) => item.trim().split(/\s+as\s+/)[0]
            .replace(/\s*\.\s*/g, '.')))
        .filter(Boolean);
}

function pythonRelativeImports(snapshot) {
    const source = snapshot.buffer.toString('utf8');
    const dynamicImports = [...source.matchAll(/\bimportlib\.import_module\s*\(([^)]*)\)/g)];
    if (dynamicImports.length && (!samePath(snapshot.path, BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.trackingCli)
        || dynamicImports.length !== 1 || dynamicImports[0][1].trim() !== 'name')) {
        fail(`unresolved dynamic Python import in ${snapshot.path}`);
    }
    if (/\b__import__\s*\(/.test(source)) fail(`unresolved Python __import__ loader in ${snapshot.path}`);
    const relative = path.relative(PYTHON_WORKING_DIRECTORY, snapshot.path).replaceAll('\\', '/');
    if (relative.startsWith('../') || !relative.endsWith('.py')) fail(`Python tool escaped source root: ${snapshot.path}`);
    const moduleParts = relative.replace(/\.py$/, '').split('/');
    if (moduleParts.at(-1) === '__init__') moduleParts.pop();
    else moduleParts.pop();
    const modules = new Set();
    for (const clause of pythonRelativeImportClauses(source)) {
        const { dots, named } = clause;
        let baseModule = null;
        if (dots) {
            const keep = moduleParts.length - (dots - 1);
            if (keep < 1) fail(`Python relative import escapes source package in ${snapshot.path}`);
            baseModule = [...moduleParts.slice(0, keep), ...named.split('.').filter(Boolean)].join('.');
            modules.add(baseModule);
        } else if (named === 'animation_fitting' || named.startsWith('animation_fitting.')) {
            baseModule = named;
            modules.add(named);
        }
        if (baseModule) {
            for (const member of clause.members) {
                const candidate = `${baseModule}.${member}`;
                if (pythonModulePath(candidate)) modules.add(candidate);
            }
        }
    }
    for (const name of pythonAbsoluteImportClauses(source)) {
        if (name === 'animation_fitting' || name.startsWith('animation_fitting.')) modules.add(name);
    }
    return [...modules].map(pythonModulePath).filter(Boolean);
}

function packageInitializers(filename) {
    const result = [];
    let current = path.dirname(filename);
    while (isInside(PYTHON_WORKING_DIRECTORY, current) && !samePath(current, PYTHON_WORKING_DIRECTORY)) {
        const init = path.join(current, '__init__.py');
        try { if (fs.lstatSync(init).isFile()) result.push(path.resolve(init)); } catch { /* no package file */ }
        current = path.dirname(current);
    }
    return result;
}

export function discoverBrowserCandidateToolDependencies() {
    const entrypoints = new Set([
        fileURLToPath(import.meta.url),
        ...Object.values(BROWSER_CANDIDATE_TOOL_SOURCE_PATHS),
    ].map((filename) => path.resolve(filename)));
    const pending = [...entrypoints];
    const snapshots = new Map();
    while (pending.length) {
        const filename = path.resolve(pending.pop());
        const key = pathKey(filename);
        if (snapshots.has(key)) continue;
        const snapshot = readSnapshot(filename, `tool dependency ${filename}`);
        snapshots.set(key, snapshot);
        const imports = filename.endsWith('.py') ? pythonRelativeImports(snapshot) : jsRelativeImports(snapshot);
        if (filename.endsWith('.py')) imports.push(...packageInitializers(filename));
        imports.forEach((dependency) => pending.push(dependency));
    }
    return [...snapshots.values()].sort((left, right) => left.realpath.localeCompare(right.realpath));
}

function validateToolDependencies(specBase, value) {
    requireExactKeys(value, ['algorithm', 'files'], 'spec.dependencyInventory');
    if (value.algorithm !== SOURCE_DISCOVERY_ALGORITHM || !Array.isArray(value.files) || !value.files.length) {
        fail('spec.dependencyInventory is invalid');
    }
    const declared = new Map();
    value.files.forEach((descriptor, index) => {
        const snapshot = pinnedSnapshot({
            base: specBase,
            descriptor,
            field: `spec.dependencyInventory.files[${index}]`,
        });
        const key = pathKey(snapshot.realpath);
        if (declared.has(key)) fail(`spec.dependencyInventory repeats ${snapshot.realpath}`);
        declared.set(key, snapshot);
    });
    const discovered = discoverBrowserCandidateToolDependencies();
    const discoveredKeys = discovered.map((snapshot) => pathKey(snapshot.realpath)).sort();
    const declaredKeys = [...declared.keys()].sort();
    if (JSON.stringify(declaredKeys) !== JSON.stringify(discoveredKeys)) {
        const missing = discovered.filter((snapshot) => !declared.has(pathKey(snapshot.realpath))).map((snapshot) => snapshot.realpath);
        const extra = [...declared.values()].filter((snapshot) => !discoveredKeys.includes(pathKey(snapshot.realpath))).map((snapshot) => snapshot.realpath);
        fail(`dependency inventory is not the exact transitive closure; missing=[${missing.join(',')}], extra=[${extra.join(',')}]`);
    }
    for (const discoveredSnapshot of discovered) {
        const declaredSnapshot = declared.get(pathKey(discoveredSnapshot.realpath));
        if (declaredSnapshot.sha256 !== discoveredSnapshot.sha256
            || declaredSnapshot.bytes !== discoveredSnapshot.bytes) {
            fail(`dependency changed during closure discovery: ${discoveredSnapshot.realpath}`);
        }
    }
    return Object.fromEntries(discovered.map((snapshot) => [pathKey(snapshot.realpath), snapshot]));
}

export function candidatePipelineIdentity(value) {
    const canonical = Buffer.from(`${JSON.stringify(value)}\n`, 'utf8');
    return sha256Buffer(canonical);
}

function validateSpecCore(specPathValue, expectedSpecSha256) {
    const specPath = path.resolve(specPathValue);
    const snapshot = readSnapshot(specPath, 'pipeline spec');
    if (snapshot.sha256 !== sha256(expectedSpecSha256, 'expected spec SHA-256')) {
        fail('pipeline spec does not match externally supplied SHA-256');
    }
    const spec = parseJson(snapshot, 'pipeline spec');
    requireExactKeys(spec, [
        'schema', 'browserOnly', 'blenderUsed', 'fittingMixerUsed', 'qaAnimationMixerUsed',
        'orchestratorExecutesSubprocesses', 'rigType', 'semanticId', 'clipName', 'candidateId',
        'outputRoot', 'candidate', 'rawSemanticPassReceipt', 'contracts', 'canonicalBundle',
        'runtime', 'dependencyInventory', 'externalPins',
    ], 'pipeline spec');
    if (spec.schema !== BROWSER_CANDIDATE_PIPELINE_SPEC_SCHEMA || spec.browserOnly !== true
        || spec.blenderUsed !== false || spec.fittingMixerUsed !== false
        || spec.qaAnimationMixerUsed !== true || spec.orchestratorExecutesSubprocesses !== false
        || spec.rigType !== 'HORSE_2') {
        fail('pipeline spec is not the browser-only Horse_2 command-author contract');
    }
    const semanticId = string(spec.semanticId, 'spec.semanticId');
    const action = resolveAnimationFittingAction({ actionId: semanticId });
    if (action.actionId !== semanticId || action.aliasApplied) {
        fail('semanticId must be one of the 30 exact taxonomy IDs; aliases are forbidden');
    }
    const clipName = string(spec.clipName, 'spec.clipName');
    const specBase = path.dirname(specPath);
    const contracts = validateAuthoritativeContracts(specBase, spec.contracts);
    const policyIds = Object.keys(BROWSER_CANDIDATE_ACTION_POLICY);
    if (JSON.stringify(policyIds) !== JSON.stringify([...contracts.taxonomyById.keys()])) {
        fail('action policy matrix must exactly match all 30 authoritative taxonomy IDs in order');
    }
    const taxonomyAction = contracts.taxonomyById.get(semanticId);
    if (!taxonomyAction || action.frameCount !== taxonomyAction.frame_profile
        || action.generationMode !== (taxonomyAction.loop ? 'loop' : 'one_shot')) {
        fail('semantic action resolution disagrees with authoritative taxonomy');
    }
    const candidate = pinnedSnapshot({ base: specBase, descriptor: spec.candidate, field: 'spec.candidate' });
    const canonical = validateCanonicalBundle(specBase, spec.canonicalBundle);
    const rawReceipt = validateRawSemanticReceipt({
        specBase,
        descriptor: spec.rawSemanticPassReceipt,
        candidate,
        action,
        contracts,
        canonical,
    });
    const identityPayload = {
        schema: 'autorig.browser-animation-candidate-identity.v2',
        rigType: 'HORSE_2',
        semanticId,
        generationMode: action.generationMode,
        frameCount: action.frameCount,
        outputFps: OUTPUT_FPS,
        candidate: evidence(candidate),
        rawSemanticPassReceipt: evidence(rawReceipt.snapshot),
        contracts: Object.fromEntries(Object.entries(contracts.snapshots).map(([name, pin]) => [name, evidence(pin)])),
        canonical: {
            immutableManifestSha256: canonical.immutableSnapshot.sha256,
            fittingBundleSha256: canonical.fittingSnapshot.sha256,
            sourceModelSha256: canonical.sourceModelSha256,
            reference: evidence(canonical.referenceSnapshot),
        },
    };
    const candidateId = candidatePipelineIdentity(identityPayload);
    if (sha256(spec.candidateId, 'spec.candidateId') !== candidateId) {
        fail('candidateId does not match the content-addressed candidate identity');
    }
    const outputRoot = resolveFrom(specBase, spec.outputRoot, 'spec.outputRoot');
    const outputParent = secureDirectory(path.dirname(outputRoot), 'pipeline output parent');
    const canonicalOutputRoot = path.join(outputParent.realpath, path.basename(outputRoot));
    if (path.basename(outputRoot) !== candidateId || !samePath(outputRoot, canonicalOutputRoot)) {
        fail('outputRoot must be the candidateId directory outside all immutable inputs');
    }
    const runtime = validateRuntime(specBase, spec.runtime);
    const dependencies = validateToolDependencies(specBase, spec.dependencyInventory);
    const immutableLocations = [
        snapshot.path,
        candidate.path,
        rawReceipt.snapshot.path,
        rawReceipt.evidenceSnapshot.path,
        canonical.directory,
        ...Object.values(contracts.snapshots).map((snapshotValue) => snapshotValue.path),
        ...Object.values(dependencies).map((snapshotValue) => snapshotValue.path),
        ...Object.values(runtime.executables).map((snapshotValue) => snapshotValue.path),
        runtime.threeModule.path,
        runtime.trackingRuntimeLock.path,
        runtime.trackingRuntimeRoot,
    ];
    if (immutableLocations.some((location) => pathsOverlap(outputRoot, location))) {
        fail('outputRoot must not contain or be contained by any immutable input/runtime root');
    }
    requireExactKeys(spec.externalPins, [
        'observationsArtifactSetSha256', 'initialFitArtifactSetSha256',
        'contactRefitInputManifestSha256',
        'contactRefitArtifactSetSha256', 'finalThreeClipSha256',
        'visualQaArtifactSetSha256',
    ], 'spec.externalPins');
    const externalPins = {
        observationsArtifactSetSha256: spec.externalPins.observationsArtifactSetSha256 == null
            ? null
            : sha256(
                spec.externalPins.observationsArtifactSetSha256,
                'spec.externalPins.observationsArtifactSetSha256',
            ),
        initialFitArtifactSetSha256: spec.externalPins.initialFitArtifactSetSha256 == null
            ? null
            : sha256(
                spec.externalPins.initialFitArtifactSetSha256,
                'spec.externalPins.initialFitArtifactSetSha256',
            ),
        contactRefitInputManifestSha256: spec.externalPins.contactRefitInputManifestSha256 == null
            ? null
            : sha256(
                spec.externalPins.contactRefitInputManifestSha256,
                'spec.externalPins.contactRefitInputManifestSha256',
            ),
        contactRefitArtifactSetSha256: spec.externalPins.contactRefitArtifactSetSha256 == null
            ? null
            : sha256(
                spec.externalPins.contactRefitArtifactSetSha256,
                'spec.externalPins.contactRefitArtifactSetSha256',
            ),
        finalThreeClipSha256: spec.externalPins.finalThreeClipSha256 == null
            ? null
            : sha256(spec.externalPins.finalThreeClipSha256, 'spec.externalPins.finalThreeClipSha256'),
        visualQaArtifactSetSha256: spec.externalPins.visualQaArtifactSetSha256 == null
            ? null
            : sha256(
                spec.externalPins.visualQaArtifactSetSha256,
                'spec.externalPins.visualQaArtifactSetSha256',
            ),
    };
    return {
        snapshot,
        spec: { ...spec, semanticId, clipName, candidateId, outputRoot, externalPins },
        action,
        taxonomyAction,
        candidate,
        contracts,
        canonical,
        rawReceipt,
        runtime,
        dependencies,
        outputParent,
        identityPayload,
    };
}

function canonicalJson(value) {
    if (value === null || typeof value !== 'object') return JSON.stringify(value);
    if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
}

function directoryEvidence(snapshot) {
    return {
        path: snapshot.path,
        realpath: snapshot.realpath,
        dev: snapshot.identity.dev,
        ino: snapshot.identity.ino,
    };
}

function secureFutureFile(filenameValue, field) {
    const filename = path.resolve(filenameValue);
    const parent = secureDirectory(path.dirname(filename), `${field} parent`);
    const canonical = path.join(parent.realpath, path.basename(filename));
    if (!samePath(filename, canonical)) fail(`${field} must use its canonical parent without aliases`);
    if (secureExists(filename, field)) {
        const resolved = resolveUnlinkedPath(filename, field);
        if (!samePath(resolved.realpath, filename)) fail(`${field} resolves through an alias`);
    }
    return { path: filename, parent };
}

function deriveImmutableInputRoots(loaded) {
    const fileSnapshots = [
        loaded.candidate,
        loaded.rawReceipt.snapshot,
        loaded.rawReceipt.evidenceSnapshot,
        ...Object.values(loaded.contracts.snapshots),
        ...Object.values(loaded.dependencies),
        ...Object.values(loaded.runtime.executables),
        loaded.runtime.threeModule,
        loaded.runtime.trackingRuntimeLock,
    ];
    const directories = [
        loaded.canonical.directorySnapshot,
        loaded.runtime.trackingRuntimeRootSnapshot,
        ...fileSnapshots.map((snapshotValue) => secureDirectory(
            path.dirname(snapshotValue.realpath),
            `immutable input parent ${snapshotValue.realpath}`,
        )),
    ];
    const unique = new Map();
    directories.forEach((directory) => unique.set(pathKey(directory.realpath), directory));
    return [...unique.values()].sort((left, right) => left.realpath.localeCompare(right.realpath));
}

function trustBinding(loaded, statePathValue, protectedStageOutputs = {
    officialTracking: null,
    initialFit: null,
    contactRefit: null,
    visualQa: null,
}) {
    const state = secureFutureFile(statePathValue, 'state output');
    if (pathsOverlap(loaded.spec.outputRoot, state.path)
        || pathsOverlap(state.path, loaded.snapshot.path)) {
        fail('state output must be outside the candidate artifact outputRoot');
    }
    const immutableRoots = deriveImmutableInputRoots(loaded);
    for (const root of immutableRoots) {
        if (pathsOverlap(root.realpath, loaded.spec.outputRoot)
            || pathsOverlap(root.realpath, state.path)) {
            fail('trusted output/state locations overlap an immutable input root');
        }
    }
    return {
        spec: evidence(loaded.snapshot),
        identity: {
            candidateId: loaded.spec.candidateId,
            semanticId: loaded.action.actionId,
            candidate: evidence(loaded.candidate),
            rawSemanticPassReceipt: evidence(loaded.rawReceipt.snapshot),
            rawSemanticEvidence: evidence(loaded.rawReceipt.evidenceSnapshot),
        },
        authority: {
            kind: loaded.rawReceipt.receipt.authority.kind,
            issuer: loaded.rawReceipt.receipt.authority.issuer,
            reviewId: loaded.rawReceipt.receipt.authority.reviewId,
        },
        roots: {
            immutableInputRoots: immutableRoots.map(directoryEvidence),
            outputParent: directoryEvidence(loaded.outputParent),
            outputRoot: loaded.spec.outputRoot,
            stateParent: directoryEvidence(state.parent),
            statePath: state.path,
            trackingRuntimeRoot: directoryEvidence(loaded.runtime.trackingRuntimeRootSnapshot),
        },
        runtimes: {
            executables: Object.fromEntries(Object.entries(loaded.runtime.executables)
                .map(([name, snapshotValue]) => [name, evidence(snapshotValue)])),
            threeModule: evidence(loaded.runtime.threeModule),
            trackingRuntimeLock: evidence(loaded.runtime.trackingRuntimeLock),
        },
        dependencyInventory: {
            algorithm: SOURCE_DISCOVERY_ALGORITHM,
            files: Object.values(loaded.dependencies).map(evidence)
                .sort((left, right) => left.realpath.localeCompare(right.realpath)),
        },
        protectedStageOutputs,
    };
}

function secretBuffer(secretValue) {
    const secret = Buffer.isBuffer(secretValue) ? Buffer.from(secretValue) : Buffer.from(String(secretValue || ''), 'utf8');
    if (secret.length < 32) fail('server trust HMAC secret must contain at least 32 bytes');
    return secret;
}

function signTrustUnsigned(unsigned, secretValue) {
    const payload = Buffer.from(canonicalJson(unsigned), 'utf8');
    return {
        algorithm: 'hmac-sha256',
        payloadSha256: sha256Buffer(payload),
        value: crypto.createHmac('sha256', secretBuffer(secretValue)).update(payload).digest('hex'),
    };
}

function protectedStageToolClosureSha256(loaded) {
    return sha256Buffer(Buffer.from(canonicalJson({
        schema: 'autorig.protected-stage-tool-closure.v1',
        dependencies: Object.values(loaded.dependencies).map(evidence)
            .sort((left, right) => left.realpath.localeCompare(right.realpath)),
        runtimes: Object.fromEntries(Object.entries(loaded.runtime.executables)
            .map(([name, snapshotValue]) => [name, evidence(snapshotValue)])),
        threeModule: evidence(loaded.runtime.threeModule),
        trackingRuntimeLock: evidence(loaded.runtime.trackingRuntimeLock),
    }), 'utf8'));
}

function protectedStageReceiptUnsigned(loaded, stage, artifactSetSha256, executorJobId) {
    if (!Object.hasOwn(PROTECTED_STAGE_PIN_FIELDS, stage)) fail(`unsupported protected stage ${stage}`);
    return {
        schema: PROTECTED_STAGE_EXECUTION_RECEIPT_SCHEMA,
        issuer: 'autorig-protected-animation-fitting-worker',
        stage,
        executorJobId: string(executorJobId, 'protected stage executorJobId'),
        candidateId: loaded.spec.candidateId,
        semanticId: loaded.action.actionId,
        artifactSetSha256: sha256(artifactSetSha256, 'protected stage artifactSetSha256'),
        toolClosureSha256: protectedStageToolClosureSha256(loaded),
    };
}

export function buildProtectedStageExecutionReceipt({
    specPath,
    expectedSpecSha256,
    stage,
    artifactSetSha256,
    executorJobId,
    secret,
}) {
    const loaded = validateSpecCore(specPath, expectedSpecSha256);
    const unsigned = protectedStageReceiptUnsigned(
        loaded,
        stage,
        artifactSetSha256,
        executorJobId,
    );
    return { ...unsigned, signature: signTrustUnsigned(unsigned, secret) };
}

function validateProtectedStageOutputs(loaded, value, stageReceiptSecret = null, requireMac = false) {
    const receipts = object(value, 'protected stage outputs');
    requireExactKeys(receipts, Object.keys(PROTECTED_STAGE_PIN_FIELDS), 'protected stage outputs');
    const result = {};
    for (const [stage, pinField] of Object.entries(PROTECTED_STAGE_PIN_FIELDS)) {
        const expectedDigest = loaded.spec.externalPins[pinField];
        const receipt = receipts[stage];
        if (expectedDigest == null) {
            if (receipt !== null) fail(`protected ${stage} receipt exists before its external artifact pin`);
            result[stage] = null;
            continue;
        }
        const row = object(receipt, `protected ${stage} receipt`);
        if (requireMac && stageReceiptSecret == null) {
            fail(`protected ${stage} receipt secret is required before server trust signing`);
        }
        requireExactKeys(row, [
            'schema', 'issuer', 'stage', 'executorJobId', 'candidateId', 'semanticId',
            'artifactSetSha256', 'toolClosureSha256', 'signature',
        ], `protected ${stage} receipt`);
        const unsigned = protectedStageReceiptUnsigned(
            loaded,
            stage,
            expectedDigest,
            row.executorJobId,
        );
        for (const [name, expected] of Object.entries(unsigned)) {
            if (row[name] !== expected) fail(`protected ${stage} receipt ${name} changed`);
        }
        requireExactKeys(row.signature, ['algorithm', 'payloadSha256', 'value'], `protected ${stage} signature`);
        if (stageReceiptSecret != null) {
            const expectedSignature = signTrustUnsigned(unsigned, stageReceiptSecret);
            if (row.signature.algorithm !== expectedSignature.algorithm
                || row.signature.payloadSha256 !== expectedSignature.payloadSha256
                || !SHA256_RE.test(row.signature.value)
                || !crypto.timingSafeEqual(
                    Buffer.from(row.signature.value, 'hex'),
                    Buffer.from(expectedSignature.value, 'hex'),
                )) {
                fail(`protected ${stage} execution receipt HMAC authentication failed`);
            }
        } else if (row.signature.algorithm !== 'hmac-sha256'
            || !SHA256_RE.test(row.signature.payloadSha256)
            || !SHA256_RE.test(row.signature.value)) {
            fail(`protected ${stage} execution receipt signature is invalid`);
        }
        result[stage] = row;
    }
    return result;
}

/** Server/API helper.  The CLI intentionally exposes no trust-authoring mode. */
export function buildServerBrowserCandidateTrustContext({
    specPath,
    expectedSpecSha256,
    statePath,
    secret,
    keyId,
    nowEpochMs = Date.now(),
    ttlMs = 10 * 60 * 1000,
    protectedStageOutputs = {
        officialTracking: null,
        initialFit: null,
        contactRefit: null,
        visualQa: null,
    },
    stageReceiptSecret = null,
}) {
    const loaded = validateSpecCore(specPath, expectedSpecSha256);
    if (stageReceiptSecret != null) {
        const trustKey = secretBuffer(secret);
        const stageKey = secretBuffer(stageReceiptSecret);
        if (trustKey.length === stageKey.length && crypto.timingSafeEqual(trustKey, stageKey)) {
            fail('server trust and protected-stage receipt HMAC secrets must be distinct');
        }
    }
    const issuedAtEpochMs = integer(nowEpochMs, 'trust issuedAtEpochMs', 0);
    const resolvedTtl = integer(ttlMs, 'trust ttlMs', 1);
    if (resolvedTtl > TRUST_MAX_TTL_MS) fail(`trust ttlMs must be <= ${TRUST_MAX_TTL_MS}`);
    const protectedOutputs = validateProtectedStageOutputs(
        loaded,
        protectedStageOutputs,
        stageReceiptSecret,
        true,
    );
    const unsigned = {
        schema: BROWSER_CANDIDATE_PIPELINE_TRUST_SCHEMA,
        issuer: 'autorig-server-animation-fitting',
        keyId: string(keyId, 'trust keyId'),
        issuedAtEpochMs,
        expiresAtEpochMs: issuedAtEpochMs + resolvedTtl,
        binding: trustBinding(loaded, statePath, protectedOutputs),
    };
    return { ...unsigned, signature: signTrustUnsigned(unsigned, secret) };
}

function validateTrustContext({
    loaded,
    trustContextPath,
    expectedTrustContextSha256,
    statePath,
    trustSecret,
    nowEpochMs = Date.now(),
}) {
    if (!trustContextPath || !expectedTrustContextSha256 || !statePath) {
        fail('authenticated server trust context, its SHA-256, and exact state path are required');
    }
    const snapshot = readSnapshot(trustContextPath, 'server trust context');
    const resolvedStatePath = path.resolve(statePath);
    if (pathsOverlap(snapshot.path, loaded.spec.outputRoot)
        || pathsOverlap(snapshot.path, resolvedStatePath)) {
        fail('server trust context must be immutable and outside output/state locations');
    }
    if (snapshot.sha256 !== sha256(expectedTrustContextSha256, 'expected trust context SHA-256')) {
        fail('server trust context does not match externally supplied SHA-256');
    }
    const context = parseJson(snapshot, 'server trust context');
    requireExactKeys(context, [
        'schema', 'issuer', 'keyId', 'issuedAtEpochMs', 'expiresAtEpochMs', 'binding', 'signature',
    ], 'server trust context');
    if (context.schema !== BROWSER_CANDIDATE_PIPELINE_TRUST_SCHEMA
        || context.issuer !== 'autorig-server-animation-fitting') {
        fail('server trust context issuer/schema is not authoritative');
    }
    string(context.keyId, 'server trust context.keyId');
    const issuedAt = integer(context.issuedAtEpochMs, 'server trust context.issuedAtEpochMs', 0);
    const expiresAt = integer(context.expiresAtEpochMs, 'server trust context.expiresAtEpochMs', issuedAt + 1);
    const now = integer(nowEpochMs, 'server trust validation time', 0);
    if (expiresAt - issuedAt > TRUST_MAX_TTL_MS
        || now < issuedAt - TRUST_CLOCK_SKEW_MS || now > expiresAt + TRUST_CLOCK_SKEW_MS) {
        fail('server trust context is expired, premature, or exceeds maximum TTL');
    }
    const protectedOutputs = validateProtectedStageOutputs(
        loaded,
        context.binding?.protectedStageOutputs,
    );
    const expectedBinding = trustBinding(loaded, statePath, protectedOutputs);
    if (canonicalJson(context.binding) !== canonicalJson(expectedBinding)) {
        fail('server trust context does not bind the exact spec/receipt/roots/runtimes/dependencies');
    }
    requireExactKeys(context.signature, ['algorithm', 'payloadSha256', 'value'], 'server trust context.signature');
    if (context.signature.algorithm !== 'hmac-sha256') fail('server trust signature algorithm changed');
    const unsigned = {
        schema: context.schema,
        issuer: context.issuer,
        keyId: context.keyId,
        issuedAtEpochMs: issuedAt,
        expiresAtEpochMs: expiresAt,
        binding: context.binding,
    };
    const expectedSignature = signTrustUnsigned(unsigned, trustSecret);
    if (context.signature.payloadSha256 !== expectedSignature.payloadSha256
        || !SHA256_RE.test(context.signature.value)
        || !crypto.timingSafeEqual(Buffer.from(context.signature.value, 'hex'), Buffer.from(expectedSignature.value, 'hex'))) {
        fail('server trust context HMAC authentication failed');
    }
    return { snapshot, context, statePath: resolvedStatePath };
}

function validateSpec(specPathValue, expectedSpecSha256, trustOptions) {
    const loaded = validateSpecCore(specPathValue, expectedSpecSha256);
    const trust = validateTrustContext({ loaded, ...trustOptions });
    return { ...loaded, trust };
}

function stagePaths(outputRoot) {
    return {
        observations: path.join(outputRoot, '01-observations'),
        initialFit: path.join(outputRoot, '02-browser-fit'),
        hoofDiagnostic: path.join(outputRoot, '03-hoof-contact-diagnostic.json'),
        groundEvidence: path.join(outputRoot, '03-sam2-ground-evidence.json'),
        contactManifest: path.join(outputRoot, '04-contact-refit-input.json'),
        contactRefit: path.join(outputRoot, '05-browser-contact-refit'),
        visualQa: path.join(outputRoot, '06-browser-visual-phase-qa'),
    };
}

function ensureOutputShape(paths, policy) {
    const outputRoot = path.dirname(paths.observations);
    if (!secureExists(outputRoot, 'outputRoot')) return;
    secureDirectory(outputRoot, 'outputRoot');
    const allowed = new Set([
        path.basename(paths.observations), path.basename(paths.initialFit), path.basename(paths.visualQa),
        ...(CONTACT_REFIT_POLICIES.has(policy) ? [
            path.basename(paths.hoofDiagnostic), path.basename(paths.groundEvidence),
            path.basename(paths.contactManifest), path.basename(paths.contactRefit),
        ] : []),
    ]);
    for (const name of fs.readdirSync(outputRoot)) {
        resolveUnlinkedPath(path.join(outputRoot, name), `outputRoot artifact ${name}`);
        if (!allowed.has(name)) fail(`outputRoot contains unexpected artifact ${name}`);
    }
}

function command(cwd, executableSnapshot, argv, pins, environment = null) {
    const cwdSnapshot = secureDirectory(cwd, 'command cwd');
    const unique = new Map();
    [executableSnapshot, ...pins].forEach((pin) => {
        if (!pin) return;
        const normalized = pin.buffer ? evidence(pin) : pin;
        const key = pathKey(normalized.realpath || normalized.path);
        const previous = unique.get(key);
        if (previous && (previous.sha256 !== normalized.sha256 || previous.bytes !== normalized.bytes)) {
            fail(`command precondition has conflicting pins for ${normalized.path}`);
        }
        unique.set(key, normalized);
    });
    const commandValue = {
        cwd: cwdSnapshot.realpath,
        executable: executableSnapshot.path,
        argv: argv.map(String),
        shell: false,
        environment,
        preconditions: [...unique.values()].sort((left, right) => left.path.localeCompare(right.path)),
    };
    const serialized = JSON.stringify([commandValue.executable, ...commandValue.argv]).toLowerCase();
    if (serialized.includes('blender')) fail('Blender command is forbidden in browser fitting pipeline');
    return commandValue;
}

function toolPins(loaded) {
    return [
        ...Object.values(loaded.dependencies),
        ...Object.values(loaded.contracts.snapshots),
        loaded.rawReceipt.snapshot,
        loaded.rawReceipt.evidenceSnapshot,
        loaded.candidate,
        loaded.canonical.immutableSnapshot,
        loaded.canonical.fittingSnapshot,
        loaded.canonical.referenceSnapshot,
        loaded.canonical.skeletonSnapshot,
        loaded.canonical.skinWeightsSnapshot,
        loaded.canonical.surfaceTopologySnapshot,
        loaded.trust.snapshot,
    ];
}

const PINNED_GIT_PYTHON_BOOTSTRAP = [
    'import runpy, subprocess, sys',
    '_pinned_git = sys.argv.pop(1)',
    '_module = sys.argv.pop(1)',
    '_original_popen = subprocess.Popen',
    'def _pinned_popen(argv, *args, **kwargs):',
    '    if isinstance(argv, (list, tuple)) and argv and str(argv[0]).lower() in ("git", "git.exe"):',
    '        argv = [_pinned_git, *list(argv)[1:]]',
    '    elif isinstance(argv, str) and argv.lstrip().lower().startswith(("git ", "git.exe ")):',
    '        raise RuntimeError("string-form bare git execution is forbidden")',
    '    return _original_popen(argv, *args, **kwargs)',
    'subprocess.Popen = _pinned_popen',
    'sys.argv[0] = _module',
    'runpy.run_module(_module, run_name="__main__")',
].join('\n');

function buildCommands(loaded, paths) {
    const { runtime, spec, action } = loaded;
    const policy = BROWSER_CANDIDATE_ACTION_POLICY[action.actionId];
    const commonPins = toolPins(loaded);
    const trackingEnvironment = {
        // The Python bootstrap rewrites runtime_lock.py's bare `git` argv to
        // the content-addressed absolute executable. Keep PATH restricted as
        // defense in depth; on Windows also remove .COM/.CMD/.BAT precedence
        // and current-directory lookup for any unexpected child invocation.
        PATH: path.dirname(runtime.executables.git.realpath),
        ...(process.platform === 'win32' ? {
            PATHEXT: '.EXE',
            NoDefaultCurrentDirectoryInExePath: '1',
        } : {}),
    };
    const observations = command(PYTHON_WORKING_DIRECTORY, runtime.executables.python, [
        '-c', PINNED_GIT_PYTHON_BOOTSTRAP,
        runtime.executables.git.path,
        'animation_fitting.tracking_runtime',
        '--runtime-root', runtime.trackingRuntimeRoot,
        '--runtime-lock', runtime.trackingRuntimeLock.path,
        'observe', '--video', loaded.candidate.path,
        '--bundle', loaded.canonical.directory,
        '--output-dir', paths.observations,
        '--device', 'cuda',
        ...(action.isLoop ? ['--loop'] : []),
        '--ffprobe', runtime.executables.ffprobe.path,
    ], [
        runtime.executables.ffprobe,
        runtime.executables.git,
        runtime.trackingRuntimeLock,
        ...commonPins,
    ], trackingEnvironment);
    const initialFit = command(TOOLS_DIRECTORY, runtime.executables.node, [
        BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.browserFit,
        '--bundle-dir', loaded.canonical.directory,
        '--observations', path.join(paths.observations, 'observations.json'),
        '--three-module', runtime.threeModule.path,
        '--output-dir', paths.initialFit,
        '--clip-name', CONTACT_REFIT_POLICIES.has(policy) ? `${spec.clipName}_BrowserFit` : spec.clipName,
        '--position-mappings', 'auto', '--include-full-body',
        ...(action.isLoop ? [
            '--c1-closure-window', '4', '--float32-loop-velocity-invariant-gates',
        ] : ['--no-loop']),
        '--emit-fitted-animation', '--emit-three-clip',
    ], [runtime.threeModule, ...commonPins]);
    const hoofDiagnostic = command(TOOLS_DIRECTORY, runtime.executables.node, [
        BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.hoofDiagnostic,
        '--observations', path.join(paths.observations, 'observations.json'),
        '--bridge-report', path.join(paths.initialFit, 'bridge-report.json'),
        '--masks-dir', path.join(paths.observations, 'masks'),
        '--output', paths.hoofDiagnostic,
        '--ground-output', paths.groundEvidence,
        '--minimum-support-feet', String(HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet),
    ], commonPins);
    const contactManifest = command(TOOLS_DIRECTORY, runtime.executables.node, [
        BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.contactManifestAuthor,
        '--bundle-dir', loaded.canonical.directory,
        '--observations', path.join(paths.observations, 'observations.json'),
        '--bridge-report', path.join(paths.initialFit, 'bridge-report.json'),
        '--initial-fit-summary', path.join(paths.initialFit, 'fit-summary.json'),
        '--contact-diagnostic', paths.hoofDiagnostic,
        '--output', paths.contactManifest,
    ], commonPins);
    const contactRefit = command(TOOLS_DIRECTORY, runtime.executables.node, [
        BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.contactRefit,
        '--input-manifest', paths.contactManifest,
        '--input-manifest-sha256', spec.externalPins.contactRefitInputManifestSha256 || 'PIN_REQUIRED',
        '--three-module', runtime.threeModule.path,
        '--output-dir', paths.contactRefit,
        '--clip-name', spec.clipName,
    ], [runtime.threeModule, ...commonPins]);
    const finalClipPath = CONTACT_REFIT_POLICIES.has(BROWSER_CANDIDATE_ACTION_POLICY[action.actionId])
        ? path.join(paths.contactRefit, 'three-clip.json')
        : path.join(paths.initialFit, 'three-clip.json');
    const visualQa = command(TOOLS_DIRECTORY, runtime.executables.node, [
        BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.semanticVisualQa,
        '--bundle-dir', loaded.canonical.directory,
        '--immutable-manifest-sha256', loaded.canonical.immutableSnapshot.sha256,
        '--fitting-bundle-sha256', loaded.canonical.fittingSnapshot.sha256,
        '--source-model-sha256', loaded.canonical.sourceModelSha256,
        '--three-clip', finalClipPath,
        '--three-clip-sha256', spec.externalPins.finalThreeClipSha256 || 'PIN_REQUIRED',
        '--semantic-id', action.actionId,
        '--three-module', runtime.threeModule.path,
        '--three-module-sha256', runtime.threeModule.sha256,
        '--three-revision', '160',
        '--chrome', runtime.executables.chrome.path,
        '--ffmpeg', runtime.executables.ffmpeg.path,
        '--ffprobe', runtime.executables.ffprobe.path,
        '--output-dir', paths.visualQa,
        ...(action.isOneShot ? ['--one-shot'] : []),
    ], [
        runtime.threeModule, runtime.executables.chrome, runtime.executables.ffmpeg,
        runtime.executables.ffprobe, ...commonPins,
    ]);
    return {
        observations, initialFit, hoofDiagnostic, contactManifest, contactRefit,
        visualQa, finalClipPath,
    };
}

function validateManifestInventory(directory, manifestSnapshot, manifest, field) {
    if (!Array.isArray(manifest.files) || !manifest.files.length) fail(`${field}.files must not be empty`);
    const listed = new Set();
    const artifacts = [];
    manifest.files.forEach((row, index) => {
        const relative = safeBundleFilename(row?.path, `${field}.files[${index}].path`);
        if (listed.has(relative)) fail(`${field} repeats ${relative}`);
        listed.add(relative);
        const snapshot = readSnapshot(path.join(directory, ...relative.split('/')), `${field} artifact ${relative}`);
        if (snapshot.bytes !== integer(row.bytes, `${field}.files[${index}].bytes`, 1)
            || snapshot.sha256 !== sha256(row.sha256, `${field}.files[${index}].sha256`)) {
            fail(`${field} artifact ${relative} changed`);
        }
        artifacts.push(snapshot);
    });
    const inventory = secureDirectoryInventory(directory, field);
    const actual = inventory.files.filter((relative) => relative !== path.basename(manifestSnapshot.path));
    if (JSON.stringify(actual.sort()) !== JSON.stringify([...listed].sort())) {
        fail(`${field} does not pin exact output inventory`);
    }
    return artifacts;
}

function inspectObservations(paths, loaded) {
    if (!secureExists(paths.observations, 'observation output')) return null;
    secureDirectory(paths.observations, 'observation output');
    const manifestSnapshot = readSnapshot(
        path.join(paths.observations, 'observation_bundle_manifest.json'),
        'observation bundle manifest',
    );
    const manifest = parseJson(manifestSnapshot, 'observation bundle manifest');
    if (manifest.schema !== 'autorig-tracking-observation-bundle.v1'
        || manifest.provenance?.source_video_sha256 !== loaded.candidate.sha256
        || manifest.provenance?.immutable_manifest_sha256 !== loaded.canonical.immutableSnapshot.sha256
        || manifest.provenance?.bundle_sha256 !== loaded.canonical.fittingSnapshot.sha256) {
        fail('observation manifest schema/provenance changed');
    }
    const artifacts = validateManifestInventory(
        paths.observations,
        manifestSnapshot,
        manifest,
        'observation bundle manifest',
    );
    const observationSnapshot = readSnapshot(
        path.join(paths.observations, 'observations.json'),
        'observations',
    );
    const observations = parseJson(observationSnapshot, 'observations');
    const provenance = object(observations.provenance, 'observations.provenance');
    if (observations.schema !== 'autorig-fitting-observations.v1'
        || observations.frame_count !== loaded.action.frameCount
        || finite(observations.fps, 'observations.fps') !== OUTPUT_FPS
        || provenance.source_video_sha256 !== loaded.candidate.sha256
        || provenance.immutable_manifest_sha256 !== loaded.canonical.immutableSnapshot.sha256
        || provenance.bundle_sha256 !== loaded.canonical.fittingSnapshot.sha256) {
        fail('observations do not bind exact candidate/action/canonical bundle');
    }
    if (!Array.isArray(observations.silhouettes)
        || observations.silhouettes.length !== loaded.action.frameCount
        || observations.silhouettes.some((row, index) => row?.frame !== index)) {
        fail('observations must contain every chronological SAM2 silhouette');
    }
    if (!Array.isArray(observations.tracks) || !observations.tracks.length
        || observations.tracks.some((track) => !Array.isArray(track?.points)
            || track.points.length !== loaded.action.frameCount)) {
        fail('observations must contain a non-empty exact-frame tracking inventory');
    }
    const outputArtifacts = [manifestSnapshot, ...artifacts];
    return {
        snapshot: observationSnapshot,
        json: observations,
        artifacts: outputArtifacts,
        artifactSetSha256: browserObservationArtifactSetSha256(
            paths.observations,
            outputArtifacts,
        ),
    };
}

function exactDirectoryFiles(directory, expected, field) {
    if (!secureExists(directory, field)) return null;
    const inventory = secureDirectoryInventory(directory, field);
    const actual = inventory.files;
    if (JSON.stringify(actual) !== JSON.stringify([...expected].sort())) {
        fail(`${field} has partial/unexpected inventory: ${actual.join(', ')}`);
    }
    return actual;
}

function normalizeSerializedTrack(raw, action, boneNames, field) {
    const track = object(raw, field);
    const name = string(track.name, `${field}.name`);
    const match = name.match(/^(.*)\.(quaternion|position)$/);
    const expectedType = match?.[2] === 'quaternion' ? 'quaternion' : (match?.[2] === 'position' ? 'vector' : null);
    if (!match || !boneNames.has(match[1]) || track.type !== expectedType) {
        fail(`${field} is not bound to an exact canonical skeleton bone/property`);
    }
    if (track.bone != null && track.bone !== match[1]) fail(`${field}.bone disagrees with its binding`);
    if (!Array.isArray(track.times) || track.times.length !== action.frameCount) {
        fail(`${field}.times must preserve all ${action.frameCount} frames`);
    }
    const times = track.times.map((value, index) => finite(value, `${field}.times[${index}]`));
    const float32Tolerance = 2e-6;
    times.forEach((time, index) => {
        if (Math.abs(time - index / OUTPUT_FPS) > float32Tolerance
            || (index > 0 && time <= times[index - 1])) {
            fail(`${field}.times does not preserve exact 30fps chronology at frame ${index}`);
        }
    });
    const itemSize = expectedType === 'quaternion' ? 4 : 3;
    if (!Array.isArray(track.values) || track.values.length !== action.frameCount * itemSize) {
        fail(`${field}.values length does not match its track type/timeline`);
    }
    const values = track.values.map((value, index) => finite(value, `${field}.values[${index}]`));
    if (expectedType === 'quaternion') {
        for (let index = 0; index < action.frameCount; index += 1) {
            const quaternion = values.slice(index * 4, index * 4 + 4);
            const norm = Math.hypot(...quaternion);
            if (Math.abs(norm - 1) > 2e-3) fail(`${field} contains non-unit quaternion at frame ${index}`);
        }
    }
    if (action.isLoop) {
        const first = values.slice(0, itemSize);
        const last = values.slice(-itemSize);
        const error = expectedType === 'quaternion'
            ? Math.min(
                Math.hypot(...first.map((value, index) => value - last[index])),
                Math.hypot(...first.map((value, index) => value + last[index])),
            )
            : Math.hypot(...first.map((value, index) => value - last[index]));
        if (error > 1e-4) fail(`${field} violates loop endpoint closure`);
    }
    return { name, bone: match[1], type: expectedType, times, values };
}

function validateTrackSet(rows, action, boneNames, field) {
    if (!Array.isArray(rows) || !rows.length) fail(`${field} must not be empty`);
    const names = new Set();
    const byBone = new Map();
    const tracks = rows.map((row, index) => {
        const track = normalizeSerializedTrack(row, action, boneNames, `${field}[${index}]`);
        if (names.has(track.name)) fail(`${field} repeats ${track.name}`);
        names.add(track.name);
        const types = byBone.get(track.bone) || new Set();
        types.add(track.type);
        byBone.set(track.bone, types);
        return track;
    });
    return { tracks, names, byBone };
}

function exactTrackPayload(contract) {
    return contract.tracks.map((track) => ({
        name: track.name,
        type: track.type,
        times: track.times,
        values: track.values,
    })).sort((left, right) => left.name.localeCompare(right.name));
}

function requireExactTrackPayload(fittedContract, clipContract, field) {
    if (canonicalJson(exactTrackPayload(fittedContract))
        !== canonicalJson(exactTrackPayload(clipContract))) {
        fail(`${field} fitted-animation and Three clip tracks are not byte-value equivalent`);
    }
}

export function browserFitArtifactSetSha256(artifacts) {
    if (!Array.isArray(artifacts) || artifacts.length !== 4) {
        fail('browser fit artifact set must contain exactly four files');
    }
    const names = ['bridge-report.json', 'fit-summary.json', 'fitted-animation.json', 'three-clip.json'];
    const files = artifacts.map((artifact, index) => {
        if (!artifact || path.basename(artifact.path || '') !== names[index]
            || !Number.isSafeInteger(artifact.bytes) || artifact.bytes <= 0
            || typeof artifact.sha256 !== 'string' || !SHA256_RE.test(artifact.sha256)) {
            fail(`browser fit artifact set entry ${index} is invalid`);
        }
        return { filename: names[index], bytes: artifact.bytes, sha256: artifact.sha256 };
    });
    return sha256Buffer(Buffer.from(canonicalJson({
        schema: 'autorig.browser-fit-artifact-set.v1',
        files,
    }), 'utf8'));
}

function browserDirectoryArtifactSetSha256(outputDirectory, artifacts, schema, label) {
    if (!Array.isArray(artifacts) || artifacts.length < 2) {
        fail(`${label} artifact set is incomplete`);
    }
    const root = path.resolve(outputDirectory);
    const files = artifacts.map((artifact, index) => {
        if (!artifact || !Number.isSafeInteger(artifact.bytes) || artifact.bytes <= 0
            || typeof artifact.sha256 !== 'string' || !SHA256_RE.test(artifact.sha256)) {
            fail(`${label} artifact set entry ${index} is invalid`);
        }
        const relativePath = path.relative(root, path.resolve(artifact.path || '')).replaceAll('\\', '/');
        if (!relativePath || relativePath === '..' || relativePath.startsWith('../')
            || path.isAbsolute(relativePath) || relativePath.split('/').some((part) => !part || part === '.' || part === '..')) {
            fail(`${label} artifact set entry ${index} escaped its output directory`);
        }
        return { relativePath, bytes: artifact.bytes, sha256: artifact.sha256 };
    }).sort((left, right) => left.relativePath.localeCompare(right.relativePath));
    if (new Set(files.map((row) => row.relativePath)).size !== files.length) {
        fail(`${label} artifact set repeats a relative path`);
    }
    return sha256Buffer(Buffer.from(canonicalJson({
        schema,
        files,
    }), 'utf8'));
}

export function browserObservationArtifactSetSha256(outputDirectory, artifacts) {
    return browserDirectoryArtifactSetSha256(
        outputDirectory,
        artifacts,
        'autorig.browser-observation-artifact-set.v1',
        'browser observation',
    );
}

export function browserVisualQaArtifactSetSha256(outputDirectory, artifacts) {
    return browserDirectoryArtifactSetSha256(
        outputDirectory,
        artifacts,
        'autorig.browser-visual-qa-artifact-set.v1',
        'browser visual QA',
    );
}

function validateThreeClip(snapshot, action, boneNames, field) {
    const clip = parseJson(snapshot, field);
    string(clip.name, `${field}.name`);
    if (clip.uuid != null) string(clip.uuid, `${field}.uuid`);
    if (clip.blendMode != null) finite(clip.blendMode, `${field}.blendMode`);
    const expectedDuration = (action.frameCount - 1) / OUTPUT_FPS;
    if (Math.abs(finite(clip.duration, `${field}.duration`) - expectedDuration) > 2e-6) {
        fail(`${field} duration changed`);
    }
    const contract = validateTrackSet(clip.tracks, action, boneNames, `${field}.tracks`);
    if ([...contract.byBone.values()].some((types) => !types.has('quaternion') || !types.has('vector'))) {
        fail(`${field} must carry quaternion and position tracks for every bound bone`);
    }
    return { json: clip, ...contract };
}

function validateFittedAnimation(fittedJson, summaryJson, action, boneNames, field) {
    if (fittedJson.schema !== 'autorig-browser-fitted-animation.v1'
        || fittedJson.loop !== action.isLoop || fittedJson.frameCount !== action.frameCount
        || finite(fittedJson.fps, `${field}.fps`) !== OUTPUT_FPS
        || Math.abs(finite(fittedJson.durationSeconds, `${field}.durationSeconds`)
            - (action.frameCount - 1) / OUTPUT_FPS) > 1e-9) {
        fail(`${field} timing/mode contract changed`);
    }
    const rows = [
        ...(Array.isArray(fittedJson.tracks) ? fittedJson.tracks : []),
        ...(Array.isArray(fittedJson.positionTracks) ? fittedJson.positionTracks : []),
        ...(fittedJson.rootTrack ? [fittedJson.rootTrack] : []),
    ];
    const tracks = validateTrackSet(rows, action, boneNames, `${field}.allTracks`);
    if (summaryJson.fit?.frameCount !== action.frameCount
        || summaryJson.fit?.durationSeconds !== fittedJson.durationSeconds
        || summaryJson.fit?.quaternionTracks !== (fittedJson.tracks || []).length
        || summaryJson.fit?.positionTracks !== (fittedJson.positionTracks || []).length
        || canonicalJson(summaryJson.fit?.qa) !== canonicalJson(fittedJson.qa)) {
        fail(`${field} track/timing/QA inventory is not bound to fit-summary.json`);
    }
    if (!Array.isArray(fittedJson.frames) || fittedJson.frames.length !== action.frameCount
        || fittedJson.frames.some((frame, index) => frame?.frame !== index)) {
        fail(`${field} debug frame chronology changed`);
    }
    return { ...tracks, json: fittedJson, frames: fittedJson.frames };
}

function validateFittedDebugEvidence({ fitted, bridge, observations, fitQa, action, field }) {
    const mappings = Array.isArray(bridge.mappings) ? bridge.mappings : fail(`${field} bridge mappings are missing`);
    const sourceTracks = new Map();
    if (!Array.isArray(observations?.tracks)) fail(`${field} source observation tracks are missing`);
    observations.tracks.forEach((track, index) => {
        const id = string(track?.id, `${field} observations.tracks[${index}].id`);
        const anchor = string(track?.anchor_id, `${field} observations.tracks[${index}].anchor_id`);
        if (sourceTracks.has(id) || !Array.isArray(track.points) || track.points.length !== action.frameCount) {
            fail(`${field} source observation track inventory changed`);
        }
        sourceTracks.set(id, { ...track, anchor });
    });
    const bindings = mappings.map((mappingValue, index) => {
        const mapping = object(mappingValue, `${field} bridge.mappings[${index}]`);
        const sourceTrackId = string(mapping.sourceTrackId, `${field} mapping sourceTrackId`);
        const sourceAnchorId = string(mapping.sourceAnchorId, `${field} mapping sourceAnchorId`);
        const source = sourceTracks.get(sourceTrackId);
        if (!source || source.anchor !== sourceAnchorId || mapping.sourceAnchorPin !== sourceAnchorId) {
            fail(`${field} bridge mapping lost its exact source track/anchor pin`);
        }
        const collection = string(mapping.collection, `${field} mapping collection`);
        if (!['limbs', 'auxiliaryChains'].includes(collection)) fail(`${field} mapping collection is unsupported`);
        const limb = string(mapping.limb, `${field} mapping limb`);
        const headIndex = integer(mapping.headIndex, `${field} mapping headIndex`, 0);
        const orderedHeadCount = integer(mapping.orderedHeadCount, `${field} mapping orderedHeadCount`, 2);
        if (headIndex >= orderedHeadCount) fail(`${field} mapping headIndex is outside its ordered chain`);
        const vector2 = (value, name) => {
            if (!Array.isArray(value) || value.length !== 2) fail(`${name} must have two coordinates`);
            return value.map((item, axis) => finite(item, `${name}[${axis}]`));
        };
        return {
            mapping,
            source,
            collection,
            limb,
            headIndex,
            orderedHeadCount,
            restPoint: vector2(mapping.restPoint, `${field} mapping restPoint`),
            offset: vector2(mapping.queryToRestOffsetPx, `${field} mapping queryToRestOffsetPx`),
        };
    });
    const groups = new Map();
    bindings.forEach((binding) => {
        const key = `${binding.collection}:${binding.limb}`;
        const rows = groups.get(key) || [];
        if (rows.some((row) => row.headIndex === binding.headIndex)) fail(`${field} repeats a fitted chain head`);
        rows.push(binding);
        groups.set(key, rows);
    });
    groups.forEach((rows) => {
        rows.sort((left, right) => left.headIndex - right.headIndex);
        if (rows.length !== rows[0].orderedHeadCount
            || rows.some((row, index) => row.headIndex !== index || row.orderedHeadCount !== rows.length)) {
            fail(`${field} does not bind every ordered fitted-chain head`);
        }
    });
    let targetSamples = 0;
    let initialErrorSum = 0;
    let finalErrorSum = 0;
    let maximumTargetErrorPx = 0;
    let maximumBoneLengthErrorPx = 0;
    fitted.frames.forEach((frame, frameIndex) => {
        groups.forEach((rows) => {
            const points = frame?.[rows[0].collection]?.[rows[0].limb]?.points;
            if (!Array.isArray(points) || points.length !== rows.length) {
                fail(`${field} debug frame ${frameIndex} lost fitted-chain points`);
            }
            const normalizedPoints = points.map((point, pointIndex) => {
                if (!Array.isArray(point) || point.length !== 2) {
                    fail(`${field} debug frame ${frameIndex} point ${pointIndex} is invalid`);
                }
                return point.map((value, axis) => finite(value, `${field} debug point[${frameIndex},${pointIndex},${axis}]`));
            });
            rows.forEach((binding, pointIndex) => {
                const sourcePoint = binding.source.points[frameIndex];
                if (sourcePoint?.visible !== true) return;
                const target = [
                    finite(sourcePoint.x, `${field} source x`) + binding.offset[0],
                    finite(sourcePoint.y, `${field} source y`) + binding.offset[1],
                ];
                const initialError = Math.hypot(
                    binding.restPoint[0] - target[0], binding.restPoint[1] - target[1],
                );
                const finalError = Math.hypot(
                    normalizedPoints[pointIndex][0] - target[0],
                    normalizedPoints[pointIndex][1] - target[1],
                );
                targetSamples += 1;
                initialErrorSum += initialError;
                finalErrorSum += finalError;
                maximumTargetErrorPx = Math.max(maximumTargetErrorPx, finalError);
            });
            for (let index = 1; index < rows.length; index += 1) {
                const restLength = Math.hypot(
                    rows[index].restPoint[0] - rows[index - 1].restPoint[0],
                    rows[index].restPoint[1] - rows[index - 1].restPoint[1],
                );
                const fittedLength = Math.hypot(
                    normalizedPoints[index][0] - normalizedPoints[index - 1][0],
                    normalizedPoints[index][1] - normalizedPoints[index - 1][1],
                );
                maximumBoneLengthErrorPx = Math.max(
                    maximumBoneLengthErrorPx, Math.abs(fittedLength - restLength),
                );
            }
        });
    });
    if (!targetSamples) fail(`${field} debug evidence has no visible target samples`);
    const metrics = {
        targetSamples,
        initialMeanTargetErrorPx: initialErrorSum / targetSamples,
        finalMeanTargetErrorPx: finalErrorSum / targetSamples,
        maximumTargetErrorPx,
        maximumBoneLengthErrorPx,
    };
    for (const [name, expected] of Object.entries(metrics)) {
        const actual = finite(fitQa[name], `${field} fit QA ${name}`);
        if (Math.abs(actual - expected) > 1e-7 * Math.max(1, Math.abs(actual), Math.abs(expected))) {
            fail(`${field} fit QA ${name} does not recompute from observations/debug frames`);
        }
    }
    return { ...metrics, chainCount: groups.size };
}

const BASE_FIT_GATE_NAMES = Object.freeze([
    'head_reconstruction_world',
    'rest_seed_alignment_px',
    'final_mean_target_error_px',
    'maximum_target_error_px',
    'bone_length_error_px',
    'joint_limit_violation_rad',
    'contact_slide_px',
    'loop_endpoint_error',
    'hierarchy_segment_drift_world',
    'hierarchy_reprojection_error_px',
    'requested_fitted_point_error_px',
]);
const LOOP_VELOCITY_GATE_NAMES = Object.freeze([
    'quaternion_angular_velocity_seam_rad_per_second',
    'position_velocity_seam_world_per_second',
]);
const BASE_FIT_TAIL_GATE_NAMES = Object.freeze([
    'unreachable_pixel_ray_ratio',
    'target_sample_coverage',
    'target_error_improved',
    'ordered_deform_heads',
    'four_limb_contacts',
    'three_clip_validate',
    'three_tracks_bound',
]);
const C1_GATE_NAMES = Object.freeze([
    'c1_quaternion_pose_seam_rad',
    'c1_position_pose_seam_world',
]);
const CONTACT_GATE_NAMES = Object.freeze([
    'pinned_contact_schedule',
    'semantic_walk_gait',
    'fitted_walk_contact_slide',
]);

function gateResult(name, actual, comparator, threshold, extra = {}) {
    let passed = false;
    if (comparator === '<=') passed = Number.isFinite(actual) && Number.isFinite(threshold) && actual <= threshold;
    else if (comparator === '>=') passed = Number.isFinite(actual) && Number.isFinite(threshold) && actual >= threshold;
    else if (comparator === '===') passed = actual === threshold;
    else fail(`unsupported canonical fit gate comparator ${comparator}`);
    if (extra.enforced === false) passed = true;
    return { name, passed, actual, comparator, threshold, ...extra };
}

function validateExactFitGates({ summary, bridge, clipContract, action, contact = false }) {
    const fitQa = object(summary.fit?.qa, 'fit summary.fit.qa');
    const hierarchy = object(summary.hierarchyClip, 'fit summary.hierarchyClip');
    const hierarchyQa = object(hierarchy.qa, 'fit summary.hierarchyClip.qa');
    const restSeedAlignment = object(summary.observations?.restSeedAlignment, 'fit summary restSeedAlignment');
    const mappings = Array.isArray(bridge.mappings) ? bridge.mappings : fail('fit bridge mappings are missing');
    const minimumVisiblePoints = integer(bridge.minimumVisiblePoints, 'fit bridge minimumVisiblePoints', 1);
    const hierarchyRayCount = integer(hierarchy.segmentRayCount, 'fit hierarchy segmentRayCount', 1);
    const expectedThresholds = {
        ...BROWSER_FIT_CANARY_DEFAULTS.gates,
        requireFourLimbContacts: contact,
    };
    let seam = null;
    let invariant = null;
    if (!contact && action.isLoop) {
        seam = measureLoopVelocitySeam(clipContract.json);
        invariant = deriveFloat32LoopVelocityInvariantGate(clipContract.json);
        expectedThresholds.maximumQuaternionAngularVelocitySeamRadPerSecond =
            invariant.maximumQuaternionAngularVelocitySeamRadPerSecond;
        expectedThresholds.maximumPositionVelocitySeamWorldPerSecond =
            invariant.maximumPositionVelocitySeamWorldPerSecond;
        const expectedVelocityGate = {
            enabled: true,
            maximumQuaternionAngularVelocitySeamRadPerSecond:
                invariant.maximumQuaternionAngularVelocitySeamRadPerSecond,
            maximumPositionVelocitySeamWorldPerSecond:
                invariant.maximumPositionVelocitySeamWorldPerSecond,
            derivation: invariant,
        };
        const c1 = clipContract.json.userData?.autorigC1PeriodicClosure;
        if (!c1 || c1.enabled !== true || c1.windowFrames !== 4 || finite(c1.poseEpsilon, 'C1 pose epsilon') !== 1e-5
            || canonicalJson(hierarchy.c1PeriodicClosure) !== canonicalJson(c1)
            || canonicalJson(bridge.c1PeriodicClosure) !== canonicalJson(c1)
            || canonicalJson(hierarchy.loopVelocitySeam) !== canonicalJson(seam)
            || canonicalJson(hierarchy.loopVelocityGate) !== canonicalJson(expectedVelocityGate)
            || canonicalJson(bridge.loopVelocityGate) !== canonicalJson(expectedVelocityGate)) {
            fail('loop fit lost its exact Float32/C1 clip-derived gate contract');
        }
    } else {
        if (hierarchy.c1PeriodicClosure?.enabled !== false
            || hierarchy.c1PeriodicClosure?.windowFrames !== null
            || canonicalJson(bridge.c1PeriodicClosure) !== canonicalJson(hierarchy.c1PeriodicClosure)
            || hierarchy.loopVelocityGate?.enabled !== false
            || bridge.loopVelocityGate?.enabled !== false) {
            fail('non-C1 fit carries an enabled or inconsistent loop-velocity contract');
        }
    }
    if (canonicalJson(summary.gates?.thresholds) !== canonicalJson(expectedThresholds)) {
        fail('fit summary gate thresholds are not the exact code-owned values');
    }
    const expectedRows = [
        gateResult('head_reconstruction_world', summary.realBundle?.maximumHeadReconstructionErrorWorld,
            '<=', expectedThresholds.maximumHeadReconstructionErrorWorld),
        gateResult('rest_seed_alignment_px', restSeedAlignment.maximumErrorPx,
            '<=', expectedThresholds.maximumRestSeedAlignmentErrorPx),
        gateResult('final_mean_target_error_px', fitQa.finalMeanTargetErrorPx,
            '<=', expectedThresholds.maximumFinalMeanTargetErrorPx),
        gateResult('maximum_target_error_px', fitQa.maximumTargetErrorPx,
            '<=', expectedThresholds.maximumTargetErrorPx),
        gateResult('bone_length_error_px', fitQa.maximumBoneLengthErrorPx,
            '<=', expectedThresholds.maximumBoneLengthErrorPx),
        gateResult('joint_limit_violation_rad', fitQa.maximumJointLimitViolationRad,
            '<=', expectedThresholds.maximumJointLimitViolationRad),
        gateResult('contact_slide_px', fitQa.maximumContactSlidePx,
            '<=', expectedThresholds.maximumContactSlidePx),
        gateResult('loop_endpoint_error', fitQa.loopEndpointError,
            '<=', expectedThresholds.maximumLoopEndpointError),
        gateResult('hierarchy_segment_drift_world', hierarchyQa.maximumSegmentLengthDriftWorld,
            '<=', expectedThresholds.maximumSegmentLengthDriftWorld),
        gateResult('hierarchy_reprojection_error_px', hierarchyQa.maximumHierarchyBakeReprojectionErrorPx,
            '<=', expectedThresholds.maximumHierarchyBakeReprojectionErrorPx),
        gateResult('requested_fitted_point_error_px', hierarchyQa.maximumRequestedFittedPointErrorPx,
            '<=', expectedThresholds.maximumRequestedFittedPointErrorPx),
        ...((!contact && action.isLoop) ? [
            gateResult('quaternion_angular_velocity_seam_rad_per_second',
                seam.quaternionAngularVelocitySeamRadPerSecond.maximum, '<=',
                invariant.maximumQuaternionAngularVelocitySeamRadPerSecond),
            gateResult('position_velocity_seam_world_per_second',
                seam.positionVelocitySeamWorldPerSecond.maximum, '<=',
                invariant.maximumPositionVelocitySeamWorldPerSecond),
        ] : []),
        gateResult('unreachable_pixel_ray_ratio', hierarchyQa.unreachablePixelRays / Math.max(hierarchyRayCount, 1),
            '<=', expectedThresholds.maximumUnreachablePixelRayRatio),
        gateResult('target_sample_coverage', fitQa.targetSamples, '>=', mappings.length * minimumVisiblePoints),
        gateResult('target_error_improved', fitQa.finalMeanTargetErrorPx, '<=', fitQa.initialMeanTargetErrorPx),
        gateResult('ordered_deform_heads', fitQa.targetMode, '===', 'ordered_deform_heads'),
        gateResult('four_limb_contacts', summary.observations?.contactCount, '===', 4, { enforced: contact }),
        gateResult('three_clip_validate', hierarchy.validate, '===', true),
        gateResult('three_tracks_bound', hierarchy.allTracksBound, '===', true),
        ...((!contact && action.isLoop) ? [
            gateResult('c1_quaternion_pose_seam_rad', seam.quaternionPoseSeamRad.maximum,
                '<=', clipContract.json.userData.autorigC1PeriodicClosure.poseEpsilon),
            gateResult('c1_position_pose_seam_world', seam.positionPoseSeamWorld.maximum,
                '<=', clipContract.json.userData.autorigC1PeriodicClosure.poseEpsilon),
        ] : []),
        ...(contact ? [
            gateResult('pinned_contact_schedule', summary.contactRefit?.scheduleStatus, '===', 'PASS'),
            gateResult('semantic_walk_gait', summary.contactRefit?.semanticGaitQa?.accepted, '===', true),
            gateResult('fitted_walk_contact_slide',
                summary.contactRefit?.fittedWalkQa?.maximumContactSlideRatio,
                '<=', HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.maximumFittedContactSlideRatio),
        ] : []),
    ];
    const expectedNames = [
        ...BASE_FIT_GATE_NAMES,
        ...((!contact && action.isLoop) ? LOOP_VELOCITY_GATE_NAMES : []),
        ...BASE_FIT_TAIL_GATE_NAMES,
        ...((!contact && action.isLoop) ? C1_GATE_NAMES : []),
        ...(contact ? CONTACT_GATE_NAMES : []),
    ];
    if (canonicalJson(expectedRows.map((row) => row.name)) !== canonicalJson(expectedNames)
        || canonicalJson(summary.gates?.results) !== canonicalJson(expectedRows)
        || summary.gates?.passed !== expectedRows.every((row) => row.passed)
        || expectedRows.some((row) => row.passed !== true)) {
        fail('fit summary does not recompute the exact canonical gate inventory');
    }
}

function validateNonRootClipMotion(contract, rootBoneNames, field, requiredMappedBones) {
    const required = new Set(
        [...requiredMappedBones].filter((bone) => !rootBoneNames.has(bone)),
    );
    if (!required.size) fail(`${field} has no mapped non-root bones to animate`);
    const moved = new Set();
    for (const track of contract.tracks) {
        if (rootBoneNames.has(track.bone)) continue;
        const size = track.type === 'quaternion' ? 4 : 3;
        const first = track.values.slice(0, size);
        for (let index = 1; index < track.times.length; index += 1) {
            const current = track.values.slice(index * size, (index + 1) * size);
            const delta = track.type === 'quaternion'
                ? Math.min(
                    Math.hypot(...current.map((value, item) => value - first[item])),
                    Math.hypot(...current.map((value, item) => value + first[item])),
                )
                : Math.hypot(...current.map((value, item) => value - first[item]));
            if (delta > 1e-4) moved.add(track.bone);
        }
    }
    const missing = [...required].filter((bone) => !moved.has(bone));
    if (missing.length) {
        fail(`${field} does not animate every mapped non-root bone: ${missing.join(', ')}`);
    }
    return moved;
}

function inspectInitialFit(paths, loaded, observations) {
    if (!exactDirectoryFiles(paths.initialFit, [
        'bridge-report.json', 'fit-summary.json', 'fitted-animation.json', 'three-clip.json',
    ], 'initial browser fit')) return null;
    const bridge = readSnapshot(path.join(paths.initialFit, 'bridge-report.json'), 'fit bridge report');
    const summary = readSnapshot(path.join(paths.initialFit, 'fit-summary.json'), 'fit summary');
    const fitted = readSnapshot(path.join(paths.initialFit, 'fitted-animation.json'), 'fitted animation');
    const clip = readSnapshot(path.join(paths.initialFit, 'three-clip.json'), 'initial Three clip');
    const bridgeJson = parseJson(bridge, 'fit bridge report');
    const summaryJson = parseJson(summary, 'fit summary');
    const fittedJson = parseJson(fitted, 'fitted animation');
    if (bridgeJson.schema !== 'autorig-browser-fit-canary-bridge-report.v1'
        || bridgeJson.status !== 'VALIDATED' || bridgeJson.browserOnly !== true
        || bridgeJson.blenderUsed !== false || bridgeJson.mixerUsed !== false
        || bridgeJson.fittingMode !== 'unconstrained_diagnostic'
        || bridgeJson.sourceContacts !== 0 || bridgeJson.preparedContacts !== 0
        || summaryJson.schema !== 'autorig-browser-fit-canary-summary.v1'
        || summaryJson.status !== 'PASS_BROWSER_FIT_GATES' || summaryJson.browserOnly !== true
        || summaryJson.blenderUsed !== false || summaryJson.mixerUsed !== false
        || summaryJson.fittingMode !== 'unconstrained_diagnostic'
        || summaryJson.approvedForBrowserContactFit !== false
        || summaryJson.approvedForAnimationLibrary !== false
        || canonicalJson(summaryJson.approvalExclusions) !== canonicalJson([
            'gait_semantics_and_phase_order',
            'fixed_camera_visual_phase_qa',
            'target_mesh_deformation_qa',
        ])
        || summaryJson.runtime?.threeRevision !== '160'
        || typeof summaryJson.runtime?.node !== 'string'
        || canonicalJson(summaryJson.fit?.options) !== canonicalJson({
            ...BROWSER_FIT_CANARY_DEFAULTS.fit,
            loop: loaded.action.isLoop,
        })) {
        fail('initial fit is not a PASS browser-only/no-fitting-mixer result');
    }
    const expectedInputs = {
        sourceVideoSha256: loaded.candidate.sha256,
        immutableManifestSha256: loaded.canonical.immutableSnapshot.sha256,
        fittingBundleSha256: loaded.canonical.fittingSnapshot.sha256,
        sourceModelSha256: loaded.canonical.sourceModelSha256,
        skeletonSha256: loaded.canonical.skeletonSnapshot.sha256,
        observationsSha256: observations.snapshot.sha256,
    };
    if (Object.entries(expectedInputs).some(([name, value]) => summaryJson.inputs?.[name] !== value
            || bridgeJson.inputs?.[name] !== value)
        || !samePath(bridgeJson.inputs?.bundleDirectory || '', loaded.canonical.directory)
        || !samePath(bridgeJson.inputs?.observationsPath || '', observations.snapshot.path)
        || canonicalJson(summaryJson.inputs) !== canonicalJson(bridgeJson.inputs)
        || summaryJson.observations?.frameCount !== loaded.action.frameCount
        || summaryJson.observations?.fps !== OUTPUT_FPS
        || summaryJson.observations?.contactCount !== 0) {
        fail('initial fit lost candidate/action/canonical/observations pins');
    }
    const fittedContract = validateFittedAnimation(
        fittedJson,
        summaryJson,
        loaded.action,
        loaded.canonical.boneNames,
        'fitted animation',
    );
    const debugEvidence = validateFittedDebugEvidence({
        fitted: fittedJson,
        bridge: bridgeJson,
        observations: observations.json,
        fitQa: summaryJson.fit.qa,
        action: loaded.action,
        field: 'initial fit',
    });
    const clipContract = validateThreeClip(
        clip,
        loaded.action,
        loaded.canonical.boneNames,
        'initial Three clip',
    );
    requireExactTrackPayload(fittedContract, clipContract, 'initial fit');
    const policy = BROWSER_CANDIDATE_ACTION_POLICY[loaded.action.actionId];
    const expectedInitialClipName = CONTACT_REFIT_POLICIES.has(policy)
        ? `${loaded.spec.clipName}_BrowserFit`
        : loaded.spec.clipName;
    if (summaryJson.hierarchyClip?.tracks !== clipContract.tracks.length
        || summaryJson.hierarchyClip?.allTracksBound !== true
        || summaryJson.hierarchyClip?.validate !== true
        || summaryJson.hierarchyClip?.name !== clipContract.json.name
        || clipContract.json.name !== expectedInitialClipName
        || Math.abs(summaryJson.hierarchyClip?.durationSeconds - clipContract.json.duration) > 2e-6
        || fittedContract.tracks.length < 2 || clipContract.tracks.length < 2) {
        fail('initial fit manifests do not bind the emitted fitted/Three track inventories');
    }
    validateExactFitGates({
        summary: summaryJson,
        bridge: bridgeJson,
        clipContract,
        action: loaded.action,
        contact: false,
    });
    validateNonRootClipMotion(
        clipContract,
        loaded.canonical.rootBoneNames,
        'initial Three clip',
        new Set(bridgeJson.mappings.map((mapping) => mapping.sourceBone)),
    );
    return {
        bridge,
        bridgeJson,
        summary,
        summaryJson,
        fitted,
        clip,
        clipContract,
        fittedContract,
        artifacts: [bridge, summary, fitted, clip],
        artifactSetSha256: browserFitArtifactSetSha256([bridge, summary, fitted, clip]),
    };
}

function inspectHoofDiagnostic(paths, loaded, observations, fit) {
    const diagnosticExists = secureExists(paths.hoofDiagnostic, 'hoof contact diagnostic');
    const groundExists = secureExists(paths.groundEvidence, 'SAM2 ground evidence');
    if (!diagnosticExists && !groundExists) return null;
    if (!diagnosticExists || !groundExists) fail('hoof diagnostic output pair is partial');
    const diagnostic = readSnapshot(paths.hoofDiagnostic, 'hoof contact diagnostic');
    const ground = readSnapshot(paths.groundEvidence, 'SAM2 ground evidence');
    const report = parseJson(diagnostic, 'hoof contact diagnostic');
    const groundReport = parseJson(ground, 'SAM2 ground evidence');
    requireExactKeys(report, ['schema', 'status', 'inputs', 'bridge', 'schedule'], 'hoof diagnostic report');
    if (report.schema !== 'autorig-browser-hoof-contact-diagnostic.v1'
        || !['PASS', 'FAIL'].includes(report.status)
        || report.inputs?.observations?.sha256 !== observations.snapshot.sha256
        || report.inputs?.bridgeReport?.sha256 !== fit.bridge.sha256
        || report.inputs?.sourceVideo?.sha256 !== loaded.candidate.sha256
        || report.inputs?.bundleManifest?.sha256 !== loaded.canonical.fittingSnapshot.sha256
        || report.inputs?.immutableManifest?.sha256 !== loaded.canonical.immutableSnapshot.sha256
        || report.inputs?.sourceSkeletonSha256 !== loaded.canonical.skeletonSnapshot.sha256
        || report.inputs?.sourceModelSha256 !== loaded.canonical.sourceModelSha256
        || report.inputs?.frames !== loaded.action.frameCount
        || finite(report.inputs?.fps, 'hoof diagnostic fps') !== OUTPUT_FPS
        || report.inputs?.loop !== loaded.action.isLoop
        || report.inputs?.trackerBackend !== HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend
        || report.inputs?.segmenterBackend !== HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend
        || report.inputs?.minimumSupportFeet
            !== HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet
        || canonicalJson(report.inputs?.resolution)
            !== canonicalJson([observations.json.width, observations.json.height])
        || groundReport.provenance?.observationsSha256 !== observations.snapshot.sha256
        || groundReport.provenance?.bridgeReportSha256 !== fit.bridge.sha256
        || groundReport.provenance?.bundleSha256 !== loaded.canonical.fittingSnapshot.sha256
        || groundReport.provenance?.immutableManifestSha256 !== loaded.canonical.immutableSnapshot.sha256
        || groundReport.provenance?.trackerBackend !== HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend
        || groundReport.provenance?.segmenterBackend !== HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend
        || groundReport.provenance?.sourceVideoSha256 !== loaded.candidate.sha256
        || groundReport.schema !== HOOF_CONTACT_INFERENCE_CONTRACT.groundEvidence) {
        fail('hoof diagnostic does not bind exact fitting inputs');
    }
    stageDescriptorMatchesSnapshot(report.inputs.observations, observations.snapshot, 'hoof inputs.observations');
    stageDescriptorMatchesSnapshot(report.inputs.bridgeReport, fit.bridge, 'hoof inputs.bridgeReport');
    stageDescriptorMatchesSnapshot(report.inputs.sourceVideo, loaded.candidate, 'hoof inputs.sourceVideo');
    stageDescriptorMatchesSnapshot(report.inputs.bundleManifest, loaded.canonical.fittingSnapshot, 'hoof inputs.bundleManifest');
    stageDescriptorMatchesSnapshot(report.inputs.immutableManifest, loaded.canonical.immutableSnapshot, 'hoof inputs.immutableManifest');
    if (!Array.isArray(report.inputs.immutableBundleFiles)
        || canonicalJson(report.inputs.immutableBundleFiles.map((row) => ({
            filename: row.filename,
            bytes: row.bytes,
            sha256: row.sha256,
        })).sort((left, right) => left.filename.localeCompare(right.filename)))
        !== canonicalJson(loaded.canonical.files.map((item) => ({
            filename: path.relative(loaded.canonical.directory, item.path).replaceAll('\\', '/'),
            bytes: item.bytes,
            sha256: item.sha256,
        })).sort((left, right) => left.filename.localeCompare(right.filename)))) {
        fail('hoof diagnostic immutable bundle file inventory changed');
    }
    const maskManifest = object(report.inputs.maskManifest, 'hoof diagnostic mask manifest');
    if (maskManifest.schema !== 'autorig-sam2-mask-manifest.v1'
        || maskManifest.sha256 !== groundReport.provenance.maskManifestSha256
        || !Array.isArray(maskManifest.files)
        || maskManifest.files.length !== loaded.action.frameCount) {
        fail('hoof diagnostic mask manifest/ground evidence binding changed');
    }
    const admittedObservationArtifacts = new Map(observations.artifacts.map((item) => [pathKey(item.path), item]));
    maskManifest.files.forEach((row, index) => {
        if (row.frame !== index || !samePath(row.path || '', path.resolve(row.path || ''))) {
            fail('hoof diagnostic mask manifest lost chronology/canonical path');
        }
        const expected = admittedObservationArtifacts.get(pathKey(path.resolve(row.path)));
        if (!expected || row.bytes !== expected.bytes || row.sha256 !== expected.sha256) {
            fail('hoof diagnostic mask was not admitted by the observation bundle manifest');
        }
    });
    let semanticObservations;
    let recomputedGround;
    let recomputedSchedule;
    try {
        semanticObservations = prepareBridgeObservations(observations.json, fit.bridgeJson);
        const loadedMasks = loadMaskFrames({
            raw: observations.json,
            observationPath: observations.snapshot.path,
        });
        if (loadedMasks.manifest.sha256 !== maskManifest.sha256
            || canonicalJson(loadedMasks.manifest.files.map((row) => ({
                frame: row.frame,
                declaredPath: row.declaredPath,
                path: row.path,
                bytes: row.bytes,
                sha256: row.sha256,
            }))) !== canonicalJson(maskManifest.files.map((row) => ({
                frame: row.frame,
                declaredPath: row.declaredPath,
                path: row.path,
                bytes: row.bytes,
                sha256: row.sha256,
            })))) {
            fail('hoof diagnostic SAM2 mask manifest does not recompute from admitted bytes');
        }
        recomputedGround = deriveSam2GroundEvidence({
            observations: semanticObservations,
            masks: loadedMasks.masks,
            options: {
                minimumSupportFeet: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet,
            },
        });
        recomputedGround.provenance = {
            ...recomputedGround.provenance,
            trackerBackend: HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend,
            observationsSha256: observations.snapshot.sha256,
            bridgeReportSha256: fit.bridge.sha256,
            bundleSha256: loaded.canonical.fittingSnapshot.sha256,
            immutableManifestSha256: loaded.canonical.immutableSnapshot.sha256,
            maskManifestSha256: loadedMasks.manifest.sha256,
        };
        if (canonicalJson(groundReport) !== canonicalJson(recomputedGround)) {
            fail('hoof diagnostic SAM2 ground evidence does not recompute from admitted masks');
        }
        recomputedSchedule = diagnoseHoofContacts({
            observations: semanticObservations,
            groundEvidence: recomputedGround,
            options: {
                minimumSupportFeet: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet,
            },
        });
        validatePinnedHoofContactSchedule({ observations: semanticObservations, schedule: report.schedule });
    } catch (error) {
        fail(`hoof diagnostic canonical recomputation failed: ${error.message}`);
    }
    if (canonicalJson(report.schedule) !== canonicalJson(recomputedSchedule)
        || report.status !== recomputedSchedule.status
        || !Array.isArray(report.schedule?.qa?.failures)
        || (report.status === 'PASS' && report.schedule.qa.failures.length !== 0)) {
        fail('hoof diagnostic schedule does not recompute from admitted TAPNext++/SAM2 evidence');
    }
    const expectedHoofTracks = HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.map((foot) => {
        const semanticId = `${foot}.hoof`;
        const mapping = fit.bridgeJson.mappings.find((row) => row.semanticAnchorId === semanticId);
        return {
            foot,
            semanticId,
            sourceTrackId: mapping.sourceTrackId,
            sourceAnchorId: mapping.sourceAnchorId,
            sourceBone: mapping.sourceBone,
        };
    });
    if (report.bridge?.semanticTracks !== semanticObservations.tracks.length
        || canonicalJson(report.bridge?.hoofTracks) !== canonicalJson(expectedHoofTracks)) {
        fail('hoof diagnostic bridge does not preserve the exact four semantic hooves');
    }
    return { passed: report.status === 'PASS', report, diagnostic, ground, artifacts: [diagnostic, ground] };
}

function inspectContactManifest(paths, loaded, observations, fit, diagnostic) {
    if (!secureExists(paths.contactManifest, 'contact refit input manifest')) return null;
    const snapshot = readSnapshot(paths.contactManifest, 'contact refit input manifest');
    if (loaded.spec.externalPins.contactRefitInputManifestSha256 == null) {
        return { awaitingPin: true, snapshot, artifacts: [snapshot] };
    }
    if (snapshot.sha256 !== loaded.spec.externalPins.contactRefitInputManifestSha256) {
        fail('contact refit input manifest does not match external spec pin');
    }
    const manifest = parseJson(snapshot, 'contact refit input manifest');
    if (manifest.schema !== 'autorig-browser-contact-refit-input.v1'
        || manifest.browserOnly !== true || manifest.blenderUsed !== false || manifest.mixerUsed !== false
        || manifest.pins?.sourceVideoSha256 !== loaded.candidate.sha256
        || manifest.pins?.fittingBundleSha256 !== loaded.canonical.fittingSnapshot.sha256
        || manifest.pins?.immutableManifestSha256 !== loaded.canonical.immutableSnapshot.sha256
        || manifest.pins?.sourceModelSha256 !== loaded.canonical.sourceModelSha256
        || manifest.pins?.sourceSkeletonSha256 !== loaded.canonical.skeletonSnapshot.sha256
        || manifest.pins?.observationsSha256 !== observations.snapshot.sha256
        || manifest.pins?.bridgeReportSha256 !== fit.bridge.sha256
        || manifest.pins?.initialFitSummarySha256 !== fit.summary.sha256
        || manifest.pins?.diagnosticSha256 !== diagnostic.diagnostic.sha256
        || !samePath(manifest.inputs?.bundleDirectory || '', loaded.canonical.directory)) {
        fail('contact refit input manifest is not exact browser-only input chain');
    }
    const inputRows = {
        observations: observations.snapshot,
        bridgeReport: fit.bridge,
        initialFitSummary: fit.summary,
        contactDiagnostic: diagnostic.diagnostic,
    };
    for (const [name, expected] of Object.entries(inputRows)) {
        const row = object(manifest.inputs?.[name], `contact manifest inputs.${name}`);
        if (!samePath(row.path, expected.path) || row.bytes !== expected.bytes || row.sha256 !== expected.sha256) {
            fail(`contact manifest input ${name} does not match exact stage artifact`);
        }
    }
    return { awaitingPin: false, snapshot, manifest, diagnostic, artifacts: [snapshot] };
}

function inspectContactRefit(paths, loaded, contactManifest, observations) {
    if (!exactDirectoryFiles(paths.contactRefit, [
        'bridge-report.json', 'fit-summary.json', 'fitted-animation.json', 'three-clip.json',
    ], 'browser contact refit')) return null;
    const bridge = readSnapshot(path.join(paths.contactRefit, 'bridge-report.json'), 'contact bridge report');
    const summary = readSnapshot(path.join(paths.contactRefit, 'fit-summary.json'), 'contact fit summary');
    const fitted = readSnapshot(path.join(paths.contactRefit, 'fitted-animation.json'), 'contact fitted animation');
    const clip = readSnapshot(path.join(paths.contactRefit, 'three-clip.json'), 'contact Three clip');
    const bridgeJson = parseJson(bridge, 'contact bridge report');
    const summaryJson = parseJson(summary, 'contact fit summary');
    const fittedJson = parseJson(fitted, 'contact fitted animation');
    if (bridgeJson.schema !== 'autorig-browser-fit-canary-bridge-report.v1'
        || bridgeJson.status !== 'VALIDATED' || bridgeJson.browserOnly !== true
        || bridgeJson.blenderUsed !== false || bridgeJson.mixerUsed !== false
        || bridgeJson.fittingMode !== 'contact_constrained_refit'
        || bridgeJson.sourceContacts !== 0 || bridgeJson.preparedContacts !== 4
        || summaryJson.schema !== 'autorig-browser-fit-canary-summary.v1'
        || summaryJson.status !== 'PASS_BROWSER_CONTACT_REFIT_GATES'
        || summaryJson.browserOnly !== true || summaryJson.blenderUsed !== false
        || summaryJson.mixerUsed !== false || summaryJson.gates?.passed !== true
        || summaryJson.approvedForBrowserContactFit !== true
        || summaryJson.approvedForAnimationLibrary !== false
        || summaryJson.observations?.contactCount !== 4
        || summaryJson.observations?.frameCount !== loaded.action.frameCount
        || summaryJson.observations?.fps !== OUTPUT_FPS
        || canonicalJson(summaryJson.approvalExclusions) !== canonicalJson([
            'fixed_camera_visual_phase_qa',
            'target_mesh_deformation_qa',
        ])
        || summaryJson.runtime?.threeRevision !== '160'
        || typeof summaryJson.runtime?.node !== 'string'
        || canonicalJson(summaryJson.fit?.options) !== canonicalJson({
            ...BROWSER_FIT_CANARY_DEFAULTS.fit,
            loop: true,
        })) {
        fail('contact refit is not a PASS four-contact browser-only result');
    }
    if (summaryJson.contactRefit?.scheduleStatus !== 'PASS'
        || summaryJson.contactRefit?.semanticGaitQa?.accepted !== true
        || summaryJson.contactRefit.semanticGaitQa?.simultaneousSwingFrameCount !== 0
        || summaryJson.contactRefit?.fittedWalkQa?.status !== 'PASS'
        || !Array.isArray(summaryJson.contactRefit?.fittedWalkQa?.failures)
        || summaryJson.contactRefit.fittedWalkQa.failures.length !== 0
        || summaryJson.contactRefit.fittedWalkQa.thresholdRatio
            !== HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.maximumFittedContactSlideRatio
        || finite(summaryJson.contactRefit.fittedWalkQa.maximumContactSlideRatio,
            'contact refit maximumContactSlideRatio')
            > finite(summaryJson.contactRefit.fittedWalkQa.thresholdRatio, 'contact refit thresholdRatio')
        || canonicalJson(summaryJson.contactRefit?.scheduleSupport)
            !== canonicalJson(contactManifest.diagnostic.report.schedule.qa.support)
        || canonicalJson(summaryJson.contactRefit?.inferredTouchdownOrder)
            !== canonicalJson(contactManifest.diagnostic.report.schedule.inferredTouchdownOrder)
        || summaryJson.inputs?.sourceVideoSha256 !== loaded.candidate.sha256
        || summaryJson.contactRefit?.provenance?.inputManifestSha256 !== contactManifest.snapshot.sha256) {
        fail('contact refit lost gait/input provenance');
    }
    const contactProvenance = object(summaryJson.contactRefit.provenance, 'contact refit provenance');
    if (contactProvenance.schema !== HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitProvenance
        || contactProvenance.source !== 'immutable_pass_diagnostic'
        || contactProvenance.browserOnly !== true || contactProvenance.blenderUsed !== false
        || contactProvenance.mixerUsed !== false
        || Object.entries({
            inputManifestSha256: contactManifest.snapshot.sha256,
            diagnosticSha256: contactManifest.manifest.pins.diagnosticSha256,
            bridgeReportSha256: contactManifest.manifest.pins.bridgeReportSha256,
            initialFitSummarySha256: contactManifest.manifest.pins.initialFitSummarySha256,
            observationsSha256: contactManifest.manifest.pins.observationsSha256,
            fittingBundleSha256: contactManifest.manifest.pins.fittingBundleSha256,
            immutableManifestSha256: contactManifest.manifest.pins.immutableManifestSha256,
            sourceVideoSha256: contactManifest.manifest.pins.sourceVideoSha256,
            sourceModelSha256: contactManifest.manifest.pins.sourceModelSha256,
            sourceSkeletonSha256: contactManifest.manifest.pins.sourceSkeletonSha256,
        }).some(([name, value]) => contactProvenance[name] !== value)) {
        fail('contact refit provenance does not bind the complete immutable walk chain');
    }
    const expectedInputs = {
        sourceVideoSha256: loaded.candidate.sha256,
        immutableManifestSha256: loaded.canonical.immutableSnapshot.sha256,
        fittingBundleSha256: loaded.canonical.fittingSnapshot.sha256,
        sourceModelSha256: loaded.canonical.sourceModelSha256,
        skeletonSha256: loaded.canonical.skeletonSnapshot.sha256,
        observationsSha256: contactManifest.manifest.pins.observationsSha256,
    };
    if (Object.entries(expectedInputs).some(([name, value]) => summaryJson.inputs?.[name] !== value
            || bridgeJson.inputs?.[name] !== value)
        || canonicalJson(summaryJson.inputs) !== canonicalJson(bridgeJson.inputs)
        || !samePath(bridgeJson.inputs?.bundleDirectory || '', loaded.canonical.directory)
        || !samePath(
            bridgeJson.inputs?.observationsPath || '',
            contactManifest.manifest.inputs.observations.path,
        )) {
        fail('contact refit bridge/summary inputs are not the exact immutable chain');
    }
    const fittedContract = validateFittedAnimation(
        fittedJson,
        summaryJson,
        loaded.action,
        loaded.canonical.boneNames,
        'contact fitted animation',
    );
    const debugEvidence = validateFittedDebugEvidence({
        fitted: fittedJson,
        bridge: bridgeJson,
        observations: observations.json,
        fitQa: summaryJson.fit.qa,
        action: loaded.action,
        field: 'contact refit',
    });
    const clipContract = validateThreeClip(
        clip,
        loaded.action,
        loaded.canonical.boneNames,
        'contact Three clip',
    );
    requireExactTrackPayload(fittedContract, clipContract, 'contact refit');
    if (summaryJson.hierarchyClip?.tracks !== clipContract.tracks.length
        || summaryJson.hierarchyClip?.allTracksBound !== true
        || summaryJson.hierarchyClip?.validate !== true
        || summaryJson.hierarchyClip?.name !== clipContract.json.name
        || clipContract.json.name !== loaded.spec.clipName
        || Math.abs(summaryJson.hierarchyClip?.durationSeconds - clipContract.json.duration) > 2e-6
        || fittedContract.tracks.length < 2 || clipContract.tracks.length < 2) {
        fail('contact refit manifests do not bind the emitted track inventories');
    }
    validateExactFitGates({
        summary: summaryJson,
        bridge: bridgeJson,
        clipContract,
        action: loaded.action,
        contact: true,
    });
    validateNonRootClipMotion(
        clipContract,
        loaded.canonical.rootBoneNames,
        'contact Three clip',
        new Set(bridgeJson.mappings.map((mapping) => mapping.sourceBone)),
    );
    return {
        bridge,
        summary,
        fitted,
        clip,
        clipContract,
        fittedContract,
        artifacts: [bridge, summary, fitted, clip],
        artifactSetSha256: browserFitArtifactSetSha256([bridge, summary, fitted, clip]),
    };
}

function inspectVisualQa(paths, loaded, finalClip) {
    if (!secureExists(paths.visualQa, 'visual QA output')) return null;
    const inspected = inspectPublishedSemanticVisualQa({
        outputDirectory: paths.visualQa,
        semanticId: loaded.action.actionId,
        expectedThreeClip: finalClip,
        expectedImmutableManifest: loaded.canonical.immutableSnapshot,
        expectedFittingBundle: loaded.canonical.fittingSnapshot,
        expectedThreeModule: loaded.runtime.threeModule,
        expectedFfmpeg: loaded.runtime.executables.ffmpeg,
    });
    const artifacts = [
        inspected.reportSnapshot,
        ...inspected.validated.inventory.map((row) => row.snapshot),
    ];
    return {
        machinePassed: inspected.evaluation.machinePassed,
        evidenceSnapshot: inspected.reportSnapshot,
        report: inspected.report,
        artifacts,
        artifactSetSha256: browserVisualQaArtifactSetSha256(paths.visualQa, artifacts),
    };
}

function artifactPinRequest(field, snapshot) {
    return {
        field,
        observedSha256NotTrusted: snapshot.sha256,
        observedBytes: snapshot.bytes,
        instruction: 'Create a new externally SHA-pinned spec revision; never edit this artifact.',
    };
}

function artifactSetPinRequest(field, digest, artifacts) {
    return {
        field,
        observedSha256NotTrusted: digest,
        observedBytes: artifacts.reduce((total, item) => total + item.bytes, 0),
        observedArtifacts: artifacts.map((item) => evidence(item)),
        instruction: 'Require a protected-worker output HMAC receipt, then create a new server-authenticated SHA-pinned spec revision; never sign caller-authored artifacts.',
    };
}

export function inspectBrowserAnimationCandidatePipeline({
    specPath,
    expectedSpecSha256,
    trustContextPath,
    expectedTrustContextSha256,
    statePath,
    trustSecret,
    nowEpochMs,
}) {
    const loaded = validateSpec(specPath, expectedSpecSha256, {
        trustContextPath,
        expectedTrustContextSha256,
        statePath,
        trustSecret,
        nowEpochMs,
    });
    const policy = BROWSER_CANDIDATE_ACTION_POLICY[loaded.action.actionId];
    if (!policy) fail('semantic action has no exact pipeline policy');
    const paths = stagePaths(loaded.spec.outputRoot);
    ensureOutputShape(paths, policy);
    const commands = buildCommands(loaded, paths);
    const completedStages = [];
    const artifacts = [];
    const complete = (stage, pins) => {
        completedStages.push(stage);
        artifacts.push(...pins.map((pin) => ({ stage, ...evidence(pin) })));
    };
    const base = {
        schema: BROWSER_CANDIDATE_PIPELINE_STATE_SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        fittingMixerUsed: false,
        qaAnimationMixerUsed: true,
        orchestratorExecutesSubprocesses: false,
        rigType: 'HORSE_2',
        semanticId: loaded.action.actionId,
        generationMode: loaded.action.generationMode,
        frameCount: loaded.action.frameCount,
        outputFps: OUTPUT_FPS,
        candidateId: loaded.spec.candidateId,
        policy,
        outputRoot: loaded.spec.outputRoot,
        spec: evidence(loaded.snapshot),
        immutableInputs: {
            candidate: evidence(loaded.candidate),
            rawSemanticPassReceipt: evidence(loaded.rawReceipt.snapshot),
            rawSemanticEvidence: evidence(loaded.rawReceipt.evidenceSnapshot),
            authenticatedServerTrustContext: evidence(loaded.trust.snapshot),
            contracts: Object.fromEntries(Object.entries(loaded.contracts.snapshots)
                .map(([name, pin]) => [name, evidence(pin)])),
            canonicalImmutableManifest: evidence(loaded.canonical.immutableSnapshot),
            canonicalFittingBundle: evidence(loaded.canonical.fittingSnapshot),
            canonicalSourceModelSha256: loaded.canonical.sourceModelSha256,
            canonicalReference: evidence(loaded.canonical.referenceSnapshot),
            canonicalSkeleton: evidence(loaded.canonical.skeletonSnapshot),
            canonicalSkinWeights: evidence(loaded.canonical.skinWeightsSnapshot),
            canonicalSurfaceTopology: evidence(loaded.canonical.surfaceTopologySnapshot),
            threeModule: { ...evidence(loaded.runtime.threeModule), revision: '160' },
            runtime: {
                executables: Object.fromEntries(Object.entries(loaded.runtime.executables)
                    .map(([name, snapshotValue]) => [name, evidence(snapshotValue)])),
                trackingRuntimeRoot: directoryEvidence(loaded.runtime.trackingRuntimeRootSnapshot),
                trackingRuntimeLock: evidence(loaded.runtime.trackingRuntimeLock),
            },
            dependencyInventory: Object.values(loaded.dependencies).map(evidence)
                .sort((left, right) => left.realpath.localeCompare(right.realpath)),
        },
    };
    const finish = (status, next = null, pinRequest = null, failures = []) => ({
        ...base,
        status,
        completedStages,
        artifacts,
        next,
        pinRequest,
        failures,
    });
    const ready = (status, stage, baseCommand) => finish(status, {
        stage,
        command: {
            ...baseCommand,
            preconditions: [
                ...baseCommand.preconditions,
                evidence(loaded.snapshot),
                ...artifacts.map(({ stage: _stage, ...pin }) => pin),
            ].filter((pin, index, rows) => rows.findIndex((row) => samePath(row.path, pin.path)) === index),
        },
    });

    if (policy === 'unsupported_gait_contact') {
        return finish(
            'FAILED_UNSUPPORTED_GAIT_CONTACT_POLICY',
            null,
            null,
            [`${loaded.action.actionId} requires a dedicated gait/contact refit implementation`],
        );
    }

    const observations = inspectObservations(paths, loaded);
    if (!observations) return ready('READY_TRACKING', 'official_tracking', commands.observations);
    complete('official_tracking', observations.artifacts);
    if (loaded.spec.externalPins.observationsArtifactSetSha256 == null) {
        return finish(
            'AWAITING_EXTERNAL_OBSERVATIONS_ARTIFACT_SET_PIN',
            null,
            artifactSetPinRequest(
                'externalPins.observationsArtifactSetSha256',
                observations.artifactSetSha256,
                observations.artifacts,
            ),
        );
    }
    if (loaded.spec.externalPins.observationsArtifactSetSha256 !== observations.artifactSetSha256) {
        fail('official tracking artifact set does not match external spec pin');
    }

    const fit = inspectInitialFit(paths, loaded, observations);
    if (!fit) return ready('READY_BROWSER_FIT', 'browser_only_fit', commands.initialFit);
    complete('browser_only_fit', fit.artifacts);
    if (loaded.spec.externalPins.initialFitArtifactSetSha256 == null) {
        return finish(
            'AWAITING_EXTERNAL_INITIAL_FIT_ARTIFACT_SET_PIN',
            null,
            artifactSetPinRequest(
                'externalPins.initialFitArtifactSetSha256',
                fit.artifactSetSha256,
                fit.artifacts,
            ),
        );
    }
    if (loaded.spec.externalPins.initialFitArtifactSetSha256 !== fit.artifactSetSha256) {
        fail('initial browser fit artifact set does not match external spec pin');
    }

    let finalClip = fit.clip;
    if (CONTACT_REFIT_POLICIES.has(policy)) {
        const diagnostic = inspectHoofDiagnostic(paths, loaded, observations, fit);
        if (!diagnostic) return ready(
            'READY_HOOF_CONTACT_DIAGNOSTIC',
            'hoof_contact_gait_diagnostic',
            commands.hoofDiagnostic,
        );
        complete('hoof_contact_gait_diagnostic', diagnostic.artifacts);
        if (!diagnostic.passed) {
            return finish('FAILED_HOOF_CONTACT_DIAGNOSTIC', null, null, diagnostic.report.schedule?.qa?.failures || []);
        }
        const manifest = inspectContactManifest(paths, loaded, observations, fit, diagnostic);
        if (!manifest) return ready(
            'READY_CONTACT_REFIT_MANIFEST',
            'contact_refit_manifest',
            commands.contactManifest,
        );
        complete('contact_refit_manifest', manifest.artifacts);
        if (manifest.awaitingPin) {
            return finish(
                'AWAITING_EXTERNAL_CONTACT_MANIFEST_PIN',
                null,
                artifactPinRequest('externalPins.contactRefitInputManifestSha256', manifest.snapshot),
            );
        }
        const refit = inspectContactRefit(paths, loaded, manifest, observations);
        if (!refit) return ready('READY_BROWSER_CONTACT_REFIT', 'browser_contact_refit', commands.contactRefit);
        complete('browser_contact_refit', refit.artifacts);
        if (loaded.spec.externalPins.contactRefitArtifactSetSha256 == null) {
            return finish(
                'AWAITING_EXTERNAL_CONTACT_REFIT_ARTIFACT_SET_PIN',
                null,
                artifactSetPinRequest(
                    'externalPins.contactRefitArtifactSetSha256',
                    refit.artifactSetSha256,
                    refit.artifacts,
                ),
            );
        }
        if (loaded.spec.externalPins.contactRefitArtifactSetSha256 !== refit.artifactSetSha256) {
            fail('browser contact refit artifact set does not match external spec pin');
        }
        finalClip = refit.clip;
    }

    if (loaded.spec.externalPins.finalThreeClipSha256 == null) {
        return finish(
            'AWAITING_EXTERNAL_THREE_CLIP_PIN',
            null,
            artifactPinRequest('externalPins.finalThreeClipSha256', finalClip),
        );
    }
    if (loaded.spec.externalPins.finalThreeClipSha256 !== finalClip.sha256) {
        fail('final Three clip does not match external spec pin');
    }
    if (!secureExists(paths.visualQa, 'visual QA output')) {
        return ready('READY_FIXED_CAMERA_DEFORMATION_QA', 'fixed_camera_deformation_qa', commands.visualQa);
    }
    const visual = inspectVisualQa(paths, loaded, finalClip);
    complete('fixed_camera_deformation_qa', visual.artifacts);
    if (!visual.machinePassed) {
        return finish('FAILED_FIXED_CAMERA_DEFORMATION_QA', null, null, ['machine_visual_or_deformation_qa']);
    }
    if (loaded.spec.externalPins.visualQaArtifactSetSha256 == null) {
        return finish(
            'AWAITING_EXTERNAL_VISUAL_QA_ARTIFACT_SET_PIN',
            null,
            artifactSetPinRequest(
                'externalPins.visualQaArtifactSetSha256',
                visual.artifactSetSha256,
                visual.artifacts,
            ),
        );
    }
    if (loaded.spec.externalPins.visualQaArtifactSetSha256 !== visual.artifactSetSha256) {
        fail('browser visual QA artifact set does not match external spec pin');
    }
    return finish('AWAITING_HUMAN');
}

function writeExclusive(filenameValue, state) {
    const future = secureFutureFile(filenameValue, 'state output');
    const filename = future.path;
    if (secureExists(filename, 'state output')) fail(`state output already exists: ${filename}`);
    const payload = Buffer.from(`${JSON.stringify(state, null, 2)}\n`, 'utf8');
    const handle = fs.openSync(filename, 'wx');
    try {
        fs.writeFileSync(handle, payload);
        fs.fsyncSync(handle);
    } finally {
        fs.closeSync(handle);
    }
    try {
        const afterParent = secureDirectory(path.dirname(filename), 'state output parent');
        if (!sameDirectoryObject(future.parent.identity, afterParent.identity)) {
            fail('state output parent changed during publication');
        }
        const written = readSnapshot(filename, 'published state');
        if (written.bytes !== payload.length || written.sha256 !== sha256Buffer(payload)) {
            fail('published state bytes changed');
        }
        return evidence(written);
    } catch (error) {
        try { fs.unlinkSync(filename); } catch { /* preserve original publication error */ }
        throw error;
    }
}

export function parseBrowserCandidatePipelineArgs(argv) {
    const values = {};
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        if (flag === '--help' || flag === '-h') { help = true; continue; }
        if (![
            '--spec', '--spec-sha256', '--trust-context', '--trust-context-sha256', '--state',
        ].includes(flag)) fail(`unknown option ${flag}`);
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) fail(`${flag} requires a value`);
        values[flag.slice(2)] = argv[++index];
    }
    if (help) return { help: true };
    for (const name of ['spec', 'spec-sha256', 'trust-context', 'trust-context-sha256', 'state']) {
        if (!values[name]) fail(`--${name} is required`);
    }
    return {
        specPath: values.spec,
        expectedSpecSha256: values['spec-sha256'],
        trustContextPath: values['trust-context'],
        expectedTrustContextSha256: values['trust-context-sha256'],
        statePath: values.state,
    };
}

function helpText() {
    return `Usage:
  node run_browser_animation_candidate_pipeline.mjs \\
    --spec pipeline-spec.json --spec-sha256 <sha256> \
    --trust-context server-trust.json --trust-context-sha256 <sha256> \
    --state immutable-state.json

Requires an unexpired server HMAC trust context supplied with
${TRUST_SECRET_ENV}. Validates one pinned raw-semantic-PASS Horse_2 video and authors only
the next exact shell=false command: official TAPNext++/SAM2 tracking, browser-only
fitting, action-specific gait/contact stages, or fixed-camera/deformation QA.
The runner never executes commands, Blender, GPU work, DB writes, production
dispatch, or approval.  Machine PASS terminates at AWAITING_HUMAN.`;
}

export function runBrowserCandidatePipelineCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseBrowserCandidatePipelineArgs(argv);
        if (config.help) { streams.stdout.write(`${helpText()}\n`); return 0; }
        const state = inspectBrowserAnimationCandidatePipeline({
            ...config,
            trustSecret: process.env[TRUST_SECRET_ENV],
        });
        const statePin = writeExclusive(config.statePath, state);
        streams.stdout.write(`${JSON.stringify({ status: state.status, state: statePin, next: state.next?.stage || null })}\n`);
        return state.status.startsWith('FAILED_') ? 3 : 0;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invoked = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invoked === import.meta.url) process.exitCode = runBrowserCandidatePipelineCli();
