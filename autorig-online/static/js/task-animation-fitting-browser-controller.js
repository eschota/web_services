import { fitBrowserAnimation } from './animation-fitting-browser-core.js';
import {
    HORSE_2_SEMANTIC_PROFILE,
    bakeFittedAnimationToThreeHierarchyClip,
    buildHorse2BrowserFittingSkeleton,
} from './animation-fitting-three-adapter.js';
import { prepareRgbObservationsForBrowser } from './animation-fitting-rgb-observation-bridge.js';
import { fitBrowserAnimationWithPinnedHoofContacts } from './animation-fitting-hoof-contact-inference.js';

const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';
const EVIDENCE_SCHEMA = 'autorig-browser-animation-fitting-controller-evidence.v1';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const MODES = new Set(['diagnostic', 'pinned-contact']);
const REQUIRED_HASH_PINS = Object.freeze([
    'immutableBundleSha256',
    'fittingBundleSha256',
    'observationsSha256',
    'sourceModelSha256',
    'threeModuleSha256',
]);

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function nonEmptyString(value, field) {
    if (typeof value !== 'string' || !value.trim()) {
        throw new Error(`${field} must be a non-empty string`);
    }
    return value.trim();
}

function sha256(value, field) {
    const result = nonEmptyString(value, field);
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function positiveInteger(value, field, minimum = 1) {
    if (!Number.isInteger(value) || value < minimum) {
        throw new Error(`${field} must be an integer of at least ${minimum}`);
    }
    return value;
}

function normalizePins(value, mode) {
    const pins = object(value, 'pins');
    const normalized = Object.fromEntries(
        REQUIRED_HASH_PINS.map((name) => [name, sha256(pins[name], `pins.${name}`)]),
    );
    if (!Object.hasOwn(pins, 'contactScheduleSha256')) {
        throw new Error('pins.contactScheduleSha256 must be explicitly declared');
    }
    if (mode === 'pinned-contact') {
        normalized.contactScheduleSha256 = sha256(
            pins.contactScheduleSha256,
            'pins.contactScheduleSha256',
        );
    } else {
        if (pins.contactScheduleSha256 !== null) {
            throw new Error('pins.contactScheduleSha256 must be null in diagnostic mode');
        }
        normalized.contactScheduleSha256 = null;
    }
    return normalized;
}

function normalizeObservations(value, pins) {
    const observations = object(value, 'observations');
    if (observations.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const frameCount = positiveInteger(observations.frame_count, 'observations.frame_count', 2);
    const width = positiveInteger(observations.width, 'observations.width');
    const height = positiveInteger(observations.height, 'observations.height');
    const fps = Number(observations.fps);
    if (!Number.isFinite(fps) || fps <= 0) throw new Error('observations.fps must be positive');
    const provenance = object(observations.provenance, 'observations.provenance');
    if (provenance.bundle_sha256 !== pins.fittingBundleSha256) {
        throw new Error('observations fitting-bundle SHA-256 does not match its pin');
    }
    if (provenance.immutable_manifest_sha256 !== pins.immutableBundleSha256) {
        throw new Error('observations immutable-bundle SHA-256 does not match its pin');
    }
    sha256(provenance.source_video_sha256, 'observations.provenance.source_video_sha256');
    return { observations, frameCount, width, height, fps, provenance };
}

function normalizePinnedContact(value, expectedSha256) {
    const pinned = object(value, 'pinnedContactSchedule');
    const declaredSha256 = sha256(pinned.sha256, 'pinnedContactSchedule.sha256');
    if (declaredSha256 !== expectedSha256) {
        throw new Error('pinned contact-schedule SHA-256 does not match its controller pin');
    }
    return {
        schedule: object(pinned.schedule, 'pinnedContactSchedule.schedule'),
        pins: object(pinned.pins, 'pinnedContactSchedule.pins'),
        sha256: declaredSha256,
    };
}

function serializeClip(THREE, clip) {
    if (typeof THREE?.AnimationClip?.toJSON !== 'function') {
        throw new Error('THREE.AnimationClip.toJSON() is required for fitting evidence');
    }
    return THREE.AnimationClip.toJSON(clip);
}

/**
 * Fit one already parsed and externally pinned Horse_2 RGB observation bundle
 * on the current browser hierarchy. This controller owns no mixer and no
 * viewer state; the caller decides how the resulting clip joins its playlist.
 */
export async function runTaskAnimationFittingInBrowser(options = {}) {
    const mode = nonEmptyString(options.mode, 'mode');
    if (!MODES.has(mode)) throw new Error('mode must be diagnostic or pinned-contact');
    const semanticId = nonEmptyString(options.semanticId, 'semanticId');
    const clipName = nonEmptyString(options.clipName, 'clipName');
    const THREE = options.THREE;
    const model = options.model;
    const camera = options.camera;
    if (!THREE) throw new Error('THREE is required');
    if (!model || typeof model.traverse !== 'function') throw new Error('current model root is required');
    if (!camera) throw new Error('current camera is required');
    if (typeof options.applyClips !== 'function') throw new Error('applyClips callback is required');

    const pins = normalizePins(options.pins, mode);
    const normalized = normalizeObservations(options.observations, pins);
    let pinnedContact = null;
    if (mode === 'pinned-contact') {
        pinnedContact = normalizePinnedContact(
            options.pinnedContactSchedule,
            pins.contactScheduleSha256,
        );
    } else if (options.pinnedContactSchedule != null) {
        throw new Error('pinnedContactSchedule is not allowed in diagnostic mode');
    }

    const skeletonOptions = options.skeletonOptions == null
        ? {}
        : object(options.skeletonOptions, 'skeletonOptions');
    const outputResolution = [normalized.width, normalized.height];
    const skeleton = buildHorse2BrowserFittingSkeleton({
        ...skeletonOptions,
        THREE,
        model,
        camera,
        sourceViewport: skeletonOptions.sourceViewport
            || HORSE_2_SEMANTIC_PROFILE.reference_resolution,
        referenceResolution: skeletonOptions.referenceResolution
            || HORSE_2_SEMANTIC_PROFILE.reference_resolution,
        outputResolution,
    });
    if (skeleton.rigType !== 'HORSE_2') throw new Error('browser fitting requires a Horse_2 skeleton');
    const sharedBoneRoot = nonEmptyString(
        skeleton.provenance?.sharedBoneRoot,
        'Horse_2 shared bone root',
    );
    const cameraContract = {
        outputResolution,
        bundleSha256: pins.fittingBundleSha256,
        immutableManifestSha256: pins.immutableBundleSha256,
    };
    const prepared = prepareRgbObservationsForBrowser({
        observations: normalized.observations,
        skeleton,
        cameraContract,
        minimumVisiblePoints: options.minimumVisiblePoints ?? 2,
    });
    const fitOptions = options.fitOptions == null
        ? {}
        : object(options.fitOptions, 'fitOptions');
    let fitResult;
    let fitted;
    if (mode === 'pinned-contact') {
        fitResult = fitBrowserAnimationWithPinnedHoofContacts({
            skeleton,
            observations: prepared,
            schedule: pinnedContact.schedule,
            pins: pinnedContact.pins,
            fitOptions,
            gaitQaOptions: options.gaitQaOptions || {},
        });
        fitted = fitResult.fitted;
    } else {
        fitted = fitBrowserAnimation({ skeleton, observations: prepared, options: fitOptions });
        fitResult = { fitted };
    }
    const hierarchy = bakeFittedAnimationToThreeHierarchyClip({
        THREE,
        model,
        camera,
        skeleton,
        fitted,
        outputResolution,
        name: clipName,
    });
    const clipJson = serializeClip(THREE, hierarchy.clip);
    const evidence = {
        schema: EVIDENCE_SCHEMA,
        status: 'READY_TO_APPLY',
        browserOnly: true,
        blenderUsed: false,
        mixerCreated: false,
        rigType: 'HORSE_2',
        mode,
        semanticId,
        clipName,
        pins: { ...pins },
        observations: {
            schema: normalized.observations.schema,
            frameCount: normalized.frameCount,
            width: normalized.width,
            height: normalized.height,
            fps: normalized.fps,
        },
        skeleton: {
            schema: skeleton.schema,
            sharedBoneRoot,
            semanticProfileId: skeleton.provenance.semanticProfileId,
            selectedTrackCount: prepared.tracks.length,
            mappingMode: prepared.provenance.browser_rgb_bridge.mappingMode,
        },
        fit: {
            schema: fitted.schema,
            frameCount: fitted.frameCount,
            fps: fitted.fps,
            durationSeconds: fitted.durationSeconds,
            loop: fitted.loop,
            qa: fitted.qa,
            ...(mode === 'pinned-contact' ? {
                contactScheduleStatus: fitResult.schedule.status,
                semanticGaitQa: fitResult.gaitQa,
                fittedWalkQa: fitResult.fittedWalkQa,
            } : {}),
        },
        hierarchyBakeQa: hierarchy.qa,
        animationClip: clipJson,
    };
    const applyResult = await options.applyClips([hierarchy.clip], evidence);
    if (applyResult === false) throw new Error('applyClips rejected the fitted animation clip');
    evidence.status = 'APPLIED';
    return { clip: hierarchy.clip, evidence, skeleton, preparedObservations: prepared, fitted };
}

export const TASK_ANIMATION_FITTING_BROWSER_CONTROLLER = Object.freeze({
    evidenceSchema: EVIDENCE_SCHEMA,
    observationsSchema: OBSERVATION_SCHEMA,
    rigType: 'HORSE_2',
    modes: Object.freeze([...MODES]),
    pins: Object.freeze([...REQUIRED_HASH_PINS, 'contactScheduleSha256']),
});
