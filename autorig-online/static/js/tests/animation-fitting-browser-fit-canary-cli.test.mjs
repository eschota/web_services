import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    evaluateBrowserFitGates,
    parseCanaryArgs,
    runCli,
    validateImmutableInputs,
} from '../../../tools/animation_fitting/browser_fit_canary.mjs';

const sha = (buffer) => crypto.createHash('sha256').update(buffer).digest('hex');
const jsonBuffer = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');

function immutableFixture() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'autorig-browser-canary-'));
    const bundleDirectory = path.join(root, 'bundle');
    fs.mkdirSync(bundleDirectory);
    const skeleton = { armatures: [{ name: 'Horse_rig', matrix_world: Array(16).fill(0), bones: [] }] };
    const surfaceAnchors = { bones: [] };
    const files = new Map([
        ['skeleton.json', jsonBuffer(skeleton)],
        ['surface_anchors.json', jsonBuffer(surfaceAnchors)],
    ]);
    const fittingBundle = {
        schema: 'autorig-actionless-fitting-bundle.v1',
        source: { filename: 'horse.blend', sha256: 'a'.repeat(64) },
        camera: {
            resolution: [768, 448],
            intrinsics: { fx: 1, fy: 1, cx: 384, cy: 224 },
            camera_to_world: Array(16).fill(0),
            world_to_camera: Array(16).fill(0),
        },
        artifacts: {
            skeleton: { filename: 'skeleton.json', bytes: files.get('skeleton.json').length, sha256: sha(files.get('skeleton.json')) },
            surface_anchors: { filename: 'surface_anchors.json', bytes: files.get('surface_anchors.json').length, sha256: sha(files.get('surface_anchors.json')) },
        },
    };
    files.set('fitting_bundle.json', jsonBuffer(fittingBundle));
    files.forEach((buffer, filename) => fs.writeFileSync(path.join(bundleDirectory, filename), buffer));
    const manifestFiles = [...files].map(([filename, buffer]) => ({
        filename,
        bytes: buffer.length,
        sha256: sha(buffer),
    }));
    const immutableManifest = {
        schema: 'autorig-fitting-immutable-copy.v1',
        bundle_file_count: manifestFiles.length,
        bundle_total_bytes: manifestFiles.reduce((sum, entry) => sum + entry.bytes, 0),
        bundle_manifest: {
            filename: 'fitting_bundle.json',
            sha256: sha(files.get('fitting_bundle.json')),
        },
        files: manifestFiles,
    };
    const immutableManifestBuffer = jsonBuffer(immutableManifest);
    fs.writeFileSync(path.join(bundleDirectory, 'immutable_manifest.json'), immutableManifestBuffer);
    const observations = {
        schema: 'autorig-fitting-observations.v1',
        frame_count: 2,
        width: 768,
        height: 448,
        fps: 30,
        tracks: [],
        contacts: [],
        provenance: {
            immutable_manifest_sha256: sha(immutableManifestBuffer),
            bundle_sha256: sha(files.get('fitting_bundle.json')),
            source_video_sha256: 'b'.repeat(64),
        },
    };
    const observationsPath = path.join(root, 'observations.json');
    fs.writeFileSync(observationsPath, jsonBuffer(observations));
    return { root, bundleDirectory, observationsPath };
}

test('canary CLI parser requires immutable inputs and preserves explicit browser options', () => {
    const parsed = parseCanaryArgs([
        '--bundle-dir', 'bundle',
        '--observations', 'observations.json',
        '--three-module', 'three.module.js',
        '--output-dir', 'output',
        '--position-mappings', 'disabled',
        '--minimum-visible-ratio', '0.8',
        '--iterations', '72',
        '--no-loop',
        '--require-four-limb-contacts',
        '--emit-three-clip',
    ]);
    assert.equal(parsed.bundleDirectory, 'bundle');
    assert.equal(parsed.positionMappings, false);
    assert.equal(parsed.minimumVisibleRatio, 0.8);
    assert.equal(parsed.fit.iterations, 72);
    assert.equal(parsed.fit.loop, false);
    assert.equal(parsed.gates.requireFourLimbContacts, true);
    assert.equal(parsed.emitThreeClip, true);
    assert.throws(() => parseCanaryArgs(['--bundle-dir', 'bundle']), /missing required option observationsPath/);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x', '--guess', 'yes',
    ]), /unknown option --guess/);
});

test('immutable input validation pins every byte and rejects bundle-root drift', (context) => {
    const fixture = immutableFixture();
    context.after(() => fs.rmSync(fixture.root, { recursive: true, force: true }));
    const validated = validateImmutableInputs(fixture);
    assert.equal(validated.integrity.bundleFileCount, 3);
    assert.equal(validated.integrity.sourceModelSha256, 'a'.repeat(64));
    assert.equal(validated.integrity.sourceVideoSha256, 'b'.repeat(64));
    fs.appendFileSync(path.join(fixture.bundleDirectory, 'skeleton.json'), 'tamper');
    assert.throws(() => validateImmutableInputs(fixture), /byte count mismatch/);
});

test('immutable input validation rejects unpinned extra bundle files', (context) => {
    const fixture = immutableFixture();
    context.after(() => fs.rmSync(fixture.root, { recursive: true, force: true }));
    fs.writeFileSync(path.join(fixture.bundleDirectory, 'stale.json'), '{}');
    assert.throws(() => validateImmutableInputs(fixture), /do not exactly match/);
});

function gateFixture(overrides = {}) {
    return {
        maximumHeadReconstructionErrorWorld: 1e-9,
        restSeedAlignment: { maximumErrorPx: 0.5 },
        prepared: {
            contacts: [
                { anchor_id: 'fore_left.hoof' },
                { anchor_id: 'fore_right.hoof' },
                { anchor_id: 'hind_left.hoof' },
                { anchor_id: 'hind_right.hoof' },
            ],
        },
        fitted: {
            qa: {
                targetSamples: 100,
                targetMode: 'ordered_deform_heads',
                initialMeanTargetErrorPx: 2,
                finalMeanTargetErrorPx: 1,
                maximumTargetErrorPx: 4,
                maximumBoneLengthErrorPx: 1e-10,
                maximumJointLimitViolationRad: 0,
                maximumContactSlidePx: 0.25,
                loopEndpointError: 0,
            },
        },
        hierarchyQa: {
            maximumSegmentLengthDriftWorld: 1e-10,
            maximumHierarchyBakeReprojectionErrorPx: 1e-10,
            maximumRequestedFittedPointErrorPx: 0.5,
            unreachablePixelRays: 4,
        },
        hierarchyRayCount: 100,
        clipValid: true,
        allTracksBound: true,
        minimumTargetSamples: 80,
        gates: { requireFourLimbContacts: true },
        ...overrides,
    };
}

test('browser-fit gates pass structure without granting gait or release approval', () => {
    const result = evaluateBrowserFitGates(gateFixture());
    assert.equal(result.passed, true);
    assert.equal(result.results.find((item) => item.name === 'four_limb_contacts').passed, true);
    assert.equal(result.results.find((item) => item.name === 'unreachable_pixel_ray_ratio').actual, 0.04);
});

test('browser-fit gates fail closed for legacy mappings, target regression, and missing contacts', () => {
    const fixture = gateFixture();
    fixture.prepared.contacts = [];
    fixture.fitted.qa.targetMode = 'legacy_three_track';
    fixture.fitted.qa.finalMeanTargetErrorPx = 2.5;
    const result = evaluateBrowserFitGates(fixture);
    assert.equal(result.passed, false);
    const failed = result.results.filter((item) => !item.passed).map((item) => item.name);
    assert.ok(failed.includes('target_error_improved'));
    assert.ok(failed.includes('ordered_deform_heads'));
    assert.ok(failed.includes('four_limb_contacts'));
});

test('CLI help is side-effect free and documents browser-only approval boundaries', async () => {
    let stdout = '';
    let stderr = '';
    const exitCode = await runCli(['--help'], {
        stdout: { write: (value) => { stdout += value; } },
        stderr: { write: (value) => { stderr += value; } },
    });
    assert.equal(exitCode, 0);
    assert.equal(stderr, '');
    assert.match(stdout, /never grants final animation approval/);
    assert.match(stdout, /--emit-three-clip/);
});
