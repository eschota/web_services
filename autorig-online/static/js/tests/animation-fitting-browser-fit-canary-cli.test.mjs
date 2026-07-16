import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { applyC1PeriodicClosureToTrackSet } from '../animation-fitting-browser-core.js';

import {
    evaluateBrowserFitGates,
    deriveFloat32LoopVelocityInvariantGate,
    measureLoopVelocitySeam,
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
        '--minimum-visible-confidence', '0.82',
        '--maximum-rest-segment-scale', '2.5',
        '--iterations', '72',
        '--c1-closure-window', '6',
        '--max-quaternion-angular-velocity-seam-rad-per-second', '0.4',
        '--max-position-velocity-seam-world-per-second', '0.2',
        '--no-loop',
        '--require-four-limb-contacts',
        '--emit-three-clip',
    ]);
    assert.equal(parsed.bundleDirectory, 'bundle');
    assert.equal(parsed.positionMappings, false);
    assert.equal(parsed.minimumVisibleRatio, 0.8);
    assert.equal(parsed.minimumVisibleConfidence, 0.82);
    assert.equal(parsed.maximumRestSegmentScale, 2.5);
    assert.equal(parsed.fit.iterations, 72);
    assert.equal(parsed.c1ClosureWindow, 6);
    assert.equal(parsed.gates.maximumQuaternionAngularVelocitySeamRadPerSecond, 0.4);
    assert.equal(parsed.gates.maximumPositionVelocitySeamWorldPerSecond, 0.2);
    assert.equal(parsed.fit.loop, false);
    assert.equal(parsed.gates.requireFourLimbContacts, true);
    assert.equal(parsed.emitThreeClip, true);
    assert.throws(() => parseCanaryArgs(['--bundle-dir', 'bundle']), /missing required option observationsPath/);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x', '--guess', 'yes',
    ]), /unknown option --guess/);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x',
        '--minimum-visible-confidence', '1.1',
    ]), /must be between 0 and 1/);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x',
        '--maximum-rest-segment-scale', '0.9',
    ]), /must be at least 1/);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x',
        '--max-quaternion-angular-velocity-seam-rad-per-second', '0.4',
    ]), /quaternion and position loop velocity seam thresholds must be provided together/);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x',
        '--c1-closure-window', '0',
    ]), /must be an integer >= 1/);
    const derived = parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x',
        '--c1-closure-window', '4', '--float32-loop-velocity-invariant-gates',
    ]);
    assert.equal(derived.float32LoopVelocityInvariantGates, true);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x',
        '--float32-loop-velocity-invariant-gates',
    ]), /require --c1-closure-window/);
    assert.throws(() => parseCanaryArgs([
        '--bundle-dir', 'bundle', '--observations', 'o', '--three-module', 't', '--output-dir', 'x',
        '--c1-closure-window', '4', '--float32-loop-velocity-invariant-gates',
        '--max-quaternion-angular-velocity-seam-rad-per-second', '0.4',
        '--max-position-velocity-seam-world-per-second', '0.2',
    ]), /mutually exclusive/);
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
    assert.equal(
        result.results.some((item) => item.name.includes('velocity_seam')),
        false,
        'velocity gates remain backward-compatible and opt-in',
    );
});

test('loop velocity seam uses quaternion rotation vectors and position derivatives', () => {
    const quaternion = (angle) => [0, 0, Math.sin(angle / 2), Math.cos(angle / 2)];
    const measured = measureLoopVelocitySeam({
        tracks: [
            {
                name: 'hoof.quaternion',
                times: [0, 1, 2],
                values: [quaternion(0), quaternion(0.1), quaternion(0)].flat(),
            },
            {
                name: 'hoof.position',
                times: [0, 1, 2],
                values: [0, 0, 0, 1, 0, 0, 0, 0, 0],
            },
        ],
    });
    assert.equal(measured.schema, 'autorig-browser-loop-velocity-seam.v1');
    assert.equal(measured.quaternionAngularVelocitySeamRadPerSecond.sampleCount, 1);
    assert.ok(Math.abs(measured.quaternionAngularVelocitySeamRadPerSecond.maximum - 0.2) < 1e-12);
    assert.equal(measured.quaternionAngularVelocitySeamRadPerSecond.trackName, 'hoof.quaternion');
    assert.equal(measured.positionVelocitySeamWorldPerSecond.sampleCount, 1);
    assert.equal(measured.positionVelocitySeamWorldPerSecond.maximum, 2);
    assert.equal(measured.positionVelocitySeamWorldPerSecond.trackName, 'hoof.position');
    assert.equal(measured.quaternionPoseSeamRad.maximum, 0);
    assert.equal(measured.positionPoseSeamWorld.maximum, 0);
});

test('Float32 invariant thresholds are derived only from precision, sampling, and clip scale', () => {
    const quaternion = (angle) => [0, 0, Math.sin(angle / 2), Math.cos(angle / 2)];
    const derived = deriveFloat32LoopVelocityInvariantGate({
        tracks: [
            {
                name: 'hoof.quaternion',
                times: [0, 0.04, 0.08],
                values: [quaternion(0), quaternion(0.1), quaternion(0)].flat(),
            },
            {
                name: 'hoof.position',
                times: [0, 0.04, 0.08],
                values: [0, 0, 0, 2, 0, 0, 0, 0, 0],
            },
        ],
    });
    const expectedQuaternion = Math.sqrt(2 ** -23) / 0.04;
    assert.equal(derived.schema, 'autorig-browser-float32-loop-velocity-invariant-gate.v1');
    assert.ok(Math.abs(derived.maximumQuaternionAngularVelocitySeamRadPerSecond - expectedQuaternion) < 1e-15);
    assert.ok(Math.abs(derived.maximumPositionVelocitySeamWorldPerSecond - expectedQuaternion * 2) < 1e-15);
    assert.equal(derived.relativeToleranceFormula, 'sqrt(2^-23)');
});

test('C1 closure stays inside derived invariants for noncommuting Float32 quaternions and unequal dt', () => {
    const multiply = (a, b) => [
        a[3] * b[0] + a[0] * b[3] + a[1] * b[2] - a[2] * b[1],
        a[3] * b[1] - a[0] * b[2] + a[1] * b[3] + a[2] * b[0],
        a[3] * b[2] + a[0] * b[1] - a[1] * b[0] + a[2] * b[3],
        a[3] * b[3] - a[0] * b[0] - a[1] * b[1] - a[2] * b[2],
    ];
    const axisAngle = (axis, angle) => {
        const length = Math.hypot(...axis);
        const sine = Math.sin(angle / 2) / length;
        return [axis[0] * sine, axis[1] * sine, axis[2] * sine, Math.cos(angle / 2)];
    };
    const base = multiply(axisAngle([1, 0, 0], 0.37), axisAngle([0, 1, 0], -0.21));
    const samples = [
        base,
        multiply(base, multiply(axisAngle([1, 1, 0], 0.31), axisAngle([0, 0, 1], -0.13))),
        multiply(base, axisAngle([0, 1, 1], 0.42)),
        multiply(base, axisAngle([1, 0, 1], -0.24)),
        multiply(base, axisAngle([1, 1, 1], 0.17)),
        multiply(base, axisAngle([0, 1, 0], -0.33)),
        multiply(base, axisAngle([1, 0, 1], 0.28)),
        multiply(base, multiply(axisAngle([1, 0, 0], -0.22), axisAngle([0, 1, 0], 0.19))),
        base.map((value) => -value),
    ];
    const clip = {
        tracks: [
            {
                name: 'hoof.quaternion',
                times: new Float32Array([0, 0.03, 0.09, 0.16, 0.24, 0.33, 0.4, 0.43, 0.5]),
                values: new Float32Array(samples.flat()),
            },
            {
                name: 'hoof.position',
                times: new Float32Array([0, 0.03, 0.09, 0.16, 0.24, 0.33, 0.4, 0.43, 0.5]),
                values: new Float32Array([0, 0, 0, 0.9, -0.2, 0.3, 1.1, 0.4, -0.2, 0.7, 0.5, 0.1,
                    0.2, 0.1, 0, -0.4, 0.2, 0.3, 0.5, -0.1, 0.2, 0.3, 0.2, -0.2, 0, 0, 0]),
            },
        ],
    };
    applyC1PeriodicClosureToTrackSet({ tracks: clip.tracks, windowFrames: 3 });
    const measured = measureLoopVelocitySeam(clip);
    const threshold = deriveFloat32LoopVelocityInvariantGate(clip);
    assert.ok(measured.quaternionAngularVelocitySeamRadPerSecond.maximum
        <= threshold.maximumQuaternionAngularVelocitySeamRadPerSecond);
    assert.ok(measured.positionVelocitySeamWorldPerSecond.maximum
        <= threshold.maximumPositionVelocitySeamWorldPerSecond);
    assert.equal(measured.quaternionPoseSeamRad.maximum, 0);
    assert.equal(measured.positionPoseSeamWorld.maximum, 0);
});

test('explicit loop velocity thresholds add fail-closed numeric gates', () => {
    const loopVelocitySeam = {
        quaternionAngularVelocitySeamRadPerSecond: { maximum: 0.3 },
        positionVelocitySeamWorldPerSecond: { maximum: 0.1 },
    };
    const gates = {
        requireFourLimbContacts: true,
        maximumQuaternionAngularVelocitySeamRadPerSecond: 0.4,
        maximumPositionVelocitySeamWorldPerSecond: 0.2,
    };
    const passed = evaluateBrowserFitGates(gateFixture({ loopVelocitySeam, gates }));
    assert.equal(passed.results.find(
        (item) => item.name === 'quaternion_angular_velocity_seam_rad_per_second',
    ).passed, true);
    assert.equal(passed.results.find(
        (item) => item.name === 'position_velocity_seam_world_per_second',
    ).passed, true);

    loopVelocitySeam.quaternionAngularVelocitySeamRadPerSecond.maximum = 0.5;
    const failed = evaluateBrowserFitGates(gateFixture({ loopVelocitySeam, gates }));
    assert.equal(failed.passed, false);
    assert.equal(failed.results.find(
        (item) => item.name === 'quaternion_angular_velocity_seam_rad_per_second',
    ).passed, false);
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
    assert.match(stdout, /--minimum-visible-confidence/);
    assert.match(stdout, /--maximum-rest-segment-scale/);
    assert.match(stdout, /--c1-closure-window/);
    assert.match(stdout, /--float32-loop-velocity-invariant-gates/);
    assert.match(stdout, /--max-quaternion-angular-velocity-seam-rad-per-second/);
    assert.match(stdout, /--max-position-velocity-seam-world-per-second/);
});
