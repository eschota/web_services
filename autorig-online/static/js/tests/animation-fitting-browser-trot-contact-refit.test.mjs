import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    applyPinnedHorseTrotDiagnostic,
    fitBrowserAnimationWithPinnedTrotContacts,
    gateFittedTrot,
    HOOF_CONTACT_INFERENCE_CONTRACT,
    validatePinnedHorseTrotDiagnostic,
} from '../animation-fitting-hoof-contact-inference.js';
import { buildTrotDiagnosticReport } from '../../../tools/animation_fitting/diagnose_browser_horse_trot.mjs';
import {
    assessNonRootClipMotion,
} from '../../../tools/animation_fitting/browser_fit_canary.mjs';
import {
    runBrowserTrotContactRefit,
    validateTrotContactRefitInputs,
} from '../../../tools/animation_fitting/browser_trot_contact_refit.mjs';
import {
    authorTrotContactRefitInputManifest,
    parseAuthorTrotRefitArgs,
} from '../../../tools/animation_fitting/author_browser_trot_contact_refit_manifest.mjs';
import { prepareBridgeObservations } from '../../../tools/animation_fitting/diagnose_browser_hoof_contacts.mjs';

const FEET = ['fore_left', 'hind_right', 'fore_right', 'hind_left'];
const sha = (value) => crypto.createHash('sha256').update(value).digest('hex');

function write(filename, value) {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value);
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, buffer);
    return { path: filename, bytes: buffer.length, sha256: sha(buffer) };
}

function writeJson(filename, value) {
    return write(filename, `${JSON.stringify(value, null, 2)}\n`);
}

function semanticTrotObservations({ pace = false } = {}) {
    const uniqueFrameCount = 48;
    const centers = pace
        ? { fore_left: 4, hind_left: 4, fore_right: 28, hind_right: 28 }
        : { fore_left: 4, hind_right: 4, fore_right: 28, hind_left: 28 };
    const tracks = [];
    for (const foot of FEET) {
        const swing = new Set([-2, -1, 0, 1, 2].map((offset) => (
            centers[foot] + offset + uniqueFrameCount
        ) % uniqueFrameCount));
        const hoofPoints = Array.from({ length: uniqueFrameCount }, (_, frame) => ({
            frame,
            x: 100 + FEET.indexOf(foot) * 25,
            y: swing.has(frame) ? 90 : 100,
            visible: true,
            confidence: 1,
        }));
        hoofPoints.push({ ...hoofPoints[0], frame: uniqueFrameCount });
        const proximalPoints = hoofPoints.map((point) => ({ ...point, y: point.y - 100 }));
        const jointPoints = hoofPoints.map((point) => ({ ...point, y: point.y - 50 }));
        tracks.push({ anchor_id: `${foot}.proximal`, points: proximalPoints });
        tracks.push({ anchor_id: `${foot}.joint`, points: jointPoints });
        tracks.push({ anchor_id: `${foot}.hoof`, points: hoofPoints });
    }
    return {
        schema: 'autorig-fitting-observations.v1',
        frame_count: 49,
        width: 384,
        height: 224,
        fps: 30,
        tracks,
        contacts: [],
        provenance: {
            source_video_sha256: 'a'.repeat(64),
            bundle_sha256: 'b'.repeat(64),
            immutable_manifest_sha256: 'c'.repeat(64),
            tracker: { backend: 'google-deepmind-tapnextpp-online' },
            segmenter: { backend: 'facebookresearch-sam2.1-video' },
        },
    };
}

function diagnostic(observations) {
    return buildTrotDiagnosticReport({
        observations,
        integrity: {
            observations: { sha256: 'd'.repeat(64) },
            bridgeReport: { sha256: 'e'.repeat(64) },
            sourceVideo: { sha256: observations.provenance.source_video_sha256 },
            bundleManifest: { sha256: observations.provenance.bundle_sha256 },
            immutableManifest: { sha256: observations.provenance.immutable_manifest_sha256 },
            sourceSkeletonSha256: 'f'.repeat(64),
            sourceModelSha256: '1'.repeat(64),
        },
        candidateId: 'trot-pass',
        sourceReference: 'immutable-test.mp4',
        runtime: { browserOnly: true, blenderUsed: false },
        createdAt: '2026-07-17T00:00:00.000Z',
    });
}

function pins(observations) {
    return {
        inputManifestSha256: '2'.repeat(64),
        diagnosticSha256: '3'.repeat(64),
        bridgeReportSha256: '4'.repeat(64),
        initialFitSummarySha256: '5'.repeat(64),
        observationsSha256: '6'.repeat(64),
        fittingBundleSha256: observations.provenance.bundle_sha256,
        immutableManifestSha256: observations.provenance.immutable_manifest_sha256,
        sourceVideoSha256: observations.provenance.source_video_sha256,
        sourceModelSha256: '7'.repeat(64),
        sourceSkeletonSha256: '8'.repeat(64),
    };
}

function fittedFromObservations(observations) {
    const byId = new Map(observations.tracks.map((track) => [track.anchor_id, track]));
    return {
        schema: 'autorig-browser-fitted-animation.v1',
        frameCount: observations.frame_count,
        fps: observations.fps,
        durationSeconds: 48 / observations.fps,
        loop: true,
        tracks: [{ name: 'leg.quaternion', values: [0, 0, 0, 1, 0, 0.1, 0, 0.995] }],
        positionTracks: [],
        rootTrack: null,
        qa: { maximumContactSlidePx: 0 },
        frames: Array.from({ length: observations.frame_count }, (_, frame) => ({
            frame,
            limbs: Object.fromEntries(FEET.map((foot) => {
                const proximal = byId.get(`${foot}.proximal`).points[frame];
                const hoof = byId.get(`${foot}.hoof`).points[frame];
                return [foot, { points: [[proximal.x, proximal.y], [hoof.x, hoof.y]] }];
            })),
        })),
    };
}

function fittingSkeleton(observations) {
    const byId = new Map(observations.tracks.map((track) => [track.anchor_id, track]));
    return {
        schema: 'autorig-browser-fitting-skeleton.v1',
        rigType: 'HORSE_2',
        limbs: Object.fromEntries(FEET.map((foot) => {
            const proximal = byId.get(`${foot}.proximal`).points[0];
            const joint = byId.get(`${foot}.joint`).points[0];
            const hoof = byId.get(`${foot}.hoof`).points[0];
            return [foot, {
                joints: [
                    {
                        bone: `${foot}_upper`,
                        restStart: [proximal.x, proximal.y],
                        restEnd: [joint.x, joint.y],
                        restQuaternion: [0, 0, 0, 1],
                        rotationAxis: [0, 0, 1],
                        minAngle: -2.8,
                        maxAngle: 2.8,
                    },
                    {
                        bone: `${foot}_lower`,
                        restStart: [joint.x, joint.y],
                        restEnd: [hoof.x, hoof.y],
                        restQuaternion: [0, 0, 0, 1],
                        rotationAxis: [0, 0, 1],
                        minAngle: -2.8,
                        maxAngle: 2.8,
                    },
                ],
                proximalTrack: `${foot}.proximal`,
                jointTrack: `${foot}.joint`,
                hoofTrack: `${foot}.hoof`,
                trackedJointIndex: 1,
            }];
        })),
    };
}

test('dedicated TROT validator recomputes diagonal QA and derives four code-owned contacts', () => {
    const observations = semanticTrotObservations();
    const report = diagnostic(observations);
    const validated = validatePinnedHorseTrotDiagnostic({ observations, diagnostic: report });
    assert.equal(validated.gaitQa.status, 'PASS');
    assert.equal(validated.schedule.schema, HOOF_CONTACT_INFERENCE_CONTRACT.trotSchedule);
    assert.equal(validated.schedule.profile.id, 'horse.diagonal_pair_trot.v1');
    assert.equal(validated.schedule.contacts.length, 4);
    assert.deepEqual(validated.schedule.profile.diagonalPairs.map((pair) => pair.feet), [
        ['fore_left', 'hind_right'],
        ['fore_right', 'hind_left'],
    ]);
    assert.ok(validated.schedule.contacts.every((contact) => contact.weight === 1 && contact.frames.length));

    const applied = applyPinnedHorseTrotDiagnostic({ observations, diagnostic: report, pins: pins(observations) });
    assert.equal(applied.observations.contacts.length, 4);
    assert.equal(applied.observations.provenance.browser_trot_hoof_contacts.humanReviewRequired, true);
    assert.equal(applied.observations.provenance.browser_trot_hoof_contacts.blenderUsed, false);
});

test('TROT validator rejects forged status, profile, pair metrics, contacts, pins, and WALK substitution', () => {
    const observations = semanticTrotObservations();
    const base = diagnostic(observations);
    for (const mutate of [
        (value) => { value.status = 'FAIL'; },
        (value) => { value.profile.id = 'horse.four_beat_walk.v1'; },
        (value) => { value.qa.pairs.left_fore_right_hind.swingDice = 1.001; },
        (value) => { value.contacts = [{ anchor_id: 'fore_left.hoof', frames: [0] }]; },
        (value) => { value.schema = 'autorig-browser-hoof-contact-diagnostic.v1'; },
    ]) {
        const forged = structuredClone(base);
        mutate(forged);
        assert.throws(
            () => validatePinnedHorseTrotDiagnostic({ observations, diagnostic: forged }),
            /TROT|trot|contacts|profile|QA/,
        );
    }
    const wrongPins = pins(observations);
    wrongPins.sourceVideoSha256 = '9'.repeat(64);
    assert.throws(
        () => applyPinnedHorseTrotDiagnostic({ observations, diagnostic: base, pins: wrongPins }),
        /source-video SHA-256/,
    );
    assert.throws(
        () => validatePinnedHorseTrotDiagnostic({
            observations: semanticTrotObservations({ pace: true }),
            diagnostic: base,
        }),
        /fail the code-owned diagonal-pair profile/,
    );
});

test('post-solver TROT QA rejects fitted pace and normalized hoof slide independently', () => {
    const source = semanticTrotObservations();
    const schedule = validatePinnedHorseTrotDiagnostic({ observations: source, diagnostic: diagnostic(source) }).schedule;
    const pass = gateFittedTrot({ fitted: fittedFromObservations(source), schedule });
    assert.equal(pass.status, 'PASS');
    assert.equal(pass.gaitQa.accepted, true);

    const pace = fittedFromObservations(semanticTrotObservations({ pace: true }));
    const paceQa = gateFittedTrot({ fitted: pace, schedule });
    assert.equal(paceQa.status, 'FAIL');
    assert.ok(paceQa.failures.includes('fitted_projected_diagonal_trot'));

    const sliding = fittedFromObservations(source);
    sliding.qa.maximumContactSlidePx = 1;
    const slideQa = gateFittedTrot({ fitted: sliding, schedule });
    assert.equal(slideQa.status, 'FAIL');
    assert.ok(slideQa.failures.includes('fitted_trot_contact_slide'));
});

test('dedicated TROT path invokes the pure browser solver and preserves diagonal fitted motion', () => {
    const observations = semanticTrotObservations();
    const report = diagnostic(observations);
    const result = fitBrowserAnimationWithPinnedTrotContacts({
        skeleton: fittingSkeleton(observations),
        observations,
        diagnosticObservations: observations,
        diagnostic: report,
        pins: pins(observations),
        fitOptions: {
            loop: true,
            iterations: 64,
            tolerance: 0.05,
            jointAttraction: 0.15,
            smoothingRadius: 1,
            loopBlendFrames: 4,
        },
    });
    assert.equal(result.runtime.browserOnly, true);
    assert.equal(result.runtime.blenderUsed, false);
    assert.equal(result.sourceGaitQa.accepted, true);
    assert.equal(result.fittedTrotQa.status, 'PASS');
    assert.equal(result.fittedTrotQa.gaitQa.accepted, true);
    assert.equal(result.observations.contacts.length, 4);
});

test('AnimationClip motion gate rejects static and root-only clips', () => {
    const track = (name, values, size) => ({ name, values, getValueSize: () => size });
    const rootOnly = assessNonRootClipMotion({ tracks: [
        track('Armature.position', [0, 0, 0, 1, 0, 0], 3),
    ] }, 'Armature');
    assert.equal(rootOnly.dynamicTrackCount, 0);
    const staticClip = assessNonRootClipMotion({ tracks: [
        track('leg.quaternion', [0, 0, 0, 1, 0, 0, 0, 1], 4),
    ] }, 'Armature');
    assert.equal(staticClip.dynamicTrackCount, 0);
    const moving = assessNonRootClipMotion({ tracks: FEET.map((foot, index) => (
        track(`${foot}.position`, [0, 0, 0, 0, 0.01 + index * 0.001, 0], 3)
    )) }, 'Armature');
    assert.equal(moving.dynamicTrackCount, 4);
    assert.equal(moving.dynamicBoneCount, 4);
});

function chainFixture() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'autorig-trot-refit-'));
    const bundleDirectory = path.join(root, 'bundle');
    const skeleton = writeJson(path.join(bundleDirectory, 'skeleton.json'), { armatures: [] });
    const anchors = writeJson(path.join(bundleDirectory, 'surface_anchors.json'), { bones: [] });
    const sourceModelSha256 = '1'.repeat(64);
    const bundleValue = {
        schema: 'autorig-actionless-fitting-bundle.v1',
        source: { filename: 'horse.glb', sha256: sourceModelSha256 },
        camera: { resolution: [384, 224] },
        artifacts: {
            skeleton: { filename: 'skeleton.json', bytes: skeleton.bytes, sha256: skeleton.sha256 },
            surface_anchors: { filename: 'surface_anchors.json', bytes: anchors.bytes, sha256: anchors.sha256 },
        },
    };
    const bundle = writeJson(path.join(bundleDirectory, 'fitting_bundle.json'), bundleValue);
    const rows = [skeleton, anchors, bundle].map((row) => ({
        filename: path.basename(row.path), bytes: row.bytes, sha256: row.sha256,
    }));
    const immutable = writeJson(path.join(bundleDirectory, 'immutable_manifest.json'), {
        schema: 'autorig-fitting-immutable-copy.v1',
        source_model: { sha256: sourceModelSha256 },
        bundle_file_count: rows.length,
        bundle_total_bytes: rows.reduce((sum, row) => sum + row.bytes, 0),
        bundle_manifest: { filename: 'fitting_bundle.json', sha256: bundle.sha256 },
        files: rows,
    });
    const video = write(path.join(root, 'source.mp4'), 'immutable-trot-video');
    const semantic = semanticTrotObservations();
    semantic.provenance.source_video = video.path;
    semantic.provenance.source_video_sha256 = video.sha256;
    semantic.provenance.bundle = bundleDirectory;
    semantic.provenance.bundle_sha256 = bundle.sha256;
    semantic.provenance.immutable_manifest_sha256 = immutable.sha256;
    const rawTracks = [];
    const mappings = [];
    let index = 0;
    for (const foot of FEET) {
        for (const role of ['proximal', 'joint', 'hoof']) {
            const semanticId = `${foot}.${role}`;
            const sourceId = `tap-${index}`;
            const sourceBone = `bone-${index}`;
            const semanticTrack = semantic.tracks.find((item) => item.anchor_id === semanticId)
                || semantic.tracks.find((item) => item.anchor_id === `${foot}.proximal`);
            rawTracks.push({
                id: sourceId,
                anchor_id: `${sourceBone}:${index}`,
                points: semanticTrack.points,
            });
            mappings.push({
                limb: foot, semanticAnchorId: semanticId, sourceTrackId: sourceId,
                sourceAnchorId: `${sourceBone}:${index}`, sourceBone,
            });
            index += 1;
        }
    }
    const rawValue = { ...semantic, tracks: rawTracks, contacts: [] };
    const observations = writeJson(path.join(root, 'observations.json'), rawValue);
    const baseInputs = {
        bundleDirectory,
        observationsPath: observations.path,
        fittingBundleSha256: bundle.sha256,
        immutableManifestSha256: immutable.sha256,
        skeletonSha256: skeleton.sha256,
        surfaceAnchorsSha256: anchors.sha256,
        observationsSha256: observations.sha256,
        sourceVideoSha256: video.sha256,
        sourceModelSha256,
        bundleFileCount: rows.length,
        bundleTotalBytes: rows.reduce((sum, row) => sum + row.bytes, 0),
    };
    const bridgeValue = {
        schema: 'autorig-browser-fit-canary-bridge-report.v1',
        status: 'VALIDATED', browserOnly: true, blenderUsed: false, mixerUsed: false,
        fittingMode: 'unconstrained_diagnostic', inputs: baseInputs,
        sourceContacts: 0, preparedContacts: 0, mappings,
    };
    const bridge = writeJson(path.join(root, 'bridge-report.json'), bridgeValue);
    const initial = writeJson(path.join(root, 'fit-summary.json'), {
        schema: 'autorig-browser-fit-canary-summary.v1',
        status: 'PASS_BROWSER_FIT_GATES', browserOnly: true, blenderUsed: false, mixerUsed: false,
        fittingMode: 'unconstrained_diagnostic', approvedForBrowserContactFit: false,
        approvedForAnimationLibrary: false, inputs: baseInputs,
        observations: { contactCount: 0 }, gates: { passed: true, results: [{ name: 'base', passed: true }] },
    });
    const bridged = prepareBridgeObservations(rawValue, bridgeValue);
    const reportValue = buildTrotDiagnosticReport({
        observations: bridged,
        integrity: {
            observations, bridgeReport: bridge, sourceVideo: video,
            bundleManifest: bundle, immutableManifest: immutable,
            sourceSkeletonSha256: skeleton.sha256, sourceModelSha256,
        },
        candidateId: 'trot-chain', sourceReference: video.path,
        runtime: { browserOnly: true, blenderUsed: false },
        createdAt: '2026-07-17T00:00:00.000Z',
    });
    const report = writeJson(path.join(root, 'trot-diagnostic.json'), reportValue);
    const manifestValue = {
        schema: 'autorig-browser-horse-trot-contact-refit-input.v1',
        browserOnly: true, blenderUsed: false, mixerUsed: false,
        gait: 'diagonal_pair_trot', humanReviewRequired: true,
        inputs: {
            bundleDirectory,
            observations: { path: observations.path, bytes: observations.bytes, sha256: observations.sha256 },
            bridgeReport: { path: bridge.path, bytes: bridge.bytes, sha256: bridge.sha256 },
            initialFitSummary: { path: initial.path, bytes: initial.bytes, sha256: initial.sha256 },
            trotDiagnostic: { path: report.path, bytes: report.bytes, sha256: report.sha256 },
        },
        pins: {
            observationsSha256: observations.sha256,
            bridgeReportSha256: bridge.sha256,
            initialFitSummarySha256: initial.sha256,
            diagnosticSha256: report.sha256,
            sourceVideoSha256: video.sha256,
            fittingBundleSha256: bundle.sha256,
            immutableManifestSha256: immutable.sha256,
            sourceModelSha256,
            sourceSkeletonSha256: skeleton.sha256,
        },
    };
    const manifestPath = path.join(root, 'trot-refit-input.json');
    const manifest = writeJson(manifestPath, manifestValue);
    return { root, manifestPath, manifest, manifestValue, reportValue };
}

test('immutable TROT chain binds source, bundle, bridge, diagnostic and fails closed before solver', async () => {
    const fixture = chainFixture();
    const validated = validateTrotContactRefitInputs({
        inputManifestPath: fixture.manifestPath,
        expectedManifestSha256: fixture.manifest.sha256,
    });
    assert.equal(validated.schedule.status, 'PASS');
    assert.equal(validated.pins.bridgeReportSha256, fixture.manifestValue.pins.bridgeReportSha256);
    assert.equal(validated.pins.diagnosticSha256, fixture.manifestValue.pins.diagnosticSha256);

    const forged = structuredClone(fixture.manifestValue);
    forged.pins.bridgeReportSha256 = '9'.repeat(64);
    const forgedManifest = writeJson(path.join(fixture.root, 'forged-manifest.json'), forged);
    assert.throws(
        () => validateTrotContactRefitInputs({
            inputManifestPath: forgedManifest.path,
            expectedManifestSha256: forgedManifest.sha256,
        }),
        /bridge file SHA-256/,
    );

    const forgedDiagnosticValue = structuredClone(fixture.reportValue);
    forgedDiagnosticValue.inputs.bridgeReport.sha256 = '8'.repeat(64);
    const forgedDiagnostic = writeJson(path.join(fixture.root, 'forged-diagnostic.json'), forgedDiagnosticValue);
    const selfRepinnedValue = structuredClone(fixture.manifestValue);
    selfRepinnedValue.inputs.trotDiagnostic = {
        path: forgedDiagnostic.path, bytes: forgedDiagnostic.bytes, sha256: forgedDiagnostic.sha256,
    };
    selfRepinnedValue.pins.diagnosticSha256 = forgedDiagnostic.sha256;
    const selfRepinned = writeJson(path.join(fixture.root, 'self-repinned-diagnostic-manifest.json'), selfRepinnedValue);
    assert.throws(
        () => validateTrotContactRefitInputs({
            inputManifestPath: selfRepinned.path,
            expectedManifestSha256: selfRepinned.sha256,
        }),
        /TROT diagnostic bridge\.sha256/,
    );

    const failedReport = { ...fixture.reportValue, status: 'FAIL', qa: { ...fixture.reportValue.qa, status: 'FAIL' } };
    const failedDiagnostic = writeJson(path.join(fixture.root, 'failed-trot.json'), failedReport);
    const failedManifestValue = structuredClone(fixture.manifestValue);
    failedManifestValue.inputs.trotDiagnostic = {
        path: failedDiagnostic.path, bytes: failedDiagnostic.bytes, sha256: failedDiagnostic.sha256,
    };
    failedManifestValue.pins.diagnosticSha256 = failedDiagnostic.sha256;
    const failedManifest = writeJson(path.join(fixture.root, 'failed-manifest.json'), failedManifestValue);
    let invoked = false;
    await assert.rejects(
        () => runBrowserTrotContactRefit({
            inputManifestPath: failedManifest.path,
            expectedManifestSha256: failedManifest.sha256,
            threeModule: import.meta.url,
            outputDirectory: path.join(fixture.root, 'must-not-exist'),
        }, {
            runBrowserFitCanary: async () => { invoked = true; },
        }),
        /pinned TROT diagnostic must be a PASS/,
    );
    assert.equal(invoked, false);
    assert.equal(fs.existsSync(path.join(fixture.root, 'must-not-exist')), false);
});

test('TROT manifest author publishes only a fully recomputed immutable chain', () => {
    const fixture = chainFixture();
    const outputPath = path.join(fixture.root, 'authored-trot-input.json');
    const result = authorTrotContactRefitInputManifest({
        bundleDirectory: fixture.manifestValue.inputs.bundleDirectory,
        observationsPath: fixture.manifestValue.inputs.observations.path,
        bridgeReportPath: fixture.manifestValue.inputs.bridgeReport.path,
        initialFitSummaryPath: fixture.manifestValue.inputs.initialFitSummary.path,
        trotDiagnosticPath: fixture.manifestValue.inputs.trotDiagnostic.path,
        outputPath,
    });
    assert.equal(result.manifest.gait, 'diagonal_pair_trot');
    assert.equal(result.manifest.humanReviewRequired, true);
    assert.equal(sha(fs.readFileSync(outputPath)), result.sha256);
    assert.throws(
        () => authorTrotContactRefitInputManifest({
            bundleDirectory: fixture.manifestValue.inputs.bundleDirectory,
            observationsPath: fixture.manifestValue.inputs.observations.path,
            bridgeReportPath: fixture.manifestValue.inputs.bridgeReport.path,
            initialFitSummaryPath: fixture.manifestValue.inputs.initialFitSummary.path,
            trotDiagnosticPath: fixture.manifestValue.inputs.trotDiagnostic.path,
            outputPath,
        }),
        /already exists/,
    );
    assert.equal(parseAuthorTrotRefitArgs([
        '--bundle-dir', 'bundle', '--observations', 'observations.json',
        '--bridge-report', 'bridge.json', '--initial-fit-summary', 'fit.json',
        '--trot-diagnostic', 'trot.json', '--output', 'manifest.json',
    ]).trotDiagnosticPath, 'trot.json');
});
