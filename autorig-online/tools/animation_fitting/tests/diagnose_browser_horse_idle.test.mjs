import assert from 'node:assert/strict';
import test from 'node:test';

import {
    buildIdleDiagnosticReport,
    HORSE_PLANTED_IDLE_DIAGNOSTIC_SCHEMA,
} from '../diagnose_browser_horse_idle.mjs';

const FRAME_COUNT = 49;

function observations({ driftFoot = null } = {}) {
    const tracks = [];
    for (const foot of ['fore_left', 'fore_right', 'hind_left', 'hind_right']) {
        tracks.push({
            anchor_id: `${foot}.hoof`,
            points: Array.from({ length: FRAME_COUNT }, (_, frame) => {
                const phase = frame / (FRAME_COUNT - 1) * Math.PI * 2;
                return {
                    frame,
                    x: 100 + Math.sin(phase) * 0.2 + (foot === driftFoot ? frame * 0.2 : 0),
                    y: 120 + Math.cos(phase) * 0.1,
                    visible: true,
                    confidence: 0.95,
                };
            }),
        });
    }
    return {
        schema: 'autorig-fitting-observations.v1',
        frame_count: FRAME_COUNT,
        width: 384,
        height: 224,
        fps: 30,
        tracks,
        contacts: [],
        provenance: {
            tracker: { backend: 'google-deepmind-tapnextpp-online' },
            segmenter: { backend: 'facebookresearch-sam2.1-video' },
        },
    };
}

function integrity() {
    const pin = (name) => ({ path: name, bytes: 1, sha256: 'a'.repeat(64) });
    return {
        observations: pin('observations.json'),
        bridgeReport: pin('bridge-report.json'),
        sourceVideo: pin('source.mp4'),
        bundleManifest: pin('fitting_bundle.json'),
        immutableManifest: pin('immutable_manifest.json'),
        sourceSkeletonSha256: 'b'.repeat(64),
        sourceModelSha256: 'c'.repeat(64),
    };
}

test('immutable planted-idle report permits contact fit but never library approval', () => {
    const report = buildIdleDiagnosticReport({
        observations: observations(),
        integrity: integrity(),
        candidateId: 'synthetic-idle-alert',
        sourceReference: 'ref33',
        relationshipNote: 'exact Renderfin source',
        createdAt: '2026-07-16T00:00:00.000Z',
    });
    assert.equal(report.schema, HORSE_PLANTED_IDLE_DIAGNOSTIC_SCHEMA);
    assert.equal(report.status, 'PASS');
    assert.equal(report.profile.id, 'horse.all_hooves_planted_idle.v1');
    assert.equal(report.decision.eligibleForContactConstrainedFit, true);
    assert.equal(report.decision.approvedForAnimationLibrary, false);
    assert.equal(report.decision.humanFixedCameraReviewRequired, true);
    assert.equal(report.decision.targetMeshDeformationQaRequired, true);
});

test('planted-idle report fail-closes a sliding hoof', () => {
    const report = buildIdleDiagnosticReport({
        observations: observations({ driftFoot: 'fore_right' }),
        integrity: integrity(),
        candidateId: 'synthetic-idle-slide',
        sourceReference: 'synthetic',
    });
    assert.equal(report.status, 'FAIL');
    assert.equal(report.decision.eligibleForContactConstrainedFit, false);
    assert.ok(report.qa.failures.includes('fore_right:horizontal_slide'));
});
