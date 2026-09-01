import assert from 'node:assert/strict';
import test from 'node:test';

import {
    buildTrotDiagnosticReport,
    HORSE_TROT_CONTACT_DIAGNOSTIC_SCHEMA,
} from '../diagnose_browser_horse_trot.mjs';

const FRAME_COUNT = 49;
const UNIQUE_FRAME_COUNT = 48;

function observations(centers) {
    return {
        schema: 'autorig-fitting-observations.v1',
        frame_count: FRAME_COUNT,
        fps: 30,
        tracks: Object.entries(centers).map(([foot, center]) => ({
            anchor_id: `${foot}.hoof`,
            points: Array.from({ length: FRAME_COUNT }, (_, frame) => {
                const uniqueFrame = frame === UNIQUE_FRAME_COUNT ? 0 : frame;
                const delta = Math.min(
                    (uniqueFrame - center + UNIQUE_FRAME_COUNT) % UNIQUE_FRAME_COUNT,
                    (center - uniqueFrame + UNIQUE_FRAME_COUNT) % UNIQUE_FRAME_COUNT,
                );
                return {
                    frame,
                    x: 100,
                    y: delta <= 2 ? 90 : 100,
                    visible: true,
                    confidence: 1,
                };
            }),
        })),
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

test('immutable TROT report authorizes diagonal-pair QA but never library approval', () => {
    const report = buildTrotDiagnosticReport({
        observations: observations({
            fore_left: 4,
            hind_right: 4,
            fore_right: 28,
            hind_left: 28,
        }),
        integrity: integrity(),
        candidateId: 'synthetic-trot',
        sourceReference: 'synthetic',
        relationshipNote: 'independent reference',
        createdAt: '2026-07-16T00:00:00.000Z',
    });
    assert.equal(report.schema, HORSE_TROT_CONTACT_DIAGNOSTIC_SCHEMA);
    assert.equal(report.status, 'PASS');
    assert.equal(report.profile.id, 'horse.diagonal_pair_trot.v1');
    assert.equal(report.decision.eligibleForContactConstrainedRefit, true);
    assert.equal(report.decision.approvedForAnimationLibrary, false);
    assert.equal(report.decision.humanFixedCameraReviewRequired, true);
});

test('TROT report fail-closes a lateral pace under the same immutable profile', () => {
    const report = buildTrotDiagnosticReport({
        observations: observations({
            fore_left: 4,
            hind_left: 4,
            fore_right: 28,
            hind_right: 28,
        }),
        integrity: integrity(),
        candidateId: 'synthetic-pace',
        sourceReference: 'synthetic',
        relationshipNote: null,
    });
    assert.equal(report.status, 'FAIL');
    assert.equal(report.decision.eligibleForContactConstrainedRefit, false);
    assert.ok(report.qa.failures.some((failure) => failure.endsWith(':diagonal_swing_mismatch')));
});
