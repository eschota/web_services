import assert from 'node:assert/strict';
import test from 'node:test';

import {
    fitRgbObservationsInBrowser,
    prepareRgbObservationsForBrowser,
    RGB_OBSERVATION_BRIDGE_CONTRACT,
} from '../animation-fitting-rgb-observation-bridge.js';

const LABELS = ['fore_left', 'fore_right', 'hind_left', 'hind_right'];
const BUNDLE_SHA = '1'.repeat(64);
const MANIFEST_SHA = '2'.repeat(64);

function skeleton() {
    const limbs = {};
    LABELS.forEach((label, index) => {
        const x = index * 3;
        const sourceBoneChain = [`${label}_upper`, `${label}_lower`, `${label}_hoof`];
        limbs[label] = {
            joints: [
                {
                    bone: sourceBoneChain[0],
                    restStart: [x, 0],
                    restEnd: [x, 1],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: -1.5,
                    maxAngle: 1.5,
                },
                {
                    bone: sourceBoneChain[1],
                    restStart: [x, 1],
                    restEnd: [x, 2],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: -1.5,
                    maxAngle: 1.5,
                },
            ],
            proximalTrack: `${label}.proximal`,
            jointTrack: `${label}.joint`,
            hoofTrack: `${label}.hoof`,
            trackedJointIndex: 1,
            sourceBoneChain,
            terminalBone: sourceBoneChain.at(-1),
        };
    });
    return {
        schema: RGB_OBSERVATION_BRIDGE_CONTRACT.skeleton,
        rigType: 'HORSE_2',
        limbs,
        projection: { outputResolution: [768, 448] },
    };
}

function canonicalObservations(frameCount = 5) {
    const tracks = [];
    let vertex = 100;
    LABELS.forEach((label, labelIndex) => {
        const x = labelIndex * 3;
        const roles = [
            [`${label}_upper`, [x, 0]],
            [`${label}_lower`, [x, 1]],
            [`${label}_hoof`, [x, 2]],
        ];
        roles.forEach(([bone, rest], roleIndex) => {
            const anchorId = `${bone}:${vertex++}`;
            tracks.push({
                id: `tap_${label}_${roleIndex}`,
                anchor_id: anchorId,
                query_frame: 0,
                points: Array.from({ length: frameCount }, (_, frame) => ({
                    frame,
                    x: rest[0] + (roleIndex === 2 ? Math.sin(frame) * 0.2 : 0),
                    y: rest[1],
                    visible: true,
                    confidence: 0.95,
                })),
            });
        });
    });
    tracks.push({
        id: 'tap_unselected_spine',
        anchor_id: 'spine:999',
        query_frame: 0,
        points: Array.from({ length: frameCount }, (_, frame) => ({
            frame, x: 5, y: 5, visible: true, confidence: 1,
        })),
    });
    return {
        schema: RGB_OBSERVATION_BRIDGE_CONTRACT.observations,
        frame_count: frameCount,
        width: 768,
        height: 448,
        fps: 30,
        tracks,
        contacts: [{
            anchor_id: tracks.find((track) => track.id === 'tap_hind_right_2').anchor_id,
            frames: [1, 2],
            ground_height: 2,
            weight: 1,
        }],
        provenance: {
            runtime: 'autorig-official-animal-tracking.v1',
            bundle_sha256: BUNDLE_SHA,
            immutable_manifest_sha256: MANIFEST_SHA,
            tracker: { backend: RGB_OBSERVATION_BRIDGE_CONTRACT.trackerBackend },
        },
    };
}

function cameraContract() {
    return {
        outputResolution: [768, 448],
        bundleSha256: BUNDLE_SHA,
        immutableManifestSha256: MANIFEST_SHA,
    };
}

test('TAPNext++ rig anchors map deterministically to the 12 browser semantic tracks', () => {
    const source = canonicalObservations();
    const prepared = prepareRgbObservationsForBrowser({
        observations: source,
        skeleton: skeleton(),
        cameraContract: cameraContract(),
    });
    assert.equal(prepared.schema, 'autorig-fitting-observations.v1');
    assert.equal(prepared.tracks.length, 12);
    assert.deepEqual(prepared.tracks.map((track) => track.anchor_id), LABELS.flatMap((label) => [
        `${label}.proximal`, `${label}.joint`, `${label}.hoof`,
    ]));
    assert.equal(prepared.contacts.length, 1);
    assert.equal(prepared.contacts[0].anchor_id, 'hind_right.hoof');
    assert.equal(prepared.provenance.browser_rgb_bridge.mappings.length, 12);
    assert.equal(source.tracks[0].anchor_id.endsWith(':100'), true, 'source is not mutated');
});

test('prepared RGB observations are accepted by the pure browser fitting solver', () => {
    const fitted = fitRgbObservationsInBrowser({
        observations: canonicalObservations(),
        skeleton: skeleton(),
        cameraContract: cameraContract(),
        options: { loop: true, smoothingRadius: 0 },
    });
    assert.equal(fitted.schema, 'autorig-browser-fitted-animation.v1');
    assert.equal(fitted.frameCount, 5);
    assert.equal(fitted.tracks.length, 8);
    assert.equal(fitted.qa.loopEndpointError, 0);
});

test('frame IDs must be a complete zero-based sequence for every RGB track', () => {
    const missing = canonicalObservations();
    missing.tracks[0].points.pop();
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: missing,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /points must contain exactly 5 frames/,
    );

    const repeated = canonicalObservations();
    repeated.tracks[0].points[4].frame = 3;
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: repeated,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /repeats frame 3/,
    );
});

test('camera resolution and immutable bundle pins fail closed on mismatch', () => {
    const wrongResolution = canonicalObservations();
    wrongResolution.width = 512;
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: wrongResolution,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /observation camera 512x448 does not match adapter output 768x448/,
    );

    const wrongBundle = canonicalObservations();
    wrongBundle.provenance.bundle_sha256 = '3'.repeat(64);
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: wrongBundle,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /bundle SHA-256 does not match the pinned camera contract/,
    );
});

test('missing, ambiguous, malformed and unusable RGB anchors are rejected', () => {
    const missing = canonicalObservations();
    missing.tracks = missing.tracks.filter((track) => !track.anchor_id.startsWith('fore_left_hoof:'));
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: missing,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /fore_left\.hoof requires exactly one RGB anchor.*found 0/,
    );

    const ambiguous = canonicalObservations();
    const duplicateBone = structuredClone(ambiguous.tracks[0]);
    duplicateBone.id = 'tap_duplicate_bone';
    duplicateBone.anchor_id = 'fore_left_upper:777';
    ambiguous.tracks.push(duplicateBone);
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: ambiguous,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /fore_left\.proximal requires exactly one RGB anchor.*found 2/,
    );

    const malformed = canonicalObservations();
    malformed.tracks[0].anchor_id = 'fore_left_upper';
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: malformed,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /immutable bone:vertex anchor format/,
    );

    const invisible = canonicalObservations();
    invisible.tracks.find((track) => track.id === 'tap_fore_left_2').points
        .forEach((point) => { point.visible = false; });
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: invisible,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /is not visible on its query frame/,
    );
});

test('non-TAPNext tracker provenance is never silently accepted as RGB fitting input', () => {
    const input = canonicalObservations();
    input.provenance.tracker.backend = 'synthetic-semantic-color-tracker';
    assert.throws(
        () => prepareRgbObservationsForBrowser({
            observations: input,
            skeleton: skeleton(),
            cameraContract: cameraContract(),
        }),
        /tracker backend must be google-deepmind-tapnextpp-online/,
    );
});
