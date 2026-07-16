import assert from 'node:assert/strict';
import test from 'node:test';

import {
    applyInferredHoofContacts,
    applyPinnedHoofContactSchedule,
    deriveSam2GroundEvidence,
    diagnoseHoofContacts,
    fitBrowserAnimationWithHoofContacts,
    fitBrowserAnimationWithPinnedHoofContacts,
    gateFittedWalk,
    HOOF_CONTACT_INFERENCE_CONTRACT,
    inferHoofContacts,
    validatePinnedHoofContactSchedule,
} from '../animation-fitting-hoof-contact-inference.js';

const FRAME_COUNT = 49;
const UNIQUE_FRAMES = 48;
const WIDTH = 512;
const HEIGHT = 320;
const GROUND_Y = 280;
const CONTACT_LENGTH = 26;
const TOUCHDOWNS = {
    hind_left: 42,
    fore_left: 6,
    hind_right: 18,
    fore_right: 30,
};
const X_BY_FOOT = {
    hind_left: 360,
    fore_left: 140,
    hind_right: 410,
    fore_right: 190,
};
const SOURCE_SHA = '8'.repeat(64);

function circularIndex(value, length = UNIQUE_FRAMES) {
    return ((value % length) + length) % length;
}

function isContact(foot, frame) {
    return circularIndex(frame - TOUCHDOWNS[foot]) < CONTACT_LENGTH;
}

function hoofPoint(foot, frame) {
    const uniqueFrame = frame === UNIQUE_FRAMES ? 0 : frame;
    const phase = circularIndex(uniqueFrame - TOUCHDOWNS[foot]);
    if (phase < CONTACT_LENGTH) return [X_BY_FOOT[foot], GROUND_Y];
    const swingPhase = (phase - CONTACT_LENGTH) / (UNIQUE_FRAMES - CONTACT_LENGTH);
    return [
        X_BY_FOOT[foot] + Math.sin(swingPhase * Math.PI * 2) * 9,
        GROUND_Y - Math.sin(swingPhase * Math.PI) * 36,
    ];
}

function makeTrack(anchorId, pointAtFrame) {
    return {
        id: `tap_${anchorId.replaceAll('.', '_')}`,
        anchor_id: anchorId,
        query_frame: 0,
        points: Array.from({ length: FRAME_COUNT }, (_, frame) => {
            const [x, y] = pointAtFrame(frame);
            return { frame, x, y, visible: true, confidence: 0.96 };
        }),
    };
}

function observations({ wrongOrder = false } = {}) {
    const original = { ...TOUCHDOWNS };
    if (wrongOrder) {
        TOUCHDOWNS.fore_left = 24;
        TOUCHDOWNS.hind_right = 12;
    }
    const tracks = HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.flatMap((foot) => [
        makeTrack(`${foot}.proximal`, () => [X_BY_FOOT[foot], 200]),
        makeTrack(`${foot}.joint`, (frame) => {
            const hoof = hoofPoint(foot, frame);
            return [(X_BY_FOOT[foot] + hoof[0]) / 2, 240];
        }),
        makeTrack(`${foot}.hoof`, (frame) => hoofPoint(foot, frame)),
    ]);
    Object.assign(TOUCHDOWNS, original);
    return {
        schema: HOOF_CONTACT_INFERENCE_CONTRACT.observations,
        frame_count: FRAME_COUNT,
        width: WIDTH,
        height: HEIGHT,
        fps: 30,
        tracks,
        silhouettes: Array.from({ length: FRAME_COUNT }, (_, frame) => ({ frame, path: `mask-${frame}.png` })),
        contacts: [],
        provenance: {
            source_video_sha256: SOURCE_SHA,
            tracker: { backend: HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend },
            segmenter: { backend: HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend },
        },
    };
}

function strictWalkObservations() {
    const source = observations();
    HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.forEach((foot) => {
        const hoof = source.tracks.find((track) => track.anchor_id === `${foot}.hoof`);
        const joint = source.tracks.find((track) => track.anchor_id === `${foot}.joint`);
        hoof.points.forEach((point, frame) => {
            const uniqueFrame = frame === UNIQUE_FRAMES ? 0 : frame;
            const phase = circularIndex(uniqueFrame - TOUCHDOWNS[foot]);
            const value = phase < 36
                ? [X_BY_FOOT[foot], GROUND_Y]
                : [
                    X_BY_FOOT[foot] + Math.sin(((phase - 36) / 12) * Math.PI * 2) * 9,
                    GROUND_Y - Math.sin(((phase - 36) / 12) * Math.PI) * 36,
                ];
            point.x = value[0];
            point.y = value[1];
            joint.points[frame].x = (X_BY_FOOT[foot] + value[0]) / 2;
            joint.points[frame].y = 240;
        });
    });
    return source;
}

function fillRect(data, left, top, right, bottom, value = 255) {
    for (let y = Math.max(0, top); y <= Math.min(HEIGHT - 1, bottom); y += 1) {
        for (let x = Math.max(0, left); x <= Math.min(WIDTH - 1, right); x += 1) {
            data[y * WIDTH + x] = value;
        }
    }
}

function masks(source = observations()) {
    const tracks = new Map(source.tracks.map((track) => [track.anchor_id, track.points]));
    return Array.from({ length: FRAME_COUNT }, (_, frame) => {
        const data = new Uint8Array(WIDTH * HEIGHT);
        fillRect(data, 90, 105, 440, 205);
        HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.forEach((foot) => {
            const point = tracks.get(`${foot}.hoof`)[frame];
            fillRect(data, Math.round(point.x) - 4, 200, Math.round(point.x) + 4, Math.round(point.y) + 4);
        });
        return { frame, width: WIDTH, height: HEIGHT, channels: 1, data };
    });
}

function evidence(source = observations()) {
    return deriveSam2GroundEvidence({ observations: source, masks: masks(source) });
}

function skeleton() {
    const limbs = {};
    HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.forEach((foot) => {
        const x = X_BY_FOOT[foot];
        limbs[foot] = {
            joints: [
                {
                    bone: `${foot}_upper`,
                    restStart: [x, 200],
                    restEnd: [x, 240],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: -2.8,
                    maxAngle: 2.8,
                },
                {
                    bone: `${foot}_lower`,
                    restStart: [x, 240],
                    restEnd: [x, 280],
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
        };
    });
    return { schema: 'autorig-browser-fitting-skeleton.v1', rigType: 'HORSE_2', limbs };
}

function refitPins(source) {
    source.provenance.bundle_sha256 = '1'.repeat(64);
    source.provenance.immutable_manifest_sha256 = '2'.repeat(64);
    return {
        inputManifestSha256: '3'.repeat(64),
        diagnosticSha256: '4'.repeat(64),
        bridgeReportSha256: '5'.repeat(64),
        initialFitSummarySha256: '6'.repeat(64),
        observationsSha256: '7'.repeat(64),
        fittingBundleSha256: source.provenance.bundle_sha256,
        immutableManifestSha256: source.provenance.immutable_manifest_sha256,
        sourceVideoSha256: source.provenance.source_video_sha256,
        sourceModelSha256: 'a'.repeat(64),
        sourceSkeletonSha256: 'b'.repeat(64),
    };
}

test('SAM2 masks bind chronological ground evidence to exact left/right hoof tracks', () => {
    const source = observations();
    const result = evidence(source);
    assert.equal(result.schema, HOOF_CONTACT_INFERENCE_CONTRACT.groundEvidence);
    assert.deepEqual(result.foot_order, ['hind_left', 'fore_left', 'hind_right', 'fore_right']);
    assert.equal(result.frames.length, FRAME_COUNT);
    assert.equal(result.frames[0].hooves.hind_left.anchorId, 'hind_left.hoof');
    assert.deepEqual(result.frames[0].hooves.hind_left.sourcePoint, [360, 280]);
    assert.ok(result.frames[0].bbox.height > 150);
    assert.equal(source.contacts.length, 0, 'source observations are not mutated');
});

test('loop-aware hysteresis recovers one lateral-sequence stance per hoof', () => {
    const source = observations();
    const result = inferHoofContacts({ observations: source, groundEvidence: evidence(source) });
    assert.equal(result.status, 'PASS');
    assert.equal(result.uniqueFrameCount, UNIQUE_FRAMES);
    assert.deepEqual(result.inferredTouchdownOrder, ['fore_left', 'hind_right', 'fore_right', 'hind_left']);
    HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.forEach((foot) => {
        assert.equal(result.feet[foot].failures.length, 0);
        assert.ok(result.feet[foot].contactFrames.length >= 20);
        assert.ok(result.feet[foot].contactFrames.length <= 32);
        assert.ok(result.feet[foot].slide.maximumStepPx < 1e-8);
    });
    const hind = result.contacts.find((contact) => contact.anchor_id === 'hind_left.hoof');
    assert.ok(hind.frames.includes(0));
    assert.ok(hind.frames.includes(48), 'duplicated loop endpoint is pinned when frame zero is contact');
    assert.ok(result.qa.support.minimum >= 2);
    assert.ok(result.qa.support.maximum <= 3);
});

test('inferred contacts are mapped into the existing pure browser solver', () => {
    const source = observations();
    const result = fitBrowserAnimationWithHoofContacts({
        skeleton: skeleton(),
        observations: source,
        groundEvidence: evidence(source),
        fitOptions: { loop: true, smoothingRadius: 0, jointAttraction: 0, iterations: 80, tolerance: 1e-7 },
    });
    assert.equal(result.fitted.schema, 'autorig-browser-fitted-animation.v1');
    assert.equal(result.schedule.status, 'PASS');
    assert.equal(result.gaitQa.status, 'PASS');
    assert.ok(result.fitted.qa.maximumContactSlidePx < 1e-5);
    assert.equal(source.contacts.length, 0, 'contact mapping is immutable');
    assert.equal(result.observations.contacts.length, 4);
});

test('a one-frame weak-evidence dip does not split stance because hysteresis is circular', () => {
    const source = observations();
    const hoof = source.tracks.find((track) => track.anchor_id === 'hind_left.hoof');
    hoof.points[8].y -= 4;
    const result = inferHoofContacts({ observations: source, groundEvidence: evidence(source) });
    assert.equal(result.feet.hind_left.failures.length, 0);
    assert.equal(result.feet.hind_left.contactFrames.includes(8), true);
    assert.equal(result.status, 'PASS');
});

test('wrong gait order is diagnosed and rejected before solver invocation', () => {
    const source = observations({ wrongOrder: true });
    const diagnostic = diagnoseHoofContacts({ observations: source, groundEvidence: evidence(source) });
    assert.equal(diagnostic.status, 'FAIL');
    assert.ok(diagnostic.qa.failures.includes('walk_footfall_order'));
    assert.throws(
        () => inferHoofContacts({ observations: source, groundEvidence: evidence(source) }),
        /walk_footfall_order/,
    );
});

test('a one-hoof support phase is rejected even when every hoof has a valid stance interval', () => {
    const source = observations();
    HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.forEach((foot) => {
        const hoof = source.tracks.find((track) => track.anchor_id === `${foot}.hoof`);
        hoof.points.forEach((point, frame) => {
            const uniqueFrame = frame === UNIQUE_FRAMES ? 0 : frame;
            const phase = circularIndex(uniqueFrame - TOUCHDOWNS[foot]);
            // Shorten each otherwise-valid stance from 26 to 19 frames.  With
            // quarter-cycle touchdown spacing this creates real one-hoof
            // support windows without changing the lateral touchdown order.
            if (phase >= 19 && phase < CONTACT_LENGTH) {
                point.x += Math.sin(((phase - 19) / 7) * Math.PI) * 5;
                point.y -= 32;
            }
        });
    });
    const diagnostic = diagnoseHoofContacts({ observations: source, groundEvidence: evidence(source) });
    assert.equal(diagnostic.status, 'FAIL');
    assert.ok(diagnostic.qa.support.minimum < 2);
    assert.ok(diagnostic.qa.failures.includes('walk_insufficient_support'));
});

test('visibility, SAM2 provenance and exact track binding fail closed', () => {
    const hidden = observations();
    hidden.tracks.find((track) => track.anchor_id === 'fore_right.hoof').points
        .slice(0, 8).forEach((point) => { point.visible = false; point.confidence = 0; });
    const hiddenDiagnostic = diagnoseHoofContacts({ observations: hidden, groundEvidence: evidence(hidden) });
    assert.equal(hiddenDiagnostic.status, 'FAIL');
    assert.ok(hiddenDiagnostic.qa.failures.includes('fore_right:insufficient_visibility'));
    assert.ok(hiddenDiagnostic.qa.failures.includes('fore_right:occlusion_gap_too_long'));
    assert.throws(
        () => inferHoofContacts({ observations: hidden, groundEvidence: evidence(hidden) }),
        /fore_right:insufficient_visibility/,
    );

    const source = observations();
    const wrongPin = evidence(source);
    wrongPin.provenance.sourceVideoSha256 = '9'.repeat(64);
    assert.throws(
        () => diagnoseHoofContacts({ observations: source, groundEvidence: wrongPin }),
        /source-video pin/,
    );

    const remapped = evidence(source);
    remapped.frames[0].hooves.fore_left.anchorId = 'fore_right.hoof';
    assert.throws(
        () => diagnoseHoofContacts({ observations: source, groundEvidence: remapped }),
        /remapped fore_left/,
    );

    const wrongTracker = observations();
    wrongTracker.provenance.tracker.backend = 'untrusted-tracker';
    assert.throws(
        () => deriveSam2GroundEvidence({ observations: wrongTracker, masks: masks(wrongTracker) }),
        /tracker backend must be google-deepmind-tapnextpp-online/,
    );
});

test('observed and post-solver foot-slide gates reject independently', () => {
    const source = observations();
    const sliding = source.tracks.find((track) => track.anchor_id === 'hind_left.hoof');
    sliding.points.forEach((point, frame) => {
        const uniqueFrame = frame === UNIQUE_FRAMES ? 0 : frame;
        if (isContact('hind_left', uniqueFrame)) {
            point.x += circularIndex(uniqueFrame - TOUCHDOWNS.hind_left) * 5;
        }
    });
    const diagnostic = diagnoseHoofContacts({
        observations: source,
        groundEvidence: evidence(source),
        options: {
            contactPlanarSpeedRatioPerFrame: 0.05,
            swingPlanarSpeedRatioPerFrame: 0.10,
            maximumObservedContactStepRatio: 0.02,
            maximumObservedContactP95StepRatio: 0.015,
        },
    });
    assert.equal(diagnostic.status, 'FAIL');
    assert.ok(diagnostic.qa.failures.some((failure) => failure.startsWith('hind_left:observed_contact_')));

    const stable = observations();
    const schedule = inferHoofContacts({ observations: stable, groundEvidence: evidence(stable) });
    const gate = gateFittedWalk({ fitted: {
        frameCount: schedule.frameCount,
        fps: schedule.fps,
        loop: schedule.loop,
        qa: { maximumContactSlidePx: 8 },
    }, schedule });
    assert.equal(gate.status, 'FAIL');
    assert.deepEqual(gate.failures, ['fitted_contact_slide']);
});

test('walk inference rejects non-loop clips and mismatched fitted schedule contracts', () => {
    const source = observations();
    const groundEvidence = evidence(source);
    assert.throws(
        () => diagnoseHoofContacts({ observations: source, groundEvidence, options: { loop: false } }),
        /supports loop=true walk clips only/,
    );
    assert.throws(
        () => fitBrowserAnimationWithHoofContacts({
            skeleton: skeleton(),
            observations: source,
            groundEvidence,
            contactOptions: { loop: true },
            fitOptions: { loop: false },
        }),
        /contactOptions\.loop and fitOptions\.loop must match/,
    );

    const schedule = inferHoofContacts({ observations: source, groundEvidence });
    const fitted = {
        frameCount: schedule.frameCount,
        fps: schedule.fps,
        loop: schedule.loop,
        qa: { maximumContactSlidePx: 0 },
    };
    assert.throws(
        () => gateFittedWalk({ fitted: { ...fitted, frameCount: fitted.frameCount - 1 }, schedule }),
        /frameCount does not match/,
    );
    assert.throws(
        () => gateFittedWalk({ fitted: { ...fitted, fps: fitted.fps + 1 }, schedule }),
        /fps does not match/,
    );
    assert.throws(
        () => gateFittedWalk({ fitted: { ...fitted, loop: false }, schedule }),
        /loop mode does not match/,
    );
});

test('contact application preserves unrelated observation contacts', () => {
    const source = observations();
    source.contacts.push({ anchor_id: 'tail.tip', frames: [4, 5], weight: 0.5 });
    const applied = applyInferredHoofContacts({ observations: source, groundEvidence: evidence(source) });
    assert.equal(applied.observations.contacts.length, 5);
    assert.deepEqual(applied.observations.contacts[0], source.contacts[0]);
});

test('pinned PASS schedule maps exactly four limbs with immutable browser-only provenance', () => {
    const source = strictWalkObservations();
    const schedule = inferHoofContacts({
        observations: source,
        groundEvidence: evidence(source),
        options: { minimumSupportFeet: 3 },
    });
    const pins = refitPins(source);
    const validated = validatePinnedHoofContactSchedule({ observations: source, schedule });
    assert.equal(validated.contacts.length, 4);
    assert.ok(validated.support.minimum >= 3);
    const applied = applyPinnedHoofContactSchedule({ observations: source, schedule, pins });
    assert.equal(source.contacts.length, 0, 'pinned schedule application is immutable');
    assert.equal(applied.observations.contacts.length, 4);
    assert.deepEqual(
        applied.observations.contacts.map((contact) => contact.anchor_id).sort(),
        HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.map((foot) => `${foot}.hoof`).sort(),
    );
    assert.deepEqual(applied.observations.provenance.browser_hoof_contacts, {
        schema: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitProvenance,
        source: 'immutable_pass_diagnostic',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        footOrder: [...HOOF_CONTACT_INFERENCE_CONTRACT.footOrder],
        support: validated.support,
        ...pins,
    });
});

test('pinned schedule rejects missing limbs, forged support and provenance/hash mismatch', () => {
    const source = strictWalkObservations();
    const schedule = inferHoofContacts({
        observations: source,
        groundEvidence: evidence(source),
        options: { minimumSupportFeet: 3 },
    });
    const pins = refitPins(source);

    const missing = structuredClone(schedule);
    missing.contacts.pop();
    assert.throws(
        () => validatePinnedHoofContactSchedule({ observations: source, schedule: missing }),
        /exactly four limb contacts/,
    );

    const forgedSupport = structuredClone(schedule);
    forgedSupport.qa.support.byFrame[0] = forgedSupport.qa.support.byFrame[0] === 0 ? 4 : 0;
    assert.throws(
        () => validatePinnedHoofContactSchedule({ observations: source, schedule: forgedSupport }),
        /support timeline is inconsistent/,
    );

    const weakContact = structuredClone(schedule);
    weakContact.contacts[0].weight = 0.000001;
    assert.throws(
        () => validatePinnedHoofContactSchedule({ observations: source, schedule: weakContact }),
        /code-owned contact weight/,
    );

    const relaxedThresholds = structuredClone(schedule);
    relaxedThresholds.qa.thresholds.maximumFittedContactSlideRatio = 1;
    assert.throws(
        () => validatePinnedHoofContactSchedule({ observations: source, schedule: relaxedThresholds }),
        /code-owned Horse contact-refit contract/,
    );

    const relaxedDuty = structuredClone(schedule);
    relaxedDuty.qa.thresholds.minimumDutyFactor = 0.01;
    assert.throws(
        () => validatePinnedHoofContactSchedule({ observations: source, schedule: relaxedDuty }),
        /code-owned Horse contact-refit contract/,
    );

    assert.throws(
        () => applyPinnedHoofContactSchedule({
            observations: source,
            schedule,
            pins: { ...pins, diagnosticSha256: 'not-a-hash' },
        }),
        /pins\.diagnosticSha256 must be a lowercase SHA-256/,
    );
    assert.throws(
        () => applyPinnedHoofContactSchedule({
            observations: source,
            schedule,
            pins: { ...pins, sourceVideoSha256: 'f'.repeat(64) },
        }),
        /source-video SHA-256 does not match observations provenance/,
    );
});

test('pinned refit rejects a self-consistently repinned two-foot support gait before solver invocation', () => {
    const source = observations();
    const schedule = inferHoofContacts({ observations: source, groundEvidence: evidence(source) });
    const pins = refitPins(source);
    assert.throws(
        () => fitBrowserAnimationWithPinnedHoofContacts({
            skeleton: skeleton(),
            observations: source,
            schedule,
            pins,
            fitOptions: { loop: true, smoothingRadius: 0, iterations: 80, tolerance: 1e-7 },
        }),
        /code-owned Horse contact-refit contract/,
    );
});

test('pinned strict walk reruns the pure browser solver and passes fitted contact-slide QA', () => {
    const source = strictWalkObservations();
    const schedule = inferHoofContacts({
        observations: source,
        groundEvidence: evidence(source),
        options: { minimumSupportFeet: 3 },
    });
    const pins = refitPins(source);
    const result = fitBrowserAnimationWithPinnedHoofContacts({
        skeleton: skeleton(),
        observations: source,
        schedule,
        pins,
        fitOptions: { loop: true, smoothingRadius: 0, jointAttraction: 0, iterations: 80, tolerance: 1e-7 },
    });
    assert.equal(result.gaitQa.accepted, true);
    assert.equal(result.gaitQa.simultaneousSwingFrameCount, 0);
    assert.equal(result.fittedWalkQa.status, 'PASS');
    assert.equal(result.observations.contacts.length, 4);
    assert.deepEqual(result.runtime, { browserOnly: true, blenderUsed: false, mixerUsed: false });
});

test('pinned refit rejects solver-produced contact slide even when schedule and semantic gait PASS', () => {
    const source = strictWalkObservations();
    const schedule = inferHoofContacts({
        observations: source,
        groundEvidence: evidence(source),
        options: { minimumSupportFeet: 3 },
    });
    const pins = refitPins(source);
    const proximal = source.tracks.find((track) => track.anchor_id === 'fore_left.proximal');
    const stance = new Set(schedule.contacts.find((contact) => contact.anchor_id === 'fore_left.hoof').frames);
    proximal.points.forEach((point, frame) => {
        if (stance.has(frame)) point.x += 180 + (frame === UNIQUE_FRAMES ? 0 : frame * 3);
    });
    assert.throws(
        () => fitBrowserAnimationWithPinnedHoofContacts({
            skeleton: skeleton(),
            observations: source,
            schedule,
            pins,
            fitOptions: { loop: true, smoothingRadius: 0, jointAttraction: 0, iterations: 80, tolerance: 1e-7 },
        }),
        /fitted_contact_slide/,
    );
});
