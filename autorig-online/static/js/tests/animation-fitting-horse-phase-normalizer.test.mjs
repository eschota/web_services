import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { assessHorseWalkGait } from '../animation-fitting-semantic-tracker.js';
import { normalizeHorseWalkPhases } from '../animation-fitting-horse-phase-normalizer.js';

const ORDER = ['hind_left', 'fore_left', 'hind_right', 'fore_right'];
const FRAME_COUNT = 49;

function cyclicWindow(center, radius = 2) {
    return new Set(Array.from({ length: radius * 2 + 1 }, (_, index) => (
        center + index - radius + FRAME_COUNT
    ) % FRAME_COUNT));
}

function observations(centers, options = {}) {
    const tracks = [];
    ORDER.forEach((label, labelIndex) => {
        const swing = options.staticLabel === label ? new Set() : cyclicWindow(centers[label]);
        const secondary = options.secondaryCenters?.[label];
        if (secondary) {
            for (const frame of cyclicWindow(secondary.center, secondary.radius ?? 1)) swing.add(frame);
        }
        for (const frame of options.secondaryFrames?.[label] || []) swing.add(frame);
        ['proximal', 'joint', 'hoof'].forEach((role, roleIndex) => {
            tracks.push({
                id: `semantic:${label}.${role}`,
                anchor_id: `${label}.${role}`,
                query_frame: 0,
                points: Array.from({ length: FRAME_COUNT }, (_, frame) => {
                    const lifted = swing.has(frame);
                    const hidden = options.missingLabel === label
                        && options.missingRole === role
                        && frame < (options.missingFrames || 4);
                    const stanceY = role === 'hoof' ? 100 : (role === 'joint' ? 72 : 45);
                    return {
                        frame,
                        x: labelIndex * 100 + frame + roleIndex * 0.1,
                        y: stanceY - (lifted ? (role === 'hoof' ? 10 : 4) : 0),
                        visible: !hidden,
                        confidence: hidden ? 0 : 0.75 + frame / 1000,
                        sample_id: `${label}:${role}:${frame}`,
                    };
                }),
            });
        });
    });
    return {
        schema: 'autorig-fitting-observations.v1',
        frame_count: FRAME_COUNT,
        width: 512,
        height: 320,
        fps: 30,
        tracks,
        silhouettes: [],
        depth: [],
        contacts: [{ anchor_id: 'hind_left.hoof', frames: [0], ground_height: -999, weight: 7 }],
        provenance: { source: 'unit-test' },
    };
}

function gaitQa(value) {
    return assessHorseWalkGait(value, {
        expectedOrder: ORDER,
        maximumSimultaneousSwingFrames: 0,
    });
}

function track(value, anchorId) {
    return value.tracks.find((item) => item.anchor_id === anchorId);
}

function extrema(points, field) {
    const values = points.map((point) => point[field]);
    return [Math.min(...values), Math.max(...values)];
}

function deepFreeze(value) {
    if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
    Object.getOwnPropertyNames(value).forEach((key) => deepFreeze(value[key]));
    return Object.freeze(value);
}

test('paired trot phases are repaired into the requested non-overlapping four-beat walk', () => {
    const input = observations({
        hind_left: 4,
        fore_left: 4,
        hind_right: 28,
        fore_right: 28,
    }, {
        secondaryFrames: {
            hind_right: [38, 39, 40, 41],
            fore_right: [38, 39, 40, 41, 42, 43, 44, 45],
        },
    });
    assert.equal(gaitQa(input).accepted, false);

    const result = normalizeHorseWalkPhases(input, {
        expectedOrder: ORDER,
        gaitQaOptions: { maximumSimultaneousSwingFrames: 0 },
        contactHeightTolerancePx: 2,
        contactVelocityTolerancePx: 2,
    });
    assert.equal(result.schema, 'autorig-horse-walk-phase-normalization.v1');
    assert.equal(result.rawGaitQa.accepted, false);
    assert.equal(result.normalizedGaitQa.accepted, true);
    assert.equal(result.normalizedGaitQa.simultaneousSwingFrameCount, 0);
    assert.equal(result.provenance.mode, 'circular_rephase');
    assert.deepEqual(result.normalizedGaitQa.expectedOrder, ORDER);
    assert.ok(result.normalizedGaitQa.phaseGaps.every((gap) => gap > 0.2 && gap < 0.3));
    assert.equal(result.provenance.limbs.hind_right.discardedSecondarySwingFrames.length, 4);
    // The gait threshold is quantile-derived, so the eight-frame synthetic lift
    // contributes five frames to the detector's secondary swing window.
    assert.equal(result.provenance.limbs.fore_right.discardedSecondarySwingFrames.length, 5);
    ORDER.forEach((label) => {
        assert.equal(result.normalizedGaitQa.limbs[label].secondarySwingFrames, 0);
        const mapping = result.provenance.limbs[label].sourceFrameByDestination;
        for (const role of ['proximal', 'joint', 'hoof']) {
            const before = track(input, `${label}.${role}`);
            const after = track(result.observations, `${label}.${role}`);
            const sourceIds = new Set(before.points.map((point) => point.sample_id));
            assert.ok(after.points.every((point) => sourceIds.has(point.sample_id)));
            after.points.forEach((point, frame) => {
                assert.equal(point.sample_id, before.points[mapping[frame]].sample_id);
                assert.equal(point.confidence, before.points[mapping[frame]].confidence);
                assert.equal(point.visible, before.points[mapping[frame]].visible);
            });
            const [beforeMinX, beforeMaxX] = extrema(before.points, 'x');
            const [afterMinX, afterMaxX] = extrema(after.points, 'x');
            const [beforeMinY, beforeMaxY] = extrema(before.points, 'y');
            const [afterMinY, afterMaxY] = extrema(after.points, 'y');
            assert.ok(afterMinX >= beforeMinX && afterMaxX <= beforeMaxX);
            assert.ok(afterMinY >= beforeMinY && afterMaxY <= beforeMaxY);
        }
    });
    assert.equal(result.provenance.invariants.fabricatedSpatialExtrema, false);
    assert.equal(result.provenance.invariants.visibilityConfidencePreserved, true);
    assert.equal(result.observations.contacts.length, 4);
    assert.ok(result.observations.contacts.every((contact) => contact.ground_height === 100));
    assert.ok(result.observations.contacts.every((contact) => contact.weight === 1));
});

test('an already valid walk passes through unchanged and normalized observations are idempotent', () => {
    const input = observations({
        hind_left: 1,
        fore_left: 13,
        hind_right: 26,
        fore_right: 38,
    });
    assert.equal(gaitQa(input).accepted, true);
    const first = normalizeHorseWalkPhases(input, {
        expectedOrder: ORDER,
        gaitQaOptions: { maximumSimultaneousSwingFrames: 0 },
    });
    assert.equal(first.provenance.mode, 'passthrough');
    assert.deepEqual(first.observations, input);
    const second = normalizeHorseWalkPhases(first.observations, {
        expectedOrder: ORDER,
        gaitQaOptions: { maximumSimultaneousSwingFrames: 0 },
    });
    assert.deepEqual(second.observations, first.observations);
    assert.deepEqual(second.normalizedGaitQa, first.normalizedGaitQa);
});

test('missing identity visibility and static swing templates fail closed', () => {
    const centers = { hind_left: 4, fore_left: 4, hind_right: 28, fore_right: 28 };
    assert.throws(
        () => normalizeHorseWalkPhases(observations(centers, {
            missingLabel: 'hind_right',
            missingRole: 'joint',
            missingFrames: 4,
        })),
        /hind_right\.joint visibility .* below 0\.950/,
    );
    assert.throws(
        () => normalizeHorseWalkPhases(observations(centers, { staticLabel: 'fore_right' })),
        /fore_right has no meaningful swing lift template/,
    );
    const wrongLength = observations(centers);
    wrongLength.frame_count = 48;
    assert.throws(
        () => normalizeHorseWalkPhases(wrongLength),
        /requires exactly 49 frames/,
    );
});

test('normalization is deterministic and never mutates a deeply frozen input', () => {
    const input = observations({
        hind_left: 4,
        fore_left: 4,
        hind_right: 28,
        fore_right: 28,
    });
    const snapshot = structuredClone(input);
    deepFreeze(input);
    const first = normalizeHorseWalkPhases(input, {
        expectedOrder: ORDER,
        gaitQaOptions: { maximumSimultaneousSwingFrames: 0 },
    });
    const second = normalizeHorseWalkPhases(input, {
        expectedOrder: ORDER,
        gaitQaOptions: { maximumSimultaneousSwingFrames: 0 },
    });
    assert.deepEqual(first, second);
    assert.deepEqual(input, snapshot);
    const repeated = normalizeHorseWalkPhases(first.observations, {
        expectedOrder: ORDER,
        gaitQaOptions: { maximumSimultaneousSwingFrames: 0 },
    });
    assert.deepEqual(repeated.observations, first.observations);
});

test('task semantic entrypoint keeps normalization behind an explicit opt-in after raw gait QA', async () => {
    const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');
    assert.match(taskHtml, /from '\/static\/js\/animation-fitting-horse-phase-normalizer\.js\?v=1'/);
    assert.match(taskHtml, /enableHorseWalkPhaseNormalization === true/);
    assert.match(taskHtml, /enable_horse_walk_phase_normalization_bool === true/);
    const rawQa = taskHtml.indexOf('rawGaitQa = isWalk ? assessHorseWalkGait');
    const optIn = taskHtml.indexOf('enableHorseWalkPhaseNormalization === true', rawQa);
    const normalize = taskHtml.indexOf('normalizeHorseWalkPhases(observations', optIn);
    const solve = taskHtml.indexOf('fitBrowserAnimation({', normalize);
    assert.ok(rawQa > 0 && optIn > rawQa && normalize > optIn && solve > normalize);
    assert.match(taskHtml, /if \(rawGaitQa && !rawGaitQa\.accepted && !allowPhaseNormalization\)/);
    assert.match(taskHtml, /opts\.maximumContactSlidePx\s*\?\? opts\.max_contact_slide_px\s*\?\? 3/);
    assert.match(taskHtml, /maximumContactSlideLimitPx < 0[\s\S]*maximumContactSlideLimitPx > 10/);
    const contactSlideLimit = taskHtml.indexOf('const maximumContactSlideLimitPx', normalize);
    const numericQa = taskHtml.indexOf('const numericAccepted', solve);
    const contactSlideGate = taskHtml.indexOf(
        'Number(qa.maximumContactSlidePx) <= maximumContactSlideLimitPx',
        numericQa,
    );
    assert.ok(contactSlideLimit > normalize && contactSlideLimit < solve);
    assert.ok(numericQa > solve && contactSlideGate > numericQa);
});
