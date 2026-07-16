import assert from 'node:assert/strict';
import test from 'node:test';

import {
    assessHorseTrotGait,
    assessHorseWalkGait,
    buildSemanticObservations,
    extractSemanticFrame,
    linearChannelToSrgbByte,
    normalizeSemanticPalette,
    srgbByteToLinear,
} from '../animation-fitting-semantic-tracker.js';

const PALETTE = {
    fore_left: [0.0, 0.85, 1.0],
    fore_right: [0.12, 0.22, 1.0],
    hind_left: [1.0, 0.72, 0.02],
    hind_right: [1.0, 0.08, 0.55],
};

function frame(width = 40, height = 30) {
    const data = new Uint8ClampedArray(width * height * 4);
    for (let index = 0; index < width * height; index += 1) {
        data[index * 4] = 150;
        data[index * 4 + 1] = 150;
        data[index * 4 + 2] = 150;
        data[index * 4 + 3] = 255;
    }
    return { width, height, data };
}

function paintRect(target, x0, y0, x1, y1, linearColor, perturb = 0) {
    const color = linearColor.map((channel, index) => Math.max(
        0,
        Math.min(255, linearChannelToSrgbByte(channel) + (index - 1) * perturb),
    ));
    for (let y = y0; y <= y1; y += 1) {
        for (let x = x0; x <= x1; x += 1) {
            const offset = (y * target.width + x) * 4;
            target.data[offset] = color[0];
            target.data[offset + 1] = color[1];
            target.data[offset + 2] = color[2];
            target.data[offset + 3] = 255;
        }
    }
}

function semanticFrame(offsets = {}, perturb = 0) {
    const result = frame();
    const xByLabel = {
        fore_left: 7,
        fore_right: 15,
        hind_left: 25,
        hind_right: 33,
    };
    Object.entries(xByLabel).forEach(([label, baseX]) => {
        const offset = offsets[label] || { x: 0, y: 0 };
        paintRect(
            result,
            baseX + offset.x,
            7 + offset.y,
            baseX + 3 + offset.x,
            26 + offset.y,
            PALETTE[label],
            perturb,
        );
    });
    return result;
}

test('linear RGB conversion round-trips semantic palette channels', () => {
    for (const color of Object.values(PALETTE)) {
        color.forEach((channel) => {
            const restored = srgbByteToLinear(linearChannelToSrgbByte(channel));
            assert.ok(Math.abs(restored - channel) < 0.005);
        });
    }
});

test('palette validation fails closed when one limb identity is absent', () => {
    assert.throws(
        () => normalizeSemanticPalette({ ...PALETTE, hind_right: undefined }),
        /linear RGB triplet/,
    );
    assert.throws(
        () => normalizeSemanticPalette({ fore_left: PALETTE.fore_left }),
        /missing fore_right/,
    );
});

test('semantic extraction keeps all four limb identities stable under colour perturbation', () => {
    const extracted = extractSemanticFrame(semanticFrame({}, 2), PALETTE, {
        minimumPixels: 40,
        colorTolerance: 0.12,
    });
    const expectedX = { fore_left: 8.5, fore_right: 16.5, hind_left: 26.5, hind_right: 34.5 };
    Object.entries(expectedX).forEach(([label, x]) => {
        const region = extracted.regions[label];
        assert.equal(region.visible, true);
        assert.equal(region.pixelCount, 80);
        assert.ok(Math.abs(region.centroid.x - x) < 0.01);
        assert.ok(region.proximal.y < region.joint.y);
        assert.ok(region.joint.y < region.hoof.y);
        assert.ok(region.confidence > 0.8);
    });
});

test('semantic observations expose proximal joint and hoof tracks without identity swaps', () => {
    const frames = [
        semanticFrame(),
        semanticFrame({ fore_left: { x: 1, y: -2 } }),
        semanticFrame({ fore_left: { x: 2, y: -3 } }),
        semanticFrame({ fore_left: { x: 1, y: -2 } }),
        semanticFrame(),
    ];
    const observations = buildSemanticObservations(frames, PALETTE, {
        fps: 30,
        minimumPixels: 40,
        colorTolerance: 0.12,
        contactHeightTolerancePx: 1,
        contactVelocityTolerancePx: 1.5,
    });
    assert.equal(observations.schema, 'autorig-fitting-observations.v1');
    assert.equal(observations.frame_count, 5);
    assert.equal(observations.tracks.length, 12);
    assert.deepEqual(observations.provenance.limb_labels, [
        'fore_left', 'fore_right', 'hind_left', 'hind_right',
    ]);

    const movingHoof = observations.tracks.find((track) => track.anchor_id === 'fore_left.hoof');
    const stableHoof = observations.tracks.find((track) => track.anchor_id === 'hind_right.hoof');
    assert.deepEqual(movingHoof.points.map((point) => Math.round(point.x)), [9, 10, 11, 10, 9]);
    assert.deepEqual(stableHoof.points.map((point) => Math.round(point.x)), [35, 35, 35, 35, 35]);
    assert.ok(stableHoof.points.every((point) => point.visible && point.confidence > 0.8));
    assert.ok(observations.contacts.some((contact) => contact.anchor_id === 'hind_right.hoof'));
});

test('missing semantic pixels become invisible tracks instead of fabricated motion', () => {
    const missing = semanticFrame();
    for (let y = 0; y < missing.height; y += 1) {
        for (let x = 32; x < 38; x += 1) {
            const offset = (y * missing.width + x) * 4;
            missing.data[offset] = 150;
            missing.data[offset + 1] = 150;
            missing.data[offset + 2] = 150;
        }
    }
    const observations = buildSemanticObservations([
        semanticFrame(),
        missing,
        semanticFrame(),
    ], PALETTE, { minimumPixels: 40, colorTolerance: 0.12 });
    const track = observations.tracks.find((item) => item.anchor_id === 'hind_right.hoof');
    assert.equal(track.points[1].visible, false);
    assert.equal(track.points[1].confidence, 0);
    assert.equal(track.points[1].x, 0);
    assert.equal(track.points[1].y, 0);
});

test('frame contract rejects mismatched dimensions before fitting', () => {
    assert.throws(
        () => buildSemanticObservations([semanticFrame(), semanticFrame({}, 0), frame(41, 30)], PALETTE),
        /same dimensions/,
    );
});

function gaitObservations(centers, options = {}) {
    const frameCount = 48;
    const tracks = [];
    for (const label of ['fore_left', 'fore_right', 'hind_left', 'hind_right']) {
        const center = centers[label];
        const swing = new Set([-2, -1, 0, 1, 2].map((offset) => (
            center + offset + frameCount
        ) % frameCount));
        tracks.push({
            anchor_id: `${label}.hoof`,
            points: Array.from({ length: frameCount }, (_, frame) => ({
                frame,
                x: frame,
                y: swing.has(frame) ? 90 : 100,
                visible: !(options.missingLabel === label && frame < 12),
                confidence: 1,
            })),
        });
    }
    return {
        schema: 'autorig-fitting-observations.v1',
        frame_count: frameCount,
        fps: 30,
        tracks,
        contacts: [],
    };
}

test('Horse gait gate accepts a cyclic four-beat single-hoof sequence', () => {
    const qa = assessHorseWalkGait(gaitObservations({
        hind_left: 1,
        fore_left: 13,
        hind_right: 25,
        fore_right: 37,
    }), { maximumSimultaneousSwingFrames: 0 });
    assert.equal(qa.accepted, true);
    assert.equal(qa.orderAccepted, true);
    assert.equal(qa.overlapAccepted, true);
    assert.equal(qa.simultaneousSwingFrameCount, 0);
    assert.ok(qa.phaseGaps.every((gap) => Math.abs(gap - 0.25) < 0.03));
});

test('Horse gait gate rejects paired trot or pace phases', () => {
    const qa = assessHorseWalkGait(gaitObservations({
        hind_left: 4,
        fore_left: 4,
        hind_right: 28,
        fore_right: 28,
    }), { maximumSimultaneousSwingFrames: 0 });
    assert.equal(qa.accepted, false);
    assert.equal(qa.orderAccepted, false);
    assert.equal(qa.overlapAccepted, false);
    assert.ok(qa.simultaneousSwingFrameCount >= 10);
});

test('Horse gait gate rejects incomplete hoof visibility', () => {
    const qa = assessHorseWalkGait(gaitObservations({
        hind_left: 1,
        fore_left: 13,
        hind_right: 25,
        fore_right: 37,
    }, { missingLabel: 'hind_right' }));
    assert.equal(qa.accepted, false);
    assert.ok(qa.limbs.hind_right.visibleFraction < qa.gates.minimumVisibleFraction);
});

test('Horse trot gate accepts only alternating diagonal LF+RH and RF+LH pairs', () => {
    const qa = assessHorseTrotGait(gaitObservations({
        fore_left: 4,
        hind_right: 4,
        fore_right: 28,
        hind_left: 28,
    }));
    assert.equal(qa.status, 'PASS');
    assert.equal(qa.accepted, true);
    assert.equal(qa.profile.id, 'horse.diagonal_pair_trot.v1');
    assert.equal(qa.profile.distinctFromWalkProfile, true);
    assert.deepEqual(qa.profile.diagonalPairs.map((pair) => pair.feet), [
        ['fore_left', 'hind_right'],
        ['fore_right', 'hind_left'],
    ]);
    assert.equal(qa.alternating, true);
    assert.ok(Object.values(qa.pairs).every((pair) => pair.accepted));
    assert.ok(Object.values(qa.pairs).every((pair) => pair.swingDice === 1));
});

test('Horse trot gate rejects lateral pace and does not weaken the WALK gate', () => {
    const pace = gaitObservations({
        fore_left: 4,
        hind_left: 4,
        fore_right: 28,
        hind_right: 28,
    });
    const trotQa = assessHorseTrotGait(pace);
    assert.equal(trotQa.status, 'FAIL');
    assert.equal(trotQa.accepted, false);
    assert.ok(trotQa.failures.some((failure) => failure.endsWith(':diagonal_swing_mismatch')));

    const walkQa = assessHorseWalkGait(pace, { maximumSimultaneousSwingFrames: 0 });
    assert.equal(walkQa.accepted, false);
    assert.equal(walkQa.overlapAccepted, false);
});

test('Horse trot gate rejects a four-hoof bound despite internally synchronized diagonals', () => {
    const qa = assessHorseTrotGait(gaitObservations({
        fore_left: 8,
        hind_right: 8,
        fore_right: 8,
        hind_left: 8,
    }));
    assert.equal(qa.status, 'FAIL');
    assert.ok(qa.failures.includes('trot_diagonal_event_spacing'));
});

test('Horse trot gate handles a duplicated start/end frame explicitly', () => {
    const source = gaitObservations({
        fore_left: 4,
        hind_right: 4,
        fore_right: 28,
        hind_left: 28,
    });
    source.frame_count = 49;
    source.tracks.forEach((track) => {
        track.points.push({ ...track.points[0], frame: 48 });
    });
    const qa = assessHorseTrotGait(source, { loopEndpointDuplicated: true });
    assert.equal(qa.status, 'PASS');
    assert.equal(qa.sourceFrameCount, 49);
    assert.equal(qa.uniqueFrameCount, 48);
});
