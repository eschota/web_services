import assert from 'node:assert/strict';
import test from 'node:test';

import {
    HORSE_V14_ANCHOR_FRAME_INDICES,
    HORSE_V14_BARRIER_FRAME_INDICES,
    HORSE_V14_FRAME_INDICES,
    HORSE_V14_INTERVAL_GUIDE_SCHEMA,
    HORSE_V14_INTERVAL_SEGMENTS,
    authorHorseV14IntervalGuidePoses,
    horseV14SinSquaredWeight,
    verifyHorseV14PostBakeHoofProjections,
} from '../animation-fitting-horse-swing-guide-author.js';

const LIMBS = ['hind_left', 'fore_left', 'hind_right', 'fore_right'];
const SWINGS = [null, 'hind_left', null, 'fore_left', null, 'hind_right', null, 'fore_right', null];

function points(y = 100) {
    return [[10, 20], [15, 60], [20, y]];
}

function sourceContract() {
    const frames = HORSE_V14_ANCHOR_FRAME_INDICES.map((frameIndex, index) => {
        const swingLimb = SWINGS[index];
        return {
            frame: index,
            limbs: Object.fromEntries(LIMBS.map((limb) => [
                limb,
                { points: points(limb === swingLimb ? 76 - index : 100) },
            ])),
        };
    });
    return {
        schema: 'autorig-browser-horse-recovery-guide-poses.v1',
        rigType: 'HORSE_2',
        resolution: [768, 448],
        guides: HORSE_V14_ANCHOR_FRAME_INDICES.map((frameIndex, index) => ({
            frameIndex,
            swingLimb: SWINGS[index],
        })),
        fitted: {
            schema: 'autorig-browser-fitted-animation.v1',
            fps: 1,
            frameCount: 9,
            durationSeconds: 8,
            frames,
            tracks: [],
        },
    };
}

function clone(value) {
    return JSON.parse(JSON.stringify(value));
}

test('v14 expands the exact nine v12 anchors into one smooth 49-frame browser interval', () => {
    const source = sourceContract();
    const before = clone(source);
    const result = authorHorseV14IntervalGuidePoses({ sourcePoseContract: source });

    assert.equal(result.schema, HORSE_V14_INTERVAL_GUIDE_SCHEMA);
    assert.equal(result.blenderUsed, false);
    assert.deepEqual(result.guideFrameIndices, HORSE_V14_FRAME_INDICES);
    assert.equal(result.fitted.frameCount, 49);
    assert.equal(result.fitted.fps, 30);
    assert.equal(result.fitted.durationSeconds, 48 / 30);
    result.guides.forEach((guide, frameIndex) => {
        assert.equal(guide.authoredClipTimeSeconds, Math.fround(frameIndex / 30));
    });
    assert.equal(result.qa.status, 'PASS');
    assert.equal(result.qa.interpolation, 'sin_squared_four_12_frame_segments_v1');
    assert.equal(result.qa.maximumAnchorPointErrorPx, 0);
    assert.equal(result.qa.maximumBarrierPointErrorPx, 0);
    assert.equal(result.qa.maximumStancePointErrorPx, 0);
    assert.equal(result.qa.endpointMaximumErrorPx, 0);
    assert.deepEqual(source, before, 'source v12 pose contract must remain immutable');

    HORSE_V14_ANCHOR_FRAME_INDICES.forEach((frameIndex, sourceIndex) => {
        assert.deepEqual(result.fitted.frames[frameIndex].limbs, source.fitted.frames[sourceIndex].limbs);
    });
    HORSE_V14_BARRIER_FRAME_INDICES.forEach((frameIndex) => {
        assert.equal(result.guides[frameIndex].swingLimb, null);
        assert.deepEqual(result.fitted.frames[frameIndex].limbs, result.fitted.frames[0].limbs);
    });
    HORSE_V14_INTERVAL_SEGMENTS.forEach((segment) => {
        for (let frameIndex = segment.startFrame + 1; frameIndex < segment.endFrame; frameIndex += 1) {
            assert.equal(result.guides[frameIndex].swingLimb, segment.swingLimb);
            const stance = LIMBS.filter((limb) => limb !== segment.swingLimb);
            stance.forEach((limb) => {
                assert.deepEqual(result.fitted.frames[frameIndex].limbs[limb], result.fitted.frames[0].limbs[limb]);
            });
        }
    });
});

test('v14 sin-squared interpolation is symmetric with exact rest and apex anchors', () => {
    for (const segment of HORSE_V14_INTERVAL_SEGMENTS) {
        assert.equal(horseV14SinSquaredWeight(segment.startFrame, segment), 0);
        assert.equal(horseV14SinSquaredWeight(segment.apexFrame, segment), 1);
        assert.equal(horseV14SinSquaredWeight(segment.endFrame, segment), 0);
        for (let offset = 1; offset < 6; offset += 1) {
            const ascending = horseV14SinSquaredWeight(segment.startFrame + offset, segment);
            const descending = horseV14SinSquaredWeight(segment.endFrame - offset, segment);
            assert.ok(Math.abs(ascending - descending) < 1e-12);
        }
    }
});

test('v14 post-bake verifier accepts all 49 exact browser hoof projections', () => {
    const poseContract = authorHorseV14IntervalGuidePoses({ sourcePoseContract: sourceContract() });
    const projectedHoovesByGuide = poseContract.fitted.frames.map((frame, frameIndex) => ({
        frameIndex,
        hooves: Object.fromEntries(LIMBS.map((limb) => [limb, frame.limbs[limb].points.at(-1)])),
    }));
    const result = verifyHorseV14PostBakeHoofProjections({
        poseContract,
        projectedHoovesByGuide,
        maximumStanceErrorPx: 0,
        maximumRequestedErrorPx: 0,
        minimumApexLiftPx: 5,
    });
    assert.equal(result.status, 'PASS');
    assert.equal(result.frameCount, 49);
    assert.equal(result.frames.length, 49);
    assert.equal(result.endpointMaximumErrorPx, 0);
    assert.equal(result.frames.filter((frame) => frame.swingLimb).length, 44);
    assert.ok(result.frames.filter((frame) => frame.swingLimb).every((frame) => frame.stanceHoofCount === 3));
});

test('v14 fails closed when a recovery barrier or source anchor order changes', () => {
    const movedBarrier = sourceContract();
    movedBarrier.fitted.frames[2].limbs.hind_left.points[2][1] -= 1;
    assert.throws(
        () => authorHorseV14IntervalGuidePoses({ sourcePoseContract: movedBarrier }),
        /barrier 12 moved hind_left/,
    );

    const reordered = sourceContract();
    reordered.guides[1].frameIndex = 7;
    assert.throws(
        () => authorHorseV14IntervalGuidePoses({ sourcePoseContract: reordered }),
        /exact nine v12 anchor guides/,
    );
});
