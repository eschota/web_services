import assert from 'node:assert/strict';
import test from 'node:test';

import {
    authorHorseV10SwingGuidePoses,
    HORSE_V10_DEFORM_CHAINS,
    HORSE_V10_GUIDE_FRAME_INDICES,
    HORSE_V10_SWING_ORDER,
    selectHorseV9SwingDonors,
    verifyHorseV10PostBakeHoofProjections,
} from '../animation-fitting-horse-swing-guide-author.js';

const FRAME_COUNT = 49;
const BUNDLE_SHA = 'c'.repeat(64);
const IMMUTABLE_MANIFEST_SHA = 'd'.repeat(64);
const LIMB_BASE = Object.freeze({
    hind_left: [440, 205],
    fore_left: [275, 205],
    hind_right: [485, 205],
    fore_right: [320, 205],
});

function restPoint(limb, index) {
    const [x, y] = LIMB_BASE[limb];
    return [x + index * 2, y + index * 13];
}

function makeSkeleton() {
    return {
        rigType: 'HORSE_2',
        projection: { outputResolution: [768, 448] },
        limbs: Object.fromEntries(HORSE_V10_SWING_ORDER.map((limb) => {
            const sourceBoneChain = [...HORSE_V10_DEFORM_CHAINS[limb]];
            const points = sourceBoneChain.map((_, index) => restPoint(limb, index));
            return [limb, {
                sourceBoneChain,
                joints: sourceBoneChain.slice(0, -1).map((bone, index) => ({
                    bone,
                    restStart: [...points[index]],
                    restEnd: [...points[index + 1]],
                })),
            }];
        })),
    };
}

function makeCandidate(candidateId, apexes = {}) {
    return {
        schema: 'autorig-fitting-observations.v1',
        frame_count: FRAME_COUNT,
        width: 768,
        height: 448,
        provenance: {
            source_video_sha256: candidateId === 'A' ? 'a'.repeat(64) : 'b'.repeat(64),
            bundle_sha256: BUNDLE_SHA,
            immutable_manifest_sha256: IMMUTABLE_MANIFEST_SHA,
        },
        tracks: HORSE_V10_SWING_ORDER.flatMap((limb) => (
            HORSE_V10_DEFORM_CHAINS[limb].map((bone, chainIndex) => ({
                id: `${candidateId}_${limb}_${chainIndex}`,
                anchor_id: `${bone}:${chainIndex}`,
                query_frame: 0,
                points: Array.from({ length: FRAME_COUNT }, (_, frame) => {
                    const rest = restPoint(limb, chainIndex);
                    const apex = apexes[limb];
                    const active = apex && frame === apex.frame;
                    return {
                        frame,
                        x: rest[0] + (active ? apex.dx * ((chainIndex + 1) / 7) : 0),
                        y: rest[1] - (active ? apex.lift * ((chainIndex + 1) / 7) : 0),
                        visible: !(active && apex.invisibleIndex === chainIndex),
                        confidence: active && apex.lowConfidenceIndex === chainIndex ? 0.4 : 0.9,
                    };
                }),
            }))
        )),
    };
}

function standardCandidates() {
    return {
        candidateA: makeCandidate('A', {
            hind_left: { frame: 38, lift: 16.6, dx: -7 },
            hind_right: { frame: 40, lift: 24.2, dx: 8 },
            fore_left: { frame: 20, lift: 1.3, dx: -1 },
            fore_right: { frame: 20, lift: 29, dx: 11 },
        }),
        candidateB: makeCandidate('B', {
            fore_right: { frame: 16, lift: 26.6, dx: 9 },
        }),
    };
}

test('authors the six immutable v10 guide poses with three exact stance hooves', () => {
    const skeleton = makeSkeleton();
    const result = authorHorseV10SwingGuidePoses({ skeleton, ...standardCandidates() });

    assert.equal(result.status, 'pose_contract_ready_not_rendered');
    assert.equal(result.renderer, null);
    assert.equal(result.browserRendererRequired, true);
    assert.equal(result.blenderUsed, false);
    assert.deepEqual(result.guideFrameIndices, HORSE_V10_GUIDE_FRAME_INDICES);
    assert.deepEqual(
        result.guides.map((guide) => [
            guide.frameIndex,
            guide.strength,
            guide.authoredClipFrame,
            guide.authoredClipTimeSeconds,
        ]),
        [
            [0, 0.8, 0, 0],
            [6, 0.7, 1, 1],
            [18, 0.7, 2, 2],
            [30, 0.7, 3, 3],
            [42, 0.7, 4, 4],
            [48, 0.8, 5, 5],
        ],
    );
    assert.deepEqual(result.selections.hind_left, {
        candidateId: 'A',
        sourceLimb: 'hind_left',
        sourceFrame: 38,
        hoofLiftPx: 16.600000000000023,
        minimumChainConfidence: 0.9,
        sourceVideoSha256: 'a'.repeat(64),
        synthesizedFromContralateralDonor: false,
        targetLimb: 'hind_left',
    });
    assert.equal(result.selections.fore_left.candidateId, 'A');
    assert.equal(result.selections.fore_left.sourceLimb, 'fore_right');
    assert.equal(result.selections.fore_left.sourceFrame, 20);
    assert.equal(result.selections.fore_left.synthesizedFromContralateralDonor, true);

    assert.equal(result.qa.status, 'PASS');
    assert.equal(result.qa.minimumStanceHooves, 3);
    assert.equal(result.qa.endpointMaximumErrorPx, 0);
    result.qa.guides.forEach((guide) => {
        assert.equal(guide.stanceHoofCount, guide.swingLimb ? 3 : 4);
        assert.equal(guide.maximumStancePointErrorPx, 0);
        if (guide.swingLimb) assert.ok(guide.swingHoofLiftPx >= 5);
    });

    const frame0 = result.fitted.frames[0];
    const frame48 = result.fitted.frames.at(-1);
    HORSE_V10_SWING_ORDER.forEach((limb) => {
        assert.deepEqual(frame0.limbs[limb], frame48.limbs[limb]);
    });

    const foreLeftGuide = result.fitted.frames[2].limbs.fore_left.points;
    const foreRightDonor = result.fitted.frames[4].limbs.fore_right.points;
    foreLeftGuide.forEach((point, index) => {
        const leftRest = restPoint('fore_left', index);
        const rightRest = restPoint('fore_right', index);
        assert.deepEqual(
            point.map((value, axis) => value - leftRest[axis]),
            foreRightDonor[index].map((value, axis) => value - rightRest[axis]),
        );
    });
});

test('selects a stronger valid candidate B fore-right donor deterministically', () => {
    const candidateA = standardCandidates().candidateA;
    const candidateB = makeCandidate('B', {
        fore_right: { frame: 22, lift: 35, dx: 13 },
    });
    const result = selectHorseV9SwingDonors({ candidateA, candidateB });

    assert.equal(result.fore_right.candidateId, 'B');
    assert.equal(result.fore_right.frame, 22);
    assert.equal(result.fore_left.candidateId, 'B');
    assert.equal(result.fore_left.frame, 22);
    assert.equal(result.fore_left.synthesized, true);
});

test('fails closed when a required deform-chain track is missing', () => {
    const candidates = standardCandidates();
    const missingBone = HORSE_V10_DEFORM_CHAINS.fore_right[3];
    candidates.candidateB.tracks = candidates.candidateB.tracks.filter(
        (track) => !track.anchor_id.startsWith(`${missingBone}:`),
    );

    assert.throws(
        () => authorHorseV10SwingGuidePoses({ skeleton: makeSkeleton(), ...candidates }),
        /missing tracked deform bone leg_stretch_dupli_001\.r/,
    );
});

test('fails closed when every apparent apex violates chain visibility', () => {
    const candidates = standardCandidates();
    candidates.candidateA = makeCandidate('A', {
        hind_left: { frame: 38, lift: 16.6, dx: -7, invisibleIndex: 5 },
        hind_right: { frame: 40, lift: 24.2, dx: 8 },
        fore_right: { frame: 20, lift: 29, dx: 11 },
    });

    assert.throws(
        () => authorHorseV10SwingGuidePoses({ skeleton: makeSkeleton(), ...candidates }),
        /candidate A hind_left has no visible swing apex/,
    );
});

test('fails closed for the adapter default 512x320 projection', () => {
    const skeleton = makeSkeleton();
    skeleton.projection.outputResolution = [512, 320];
    assert.throws(
        () => authorHorseV10SwingGuidePoses({ skeleton, ...standardCandidates() }),
        /outputResolution must be exactly 768x448/,
    );
});

test('fails closed for unpinned observations and invalid thresholds', () => {
    const candidates = standardCandidates();
    candidates.candidateA.provenance.source_video_sha256 = 'not-pinned';
    assert.throws(
        () => authorHorseV10SwingGuidePoses({ skeleton: makeSkeleton(), ...candidates }),
        /source_video_sha256 must be lowercase SHA-256/,
    );

    assert.throws(
        () => authorHorseV10SwingGuidePoses({
            skeleton: makeSkeleton(),
            ...standardCandidates(),
            minimumSwingLiftPx: 0,
        }),
        /minimumSwingLiftPx must be positive/,
    );
    assert.throws(
        () => authorHorseV10SwingGuidePoses({
            skeleton: makeSkeleton(),
            ...standardCandidates(),
            stanceTolerancePx: -1,
        }),
        /stanceTolerancePx must be non-negative/,
    );
});

function exactProjectedHooves(poseContract) {
    return poseContract.guides.map((guide, index) => ({
        frameIndex: guide.frameIndex,
        hooves: Object.fromEntries(HORSE_V10_SWING_ORDER.map((limb) => [
            limb,
            [...poseContract.fitted.frames[index].limbs[limb].points.at(-1)],
        ])),
    }));
}

test('post-bake QA requires exactly three unchanged stance hooves and the requested swing lift', () => {
    const poseContract = authorHorseV10SwingGuidePoses({ skeleton: makeSkeleton(), ...standardCandidates() });
    const projectedHoovesByGuide = exactProjectedHooves(poseContract);
    const result = verifyHorseV10PostBakeHoofProjections({
        poseContract,
        projectedHoovesByGuide,
        maximumStanceErrorPx: 0.01,
        maximumRequestedErrorPx: 3,
    });
    assert.equal(result.status, 'PASS');
    assert.equal(result.hierarchyBakeVerified, true);
    assert.equal(result.endpointMaximumErrorPx, 0);
    assert.deepEqual(result.guides.map((guide) => guide.stanceHoofCount), [4, 3, 3, 3, 3, 4]);

    const movedStance = structuredClone(projectedHoovesByGuide);
    movedStance[1].hooves.fore_left[0] += 0.02;
    assert.throws(
        () => verifyHorseV10PostBakeHoofProjections({
            poseContract,
            projectedHoovesByGuide: movedStance,
            maximumStanceErrorPx: 0.01,
            maximumRequestedErrorPx: 3,
        }),
        /post-bake stance hoof error exceeds tolerance/,
    );

    const missedSwing = structuredClone(projectedHoovesByGuide);
    missedSwing[1].hooves.hind_left = [...projectedHoovesByGuide[0].hooves.hind_left];
    assert.throws(
        () => verifyHorseV10PostBakeHoofProjections({
            poseContract,
            projectedHoovesByGuide: missedSwing,
            maximumStanceErrorPx: 0.01,
            maximumRequestedErrorPx: 100,
        }),
        /post-bake swing hoof lift is too small/,
    );
});
