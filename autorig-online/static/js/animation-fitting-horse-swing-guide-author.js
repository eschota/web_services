const FITTED_SCHEMA = 'autorig-browser-fitted-animation.v1';
const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

export const HORSE_V10_SWING_GUIDE_SCHEMA = 'autorig-browser-horse-swing-guide-poses.v1';
export const HORSE_V10_GUIDE_FRAME_INDICES = Object.freeze([0, 6, 18, 30, 42, 48]);
export const HORSE_V10_SWING_ORDER = Object.freeze([
    'hind_left',
    'fore_left',
    'hind_right',
    'fore_right',
]);

export const HORSE_V10_DEFORM_CHAINS = Object.freeze({
    hind_left: Object.freeze([
        'c_thigh_b.l',
        'thigh_twist.l',
        'thigh_stretch.l',
        'leg_stretch.l',
        'leg_twist.l',
        'foot.l',
        'toes_01.l',
    ]),
    fore_left: Object.freeze([
        'c_thigh_b_dupli_001.l',
        'thigh_twist_dupli_001.l',
        'thigh_stretch_dupli_001.l',
        'leg_stretch_dupli_001.l',
        'leg_twist_dupli_001.l',
        'foot_dupli_001.l',
        'toes_01_dupli_001.l',
    ]),
    hind_right: Object.freeze([
        'c_thigh_b.r',
        'thigh_twist.r',
        'thigh_stretch.r',
        'leg_stretch.r',
        'leg_twist.r',
        'foot.r',
        'toes_01.r',
    ]),
    fore_right: Object.freeze([
        'c_thigh_b_dupli_001.r',
        'thigh_twist_dupli_001.r',
        'thigh_stretch_dupli_001.r',
        'leg_stretch_dupli_001.r',
        'leg_twist_dupli_001.r',
        'foot_dupli_001.r',
        'toes_01_dupli_001.r',
    ]),
});

export const HORSE_V9_TEMPLATE_WINDOWS = Object.freeze({
    A: Object.freeze({
        hind_left: Object.freeze([36, 43]),
        hind_right: Object.freeze([36, 43]),
        fore_right: Object.freeze([12, 25]),
    }),
    B: Object.freeze({
        fore_right: Object.freeze([13, 26]),
    }),
});

function finite(value, field) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error(`${field} must be finite`);
    return number;
}

function integer(value, field) {
    const number = Number(value);
    if (!Number.isInteger(number)) throw new Error(`${field} must be an integer`);
    return number;
}

function point2(value, field) {
    if (!Array.isArray(value) || value.length !== 2) throw new Error(`${field} must be [x, y]`);
    return [finite(value[0], `${field}[0]`), finite(value[1], `${field}[1]`)];
}

function clonePoint(value) {
    return [value[0], value[1]];
}

function validateObservations(value, label) {
    if (!value || typeof value !== 'object') throw new Error(`${label} observations are required`);
    if (value.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`${label}.schema must be ${OBSERVATION_SCHEMA}`);
    }
    if (integer(value.frame_count, `${label}.frame_count`) !== 49) {
        throw new Error(`${label} must contain exactly 49 frames`);
    }
    if (integer(value.width, `${label}.width`) !== 768 || integer(value.height, `${label}.height`) !== 448) {
        throw new Error(`${label} must use the canonical 768x448 resolution`);
    }
    if (!Array.isArray(value.tracks) || !value.tracks.length) {
        throw new Error(`${label}.tracks must not be empty`);
    }
    const provenance = value.provenance;
    if (!provenance || typeof provenance !== 'object') {
        throw new Error(`${label}.provenance is required`);
    }
    ['source_video_sha256', 'bundle_sha256', 'immutable_manifest_sha256'].forEach((field) => {
        if (!SHA256_PATTERN.test(String(provenance[field] || ''))) {
            throw new Error(`${label}.provenance.${field} must be lowercase SHA-256`);
        }
    });
    return value;
}

function trackBone(track, field) {
    const anchorId = String(track?.anchor_id || '');
    const separator = anchorId.lastIndexOf(':');
    if (separator <= 0) throw new Error(`${field}.anchor_id must contain bone:vertex`);
    return anchorId.slice(0, separator);
}

function tracksByBone(observations, label) {
    const result = new Map();
    observations.tracks.forEach((track, index) => {
        const bone = trackBone(track, `${label}.tracks[${index}]`);
        if (result.has(bone)) throw new Error(`${label} has duplicate tracked bone ${bone}`);
        if (track.query_frame !== 0) {
            throw new Error(`${label} track ${bone} query_frame must be the actionless frame 0`);
        }
        if (!Array.isArray(track.points) || track.points.length !== 49) {
            throw new Error(`${label} track ${bone} must contain 49 points`);
        }
        track.points.forEach((point, frameIndex) => {
            if (integer(point.frame, `${label}.${bone}.points[${frameIndex}].frame`) !== frameIndex) {
                throw new Error(`${label} track ${bone} frame order is invalid`);
            }
            finite(point.x, `${label}.${bone}.points[${frameIndex}].x`);
            finite(point.y, `${label}.${bone}.points[${frameIndex}].y`);
            finite(point.confidence, `${label}.${bone}.points[${frameIndex}].confidence`);
            if (typeof point.visible !== 'boolean') {
                throw new Error(`${label}.${bone}.points[${frameIndex}].visible must be boolean`);
            }
        });
        result.set(bone, track);
    });
    return result;
}

function requiredTracks(trackMap, chain, label) {
    return chain.map((bone) => {
        const track = trackMap.get(bone);
        if (!track) throw new Error(`${label} is missing tracked deform bone ${bone}`);
        return track;
    });
}

function candidateApex({ candidateId, observations, limb, window, minimumConfidence, minimumLiftPx }) {
    const chain = HORSE_V10_DEFORM_CHAINS[limb];
    const trackMap = tracksByBone(observations, `candidate ${candidateId}`);
    const tracks = requiredTracks(trackMap, chain, `candidate ${candidateId} ${limb}`);
    const baselinePoints = tracks.map((track) => track.points[track.query_frame]);
    if (baselinePoints.some((point) => !point.visible || point.confidence < minimumConfidence)) {
        throw new Error(`candidate ${candidateId} ${limb} actionless baseline is not confidently visible`);
    }
    const hoofTrack = tracks.at(-1);
    const hoofRest = hoofTrack.points[hoofTrack.query_frame];
    const [start, end] = window;
    const rows = [];
    for (let frame = start; frame <= end; frame += 1) {
        const points = tracks.map((track) => track.points[frame]);
        if (points.some((point) => !point.visible)) continue;
        const minimumChainConfidence = Math.min(...points.map((point) => point.confidence));
        if (minimumChainConfidence < minimumConfidence) continue;
        const hoof = points.at(-1);
        const hoofLiftPx = hoofRest.y - hoof.y;
        if (hoofLiftPx < minimumLiftPx) continue;
        rows.push({
            candidateId,
            observations,
            limb,
            frame,
            tracks,
            hoofLiftPx,
            minimumChainConfidence,
            score: hoofLiftPx,
        });
    }
    if (!rows.length) {
        throw new Error(
            `candidate ${candidateId} ${limb} has no visible swing apex in frames ${start}-${end}`,
        );
    }
    rows.sort((left, right) => (
        right.score - left.score
        || right.minimumChainConfidence - left.minimumChainConfidence
        || left.frame - right.frame
    ));
    return rows[0];
}

function publicSelection(row) {
    return {
        candidateId: row.candidateId,
        sourceLimb: row.sourceLimb || row.limb,
        sourceFrame: row.frame,
        hoofLiftPx: row.hoofLiftPx,
        minimumChainConfidence: row.minimumChainConfidence,
        sourceVideoSha256: String(row.observations?.provenance?.source_video_sha256 || ''),
    };
}

export function selectHorseV9SwingDonors(options = {}) {
    const candidateA = validateObservations(options.candidateA, 'candidate A');
    const candidateB = validateObservations(options.candidateB, 'candidate B');
    const minimumConfidence = finite(options.minimumConfidence ?? 0.5, 'minimumConfidence');
    const minimumLiftPx = finite(options.minimumLiftPx ?? 5, 'minimumLiftPx');
    if (minimumConfidence < 0 || minimumConfidence > 1) {
        throw new Error('minimumConfidence must be in [0, 1]');
    }
    if (minimumLiftPx <= 0) throw new Error('minimumLiftPx must be positive');

    const hindLeft = candidateApex({
        candidateId: 'A',
        observations: candidateA,
        limb: 'hind_left',
        window: HORSE_V9_TEMPLATE_WINDOWS.A.hind_left,
        minimumConfidence,
        minimumLiftPx,
    });
    const hindRight = candidateApex({
        candidateId: 'A',
        observations: candidateA,
        limb: 'hind_right',
        window: HORSE_V9_TEMPLATE_WINDOWS.A.hind_right,
        minimumConfidence,
        minimumLiftPx,
    });
    const foreRightRows = [
        candidateApex({
            candidateId: 'A',
            observations: candidateA,
            limb: 'fore_right',
            window: HORSE_V9_TEMPLATE_WINDOWS.A.fore_right,
            minimumConfidence,
            minimumLiftPx,
        }),
        candidateApex({
            candidateId: 'B',
            observations: candidateB,
            limb: 'fore_right',
            window: HORSE_V9_TEMPLATE_WINDOWS.B.fore_right,
            minimumConfidence,
            minimumLiftPx,
        }),
    ].sort((left, right) => (
        right.score - left.score
        || right.minimumChainConfidence - left.minimumChainConfidence
        || left.candidateId.localeCompare(right.candidateId)
        || left.frame - right.frame
    ));
    const foreRight = foreRightRows[0];
    return {
        hind_left: hindLeft,
        fore_left: {
            ...foreRight,
            limb: 'fore_left',
            sourceLimb: 'fore_right',
            synthesized: true,
        },
        hind_right: hindRight,
        fore_right: foreRight,
    };
}

function restPoints(limb, label) {
    const expectedChain = HORSE_V10_DEFORM_CHAINS[label];
    if (!limb || !Array.isArray(limb.sourceBoneChain) || !Array.isArray(limb.joints)) {
        throw new Error(`skeleton limb ${label} is invalid`);
    }
    if (
        limb.sourceBoneChain.length !== expectedChain.length
        || limb.sourceBoneChain.some((bone, index) => bone !== expectedChain[index])
        || limb.joints.length !== expectedChain.length - 1
    ) {
        throw new Error(`skeleton limb ${label} does not match the Horse_2 deform chain`);
    }
    const points = limb.joints.map((joint, index) => point2(joint.restStart, `${label}.joints[${index}].restStart`));
    points.push(point2(limb.joints.at(-1).restEnd, `${label}.terminal.restEnd`));
    return points;
}

function donorDisplacements(selection, targetLabel) {
    const sourceLabel = selection.sourceLimb || selection.limb;
    const sourceChain = HORSE_V10_DEFORM_CHAINS[sourceLabel];
    const tracks = selection.tracks;
    if (!Array.isArray(tracks) || tracks.length !== sourceChain.length) {
        throw new Error(`${targetLabel} donor track inventory is incomplete`);
    }
    return tracks.map((track, index) => {
        const sourceBone = trackBone(track, `${targetLabel}.donor[${index}]`);
        if (sourceBone !== sourceChain[index]) {
            throw new Error(`${targetLabel} donor chain order changed at ${sourceBone}`);
        }
        const rest = track.points[track.query_frame];
        const point = track.points[selection.frame];
        if (!point.visible) throw new Error(`${targetLabel} donor ${sourceBone} is not visible`);
        return [point.x - rest.x, point.y - rest.y];
    });
}

function maximumPointError(left, right) {
    return Math.max(...left.map((point, index) => Math.hypot(
        point[0] - right[index][0],
        point[1] - right[index][1],
    )));
}

function projectedHoofRow(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return Object.fromEntries(HORSE_V10_SWING_ORDER.map((limb) => [
        limb,
        point2(value[limb], `${field}.${limb}`),
    ]));
}

/**
 * Verify the terminal hoof heads after the authored pose was baked through the
 * actual Three.js Horse_2 hierarchy. This is intentionally separate from the
 * 2D pose-contract QA: a renderer must prove that the sampled AnimationClip
 * still leaves exactly three non-swing hooves on their actionless projections.
 */
export function verifyHorseV10PostBakeHoofProjections(options = {}) {
    const contract = options.poseContract;
    if (!contract || contract.schema !== HORSE_V10_SWING_GUIDE_SCHEMA) {
        throw new Error(`poseContract.schema must be ${HORSE_V10_SWING_GUIDE_SCHEMA}`);
    }
    if (!Array.isArray(contract.guides) || contract.guides.length !== HORSE_V10_GUIDE_FRAME_INDICES.length) {
        throw new Error('poseContract must contain the six Horse v10 guides');
    }
    const projected = options.projectedHoovesByGuide;
    if (!Array.isArray(projected) || projected.length !== contract.guides.length) {
        throw new Error('projectedHoovesByGuide must contain the six sampled guide rows');
    }
    const maximumStanceErrorPx = finite(options.maximumStanceErrorPx ?? 1, 'maximumStanceErrorPx');
    const maximumRequestedErrorPx = finite(options.maximumRequestedErrorPx ?? 1, 'maximumRequestedErrorPx');
    const minimumSwingLiftPx = finite(
        options.minimumSwingLiftPx ?? contract.qa?.minimumSwingLiftPx ?? 5,
        'minimumSwingLiftPx',
    );
    if (maximumStanceErrorPx < 0 || maximumRequestedErrorPx < 0) {
        throw new Error('post-bake error thresholds must be non-negative');
    }
    if (minimumSwingLiftPx <= 0) throw new Error('minimumSwingLiftPx must be positive');

    const rows = projected.map((value, index) => {
        const guide = contract.guides[index];
        if (integer(value?.frameIndex, `projectedHoovesByGuide[${index}].frameIndex`) !== guide.frameIndex) {
            throw new Error(`projected guide row ${index} does not match frame ${guide.frameIndex}`);
        }
        return {
            frameIndex: guide.frameIndex,
            swingLimb: guide.swingLimb,
            hooves: projectedHoofRow(value.hooves, `projectedHoovesByGuide[${index}].hooves`),
        };
    });
    const rest = rows[0].hooves;
    const frame0Fitted = contract.fitted?.frames?.[0];
    if (!frame0Fitted) throw new Error('poseContract.fitted frame 0 is required');

    const qaGuides = rows.map((row, index) => {
        const desiredFrame = contract.fitted.frames[index];
        if (!desiredFrame) throw new Error(`poseContract.fitted frame ${index} is required`);
        const stanceLimbs = HORSE_V10_SWING_ORDER.filter((limb) => limb !== row.swingLimb);
        const expectedStanceCount = row.swingLimb ? 3 : 4;
        if (stanceLimbs.length !== expectedStanceCount) {
            throw new Error(`guide frame ${row.frameIndex} does not define exactly ${expectedStanceCount} stance limbs`);
        }
        const stanceErrors = Object.fromEntries(stanceLimbs.map((limb) => [
            limb,
            Math.hypot(row.hooves[limb][0] - rest[limb][0], row.hooves[limb][1] - rest[limb][1]),
        ]));
        const requestedErrors = Object.fromEntries(HORSE_V10_SWING_ORDER.map((limb) => {
            const desired = point2(
                desiredFrame.limbs?.[limb]?.points?.at(-1),
                `poseContract.fitted.frames[${index}].${limb}.terminal`,
            );
            return [limb, Math.hypot(
                row.hooves[limb][0] - desired[0],
                row.hooves[limb][1] - desired[1],
            )];
        }));
        const actualMaximumStanceErrorPx = Math.max(0, ...Object.values(stanceErrors));
        const actualMaximumRequestedErrorPx = Math.max(...Object.values(requestedErrors));
        if (actualMaximumStanceErrorPx > maximumStanceErrorPx) {
            throw new Error(`guide frame ${row.frameIndex} post-bake stance hoof error exceeds tolerance`);
        }
        if (actualMaximumRequestedErrorPx > maximumRequestedErrorPx) {
            throw new Error(`guide frame ${row.frameIndex} post-bake requested hoof error exceeds tolerance`);
        }
        let swingHoofLiftPx = 0;
        if (row.swingLimb) {
            swingHoofLiftPx = rest[row.swingLimb][1] - row.hooves[row.swingLimb][1];
            if (swingHoofLiftPx < minimumSwingLiftPx) {
                throw new Error(`guide frame ${row.frameIndex} post-bake swing hoof lift is too small`);
            }
        }
        return {
            frameIndex: row.frameIndex,
            swingLimb: row.swingLimb,
            stanceLimbs,
            stanceHoofCount: stanceLimbs.length,
            maximumStanceErrorPx: actualMaximumStanceErrorPx,
            maximumRequestedErrorPx: actualMaximumRequestedErrorPx,
            swingHoofLiftPx,
        };
    });
    const endpointMaximumErrorPx = Math.max(...HORSE_V10_SWING_ORDER.map((limb) => Math.hypot(
        rows[0].hooves[limb][0] - rows.at(-1).hooves[limb][0],
        rows[0].hooves[limb][1] - rows.at(-1).hooves[limb][1],
    )));
    if (endpointMaximumErrorPx > maximumStanceErrorPx) {
        throw new Error('post-bake frame 0 and frame 48 hoof projections differ');
    }
    return {
        status: 'PASS',
        hierarchyBakeVerified: true,
        minimumStanceHooves: 3,
        maximumStanceErrorPx,
        maximumRequestedErrorPx,
        minimumSwingLiftPx,
        endpointMaximumErrorPx,
        guides: qaGuides,
    };
}

export function authorHorseV10SwingGuidePoses(options = {}) {
    const skeleton = options.skeleton;
    if (!skeleton || skeleton.rigType !== 'HORSE_2' || !skeleton.limbs) {
        throw new Error('Horse_2 browser fitting skeleton is required');
    }
    if (
        !Array.isArray(skeleton.projection?.outputResolution)
        || skeleton.projection.outputResolution.length !== 2
        || skeleton.projection.outputResolution[0] !== 768
        || skeleton.projection.outputResolution[1] !== 448
    ) {
        throw new Error('Horse_2 browser fitting skeleton outputResolution must be exactly 768x448');
    }
    const minimumSwingLiftPx = finite(options.minimumSwingLiftPx ?? 5, 'minimumSwingLiftPx');
    const stanceTolerancePx = finite(options.stanceTolerancePx ?? 1e-9, 'stanceTolerancePx');
    if (minimumSwingLiftPx <= 0) throw new Error('minimumSwingLiftPx must be positive');
    if (stanceTolerancePx < 0) throw new Error('stanceTolerancePx must be non-negative');
    const selections = selectHorseV9SwingDonors(options);
    const rests = Object.fromEntries(HORSE_V10_SWING_ORDER.map((label) => [
        label,
        restPoints(skeleton.limbs[label], label),
    ]));
    const guideRows = [
        { frameIndex: 0, role: 'actionless_default_cycle_origin', swingLimb: null, strength: 0.8 },
        { frameIndex: 6, role: 'hind_left_single_hoof_swing_apex', swingLimb: 'hind_left', strength: 0.7 },
        { frameIndex: 18, role: 'fore_left_single_hoof_swing_apex', swingLimb: 'fore_left', strength: 0.7 },
        { frameIndex: 30, role: 'hind_right_single_hoof_swing_apex', swingLimb: 'hind_right', strength: 0.7 },
        { frameIndex: 42, role: 'fore_right_single_hoof_swing_apex', swingLimb: 'fore_right', strength: 0.7 },
        { frameIndex: 48, role: 'actionless_default_cycle_endpoint', swingLimb: null, strength: 0.8 },
    ];
    const frames = guideRows.map((guide, authoredIndex) => {
        const limbs = Object.fromEntries(HORSE_V10_SWING_ORDER.map((label) => [
            label,
            { points: rests[label].map(clonePoint) },
        ]));
        if (guide.swingLimb) {
            const displacements = donorDisplacements(selections[guide.swingLimb], guide.swingLimb);
            limbs[guide.swingLimb].points = rests[guide.swingLimb].map((point, index) => [
                point[0] + displacements[index][0],
                point[1] + displacements[index][1],
            ]);
        }
        return {
            frame: authoredIndex,
            targetGuideFrame: guide.frameIndex,
            role: guide.role,
            swingLimb: guide.swingLimb,
            strength: guide.strength,
            authoredClipFrame: authoredIndex,
            authoredClipTimeSeconds: authoredIndex,
            limbs,
        };
    });

    const qaGuides = frames.map((frame) => {
        const stanceLimbs = HORSE_V10_SWING_ORDER.filter((label) => label !== frame.swingLimb);
        const stanceErrors = Object.fromEntries(stanceLimbs.map((label) => [
            label,
            maximumPointError(frame.limbs[label].points, rests[label]),
        ]));
        if (Object.values(stanceErrors).some((error) => error > stanceTolerancePx)) {
            throw new Error(`guide frame ${frame.targetGuideFrame} moved a stance limb`);
        }
        let swingHoofLiftPx = 0;
        if (frame.swingLimb) {
            const restHoof = rests[frame.swingLimb].at(-1);
            const swingHoof = frame.limbs[frame.swingLimb].points.at(-1);
            swingHoofLiftPx = restHoof[1] - swingHoof[1];
            if (swingHoofLiftPx < minimumSwingLiftPx) {
                throw new Error(
                    `guide frame ${frame.targetGuideFrame} ${frame.swingLimb} lift is too small`,
                );
            }
        }
        return {
            frameIndex: frame.targetGuideFrame,
            swingLimb: frame.swingLimb,
            stanceLimbs,
            stanceHoofCount: stanceLimbs.length,
            maximumStancePointErrorPx: Math.max(0, ...Object.values(stanceErrors)),
            swingHoofLiftPx,
        };
    });
    const endpointMaximumErrorPx = Math.max(...HORSE_V10_SWING_ORDER.map((label) => (
        maximumPointError(frames[0].limbs[label].points, frames.at(-1).limbs[label].points)
    )));
    if (endpointMaximumErrorPx > 0) {
        throw new Error('authored frame 0 and frame 48 poses are not identical');
    }

    const fitted = {
        schema: FITTED_SCHEMA,
        fps: 1,
        frameCount: frames.length,
        durationSeconds: frames.length - 1,
        frames: frames.map((frame) => ({
            frame: frame.frame,
            limbs: frame.limbs,
        })),
        tracks: [],
    };
    return {
        schema: HORSE_V10_SWING_GUIDE_SCHEMA,
        status: 'pose_contract_ready_not_rendered',
        rigType: 'HORSE_2',
        resolution: [768, 448],
        guideFrameIndices: [...HORSE_V10_GUIDE_FRAME_INDICES],
        swingOrder: [...HORSE_V10_SWING_ORDER],
        renderer: null,
        browserRendererRequired: true,
        blenderUsed: false,
        selections: Object.fromEntries(HORSE_V10_SWING_ORDER.map((label) => [
            label,
            {
                ...publicSelection(selections[label]),
                synthesizedFromContralateralDonor: label === 'fore_left',
                targetLimb: label,
            },
        ])),
        guides: frames.map((frame) => ({
            frameIndex: frame.targetGuideFrame,
            role: frame.role,
            swingLimb: frame.swingLimb,
            strength: frame.strength,
            authoredClipFrame: frame.authoredClipFrame,
            authoredClipTimeSeconds: frame.authoredClipTimeSeconds,
        })),
        fitted,
        qa: {
            status: 'PASS',
            minimumStanceHooves: 3,
            stanceTolerancePx,
            minimumSwingLiftPx,
            endpointMaximumErrorPx,
            guides: qaGuides,
        },
    };
}
