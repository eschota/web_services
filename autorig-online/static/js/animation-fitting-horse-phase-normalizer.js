import { assessHorseWalkGait } from './animation-fitting-semantic-tracker.js';

const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';
const NORMALIZATION_SCHEMA = 'autorig-horse-walk-phase-normalization.v1';
const REQUIRED_FRAME_COUNT = 49;
const DEFAULT_ORDER = Object.freeze([
    'hind_left',
    'fore_left',
    'hind_right',
    'fore_right',
]);
const TRACK_ROLES = Object.freeze(['proximal', 'joint', 'hoof']);

function finite(value, field) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error(`${field} must be finite`);
    return number;
}

function clamp(value, minimum, maximum) {
    return Math.min(maximum, Math.max(minimum, value));
}

function modulo(value, divisor) {
    return ((value % divisor) + divisor) % divisor;
}

function cloneValue(value) {
    if (Array.isArray(value)) return value.map(cloneValue);
    if (ArrayBuffer.isView(value)) return new value.constructor(value);
    if (!value || typeof value !== 'object') return value;
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, cloneValue(item)]));
}

function expectedOrder(value) {
    const order = value || DEFAULT_ORDER;
    if (!Array.isArray(order) || order.length !== 4 || new Set(order).size !== 4) {
        throw new Error('expectedOrder must contain four unique Horse limb labels');
    }
    const allowed = new Set(DEFAULT_ORDER);
    if (order.some((label) => !allowed.has(label))) {
        throw new Error('expectedOrder contains an unsupported Horse limb label');
    }
    return [...order];
}

function trackMap(observations) {
    if (!Array.isArray(observations.tracks)) throw new Error('observations.tracks must be an array');
    const result = new Map();
    observations.tracks.forEach((track) => {
        const anchorId = String(track?.anchor_id || '');
        if (!anchorId) throw new Error('every semantic track needs anchor_id');
        if (result.has(anchorId)) throw new Error(`semantic track is duplicated: ${anchorId}`);
        result.set(anchorId, track);
    });
    return result;
}

function orderedPoints(track, frameCount) {
    if (!Array.isArray(track?.points) || track.points.length !== frameCount) {
        throw new Error(`${track?.anchor_id || 'semantic track'} must contain exactly ${frameCount} points`);
    }
    const byFrame = new Map();
    track.points.forEach((point) => {
        const frame = Number(point?.frame);
        if (!Number.isInteger(frame) || frame < 0 || frame >= frameCount || byFrame.has(frame)) {
            throw new Error(`${track.anchor_id} contains a missing, duplicate or invalid frame`);
        }
        const normalized = {
            ...cloneValue(point),
            frame,
            x: finite(point.x, `${track.anchor_id}[${frame}].x`),
            y: finite(point.y, `${track.anchor_id}[${frame}].y`),
            visible: Boolean(point.visible),
            confidence: finite(point.confidence ?? (point.visible ? 1 : 0), `${track.anchor_id}[${frame}].confidence`),
        };
        byFrame.set(frame, normalized);
    });
    if (byFrame.size !== frameCount) throw new Error(`${track.anchor_id} is missing one or more frames`);
    return Array.from({ length: frameCount }, (_, frame) => byFrame.get(frame));
}

function validateIdentityAndVisibility(observations, order, options) {
    const frameCount = observations.frame_count;
    const minimumVisibleFraction = clamp(finite(
        options.minimumVisibleFraction ?? 0.95,
        'minimumVisibleFraction',
    ), 0, 1);
    const tracks = trackMap(observations);
    const orderedByAnchor = new Map();
    order.forEach((label) => {
        TRACK_ROLES.forEach((role) => {
            const anchorId = `${label}.${role}`;
            const track = tracks.get(anchorId);
            if (!track) throw new Error(`Horse phase normalization is missing ${anchorId}`);
            const points = orderedPoints(track, frameCount);
            const visibleFraction = points.filter((point) => point.visible).length / frameCount;
            if (visibleFraction < minimumVisibleFraction) {
                throw new Error(`${anchorId} visibility ${visibleFraction.toFixed(3)} is below ${minimumVisibleFraction.toFixed(3)}`);
            }
            orderedByAnchor.set(anchorId, points);
        });
    });
    return { tracks, orderedByAnchor, minimumVisibleFraction };
}

function quantile(values, fraction) {
    if (!values.length) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const index = clamp(fraction, 0, 1) * (sorted.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const alpha = index - lower;
    return sorted[lower] + (sorted[upper] - sorted[lower]) * alpha;
}

function recomputeContacts(observations, order, options) {
    const frameCount = observations.frame_count;
    const targetAnchors = new Set(order.map((label) => `${label}.hoof`));
    const preserved = (observations.contacts || [])
        .filter((contact) => !targetAnchors.has(contact?.anchor_id))
        .map(cloneValue);
    const byAnchor = trackMap(observations);
    const heightTolerance = Math.max(0, finite(options.contactHeightTolerancePx ?? 3, 'contactHeightTolerancePx'));
    const velocityTolerance = Math.max(0, finite(options.contactVelocityTolerancePx ?? 2, 'contactVelocityTolerancePx'));
    const weight = finite(options.contactWeight ?? 1, 'contactWeight');
    const recomputed = [];
    order.forEach((label) => {
        const anchorId = `${label}.hoof`;
        const points = orderedPoints(byAnchor.get(anchorId), frameCount);
        const visible = points.filter((point) => point.visible);
        const groundY = quantile(visible.map((point) => point.y), 0.9);
        const frames = [];
        points.forEach((point, frame) => {
            if (!point.visible || Math.abs(point.y - groundY) > heightTolerance) return;
            const previous = points[modulo(frame - 1, frameCount)];
            const next = points[modulo(frame + 1, frameCount)];
            const velocities = [previous, next]
                .filter((neighbor) => neighbor.visible)
                .map((neighbor) => Math.hypot(point.x - neighbor.x, point.y - neighbor.y));
            const velocity = velocities.length ? Math.min(...velocities) : Infinity;
            if (velocity <= velocityTolerance) frames.push(frame);
        });
        if (frames.length) {
            recomputed.push({
                anchor_id: anchorId,
                frames,
                ground_height: groundY,
                weight,
            });
        }
    });
    return [...preserved, ...recomputed];
}

function warpTrack(track, ordered, sourceFrameByDestination, frameCount) {
    const points = Array.from({ length: frameCount }, (_, destinationFrame) => {
        const sourceFrame = sourceFrameByDestination[destinationFrame];
        return { ...cloneValue(ordered[sourceFrame]), frame: destinationFrame };
    });
    return {
        ...cloneValue(track),
        query_frame: points.find((point) => point.visible)?.frame ?? 0,
        points,
    };
}

function cyclicRuns(mask) {
    const runs = [];
    let active = null;
    mask.forEach((value, frame) => {
        if (value && !active) {
            active = [frame];
            runs.push(active);
        } else if (value) {
            active.push(frame);
        } else {
            active = null;
        }
    });
    if (runs.length > 1 && mask[0] && mask.at(-1)) {
        const first = runs.shift();
        const last = runs.pop();
        runs.unshift([...last, ...first]);
    }
    return runs;
}

function resampleOrderedFrames(frames, count, requiredFrame = null) {
    if (!Array.isArray(frames) || !frames.length || count <= 0) {
        throw new Error('phase template resampling needs source and destination frames');
    }
    if (count === 1) return [requiredFrame ?? frames[Math.floor(frames.length / 2)]];
    const requiredIndex = requiredFrame === null ? -1 : frames.indexOf(requiredFrame);
    if (requiredIndex < 0) {
        return Array.from({ length: count }, (_, index) => (
            frames[Math.round(index * (frames.length - 1) / (count - 1))]
        ));
    }
    const targetRequiredIndex = Math.round(requiredIndex * (count - 1) / Math.max(1, frames.length - 1));
    return Array.from({ length: count }, (_, index) => {
        if (index === targetRequiredIndex) return requiredFrame;
        if (index < targetRequiredIndex) {
            if (targetRequiredIndex === 0) return frames[0];
            return frames[Math.round(index * requiredIndex / targetRequiredIndex)];
        }
        const destinationSpan = count - 1 - targetRequiredIndex;
        if (destinationSpan === 0) return frames.at(-1);
        return frames[Math.round(
            requiredIndex + (index - targetRequiredIndex) * (frames.length - 1 - requiredIndex) / destinationSpan,
        )];
    });
}

function circularCenterFrame(frames, frameCount) {
    let x = 0;
    let y = 0;
    frames.forEach((frame) => {
        const angle = frame / frameCount * Math.PI * 2;
        x += Math.cos(angle);
        y += Math.sin(angle);
    });
    if (Math.hypot(x, y) <= 1e-9) return null;
    const phase = modulo(Math.atan2(y, x) / (Math.PI * 2), 1);
    return modulo(Math.round(phase * frameCount), frameCount);
}

function analyzeSwingTemplate(points, rawLimbQa, frameCount, options, label) {
    const visible = points.filter((point) => point.visible);
    const groundY = quantile(visible.map((point) => point.y), 0.9);
    const maximumLiftPx = Math.max(...visible.map((point) => Math.max(0, groundY - point.y)));
    const minimumLiftPx = Math.max(0, finite(
        options.minimumLiftPx ?? rawLimbQa?.swingThresholdPx ?? 3,
        'minimumLiftPx',
    ));
    const threshold = Math.max(minimumLiftPx, maximumLiftPx * 0.3);
    const lifts = points.map((point) => point.visible ? Math.max(0, groundY - point.y) : 0);
    const swingMask = lifts.map((lift, frame) => Boolean(points[frame].visible && lift >= threshold));
    const runs = cyclicRuns(swingMask).sort((a, b) => b.length - a.length);
    const principal = runs[0] || [];
    const secondary = runs.slice(1).flat();
    const minimumSwingFrames = Math.max(2, Math.trunc(finite(
        options.minimumSwingFrames ?? 2,
        'minimumSwingFrames',
    )));
    const maximumDiscarded = Math.max(0, Math.trunc(finite(
        options.maximumDiscardedSecondarySwingFrames ?? Math.floor(frameCount / 3),
        'maximumDiscardedSecondarySwingFrames',
    )));
    if (maximumLiftPx < minimumLiftPx || principal.length < minimumSwingFrames) {
        throw new Error(`${label} has no meaningful swing lift template`);
    }
    if (secondary.length > maximumDiscarded) {
        throw new Error(`${label} has too many secondary swing frames to repair safely`);
    }
    const centerFrame = circularCenterFrame(principal, frameCount);
    if (centerFrame === null) throw new Error(`${label} swing template has no stable phase center`);
    const peakFrame = principal.reduce((best, frame) => (
        lifts[frame] > lifts[best] ? frame : best
    ), principal[0]);
    const stance = [];
    const swingSet = new Set(runs.flat());
    const stanceStart = modulo(principal.at(-1) + 1, frameCount);
    for (let offset = 0; offset < frameCount; offset += 1) {
        const frame = modulo(stanceStart + offset, frameCount);
        if (!swingSet.has(frame)) stance.push(frame);
    }
    if (stance.length < Math.ceil(frameCount / 2)) {
        throw new Error(`${label} does not contain a meaningful stance template`);
    }
    return {
        principal,
        secondary,
        stance,
        centerFrame,
        peakFrame,
        maximumLiftPx,
        threshold,
    };
}

function assertRepairableTemplates(rawQa, validation, order, frameCount, options) {
    const minimumSwingFrames = Math.max(2, Math.trunc(finite(
        options.minimumSwingFrames ?? 2,
        'minimumSwingFrames',
    )));
    const maximumSwingFrames = Math.max(minimumSwingFrames, Math.trunc(finite(
        options.maximumSwingFrames ?? Math.floor(frameCount / 4) - 1,
        'maximumSwingFrames',
    )));
    const templates = {};
    order.forEach((label) => {
        const limb = rawQa.limbs?.[label];
        if (limb?.visibleFraction < rawQa.gates.minimumVisibleFraction) {
            throw new Error(`${label} has a hard hoof visibility failure`);
        }
        const template = analyzeSwingTemplate(
            validation.orderedByAnchor.get(`${label}.hoof`),
            limb,
            frameCount,
            options,
            label,
        );
        if (template.principal.length < minimumSwingFrames || template.principal.length > maximumSwingFrames) {
            throw new Error(`${label} swing template cannot fit one Horse walk quarter`);
        }
        templates[label] = template;
    });
    return templates;
}

function phaseMapping(template, targetCenter, frameCount, options) {
    const maximumTargetSwingFrames = Math.max(2, Math.trunc(finite(
        options.maximumTargetSwingFrames ?? Math.floor(frameCount / 4) - 2,
        'maximumTargetSwingFrames',
    )));
    const targetSwingLength = Math.min(template.principal.length, maximumTargetSwingFrames);
    const swingStart = modulo(targetCenter - Math.floor(targetSwingLength / 2), frameCount);
    const targetSwingFrames = Array.from({ length: targetSwingLength }, (_, index) => (
        modulo(swingStart + index, frameCount)
    ));
    const targetSwingSet = new Set(targetSwingFrames);
    const targetStanceFrames = [];
    const stanceStart = modulo(targetSwingFrames.at(-1) + 1, frameCount);
    for (let offset = 0; offset < frameCount; offset += 1) {
        const frame = modulo(stanceStart + offset, frameCount);
        if (!targetSwingSet.has(frame)) targetStanceFrames.push(frame);
    }
    const swingSamples = resampleOrderedFrames(template.principal, targetSwingLength, template.peakFrame);
    const stanceSamples = resampleOrderedFrames(template.stance, targetStanceFrames.length);
    const sourceFrameByDestination = Array(frameCount).fill(null);
    targetSwingFrames.forEach((frame, index) => { sourceFrameByDestination[frame] = swingSamples[index]; });
    targetStanceFrames.forEach((frame, index) => { sourceFrameByDestination[frame] = stanceSamples[index]; });
    if (sourceFrameByDestination.some((frame) => !Number.isInteger(frame))) {
        throw new Error('phase template did not map every destination frame');
    }
    return { sourceFrameByDestination, targetSwingFrames, targetStanceFrames };
}

function normalizationProvenance(mode, order, rawQa, normalizedQa, limbs = {}) {
    return {
        schema: NORMALIZATION_SCHEMA,
        mode,
        algorithm: mode === 'passthrough'
            ? 'raw_gait_accepted_no_transform'
            : 'deterministic_per_limb_piecewise_phase_template_warp',
        expectedOrder: [...order],
        frameCount: REQUIRED_FRAME_COUNT,
        rawAccepted: Boolean(rawQa.accepted),
        normalizedAccepted: Boolean(normalizedQa.accepted),
        limbs: cloneValue(limbs),
        invariants: {
            pointSampling: mode === 'passthrough'
                ? 'unchanged'
                : 'source_sample_piecewise_phase_resampling_no_interpolation',
            visibilityConfidencePreserved: true,
            fabricatedSpatialExtrema: false,
            contacts: mode === 'passthrough' ? 'unchanged' : 'recomputed_from_normalized_hooves',
        },
    };
}

/**
 * Repair only gait phase/order defects. Identity, visibility and source motion
 * must already be usable; spatial samples are selected from the source cycle,
 * never interpolated or extrapolated.
 */
export function normalizeHorseWalkPhases(observationsValue, options = {}) {
    if (!observationsValue || observationsValue.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const frameCount = Number(observationsValue.frame_count);
    if (frameCount !== REQUIRED_FRAME_COUNT) {
        throw new Error(`Horse phase normalization requires exactly ${REQUIRED_FRAME_COUNT} frames`);
    }
    const order = expectedOrder(options.expectedOrder);
    const validation = validateIdentityAndVisibility(observationsValue, order, options);
    const gaitOptions = {
        ...(options.gaitQaOptions || {}),
        expectedOrder: order,
    };
    const rawGaitQa = assessHorseWalkGait(observationsValue, gaitOptions);
    if (rawGaitQa.accepted) {
        const observations = cloneValue(observationsValue);
        const provenance = normalizationProvenance('passthrough', order, rawGaitQa, rawGaitQa);
        return {
            schema: NORMALIZATION_SCHEMA,
            observations,
            rawGaitQa,
            normalizedGaitQa: cloneValue(rawGaitQa),
            provenance,
        };
    }

    const templates = assertRepairableTemplates(rawGaitQa, validation, order, frameCount, options);
    const baseCenter = templates[order[0]].centerFrame;
    const limbProvenance = {};
    const mappings = {};
    order.forEach((label, index) => {
        const targetCenterFrame = modulo(Math.round(baseCenter + index * frameCount / 4), frameCount);
        const mapping = phaseMapping(templates[label], targetCenterFrame, frameCount, options);
        mappings[label] = mapping.sourceFrameByDestination;
        limbProvenance[label] = {
            sourceCenterFrame: templates[label].centerFrame,
            targetCenterFrame,
            rawPrincipalSwingFrames: [...templates[label].principal],
            discardedSecondarySwingFrames: [...templates[label].secondary],
            normalizedSwingFrames: [...mapping.targetSwingFrames],
            sourceFrameByDestination: [...mapping.sourceFrameByDestination],
        };
    });

    const targetAnchors = new Set(order.flatMap((label) => TRACK_ROLES.map((role) => `${label}.${role}`)));
    const observations = {
        ...cloneValue(observationsValue),
        tracks: observationsValue.tracks.map((track) => {
            if (!targetAnchors.has(track.anchor_id)) return cloneValue(track);
            const label = String(track.anchor_id).split('.')[0];
            return warpTrack(
                track,
                validation.orderedByAnchor.get(track.anchor_id),
                mappings[label],
                frameCount,
            );
        }),
    };
    validateIdentityAndVisibility(observations, order, options);
    observations.contacts = recomputeContacts(observations, order, options);
    const strictGaitOptions = {
        ...gaitOptions,
        maximumSimultaneousSwingFrames: 0,
    };
    const normalizedGaitQa = assessHorseWalkGait(observations, strictGaitOptions);
    if (!normalizedGaitQa.accepted
        || normalizedGaitQa.simultaneousSwingFrameCount !== 0
        || order.some((label) => Number(normalizedGaitQa.limbs[label]?.secondarySwingFrames) !== 0)) {
        throw new Error('Horse phase normalization could not produce four non-overlapping swing windows');
    }
    const provenance = normalizationProvenance(
        'circular_rephase',
        order,
        rawGaitQa,
        normalizedGaitQa,
        limbProvenance,
    );
    observations.provenance = {
        ...cloneValue(observations.provenance || {}),
        horse_walk_phase_normalization: cloneValue(provenance),
    };
    return {
        schema: NORMALIZATION_SCHEMA,
        observations,
        rawGaitQa,
        normalizedGaitQa,
        provenance,
    };
}

export const HORSE_WALK_PHASE_NORMALIZATION = Object.freeze({
    schema: NORMALIZATION_SCHEMA,
    frameCount: REQUIRED_FRAME_COUNT,
    expectedOrder: DEFAULT_ORDER,
});
