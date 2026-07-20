import { fitBrowserAnimation } from './animation-fitting-browser-core.js?v=3';
import { assessHorseWalkGait } from './animation-fitting-semantic-tracker.js?v=1';

const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';
const GROUND_EVIDENCE_SCHEMA = 'autorig-browser-sam2-ground-evidence.v1';
const CONTACT_SCHEDULE_SCHEMA = 'autorig-browser-hoof-contact-schedule.v1';
const CONTACT_REFIT_PROVENANCE_SCHEMA = 'autorig-browser-contact-refit-provenance.v1';
const TRACKER_BACKEND = 'google-deepmind-tapnextpp-online';
const SEGMENTER_BACKEND = 'facebookresearch-sam2.1-video';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

const WALK_FOOT_ORDER = Object.freeze([
    'hind_left',
    'fore_left',
    'hind_right',
    'fore_right',
]);

const DEFAULTS = Object.freeze({
    loop: true,
    foregroundThreshold: 127,
    localMaskRadiusRatio: 0.015,
    minimumForegroundFraction: 0.002,
    maximumForegroundFraction: 0.75,
    minimumVisibleFraction: 0.90,
    maximumOcclusionFrames: 2,
    maximumLoopSeamRatio: 0.04,
    smoothingRadius: 1,
    contactHeightRatio: 0.025,
    swingHeightRatio: 0.065,
    contactVerticalSpeedRatioPerFrame: 0.012,
    swingVerticalSpeedRatioPerFrame: 0.030,
    contactPlanarSpeedRatioPerFrame: 0.012,
    swingPlanarSpeedRatioPerFrame: 0.025,
    maximumLocalMaskGapRatio: 0.12,
    maximumContactGapFrames: 2,
    minimumContactFrames: 5,
    minimumDutyFactor: 0.38,
    maximumDutyFactor: 0.80,
    minimumTouchdownGapPhase: 0.10,
    maximumTouchdownGapPhase: 0.40,
    // A controller-ready four-beat walk must never enter a one-hoof support
    // or aerial phase.  Two planted hooves is the biomechanical floor; three
    // is the normal transfer state and a brief four-hoof overlap is gated
    // separately below.
    minimumSupportFeet: 2,
    maximumFourSupportFrames: 3,
    maximumObservedContactStepRatio: 0.030,
    maximumObservedContactP95StepRatio: 0.020,
    maximumObservedContactSpanRatio: 0.10,
    maximumFittedContactSlideRatio: 0.002,
});

const CONTACT_REFIT_THRESHOLDS = Object.freeze({
    contactWeight: 1,
    minimumDutyFactor: DEFAULTS.minimumDutyFactor,
    maximumDutyFactor: DEFAULTS.maximumDutyFactor,
    minimumTouchdownGapPhase: DEFAULTS.minimumTouchdownGapPhase,
    maximumTouchdownGapPhase: DEFAULTS.maximumTouchdownGapPhase,
    minimumSupportFeet: 3,
    maximumFourSupportFrames: DEFAULTS.maximumFourSupportFrames,
    maximumFittedContactSlideRatio: DEFAULTS.maximumFittedContactSlideRatio,
});

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function sha256(value, field) {
    if (typeof value !== 'string' || !SHA256_PATTERN.test(value)) {
        throw new Error(`${field} must be a lowercase SHA-256`);
    }
    return value;
}

function finite(value, field) {
    const result = Number(value);
    if (!Number.isFinite(result)) throw new Error(`${field} must be finite`);
    return result;
}

function integer(value, field, minimum = 0) {
    if (!Number.isInteger(value) || value < minimum) {
        throw new Error(`${field} must be an integer of at least ${minimum}`);
    }
    return value;
}

function ratio(value, field, { positive = false } = {}) {
    const result = finite(value, field);
    if ((positive ? result <= 0 : result < 0) || result > 1) {
        throw new Error(`${field} must be ${positive ? 'inside (0, 1]' : 'inside [0, 1]'}`);
    }
    return result;
}

function clamp(value, minimum, maximum) {
    return Math.min(maximum, Math.max(minimum, value));
}

function median(values) {
    if (!values.length) throw new Error('median requires at least one value');
    const ordered = [...values].sort((first, second) => first - second);
    const middle = Math.floor(ordered.length / 2);
    return ordered.length % 2
        ? ordered[middle]
        : (ordered[middle - 1] + ordered[middle]) / 2;
}

function quantile(values, fraction) {
    if (!values.length) throw new Error('quantile requires at least one value');
    const ordered = [...values].sort((first, second) => first - second);
    const index = (ordered.length - 1) * clamp(fraction, 0, 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    if (lower === upper) return ordered[lower];
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower);
}

function distance(first, second) {
    return Math.hypot(first[0] - second[0], first[1] - second[1]);
}

function circularIndex(index, length) {
    return ((index % length) + length) % length;
}

function circularRuns(mask) {
    const length = mask.length;
    if (!length || !mask.some(Boolean)) return [];
    if (mask.every(Boolean)) return [Array.from({ length }, (_, index) => index)];
    const firstFalse = mask.findIndex((value) => !value);
    const runs = [];
    let current = [];
    for (let offset = 1; offset <= length; offset += 1) {
        const index = (firstFalse + offset) % length;
        if (mask[index]) current.push(index);
        else if (current.length) {
            runs.push(current);
            current = [];
        }
    }
    if (current.length) runs.push(current);
    return runs;
}

function longestCircularFalseRun(mask) {
    if (mask.every(Boolean)) return 0;
    if (!mask.some(Boolean)) return mask.length;
    return Math.max(...circularRuns(mask.map((value) => !value)).map((run) => run.length));
}

function circularMedian(values, radius) {
    if (!radius) return [...values];
    return values.map((_, frame) => median(Array.from(
        { length: radius * 2 + 1 },
        (_unused, offset) => values[circularIndex(frame + offset - radius, values.length)],
    )));
}

function normalizeOptions(value = {}) {
    const options = { ...DEFAULTS, ...object(value, 'options') };
    if (typeof options.loop !== 'boolean') throw new Error('options.loop must be boolean');
    if (!options.loop) throw new Error('hoof-contact inference supports loop=true walk clips only');
    options.foregroundThreshold = integer(options.foregroundThreshold, 'options.foregroundThreshold');
    if (options.foregroundThreshold > 255) throw new Error('options.foregroundThreshold must not exceed 255');
    [
        'localMaskRadiusRatio',
        'minimumForegroundFraction',
        'maximumForegroundFraction',
        'minimumVisibleFraction',
        'maximumLoopSeamRatio',
        'contactHeightRatio',
        'swingHeightRatio',
        'contactVerticalSpeedRatioPerFrame',
        'swingVerticalSpeedRatioPerFrame',
        'contactPlanarSpeedRatioPerFrame',
        'swingPlanarSpeedRatioPerFrame',
        'maximumLocalMaskGapRatio',
        'minimumDutyFactor',
        'maximumDutyFactor',
        'minimumTouchdownGapPhase',
        'maximumTouchdownGapPhase',
        'maximumObservedContactStepRatio',
        'maximumObservedContactP95StepRatio',
        'maximumObservedContactSpanRatio',
        'maximumFittedContactSlideRatio',
    ].forEach((name) => { options[name] = ratio(options[name], `options.${name}`, { positive: true }); });
    ['maximumOcclusionFrames', 'smoothingRadius', 'maximumContactGapFrames',
        'minimumContactFrames', 'minimumSupportFeet', 'maximumFourSupportFrames']
        .forEach((name) => { options[name] = integer(options[name], `options.${name}`); });
    if (options.minimumForegroundFraction >= options.maximumForegroundFraction) {
        throw new Error('foreground fraction bounds are inverted');
    }
    if (options.contactHeightRatio >= options.swingHeightRatio) {
        throw new Error('contactHeightRatio must be below swingHeightRatio');
    }
    if (options.contactVerticalSpeedRatioPerFrame >= options.swingVerticalSpeedRatioPerFrame) {
        throw new Error('contact vertical-speed threshold must be below swing threshold');
    }
    if (options.contactPlanarSpeedRatioPerFrame >= options.swingPlanarSpeedRatioPerFrame) {
        throw new Error('contact planar-speed threshold must be below swing threshold');
    }
    if (options.minimumDutyFactor >= options.maximumDutyFactor) {
        throw new Error('duty-factor bounds are inverted');
    }
    if (options.minimumTouchdownGapPhase >= options.maximumTouchdownGapPhase) {
        throw new Error('touchdown-gap bounds are inverted');
    }
    return options;
}

function normalizePoint(pointValue, field, frameCount) {
    const point = object(pointValue, field);
    const frame = integer(point.frame, `${field}.frame`);
    if (frame >= frameCount) throw new Error(`${field}.frame is outside the observation range`);
    if (typeof point.visible !== 'boolean') throw new Error(`${field}.visible must be boolean`);
    const confidence = finite(point.confidence ?? (point.visible ? 1 : 0), `${field}.confidence`);
    if (confidence < 0 || confidence > 1) throw new Error(`${field}.confidence must be inside [0, 1]`);
    return {
        frame,
        x: finite(point.x, `${field}.x`),
        y: finite(point.y, `${field}.y`),
        visible: point.visible,
        confidence,
    };
}

function normalizeObservations(value) {
    const observations = object(value, 'observations');
    if (observations.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const frameCount = integer(observations.frame_count, 'observations.frame_count', 8);
    const width = integer(observations.width, 'observations.width', 1);
    const height = integer(observations.height, 'observations.height', 1);
    const fps = finite(observations.fps, 'observations.fps');
    if (fps <= 0) throw new Error('observations.fps must be positive');
    if (!Array.isArray(observations.tracks)) throw new Error('observations.tracks must be an array');
    const tracks = new Map();
    observations.tracks.forEach((trackValue, trackIndex) => {
        const field = `observations.tracks[${trackIndex}]`;
        const track = object(trackValue, field);
        if (typeof track.anchor_id !== 'string' || !track.anchor_id) {
            throw new Error(`${field}.anchor_id must be a non-empty string`);
        }
        if (tracks.has(track.anchor_id)) throw new Error(`duplicate observation track ${track.anchor_id}`);
        if (!Array.isArray(track.points) || track.points.length !== frameCount) {
            throw new Error(`${track.anchor_id} must contain exactly ${frameCount} points`);
        }
        const points = Array(frameCount);
        track.points.forEach((point, pointIndex) => {
            const normalized = normalizePoint(point, `${field}.points[${pointIndex}]`, frameCount);
            if (points[normalized.frame]) throw new Error(`${track.anchor_id} repeats frame ${normalized.frame}`);
            points[normalized.frame] = normalized;
        });
        if (points.some((point) => !point)) throw new Error(`${track.anchor_id} has a missing frame`);
        tracks.set(track.anchor_id, points);
    });
    WALK_FOOT_ORDER.forEach((foot) => {
        [`${foot}.proximal`, `${foot}.hoof`].forEach((anchorId) => {
            if (!tracks.has(anchorId)) throw new Error(`observations are missing exact semantic track ${anchorId}`);
        });
    });
    const provenance = object(observations.provenance, 'observations.provenance');
    if (provenance.tracker?.backend !== TRACKER_BACKEND) {
        throw new Error(`observations tracker backend must be ${TRACKER_BACKEND}`);
    }
    if (provenance.segmenter?.backend !== SEGMENTER_BACKEND) {
        throw new Error(`observations segmenter backend must be ${SEGMENTER_BACKEND}`);
    }
    const sourceVideoSha256 = provenance.source_video_sha256;
    if (typeof sourceVideoSha256 !== 'string' || !SHA256_PATTERN.test(sourceVideoSha256)) {
        throw new Error('observations.provenance.source_video_sha256 must be a lowercase SHA-256');
    }
    return { observations, frameCount, width, height, fps, tracks, sourceVideoSha256 };
}

function normalizeMask(maskValue, expectedFrame, width, height) {
    const mask = object(maskValue, `masks[${expectedFrame}]`);
    if (integer(mask.frame, `masks[${expectedFrame}].frame`) !== expectedFrame) {
        throw new Error(`masks[${expectedFrame}].frame does not preserve chronological identity`);
    }
    if (integer(mask.width, `masks[${expectedFrame}].width`, 1) !== width
        || integer(mask.height, `masks[${expectedFrame}].height`, 1) !== height) {
        throw new Error(`masks[${expectedFrame}] resolution does not match observations`);
    }
    const channels = integer(mask.channels ?? 1, `masks[${expectedFrame}].channels`, 1);
    if (![1, 4].includes(channels)) throw new Error('SAM2 masks must use one or four channels');
    const data = mask.data;
    if (!data || typeof data.length !== 'number' || data.length !== width * height * channels) {
        throw new Error(`masks[${expectedFrame}].data has an invalid length`);
    }
    return { frame: expectedFrame, width, height, channels, data };
}

function foregroundAt(mask, pixelIndex, threshold) {
    const offset = pixelIndex * mask.channels;
    if (mask.channels === 1) return Number(mask.data[offset]) > threshold;
    return Number(mask.data[offset + 3]) > 0
        && Math.max(Number(mask.data[offset]), Number(mask.data[offset + 1]), Number(mask.data[offset + 2])) > threshold;
}

function scanMask(mask, threshold, minimumFraction, maximumFraction) {
    let left = mask.width;
    let right = -1;
    let top = mask.height;
    let bottom = -1;
    let foregroundPixels = 0;
    const columnBottoms = new Int32Array(mask.width).fill(-1);
    for (let y = 0; y < mask.height; y += 1) {
        const row = y * mask.width;
        for (let x = 0; x < mask.width; x += 1) {
            if (!foregroundAt(mask, row + x, threshold)) continue;
            foregroundPixels += 1;
            left = Math.min(left, x);
            right = Math.max(right, x);
            top = Math.min(top, y);
            bottom = Math.max(bottom, y);
            columnBottoms[x] = y;
        }
    }
    const fraction = foregroundPixels / (mask.width * mask.height);
    if (fraction < minimumFraction || fraction > maximumFraction || bottom < top || right < left) {
        throw new Error(`SAM2 mask frame ${mask.frame} foreground fraction ${fraction.toFixed(6)} is outside contract`);
    }
    return {
        bbox: { left, right, top, bottom, width: right - left + 1, height: bottom - top + 1 },
        foregroundPixels,
        foregroundFraction: fraction,
        columnBottoms,
    };
}

/**
 * Convert decoded SAM2 masks into evidence pinned to exact semantic hoof tracks.
 * Browser callers can supply ImageData (`channels: 4`); the Node diagnostic
 * supplies the same masks decoded as grayscale (`channels: 1`).
 */
export function deriveSam2GroundEvidence({ observations: observationValue, masks, options: optionValue = {} } = {}) {
    const normalized = normalizeObservations(observationValue);
    const options = normalizeOptions(optionValue);
    if (!Array.isArray(masks) || masks.length !== normalized.frameCount) {
        throw new Error(`masks must contain exactly ${normalized.frameCount} chronological frames`);
    }
    const radius = Math.max(2, Math.round(normalized.width * options.localMaskRadiusRatio));
    const frames = masks.map((maskValue, frame) => {
        const mask = normalizeMask(maskValue, frame, normalized.width, normalized.height);
        const scanned = scanMask(
            mask,
            options.foregroundThreshold,
            options.minimumForegroundFraction,
            options.maximumForegroundFraction,
        );
        const hooves = {};
        WALK_FOOT_ORDER.forEach((foot) => {
            const anchorId = `${foot}.hoof`;
            const point = normalized.tracks.get(anchorId)[frame];
            const center = clamp(Math.round(point.x), 0, normalized.width - 1);
            const candidates = [];
            for (let x = Math.max(0, center - radius); x <= Math.min(normalized.width - 1, center + radius); x += 1) {
                if (scanned.columnBottoms[x] >= 0) candidates.push(scanned.columnBottoms[x]);
            }
            if (!candidates.length) {
                throw new Error(`SAM2 mask frame ${frame} has no local foreground near exact track ${anchorId}`);
            }
            const localBottomY = Math.max(...candidates);
            hooves[foot] = {
                anchorId,
                sourcePoint: [point.x, point.y],
                sourceVisible: point.visible,
                sourceConfidence: point.confidence,
                localBottomY,
                globalBottomY: scanned.bbox.bottom,
                localBottomGapPx: localBottomY - point.y,
                globalBottomGapPx: scanned.bbox.bottom - point.y,
            };
        });
        return {
            frame,
            bbox: scanned.bbox,
            foregroundPixels: scanned.foregroundPixels,
            foregroundFraction: scanned.foregroundFraction,
            hooves,
        };
    });
    return {
        schema: GROUND_EVIDENCE_SCHEMA,
        frame_count: normalized.frameCount,
        width: normalized.width,
        height: normalized.height,
        foot_order: [...WALK_FOOT_ORDER],
        frames,
        provenance: {
            sourceVideoSha256: normalized.sourceVideoSha256,
            segmenterBackend: SEGMENTER_BACKEND,
            localMaskRadiusPx: radius,
        },
    };
}

function normalizeGroundEvidence(value, observations) {
    const evidence = object(value, 'groundEvidence');
    if (evidence.schema !== GROUND_EVIDENCE_SCHEMA) {
        throw new Error(`groundEvidence.schema must be ${GROUND_EVIDENCE_SCHEMA}`);
    }
    if (evidence.frame_count !== observations.frameCount
        || evidence.width !== observations.width
        || evidence.height !== observations.height) {
        throw new Error('groundEvidence dimensions do not match observations');
    }
    if (evidence.provenance?.sourceVideoSha256 !== observations.sourceVideoSha256) {
        throw new Error('groundEvidence source-video pin does not match observations');
    }
    if (evidence.provenance?.segmenterBackend !== SEGMENTER_BACKEND) {
        throw new Error(`groundEvidence segmenter must be ${SEGMENTER_BACKEND}`);
    }
    if (!Array.isArray(evidence.foot_order)
        || evidence.foot_order.length !== WALK_FOOT_ORDER.length
        || evidence.foot_order.some((foot, index) => foot !== WALK_FOOT_ORDER[index])) {
        throw new Error('groundEvidence changed exact left/right horse foot identities');
    }
    if (!Array.isArray(evidence.frames) || evidence.frames.length !== observations.frameCount) {
        throw new Error('groundEvidence must cover every observation frame');
    }
    return evidence.frames.map((frameValue, frame) => {
        const row = object(frameValue, `groundEvidence.frames[${frame}]`);
        if (row.frame !== frame) throw new Error('groundEvidence frame order is not chronological');
        const bbox = object(row.bbox, `groundEvidence.frames[${frame}].bbox`);
        const bboxHeight = finite(bbox.height, `groundEvidence.frames[${frame}].bbox.height`);
        if (bboxHeight <= 0) throw new Error('groundEvidence bbox height must be positive');
        const hooves = object(row.hooves, `groundEvidence.frames[${frame}].hooves`);
        const normalizedHooves = {};
        WALK_FOOT_ORDER.forEach((foot) => {
            const hoof = object(hooves[foot], `groundEvidence.frames[${frame}].hooves.${foot}`);
            if (hoof.anchorId !== `${foot}.hoof`) throw new Error(`groundEvidence remapped ${foot}`);
            const point = observations.tracks.get(hoof.anchorId)[frame];
            if (!Array.isArray(hoof.sourcePoint) || hoof.sourcePoint.length !== 2
                || Math.abs(finite(hoof.sourcePoint[0], 'sourcePoint[0]') - point.x) > 1e-6
                || Math.abs(finite(hoof.sourcePoint[1], 'sourcePoint[1]') - point.y) > 1e-6) {
                throw new Error(`groundEvidence ${foot} source point is not bound to current observations`);
            }
            normalizedHooves[foot] = {
                localBottomY: finite(hoof.localBottomY, `${foot}.localBottomY`),
                globalBottomY: finite(hoof.globalBottomY, `${foot}.globalBottomY`),
                localBottomGapPx: finite(hoof.localBottomGapPx, `${foot}.localBottomGapPx`),
                globalBottomGapPx: finite(hoof.globalBottomGapPx, `${foot}.globalBottomGapPx`),
            };
        });
        return { frame, bboxHeight, hooves: normalizedHooves };
    });
}

function fillShortCircularGaps(points, maximumGap, field) {
    const result = points.map((point) => (point ? [...point] : null));
    const visible = result.map(Boolean);
    const longestGap = longestCircularFalseRun(visible);
    if (longestGap > maximumGap) {
        throw new Error(`${field} has an occlusion gap of ${longestGap} frames; maximum is ${maximumGap}`);
    }
    circularRuns(visible.map((value) => !value)).forEach((run) => {
        const before = circularIndex(run[0] - 1, result.length);
        const after = circularIndex(run.at(-1) + 1, result.length);
        if (!result[before] || !result[after]) throw new Error(`${field} cannot interpolate an unbounded gap`);
        run.forEach((frame, offset) => {
            const alpha = (offset + 1) / (run.length + 1);
            result[frame] = [
                result[before][0] + (result[after][0] - result[before][0]) * alpha,
                result[before][1] + (result[after][1] - result[before][1]) * alpha,
            ];
        });
    });
    return { points: result, visible, longestGap };
}

function expandHysteresis(strong, retain) {
    const contact = [...strong];
    const queue = strong.flatMap((value, index) => (value ? [index] : []));
    while (queue.length) {
        const frame = queue.shift();
        for (const neighbor of [circularIndex(frame - 1, contact.length), circularIndex(frame + 1, contact.length)]) {
            if (!contact[neighbor] && retain[neighbor]) {
                contact[neighbor] = true;
                queue.push(neighbor);
            }
        }
    }
    return contact;
}

function bridgeShortContactGaps(contact, retain, maximumGap) {
    const result = [...contact];
    circularRuns(result.map((value) => !value)).forEach((run) => {
        if (run.length <= maximumGap && run.every((frame) => retain[frame])) {
            run.forEach((frame) => { result[frame] = true; });
        }
    });
    return result;
}

function orderedRunSteps(run, points) {
    const steps = [];
    for (let index = 1; index < run.length; index += 1) {
        steps.push(distance(points[run[index - 1]], points[run[index]]));
    }
    return steps;
}

function summarizeContactRun(run, points, characteristicHeightPx) {
    const steps = orderedRunSteps(run, points);
    const pin = [
        median(run.map((frame) => points[frame][0])),
        median(run.map((frame) => points[frame][1])),
    ];
    const spans = run.map((frame) => distance(points[frame], pin));
    return {
        frames: [...run],
        touchdownFrame: run[0],
        liftoffFrame: circularIndex(run.at(-1) + 1, points.length),
        dutyFactor: run.length / points.length,
        slide: {
            samples: steps.length,
            maximumStepPx: steps.length ? Math.max(...steps) : 0,
            p95StepPx: steps.length ? quantile(steps, 0.95) : 0,
            maximumSpanPx: spans.length ? Math.max(...spans) : 0,
            maximumStepRatio: steps.length ? Math.max(...steps) / characteristicHeightPx : 0,
            p95StepRatio: steps.length ? quantile(steps, 0.95) / characteristicHeightPx : 0,
            maximumSpanRatio: spans.length ? Math.max(...spans) / characteristicHeightPx : 0,
        },
    };
}

function analyzeFoot(foot, observations, groundFrames, uniqueCount, options) {
    const track = observations.tracks.get(`${foot}.hoof`).slice(0, uniqueCount);
    const visibleCount = track.filter((point) => point.visible).length;
    const visibleFraction = visibleCount / uniqueCount;
    const rawPoints = track.map((point) => (point.visible ? [point.x, point.y] : null));
    const rawVisibility = rawPoints.map(Boolean);
    const rawLongestGap = longestCircularFalseRun(rawVisibility);
    const bboxHeights = groundFrames.slice(0, uniqueCount).map((frame) => frame.bboxHeight);
    const characteristicHeightPx = median(bboxHeights);
    if (visibleFraction < options.minimumVisibleFraction || rawLongestGap > options.maximumOcclusionFrames) {
        const failures = [];
        if (visibleFraction < options.minimumVisibleFraction) failures.push('insufficient_visibility');
        if (rawLongestGap > options.maximumOcclusionFrames) failures.push('occlusion_gap_too_long');
        failures.push('no_contact_interval');
        return {
            foot,
            anchorId: `${foot}.hoof`,
            visibleFrames: visibleCount,
            visibleFraction,
            longestOcclusionFrames: rawLongestGap,
            characteristicHeightPx,
            floorOffsetPx: null,
            strongEvidenceFrames: [],
            retainEvidenceFrames: [],
            contactFrames: [],
            candidateIntervals: [],
            touchdownFrame: null,
            liftoffFrame: null,
            dutyFactor: 0,
            slide: {
                samples: 0,
                maximumStepPx: null,
                p95StepPx: null,
                maximumSpanPx: null,
                maximumStepRatio: null,
                p95StepRatio: null,
                maximumSpanRatio: null,
            },
            ranges: null,
            failures,
        };
    }
    const filled = fillShortCircularGaps(rawPoints, options.maximumOcclusionFrames, `${foot}.hoof`);
    const rawGroundGaps = groundFrames.slice(0, uniqueCount).map((frame) => frame.hooves[foot].globalBottomGapPx);
    const floorOffsetPx = quantile(
        rawGroundGaps.filter((_gap, frame) => filled.visible[frame]),
        0.10,
    );
    const heightsPx = rawGroundGaps.map((gap) => Math.max(0, gap - floorOffsetPx));
    const smoothedHeight = circularMedian(heightsPx, options.smoothingRadius);
    const smoothedX = circularMedian(filled.points.map((point) => point[0]), options.smoothingRadius);
    const smoothedY = circularMedian(filled.points.map((point) => point[1]), options.smoothingRadius);
    const verticalSpeedRatio = smoothedHeight.map((value, frame) => Math.abs(
        smoothedHeight[circularIndex(frame + 1, uniqueCount)]
        - smoothedHeight[circularIndex(frame - 1, uniqueCount)],
    ) / (2 * characteristicHeightPx));
    const planarSpeedRatio = smoothedHeight.map((_value, frame) => distance(
        [smoothedX[frame], smoothedY[frame]],
        [smoothedX[circularIndex(frame - 1, uniqueCount)], smoothedY[circularIndex(frame - 1, uniqueCount)]],
    ) / characteristicHeightPx);
    const heightRatio = smoothedHeight.map((height) => height / characteristicHeightPx);
    const localMaskGapRatio = groundFrames.slice(0, uniqueCount).map((frame) => (
        Math.abs(frame.hooves[foot].localBottomGapPx) / frame.bboxHeight
    ));
    const reliable = track.map((point, frame) => point.visible
        && point.confidence >= 0.5
        && localMaskGapRatio[frame] <= options.maximumLocalMaskGapRatio);
    const strong = reliable.map((value, frame) => value
        && heightRatio[frame] <= options.contactHeightRatio
        && verticalSpeedRatio[frame] <= options.contactVerticalSpeedRatioPerFrame
        && planarSpeedRatio[frame] <= options.contactPlanarSpeedRatioPerFrame);
    const retain = reliable.map((value, frame) => value
        && heightRatio[frame] <= options.swingHeightRatio
        && verticalSpeedRatio[frame] <= options.swingVerticalSpeedRatioPerFrame
        && planarSpeedRatio[frame] <= options.swingPlanarSpeedRatioPerFrame);
    let contact = bridgeShortContactGaps(
        expandHysteresis(strong, retain),
        retain,
        options.maximumContactGapFrames,
    );
    contact = contact.map((value, frame) => value && reliable[frame]);
    const runs = circularRuns(contact).filter((run) => run.length >= options.minimumContactFrames);
    const candidateIntervals = runs.map((run) => summarizeContactRun(
        run,
        filled.points,
        characteristicHeightPx,
    ));
    const selectedInterval = candidateIntervals.length === 1 ? candidateIntervals[0] : null;
    const selected = selectedInterval?.frames ?? null;
    if (!selected) contact = Array(uniqueCount).fill(false);
    const touchdown = selectedInterval?.touchdownFrame ?? null;
    const liftoff = selectedInterval?.liftoffFrame ?? null;
    const slide = selectedInterval?.slide ?? {
        samples: 0,
        maximumStepPx: null,
        p95StepPx: null,
        maximumSpanPx: null,
        maximumStepRatio: null,
        p95StepRatio: null,
        maximumSpanRatio: null,
    };
    const failures = [];
    if (visibleFraction < options.minimumVisibleFraction) failures.push('insufficient_visibility');
    if (!strong.some(Boolean)) failures.push('no_strong_contact_evidence');
    if (runs.length !== 1) failures.push(runs.length ? 'multiple_contact_intervals' : 'no_contact_interval');
    const dutyFactor = selected ? selected.length / uniqueCount : 0;
    if (selected && (dutyFactor < options.minimumDutyFactor || dutyFactor > options.maximumDutyFactor)) {
        failures.push('duty_factor_out_of_range');
    }
    if (selected && slide.maximumStepRatio > options.maximumObservedContactStepRatio) {
        failures.push('observed_contact_step_too_large');
    }
    if (selected && slide.p95StepRatio > options.maximumObservedContactP95StepRatio) {
        failures.push('observed_contact_p95_step_too_large');
    }
    if (selected && slide.maximumSpanRatio > options.maximumObservedContactSpanRatio) {
        failures.push('observed_contact_span_too_large');
    }
    return {
        foot,
        anchorId: `${foot}.hoof`,
        visibleFrames: visibleCount,
        visibleFraction,
        longestOcclusionFrames: filled.longestGap,
        characteristicHeightPx,
        floorOffsetPx,
        strongEvidenceFrames: strong.flatMap((value, frame) => (value ? [frame] : [])),
        retainEvidenceFrames: retain.flatMap((value, frame) => (value ? [frame] : [])),
        contactFrames: selected ? [...selected] : [],
        candidateIntervals,
        touchdownFrame: touchdown,
        liftoffFrame: liftoff,
        dutyFactor,
        slide,
        ranges: {
            heightRatio: [Math.min(...heightRatio), Math.max(...heightRatio)],
            verticalSpeedRatioPerFrame: [Math.min(...verticalSpeedRatio), Math.max(...verticalSpeedRatio)],
            planarSpeedRatioPerFrame: [Math.min(...planarSpeedRatio), Math.max(...planarSpeedRatio)],
            localMaskGapRatio: [Math.min(...localMaskGapRatio), Math.max(...localMaskGapRatio)],
        },
        failures,
    };
}

function cyclicOrderMatches(actual, expected) {
    if (actual.length !== expected.length) return false;
    return expected.some((foot, offset) => foot === actual[0]
        && actual.every((value, index) => value === expected[(offset + index) % expected.length]));
}

/** Return all evidence and rejected gates without authorizing solver input. */
export function diagnoseHoofContacts({ observations: observationValue, groundEvidence, options: optionValue = {} } = {}) {
    const observations = normalizeObservations(observationValue);
    const options = normalizeOptions(optionValue);
    const groundFrames = normalizeGroundEvidence(groundEvidence, observations);
    const uniqueFrameCount = options.loop ? observations.frameCount - 1 : observations.frameCount;
    if (uniqueFrameCount < 8) throw new Error('hoof-contact inference requires at least eight unique frames');
    const failures = [];
    if (options.loop) {
        WALK_FOOT_ORDER.forEach((foot) => {
            const points = observations.tracks.get(`${foot}.hoof`);
            const height = median(groundFrames.map((frame) => frame.bboxHeight));
            if (!points[0].visible || !points.at(-1).visible
                || distance([points[0].x, points[0].y], [points.at(-1).x, points.at(-1).y]) / height > options.maximumLoopSeamRatio) {
                failures.push(`loop_seam_${foot}`);
            }
        });
    }
    const feet = Object.fromEntries(WALK_FOOT_ORDER.map((foot) => [
        foot,
        analyzeFoot(foot, observations, groundFrames, uniqueFrameCount, options),
    ]));
    WALK_FOOT_ORDER.forEach((foot) => {
        feet[foot].failures.forEach((failure) => failures.push(`${foot}:${failure}`));
    });
    const complete = WALK_FOOT_ORDER.every((foot) => feet[foot].touchdownFrame != null);
    const actualOrder = complete
        ? [...WALK_FOOT_ORDER].sort((first, second) => feet[first].touchdownFrame - feet[second].touchdownFrame)
        : [];
    const touchdownGaps = [];
    if (complete) {
        for (let index = 0; index < actualOrder.length; index += 1) {
            const current = feet[actualOrder[index]].touchdownFrame;
            const next = feet[actualOrder[(index + 1) % actualOrder.length]].touchdownFrame;
            touchdownGaps.push({
                from: actualOrder[index],
                to: actualOrder[(index + 1) % actualOrder.length],
                frames: circularIndex(next - current, uniqueFrameCount),
                phase: circularIndex(next - current, uniqueFrameCount) / uniqueFrameCount,
            });
        }
        if (!cyclicOrderMatches(actualOrder, WALK_FOOT_ORDER)) failures.push('walk_footfall_order');
        if (touchdownGaps.some((gap) => gap.phase < options.minimumTouchdownGapPhase
            || gap.phase > options.maximumTouchdownGapPhase)) {
            failures.push('walk_touchdown_spacing');
        }
    } else {
        failures.push('walk_schedule_incomplete');
    }
    const supportByFrame = Array.from({ length: uniqueFrameCount }, (_, frame) => (
        WALK_FOOT_ORDER.filter((foot) => feet[foot].contactFrames.includes(frame)).length
    ));
    const fourSupportFrames = supportByFrame.filter((count) => count === 4).length;
    const minimumSupport = Math.min(...supportByFrame);
    if (minimumSupport === 0) failures.push('walk_flight_phase');
    else if (minimumSupport < options.minimumSupportFeet) failures.push('walk_insufficient_support');
    if (fourSupportFrames > options.maximumFourSupportFrames) failures.push('walk_excess_four_support');
    const uniqueFailures = [...new Set(failures)];
    const contacts = WALK_FOOT_ORDER.map((foot) => {
        const frames = [...feet[foot].contactFrames];
        if (options.loop && frames.includes(0)) frames.push(uniqueFrameCount);
        return { anchor_id: `${foot}.hoof`, frames: frames.sort((a, b) => a - b), weight: 1 };
    });
    return {
        schema: CONTACT_SCHEDULE_SCHEMA,
        status: uniqueFailures.length ? 'FAIL' : 'PASS',
        frameCount: observations.frameCount,
        uniqueFrameCount,
        fps: observations.fps,
        loop: options.loop,
        footOrderContract: [...WALK_FOOT_ORDER],
        inferredTouchdownOrder: actualOrder,
        touchdownGaps,
        contacts,
        feet,
        qa: {
            failures: uniqueFailures,
            support: {
                minimum: minimumSupport,
                maximum: Math.max(...supportByFrame),
                fourSupportFrames,
                byFrame: supportByFrame,
            },
            thresholds: { ...options },
            sourceVideoSha256: observations.sourceVideoSha256,
            segmenterBackend: SEGMENTER_BACKEND,
        },
    };
}

/** Fail closed: only a fully valid lateral-sequence walk can reach the solver. */
export function inferHoofContacts(args = {}) {
    const diagnostic = diagnoseHoofContacts(args);
    if (diagnostic.status !== 'PASS') {
        const error = new Error(`hoof-contact inference rejected: ${diagnostic.qa.failures.join(', ')}`);
        error.diagnostic = diagnostic;
        throw error;
    }
    return diagnostic;
}

function exactIntegerFrames(value, field, frameCount, { ascending = true } = {}) {
    if (!Array.isArray(value) || !value.length) throw new Error(`${field} must not be empty`);
    const frames = value.map((frame, index) => {
        const result = integer(frame, `${field}[${index}]`);
        if (result >= frameCount) throw new Error(`${field}[${index}] is outside the schedule`);
        return result;
    });
    if (new Set(frames).size !== frames.length) throw new Error(`${field} repeats a frame`);
    const ordered = [...frames].sort((first, second) => first - second);
    if (ascending && !frames.every((frame, index) => frame === ordered[index])) {
        throw new Error(`${field} must be strictly ascending`);
    }
    return frames;
}

function exactShaPins(value, observations) {
    const pins = object(value, 'pins');
    const required = [
        'inputManifestSha256',
        'diagnosticSha256',
        'bridgeReportSha256',
        'initialFitSummarySha256',
        'observationsSha256',
        'fittingBundleSha256',
        'immutableManifestSha256',
        'sourceVideoSha256',
        'sourceModelSha256',
        'sourceSkeletonSha256',
    ];
    const normalized = Object.fromEntries(required.map((name) => [name, sha256(pins[name], `pins.${name}`)]));
    const provenance = object(observations.provenance, 'observations.provenance');
    if (normalized.sourceVideoSha256 !== provenance.source_video_sha256) {
        throw new Error('contact-refit source-video SHA-256 does not match observations provenance');
    }
    if (normalized.fittingBundleSha256 !== provenance.bundle_sha256) {
        throw new Error('contact-refit bundle SHA-256 does not match observations provenance');
    }
    if (normalized.immutableManifestSha256 !== provenance.immutable_manifest_sha256) {
        throw new Error('contact-refit immutable-manifest SHA-256 does not match observations provenance');
    }
    return normalized;
}

/**
 * Validate an immutable PASS schedule before it is allowed to become solver
 * input.  The status string is not trusted: the four semantic contacts,
 * cyclic order and support envelope are reconstructed from the schedule.
 */
export function validatePinnedHoofContactSchedule({ observations: observationValue, schedule: scheduleValue } = {}) {
    const normalizedObservations = normalizeObservations(observationValue);
    const schedule = object(scheduleValue, 'schedule');
    if (schedule.schema !== CONTACT_SCHEDULE_SCHEMA || schedule.status !== 'PASS') {
        throw new Error('pinned hoof-contact schedule must be a PASS schedule');
    }
    if (integer(schedule.frameCount, 'schedule.frameCount', 2) !== normalizedObservations.frameCount) {
        throw new Error('pinned hoof-contact schedule frameCount does not match observations');
    }
    const uniqueFrameCount = integer(schedule.uniqueFrameCount, 'schedule.uniqueFrameCount', 1);
    if (uniqueFrameCount !== normalizedObservations.frameCount - 1) {
        throw new Error('pinned hoof-contact schedule must preserve one duplicated loop endpoint');
    }
    if (Math.abs(finite(schedule.fps, 'schedule.fps') - normalizedObservations.fps) > 1e-9) {
        throw new Error('pinned hoof-contact schedule fps does not match observations');
    }
    if (schedule.loop !== true) throw new Error('pinned hoof-contact schedule must be loop=true');
    if (!Array.isArray(schedule.footOrderContract)
        || schedule.footOrderContract.length !== WALK_FOOT_ORDER.length
        || schedule.footOrderContract.some((foot, index) => foot !== WALK_FOOT_ORDER[index])) {
        throw new Error('pinned hoof-contact schedule foot-order contract is invalid');
    }
    if (!Array.isArray(schedule.inferredTouchdownOrder)
        || !cyclicOrderMatches(schedule.inferredTouchdownOrder, WALK_FOOT_ORDER)) {
        throw new Error('pinned hoof-contact schedule gait order is invalid');
    }
    if (!Array.isArray(schedule.contacts) || schedule.contacts.length !== WALK_FOOT_ORDER.length) {
        throw new Error('pinned hoof-contact schedule must contain exactly four limb contacts');
    }
    const contacts = new Map();
    schedule.contacts.forEach((contactValue, index) => {
        const contact = object(contactValue, `schedule.contacts[${index}]`);
        const anchorId = contact.anchor_id;
        const foot = typeof anchorId === 'string' && anchorId.endsWith('.hoof')
            ? anchorId.slice(0, -'.hoof'.length)
            : null;
        if (!WALK_FOOT_ORDER.includes(foot) || anchorId !== `${foot}.hoof`) {
            throw new Error(`schedule.contacts[${index}] is not an exact Horse hoof contact`);
        }
        if (contacts.has(foot)) throw new Error(`pinned hoof-contact schedule repeats ${foot}`);
        const frames = exactIntegerFrames(contact.frames, `schedule.contacts[${index}].frames`, normalizedObservations.frameCount);
        const weight = finite(contact.weight, `schedule.contacts[${index}].weight`);
        if (weight !== CONTACT_REFIT_THRESHOLDS.contactWeight) {
            throw new Error(`schedule.contacts[${index}].weight must equal the code-owned contact weight`);
        }
        contacts.set(foot, { anchor_id: anchorId, frames, weight });
    });
    WALK_FOOT_ORDER.forEach((foot) => {
        if (!contacts.has(foot)) throw new Error(`pinned hoof-contact schedule is missing ${foot}`);
    });

    const feet = object(schedule.feet, 'schedule.feet');
    const touchdownByFoot = new Map();
    WALK_FOOT_ORDER.forEach((foot) => {
        const detail = object(feet[foot], `schedule.feet.${foot}`);
        if (!Array.isArray(detail.failures) || detail.failures.length) {
            throw new Error(`pinned hoof-contact schedule has rejected ${foot} evidence`);
        }
        const contactFrames = exactIntegerFrames(
            detail.contactFrames,
            `schedule.feet.${foot}.contactFrames`,
            uniqueFrameCount,
            { ascending: false },
        );
        const declaredUnique = contacts.get(foot).frames
            .filter((frame) => frame < uniqueFrameCount);
        const canonicalContactFrames = [...contactFrames].sort((first, second) => first - second);
        if (JSON.stringify(canonicalContactFrames) !== JSON.stringify(declaredUnique)) {
            throw new Error(`pinned hoof-contact frames disagree for ${foot}`);
        }
        const endpointPresent = contacts.get(foot).frames.includes(uniqueFrameCount);
        if (endpointPresent !== contactFrames.includes(0)) {
            throw new Error(`pinned hoof-contact loop endpoint disagrees for ${foot}`);
        }
        const touchdown = integer(detail.touchdownFrame, `schedule.feet.${foot}.touchdownFrame`);
        const liftoff = integer(detail.liftoffFrame, `schedule.feet.${foot}.liftoffFrame`);
        if (touchdown >= uniqueFrameCount || liftoff >= uniqueFrameCount) {
            throw new Error(`pinned hoof-contact phase is outside the loop for ${foot}`);
        }
        if (contactFrames[0] !== touchdown
            || contactFrames.some((frame, index) => index > 0
                && frame !== circularIndex(contactFrames[index - 1] + 1, uniqueFrameCount))
            || liftoff !== circularIndex(contactFrames.at(-1) + 1, uniqueFrameCount)) {
            throw new Error(`pinned hoof-contact interval is not one chronological cyclic run for ${foot}`);
        }
        touchdownByFoot.set(foot, touchdown);
        const dutyFactor = ratio(detail.dutyFactor, `schedule.feet.${foot}.dutyFactor`, { positive: true });
        if (Math.abs(dutyFactor - contactFrames.length / uniqueFrameCount) > 1e-9) {
            throw new Error(`pinned hoof-contact duty factor disagrees for ${foot}`);
        }
        if (dutyFactor < CONTACT_REFIT_THRESHOLDS.minimumDutyFactor
            || dutyFactor > CONTACT_REFIT_THRESHOLDS.maximumDutyFactor) {
            throw new Error(`pinned hoof-contact duty factor fails code-owned bounds for ${foot}`);
        }
        if (finite(detail.characteristicHeightPx, `schedule.feet.${foot}.characteristicHeightPx`) <= 0) {
            throw new Error(`schedule.feet.${foot}.characteristicHeightPx must be positive`);
        }
    });
    const touchdownOrder = [...WALK_FOOT_ORDER]
        .sort((first, second) => touchdownByFoot.get(first) - touchdownByFoot.get(second));
    if (JSON.stringify(touchdownOrder) !== JSON.stringify(schedule.inferredTouchdownOrder)) {
        throw new Error('pinned hoof-contact touchdown order does not match per-foot phases');
    }

    const qa = object(schedule.qa, 'schedule.qa');
    if (!Array.isArray(qa.failures) || qa.failures.length) {
        throw new Error('pinned hoof-contact schedule QA contains failures');
    }
    if (sha256(qa.sourceVideoSha256, 'schedule.qa.sourceVideoSha256')
        !== normalizedObservations.sourceVideoSha256) {
        throw new Error('pinned hoof-contact source-video SHA-256 does not match observations');
    }
    if (qa.segmenterBackend !== SEGMENTER_BACKEND) {
        throw new Error(`pinned hoof-contact segmenter backend must be ${SEGMENTER_BACKEND}`);
    }
    const thresholds = object(qa.thresholds, 'schedule.qa.thresholds');
    const minimumSupportFeet = integer(thresholds.minimumSupportFeet, 'schedule.qa.thresholds.minimumSupportFeet', 2);
    const maximumFourSupportFrames = integer(
        thresholds.maximumFourSupportFrames,
        'schedule.qa.thresholds.maximumFourSupportFrames',
    );
    const maximumFittedContactSlideRatio = ratio(
        thresholds.maximumFittedContactSlideRatio,
        'schedule.qa.thresholds.maximumFittedContactSlideRatio',
        { positive: true },
    );
    const minimumTouchdownGapPhase = ratio(
        thresholds.minimumTouchdownGapPhase,
        'schedule.qa.thresholds.minimumTouchdownGapPhase',
        { positive: true },
    );
    const maximumTouchdownGapPhase = ratio(
        thresholds.maximumTouchdownGapPhase,
        'schedule.qa.thresholds.maximumTouchdownGapPhase',
        { positive: true },
    );
    const minimumDutyFactor = ratio(
        thresholds.minimumDutyFactor,
        'schedule.qa.thresholds.minimumDutyFactor',
        { positive: true },
    );
    const maximumDutyFactor = ratio(
        thresholds.maximumDutyFactor,
        'schedule.qa.thresholds.maximumDutyFactor',
        { positive: true },
    );
    const exactThresholds = {
        minimumSupportFeet,
        maximumFourSupportFrames,
        maximumFittedContactSlideRatio,
        minimumTouchdownGapPhase,
        maximumTouchdownGapPhase,
        minimumDutyFactor,
        maximumDutyFactor,
    };
    if (Object.entries(exactThresholds).some(([name, value]) => value !== CONTACT_REFIT_THRESHOLDS[name])) {
        throw new Error('pinned hoof-contact schedule thresholds must equal the code-owned Horse contact-refit contract');
    }
    if (!Array.isArray(schedule.touchdownGaps) || schedule.touchdownGaps.length !== WALK_FOOT_ORDER.length) {
        throw new Error('pinned hoof-contact schedule must contain four touchdown gaps');
    }
    schedule.inferredTouchdownOrder.forEach((foot, index) => {
        const next = schedule.inferredTouchdownOrder[(index + 1) % WALK_FOOT_ORDER.length];
        const frames = circularIndex(touchdownByFoot.get(next) - touchdownByFoot.get(foot), uniqueFrameCount);
        const phase = frames / uniqueFrameCount;
        const declared = object(schedule.touchdownGaps[index], `schedule.touchdownGaps[${index}]`);
        if (declared.from !== foot || declared.to !== next || declared.frames !== frames
            || Math.abs(finite(declared.phase, `schedule.touchdownGaps[${index}].phase`) - phase) > 1e-12) {
            throw new Error('pinned hoof-contact touchdown-gap provenance is inconsistent');
        }
        if (phase < minimumTouchdownGapPhase || phase > maximumTouchdownGapPhase) {
            throw new Error('pinned hoof-contact schedule fails touchdown-spacing gates');
        }
    });
    const supportByFrame = Array.from({ length: uniqueFrameCount }, (_, frame) => (
        WALK_FOOT_ORDER.filter((foot) => contacts.get(foot).frames.includes(frame)).length
    ));
    const support = object(qa.support, 'schedule.qa.support');
    if (!Array.isArray(support.byFrame)
        || support.byFrame.length !== supportByFrame.length
        || support.byFrame.some((count, frame) => count !== supportByFrame[frame])) {
        throw new Error('pinned hoof-contact support timeline is inconsistent');
    }
    const minimumSupport = Math.min(...supportByFrame);
    const maximumSupport = Math.max(...supportByFrame);
    const fourSupportFrames = supportByFrame.filter((count) => count === 4).length;
    if (support.minimum !== minimumSupport || support.maximum !== maximumSupport
        || support.fourSupportFrames !== fourSupportFrames) {
        throw new Error('pinned hoof-contact support summary is inconsistent');
    }
    if (minimumSupport < minimumSupportFeet || fourSupportFrames > maximumFourSupportFrames) {
        throw new Error('pinned hoof-contact schedule fails walk support gates');
    }
    return {
        schedule,
        contacts: WALK_FOOT_ORDER.map((foot) => ({
            ...contacts.get(foot),
            frames: [...contacts.get(foot).frames],
        })),
        sourceVideoSha256: normalizedObservations.sourceVideoSha256,
        support: { minimum: minimumSupport, maximum: maximumSupport, fourSupportFrames },
    };
}

/** Apply an externally pinned PASS schedule without mutating diagnostic input. */
export function applyPinnedHoofContactSchedule({ observations, schedule, pins } = {}) {
    const validated = validatePinnedHoofContactSchedule({ observations, schedule });
    const normalizedPins = exactShaPins(pins, observations);
    const hoofIds = new Set(WALK_FOOT_ORDER.map((foot) => `${foot}.hoof`));
    const result = structuredClone(observations);
    result.contacts = [
        ...(Array.isArray(result.contacts) ? result.contacts : [])
            .filter((contact) => !hoofIds.has(contact?.anchor_id)),
        ...validated.contacts.map((contact) => ({ ...contact, frames: [...contact.frames] })),
    ];
    result.provenance = {
        ...(result.provenance || {}),
        browser_hoof_contacts: {
            schema: CONTACT_REFIT_PROVENANCE_SCHEMA,
            source: 'immutable_pass_diagnostic',
            browserOnly: true,
            blenderUsed: false,
            mixerUsed: false,
            footOrder: [...WALK_FOOT_ORDER],
            support: { ...validated.support },
            ...normalizedPins,
        },
    };
    return { observations: result, schedule: structuredClone(schedule), pins: normalizedPins };
}

export function applyInferredHoofContacts({ observations, groundEvidence, options = {} } = {}) {
    const schedule = inferHoofContacts({ observations, groundEvidence, options });
    const hoofIds = new Set(WALK_FOOT_ORDER.map((foot) => `${foot}.hoof`));
    const result = structuredClone(observations);
    result.contacts = [
        ...(Array.isArray(result.contacts) ? result.contacts : [])
            .filter((contact) => !hoofIds.has(contact?.anchor_id)),
        ...schedule.contacts.map((contact) => ({ ...contact, frames: [...contact.frames] })),
    ];
    result.provenance = {
        ...(result.provenance || {}),
        browser_hoof_contacts: {
            schema: CONTACT_SCHEDULE_SCHEMA,
            sourceVideoSha256: schedule.qa.sourceVideoSha256,
            segmenterBackend: schedule.qa.segmenterBackend,
            footOrder: [...schedule.footOrderContract],
        },
    };
    return { observations: result, schedule };
}

export function gateFittedWalk({ fitted, schedule } = {}) {
    if (!schedule || schedule.schema !== CONTACT_SCHEDULE_SCHEMA || schedule.status !== 'PASS') {
        throw new Error('a PASS hoof-contact schedule is required for fitted-walk QA');
    }
    if (!fitted || typeof fitted !== 'object' || !fitted.qa) throw new Error('fitted browser animation QA is required');
    if (!Number.isInteger(schedule.frameCount) || schedule.frameCount < 2
        || !Number.isInteger(fitted.frameCount) || fitted.frameCount !== schedule.frameCount) {
        throw new Error('fitted frameCount does not match the hoof-contact schedule');
    }
    const scheduleFps = finite(schedule.fps, 'schedule.fps');
    const fittedFps = finite(fitted.fps, 'fitted.fps');
    if (scheduleFps <= 0 || fittedFps <= 0 || Math.abs(scheduleFps - fittedFps) > 1e-9) {
        throw new Error('fitted fps does not match the hoof-contact schedule');
    }
    if (typeof schedule.loop !== 'boolean' || typeof fitted.loop !== 'boolean'
        || fitted.loop !== schedule.loop) {
        throw new Error('fitted loop mode does not match the hoof-contact schedule');
    }
    const maximumSlidePx = finite(fitted.qa.maximumContactSlidePx, 'fitted.qa.maximumContactSlidePx');
    const characteristicHeightPx = median(WALK_FOOT_ORDER.map((foot) => schedule.feet[foot].characteristicHeightPx));
    const maximumSlideRatio = maximumSlidePx / characteristicHeightPx;
    const failures = maximumSlideRatio > schedule.qa.thresholds.maximumFittedContactSlideRatio
        ? ['fitted_contact_slide']
        : [];
    return {
        status: failures.length ? 'FAIL' : 'PASS',
        failures,
        maximumContactSlidePx: maximumSlidePx,
        maximumContactSlideRatio: maximumSlideRatio,
        thresholdRatio: schedule.qa.thresholds.maximumFittedContactSlideRatio,
    };
}

export function fitBrowserAnimationWithHoofContacts({
    skeleton,
    observations,
    groundEvidence,
    contactOptions = {},
    fitOptions = {},
} = {}) {
    const contactLoop = contactOptions.loop ?? true;
    const fitLoop = fitOptions.loop ?? true;
    if (contactLoop !== fitLoop) {
        throw new Error('contactOptions.loop and fitOptions.loop must match');
    }
    const applied = applyInferredHoofContacts({ observations, groundEvidence, options: contactOptions });
    const fitted = fitBrowserAnimation({ skeleton, observations: applied.observations, options: fitOptions });
    const gaitQa = gateFittedWalk({ fitted, schedule: applied.schedule });
    if (gaitQa.status !== 'PASS') {
        const error = new Error(`fitted walk rejected: ${gaitQa.failures.join(', ')}`);
        error.schedule = applied.schedule;
        error.fitted = fitted;
        error.gaitQa = gaitQa;
        throw error;
    }
    return { fitted, schedule: applied.schedule, gaitQa, observations: applied.observations };
}

/**
 * Final browser-only refit stage.  Unlike inference, this consumes only the
 * immutable PASS diagnostic schedule supplied by the controlled CLI.
 */
export function fitBrowserAnimationWithPinnedHoofContacts({
    skeleton,
    observations,
    schedule,
    pins,
    fitOptions = {},
    gaitQaOptions = {},
} = {}) {
    if ((fitOptions.loop ?? true) !== true) {
        throw new Error('pinned Horse contact refit requires fitOptions.loop=true');
    }
    const applied = applyPinnedHoofContactSchedule({ observations, schedule, pins });
    const gaitQa = assessHorseWalkGait(applied.observations, {
        ...gaitQaOptions,
        expectedOrder: [...WALK_FOOT_ORDER],
        maximumSimultaneousSwingFrames: 0,
    });
    if (gaitQa.accepted !== true) {
        const error = new Error('pinned Horse contact refit rejected by semantic gait QA');
        error.schedule = applied.schedule;
        error.gaitQa = gaitQa;
        throw error;
    }
    const fitted = fitBrowserAnimation({
        skeleton,
        observations: applied.observations,
        options: { ...fitOptions, loop: true },
    });
    const fittedWalkQa = gateFittedWalk({ fitted, schedule: applied.schedule });
    if (fittedWalkQa.status !== 'PASS') {
        const error = new Error(`pinned Horse contact refit rejected: ${fittedWalkQa.failures.join(', ')}`);
        error.schedule = applied.schedule;
        error.fitted = fitted;
        error.fittedWalkQa = fittedWalkQa;
        throw error;
    }
    return {
        fitted,
        schedule: applied.schedule,
        fittedWalkQa,
        gaitQa,
        observations: applied.observations,
        pins: applied.pins,
        runtime: { browserOnly: true, blenderUsed: false, mixerUsed: false },
    };
}

export const HOOF_CONTACT_INFERENCE_CONTRACT = Object.freeze({
    observations: OBSERVATION_SCHEMA,
    groundEvidence: GROUND_EVIDENCE_SCHEMA,
    schedule: CONTACT_SCHEDULE_SCHEMA,
    contactRefitProvenance: CONTACT_REFIT_PROVENANCE_SCHEMA,
    contactRefitThresholds: CONTACT_REFIT_THRESHOLDS,
    trackerBackend: TRACKER_BACKEND,
    segmenterBackend: SEGMENTER_BACKEND,
    footOrder: WALK_FOOT_ORDER,
    defaults: DEFAULTS,
});
