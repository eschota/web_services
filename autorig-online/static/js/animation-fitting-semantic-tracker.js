const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';

const DEFAULT_LIMB_LABELS = Object.freeze([
    'fore_left',
    'fore_right',
    'hind_left',
    'hind_right',
]);

function finiteNumber(value, field) {
    const result = Number(value);
    if (!Number.isFinite(result)) throw new Error(`${field} must be finite`);
    return result;
}

function clamp(value, minimum, maximum) {
    return Math.min(maximum, Math.max(minimum, value));
}

export function srgbByteToLinear(value) {
    const channel = clamp(finiteNumber(value, 'sRGB channel') / 255, 0, 1);
    return channel <= 0.04045
        ? channel / 12.92
        : ((channel + 0.055) / 1.055) ** 2.4;
}

export function linearChannelToSrgbByte(value) {
    const channel = clamp(finiteNumber(value, 'linear channel'), 0, 1);
    const srgb = channel <= 0.0031308
        ? channel * 12.92
        : 1.055 * (channel ** (1 / 2.4)) - 0.055;
    return Math.round(clamp(srgb, 0, 1) * 255);
}

function normalizeColor(value, field) {
    if (!Array.isArray(value) || value.length !== 3) {
        throw new Error(`${field} must be a linear RGB triplet`);
    }
    return value.map((channel, index) => {
        const result = finiteNumber(channel, `${field}[${index}]`);
        if (result < 0 || result > 1) throw new Error(`${field}[${index}] must be inside [0, 1]`);
        return result;
    });
}

export function normalizeSemanticPalette(value, labels = DEFAULT_LIMB_LABELS) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error('semantic palette must be an object');
    }
    const normalized = {};
    labels.forEach((label) => {
        if (!(label in value)) throw new Error(`semantic palette is missing ${label}`);
        normalized[label] = normalizeColor(value[label], `semantic palette.${label}`);
    });
    return normalized;
}

function normalizeFrame(frame) {
    const width = Number(frame?.width);
    const height = Number(frame?.height);
    const data = frame?.data;
    if (!Number.isInteger(width) || width <= 0 || !Number.isInteger(height) || height <= 0) {
        throw new Error('semantic frame width and height must be positive integers');
    }
    if (!data || typeof data.length !== 'number' || data.length !== width * height * 4) {
        throw new Error('semantic frame must contain width * height * 4 RGBA bytes');
    }
    return { width, height, data };
}

function averageRows(accumulator, startCount, endCount) {
    let seen = 0;
    let count = 0;
    let x = 0;
    let y = 0;
    for (let row = 0; row < accumulator.rowCounts.length; row += 1) {
        const rowCount = accumulator.rowCounts[row];
        if (!rowCount) continue;
        const rowStart = seen;
        const rowEnd = seen + rowCount;
        const overlap = Math.max(0, Math.min(rowEnd, endCount) - Math.max(rowStart, startCount));
        if (overlap > 0) {
            x += (accumulator.rowXSums[row] / rowCount) * overlap;
            y += row * overlap;
            count += overlap;
        }
        seen = rowEnd;
        if (seen >= endCount) break;
    }
    if (!count) return null;
    return { x: x / count, y: y / count };
}

function bandPoint(accumulator, fromFraction, toFraction) {
    const start = Math.floor(accumulator.count * clamp(fromFraction, 0, 1));
    const end = Math.max(start + 1, Math.ceil(accumulator.count * clamp(toFraction, 0, 1)));
    return averageRows(accumulator, start, Math.min(end, accumulator.count));
}

function distanceSquared(rgb, target) {
    const r = rgb[0] - target[0];
    const g = rgb[1] - target[1];
    const b = rgb[2] - target[2];
    return r * r + g * g + b * b;
}

/**
 * Segment one decoded frame rendered from the semantic LTX reference.
 *
 * Palette values are linear RGB. Input bytes are decoded from sRGB before
 * comparison so the browser and Blender reference use the same colour space.
 */
export function extractSemanticFrame(frameValue, paletteValue, options = {}) {
    const frame = normalizeFrame(frameValue);
    const labels = options.labels || DEFAULT_LIMB_LABELS;
    const palette = normalizeSemanticPalette(paletteValue, labels);
    const colorTolerance = finiteNumber(options.colorTolerance ?? 0.18, 'colorTolerance');
    const alphaThreshold = clamp(finiteNumber(options.alphaThreshold ?? 0.5, 'alphaThreshold'), 0, 1);
    const minimumPixels = Math.max(1, Math.trunc(finiteNumber(options.minimumPixels ?? 24, 'minimumPixels')));
    const maximumDistanceSquared = colorTolerance ** 2;
    const accumulators = Object.fromEntries(labels.map((label) => [label, {
        count: 0,
        x: 0,
        y: 0,
        colorDistanceSquared: 0,
        minX: frame.width,
        minY: frame.height,
        maxX: -1,
        maxY: -1,
        rowCounts: new Uint32Array(frame.height),
        rowXSums: new Float64Array(frame.height),
    }]));

    for (let y = 0; y < frame.height; y += 1) {
        for (let x = 0; x < frame.width; x += 1) {
            const offset = (y * frame.width + x) * 4;
            if ((Number(frame.data[offset + 3]) / 255) < alphaThreshold) continue;
            const rgb = [
                srgbByteToLinear(frame.data[offset]),
                srgbByteToLinear(frame.data[offset + 1]),
                srgbByteToLinear(frame.data[offset + 2]),
            ];
            let bestLabel = null;
            let bestDistance = Infinity;
            labels.forEach((label) => {
                const distance = distanceSquared(rgb, palette[label]);
                if (distance < bestDistance) {
                    bestDistance = distance;
                    bestLabel = label;
                }
            });
            if (!bestLabel || bestDistance > maximumDistanceSquared) continue;
            const accumulator = accumulators[bestLabel];
            accumulator.count += 1;
            accumulator.x += x;
            accumulator.y += y;
            accumulator.colorDistanceSquared += bestDistance;
            accumulator.minX = Math.min(accumulator.minX, x);
            accumulator.minY = Math.min(accumulator.minY, y);
            accumulator.maxX = Math.max(accumulator.maxX, x);
            accumulator.maxY = Math.max(accumulator.maxY, y);
            accumulator.rowCounts[y] += 1;
            accumulator.rowXSums[y] += x;
        }
    }

    const regions = {};
    labels.forEach((label) => {
        const accumulator = accumulators[label];
        const visible = accumulator.count >= minimumPixels;
        const meanDistance = accumulator.count
            ? Math.sqrt(accumulator.colorDistanceSquared / accumulator.count)
            : Infinity;
        const coverageConfidence = clamp(accumulator.count / minimumPixels, 0, 1);
        const colorConfidence = Number.isFinite(meanDistance)
            ? clamp(1 - meanDistance / Math.max(colorTolerance, 1e-9), 0, 1)
            : 0;
        regions[label] = {
            label,
            visible,
            pixelCount: accumulator.count,
            confidence: visible ? coverageConfidence * colorConfidence : 0,
            meanColorDistance: meanDistance,
            centroid: accumulator.count ? {
                x: accumulator.x / accumulator.count,
                y: accumulator.y / accumulator.count,
            } : null,
            proximal: visible ? bandPoint(accumulator, 0, 0.16) : null,
            joint: visible ? bandPoint(accumulator, 0.42, 0.58) : null,
            hoof: visible ? bandPoint(accumulator, 0.86, 1) : null,
            bounds: accumulator.count ? {
                minX: accumulator.minX,
                minY: accumulator.minY,
                maxX: accumulator.maxX,
                maxY: accumulator.maxY,
            } : null,
        };
    });
    return { width: frame.width, height: frame.height, regions };
}

function firstVisibleFrame(points) {
    return points.find((point) => point.visible)?.frame ?? 0;
}

function contactFramesForTrack(points, options = {}) {
    const visible = points.filter((point) => point.visible);
    if (!visible.length) return { frames: [], groundY: null };
    const groundY = options.groundY ?? Math.max(...visible.map((point) => point.y));
    const heightTolerance = finiteNumber(options.contactHeightTolerancePx ?? 3, 'contactHeightTolerancePx');
    const velocityTolerance = finiteNumber(options.contactVelocityTolerancePx ?? 2, 'contactVelocityTolerancePx');
    const byFrame = new Map(points.map((point) => [point.frame, point]));
    const frames = [];
    visible.forEach((point) => {
        const previous = byFrame.get(point.frame - 1);
        const next = byFrame.get(point.frame + 1);
        const neighbor = previous?.visible ? previous : (next?.visible ? next : null);
        const velocity = neighbor ? Math.hypot(point.x - neighbor.x, point.y - neighbor.y) : 0;
        if (Math.abs(point.y - groundY) <= heightTolerance && velocity <= velocityTolerance) {
            frames.push(point.frame);
        }
    });
    return { frames, groundY };
}

/** Convert segmented semantic frames to the existing observations.v1 contract. */
export function buildSemanticObservations(frameValues, paletteValue, options = {}) {
    if (!Array.isArray(frameValues) || frameValues.length < 2) {
        throw new Error('at least two semantic frames are required');
    }
    const fps = finiteNumber(options.fps ?? 30, 'fps');
    if (fps <= 0) throw new Error('fps must be positive');
    const labels = options.labels || DEFAULT_LIMB_LABELS;
    const extracted = frameValues.map((frame) => extractSemanticFrame(frame, paletteValue, {
        ...options,
        labels,
    }));
    const { width, height } = extracted[0];
    if (extracted.some((frame) => frame.width !== width || frame.height !== height)) {
        throw new Error('all semantic frames must use the same dimensions');
    }

    const tracks = [];
    const contacts = [];
    labels.forEach((label) => {
        ['proximal', 'joint', 'hoof'].forEach((role) => {
            const points = extracted.map((frame, frameIndex) => {
                const region = frame.regions[label];
                const point = region?.[role];
                return {
                    frame: frameIndex,
                    x: point?.x ?? 0,
                    y: point?.y ?? 0,
                    visible: Boolean(region?.visible && point),
                    confidence: region?.confidence ?? 0,
                };
            });
            const anchorId = `${label}.${role}`;
            tracks.push({
                id: `semantic:${anchorId}`,
                anchor_id: anchorId,
                query_frame: firstVisibleFrame(points),
                points,
            });
            if (role === 'hoof') {
                const contact = contactFramesForTrack(points, options);
                if (contact.frames.length) {
                    contacts.push({
                        anchor_id: anchorId,
                        frames: contact.frames,
                        ground_height: contact.groundY,
                        weight: finiteNumber(options.contactWeight ?? 1, 'contactWeight'),
                    });
                }
            }
        });
    });

    return {
        schema: OBSERVATION_SCHEMA,
        frame_count: extracted.length,
        width,
        height,
        fps,
        tracks,
        silhouettes: [],
        depth: [],
        contacts,
        provenance: {
            source: 'browser_semantic_ltx_tracker',
            color_space: 'srgb_bytes_to_linear_rgb',
            limb_labels: [...labels],
            color_tolerance: finiteNumber(options.colorTolerance ?? 0.18, 'colorTolerance'),
            minimum_pixels: Math.max(1, Math.trunc(options.minimumPixels ?? 24)),
        },
    };
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

function circularCenter(frames, frameCount) {
    let x = 0;
    let y = 0;
    frames.forEach((frame) => {
        const angle = (frame / frameCount) * Math.PI * 2;
        x += Math.cos(angle);
        y += Math.sin(angle);
    });
    const angle = Math.atan2(y, x);
    return ((angle / (Math.PI * 2)) + 1) % 1;
}

/**
 * Release gate for a controller-friendly four-beat Horse walk.
 *
 * The gate operates on the tracked hoof pixels, accepts a cyclic phase offset,
 * and rejects paired/trotting/pacing motion before any clip is applied.
 */
export function assessHorseWalkGait(observations, options = {}) {
    if (!observations || observations.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const frameCount = Number(observations.frame_count);
    if (!Number.isInteger(frameCount) || frameCount < 8) {
        throw new Error('Horse walk gait QA requires at least eight frames');
    }
    const expectedOrder = options.expectedOrder || [
        'hind_left',
        'fore_left',
        'hind_right',
        'fore_right',
    ];
    if (!Array.isArray(expectedOrder) || expectedOrder.length !== 4 || new Set(expectedOrder).size !== 4) {
        throw new Error('expectedOrder must contain four unique limb labels');
    }
    const minimumVisibleFraction = clamp(finiteNumber(
        options.minimumVisibleFraction ?? 0.95,
        'minimumVisibleFraction',
    ), 0, 1);
    const minimumLiftPx = Math.max(0, finiteNumber(options.minimumLiftPx ?? 3, 'minimumLiftPx'));
    const minimumPhaseGap = clamp(finiteNumber(options.minimumPhaseGap ?? 0.08, 'minimumPhaseGap'), 0, 0.25);
    const maximumPhaseGap = clamp(finiteNumber(options.maximumPhaseGap ?? 0.42, 'maximumPhaseGap'), 0.25, 1);
    const maximumSimultaneousSwingFrames = Math.max(0, Math.trunc(finiteNumber(
        options.maximumSimultaneousSwingFrames ?? 2,
        'maximumSimultaneousSwingFrames',
    )));
    const maximumSecondarySwingFrames = Math.max(0, Math.trunc(finiteNumber(
        options.maximumSecondarySwingFrames ?? 3,
        'maximumSecondarySwingFrames',
    )));

    const trackByAnchor = new Map((observations.tracks || []).map((track) => [track.anchor_id, track]));
    const limbs = {};
    const swingCount = new Uint8Array(frameCount);
    expectedOrder.forEach((label) => {
        const track = trackByAnchor.get(`${label}.hoof`);
        if (!track || !Array.isArray(track.points)) {
            throw new Error(`Horse walk gait QA is missing ${label}.hoof`);
        }
        const points = Array.from({ length: frameCount }, (_, frame) => {
            const point = track.points.find((item) => Number(item?.frame) === frame) || track.points[frame];
            return point?.visible ? point : null;
        });
        const visible = points.filter(Boolean);
        const visibleFraction = visible.length / frameCount;
        const groundY = quantile(visible.map((point) => Number(point.y)), 0.9);
        const lifts = points.map((point) => point ? Math.max(0, groundY - Number(point.y)) : 0);
        const maximumLift = Math.max(...lifts);
        const threshold = Math.max(minimumLiftPx, maximumLift * 0.3);
        const swingMask = lifts.map((lift, frame) => Boolean(points[frame] && lift >= threshold));
        swingMask.forEach((swing, frame) => { if (swing) swingCount[frame] += 1; });
        const runs = cyclicRuns(swingMask).sort((a, b) => b.length - a.length);
        const principal = runs[0] || [];
        const secondaryFrames = runs.slice(1).reduce((sum, run) => sum + run.length, 0);
        limbs[label] = {
            visibleFraction,
            groundY,
            maximumLiftPx: maximumLift,
            swingThresholdPx: threshold,
            swingFrames: principal,
            swingCenterPhase: principal.length ? circularCenter(principal, frameCount) : null,
            secondarySwingFrames: secondaryFrames,
            accepted: visibleFraction >= minimumVisibleFraction
                && maximumLift >= minimumLiftPx
                && principal.length > 0
                && secondaryFrames <= maximumSecondarySwingFrames,
        };
    });

    const simultaneousSwingFrames = [...swingCount]
        .map((count, frame) => ({ frame, count }))
        .filter((row) => row.count > 1);
    const unsupportedFrames = [...swingCount]
        .map((count, frame) => ({ frame, count }))
        .filter((row) => row.count > 1);
    const phases = expectedOrder.map((label) => limbs[label].swingCenterPhase);
    const phaseGaps = phases.map((phase, index) => {
        if (!Number.isFinite(phase) || !Number.isFinite(phases[(index + 1) % phases.length])) return 0;
        return (phases[(index + 1) % phases.length] - phase + 1) % 1;
    });
    const orderAccepted = phaseGaps.every((gap) => gap >= minimumPhaseGap && gap <= maximumPhaseGap);
    const overlapAccepted = simultaneousSwingFrames.length <= maximumSimultaneousSwingFrames;
    const accepted = expectedOrder.every((label) => limbs[label].accepted)
        && orderAccepted
        && overlapAccepted;
    return {
        schema: 'autorig-horse-walk-gait-qa.v1',
        accepted,
        expectedOrder: [...expectedOrder],
        phaseGaps,
        orderAccepted,
        overlapAccepted,
        simultaneousSwingFrameCount: simultaneousSwingFrames.length,
        simultaneousSwingFrames,
        unsupportedFrameCount: unsupportedFrames.length,
        limbs,
        gates: {
            minimumVisibleFraction,
            minimumLiftPx,
            minimumPhaseGap,
            maximumPhaseGap,
            maximumSimultaneousSwingFrames,
            maximumSecondarySwingFrames,
        },
    };
}

const HORSE_TROT_DIAGONAL_PAIRS = Object.freeze([
    Object.freeze({
        id: 'left_fore_right_hind',
        feet: Object.freeze(['fore_left', 'hind_right']),
    }),
    Object.freeze({
        id: 'right_fore_left_hind',
        feet: Object.freeze(['fore_right', 'hind_left']),
    }),
]);

function binaryDice(first, second) {
    let firstCount = 0;
    let secondCount = 0;
    let intersection = 0;
    first.forEach((value, frame) => {
        if (value) firstCount += 1;
        if (second[frame]) secondCount += 1;
        if (value && second[frame]) intersection += 1;
    });
    return firstCount + secondCount ? (2 * intersection) / (firstCount + secondCount) : 0;
}

function correlation(first, second, shift = 0) {
    const count = first.length;
    const firstMean = first.reduce((sum, value) => sum + value, 0) / count;
    const secondMean = second.reduce((sum, value) => sum + value, 0) / count;
    let covariance = 0;
    let firstVariance = 0;
    let secondVariance = 0;
    first.forEach((value, frame) => {
        const firstDelta = value - firstMean;
        const secondDelta = second[(frame + shift + count) % count] - secondMean;
        covariance += firstDelta * secondDelta;
        firstVariance += firstDelta * firstDelta;
        secondVariance += secondDelta * secondDelta;
    });
    const denominator = Math.sqrt(firstVariance * secondVariance);
    return denominator > 1e-12 ? covariance / denominator : 0;
}

function circularGap(first, second, frameCount) {
    return ((second - first) % frameCount + frameCount) % frameCount;
}

/**
 * Release gate for a controller-friendly diagonal-pair Horse trot.
 *
 * This is deliberately separate from assessHorseWalkGait: a trot requires
 * LF+RH and RF+LH to swing/contact as two diagonal pairs, permits a short
 * suspension phase, and requires the two pair events to alternate.  It never
 * relaxes the four-beat WALK contract or reinterprets a lateral/bound gait as
 * a trot.
 */
export function assessHorseTrotGait(observations, options = {}) {
    if (!observations || observations.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const sourceFrameCount = Number(observations.frame_count);
    if (!Number.isInteger(sourceFrameCount) || sourceFrameCount < 8) {
        throw new Error('Horse trot gait QA requires at least eight frames');
    }
    const loopEndpointDuplicated = options.loopEndpointDuplicated === true;
    const frameCount = sourceFrameCount - (loopEndpointDuplicated ? 1 : 0);
    if (frameCount < 8) throw new Error('Horse trot gait QA requires at least eight unique frames');
    const minimumVisibleFraction = clamp(finiteNumber(
        options.minimumVisibleFraction ?? 0.95,
        'minimumVisibleFraction',
    ), 0, 1);
    const minimumLiftPx = Math.max(0, finiteNumber(options.minimumLiftPx ?? 3, 'minimumLiftPx'));
    const relativeSwingThreshold = clamp(finiteNumber(
        options.relativeSwingThreshold ?? 0.3,
        'relativeSwingThreshold',
    ), 0.05, 0.8);
    const relativeContactThreshold = clamp(finiteNumber(
        options.relativeContactThreshold ?? 0.12,
        'relativeContactThreshold',
    ), 0, relativeSwingThreshold);
    const minimumContactHeightPx = Math.max(0, finiteNumber(
        options.minimumContactHeightPx ?? 1.5,
        'minimumContactHeightPx',
    ));
    const minimumSwingFrames = Math.max(1, Math.trunc(finiteNumber(
        options.minimumSwingFrames ?? 2,
        'minimumSwingFrames',
    )));
    const maximumSwingIntervals = Math.max(1, Math.trunc(finiteNumber(
        options.maximumSwingIntervals ?? 3,
        'maximumSwingIntervals',
    )));
    const minimumDiagonalSwingDice = clamp(finiteNumber(
        options.minimumDiagonalSwingDice ?? 0.55,
        'minimumDiagonalSwingDice',
    ), 0, 1);
    const minimumDiagonalContactDice = clamp(finiteNumber(
        options.minimumDiagonalContactDice ?? 0.7,
        'minimumDiagonalContactDice',
    ), 0, 1);
    const minimumDiagonalLiftCorrelation = clamp(finiteNumber(
        options.minimumDiagonalLiftCorrelation ?? 0.45,
        'minimumDiagonalLiftCorrelation',
    ), -1, 1);
    const maximumDiagonalLagPhase = clamp(finiteNumber(
        options.maximumDiagonalLagPhase ?? 0.1,
        'maximumDiagonalLagPhase',
    ), 0, 0.25);
    const maximumSuspensionFraction = clamp(finiteNumber(
        options.maximumSuspensionFraction ?? 0.15,
        'maximumSuspensionFraction',
    ), 0, 1);
    const minimumEventSpacingFactor = clamp(finiteNumber(
        options.minimumEventSpacingFactor ?? 0.5,
        'minimumEventSpacingFactor',
    ), 0, 1);
    const maximumEventSpacingFactor = Math.max(1, finiteNumber(
        options.maximumEventSpacingFactor ?? 1.5,
        'maximumEventSpacingFactor',
    ));

    const footOrder = ['fore_left', 'hind_right', 'fore_right', 'hind_left'];
    const trackByAnchor = new Map((observations.tracks || []).map((track) => [track.anchor_id, track]));
    const limbs = {};
    footOrder.forEach((label) => {
        const track = trackByAnchor.get(`${label}.hoof`);
        if (!track || !Array.isArray(track.points)) {
            throw new Error(`Horse trot gait QA is missing ${label}.hoof`);
        }
        const points = Array.from({ length: frameCount }, (_, frame) => {
            const point = track.points.find((item) => Number(item?.frame) === frame) || track.points[frame];
            return point?.visible ? point : null;
        });
        const visible = points.filter(Boolean);
        const visibleFraction = visible.length / frameCount;
        const groundY = visible.length ? quantile(visible.map((point) => Number(point.y)), 0.9) : 0;
        const liftPx = points.map((point) => point ? Math.max(0, groundY - Number(point.y)) : 0);
        const maximumLiftPx = Math.max(...liftPx);
        const swingThresholdPx = Math.max(minimumLiftPx, maximumLiftPx * relativeSwingThreshold);
        const contactThresholdPx = Math.max(minimumContactHeightPx, maximumLiftPx * relativeContactThreshold);
        const rawSwingMask = liftPx.map((lift, frame) => Boolean(points[frame] && lift >= swingThresholdPx));
        const swingRuns = cyclicRuns(rawSwingMask).filter((run) => run.length >= minimumSwingFrames);
        const acceptedSwingFrames = new Set(swingRuns.flat());
        const swingMask = rawSwingMask.map((_value, frame) => acceptedSwingFrames.has(frame));
        const contactMask = liftPx.map((lift, frame) => Boolean(points[frame] && lift <= contactThresholdPx));
        limbs[label] = {
            visibleFraction,
            groundY,
            maximumLiftPx,
            swingThresholdPx,
            contactThresholdPx,
            swingFrames: swingMask.flatMap((value, frame) => value ? [frame] : []),
            contactFrames: contactMask.flatMap((value, frame) => value ? [frame] : []),
            swingIntervals: swingRuns.map((run) => [...run]),
            swingCenterPhases: swingRuns.map((run) => circularCenter(run, frameCount)),
            accepted: visibleFraction >= minimumVisibleFraction
                && maximumLiftPx >= minimumLiftPx
                && swingRuns.length >= 1
                && swingRuns.length <= maximumSwingIntervals,
            liftPx,
            swingMask,
            contactMask,
        };
    });

    const pairs = {};
    HORSE_TROT_DIAGONAL_PAIRS.forEach(({ id, feet }) => {
        const [first, second] = feet;
        const firstLimb = limbs[first];
        const secondLimb = limbs[second];
        const correlations = Array.from({ length: frameCount }, (_, shift) => (
            correlation(firstLimb.liftPx, secondLimb.liftPx, shift)
        ));
        const bestCorrelation = Math.max(...correlations);
        const bestShift = correlations.indexOf(bestCorrelation);
        const signedBestShift = bestShift > frameCount / 2 ? bestShift - frameCount : bestShift;
        const consensusSwingMask = firstLimb.swingMask.map((value, frame) => value && secondLimb.swingMask[frame]);
        const consensusSwingRuns = cyclicRuns(consensusSwingMask).filter((run) => run.length >= minimumSwingFrames);
        const swingDice = binaryDice(firstLimb.swingMask, secondLimb.swingMask);
        const contactDice = binaryDice(firstLimb.contactMask, secondLimb.contactMask);
        const zeroLagCorrelation = correlations[0];
        const lagPhase = Math.abs(signedBestShift) / frameCount;
        const accepted = firstLimb.accepted
            && secondLimb.accepted
            && swingDice >= minimumDiagonalSwingDice
            && contactDice >= minimumDiagonalContactDice
            && zeroLagCorrelation >= minimumDiagonalLiftCorrelation
            && lagPhase <= maximumDiagonalLagPhase
            && consensusSwingRuns.length >= 1
            && consensusSwingRuns.length <= maximumSwingIntervals;
        pairs[id] = {
            feet: [...feet],
            swingDice,
            contactDice,
            zeroLagLiftCorrelation: zeroLagCorrelation,
            bestLiftCorrelation: bestCorrelation,
            bestLagFrames: signedBestShift,
            bestLagPhase: lagPhase,
            swingIntervals: consensusSwingRuns.map((run) => [...run]),
            swingCenterPhases: consensusSwingRuns.map((run) => circularCenter(run, frameCount)),
            accepted,
        };
    });

    const events = Object.entries(pairs).flatMap(([pair, detail]) => (
        detail.swingCenterPhases.map((phase) => ({ pair, phase, frame: phase * frameCount }))
    )).sort((first, second) => first.phase - second.phase);
    const expectedEventGap = events.length ? frameCount / events.length : frameCount;
    const eventGaps = events.map((event, index) => {
        const next = events[(index + 1) % events.length];
        const frames = circularGap(event.frame, next.frame, frameCount);
        return {
            from: event.pair,
            to: next.pair,
            frames,
            factorOfExpected: frames / expectedEventGap,
        };
    });
    const eventCounts = Object.fromEntries(Object.keys(pairs).map((id) => [
        id,
        events.filter((event) => event.pair === id).length,
    ]));
    const alternating = events.length >= 2
        && new Set(Object.values(eventCounts)).size === 1
        && events.every((event, index) => event.pair !== events[(index + 1) % events.length].pair);
    const eventSpacingAccepted = events.length >= 2 && eventGaps.every((gap) => (
        gap.factorOfExpected >= minimumEventSpacingFactor
        && gap.factorOfExpected <= maximumEventSpacingFactor
    ));
    const allSwingFrames = Array.from({ length: frameCount }, (_, frame) => (
        footOrder.every((foot) => limbs[foot].swingMask[frame]) ? frame : null
    )).filter((frame) => frame != null);
    const suspensionFraction = allSwingFrames.length / frameCount;
    const failures = [];
    footOrder.forEach((foot) => {
        if (!limbs[foot].accepted) failures.push(`${foot}:invalid_swing_evidence`);
    });
    Object.entries(pairs).forEach(([pair, detail]) => {
        if (detail.swingDice < minimumDiagonalSwingDice) failures.push(`${pair}:diagonal_swing_mismatch`);
        if (detail.contactDice < minimumDiagonalContactDice) failures.push(`${pair}:diagonal_contact_mismatch`);
        if (detail.zeroLagLiftCorrelation < minimumDiagonalLiftCorrelation) failures.push(`${pair}:diagonal_lift_correlation`);
        if (detail.bestLagPhase > maximumDiagonalLagPhase) failures.push(`${pair}:diagonal_phase_lag`);
        if (!detail.swingIntervals.length) failures.push(`${pair}:no_diagonal_swing_event`);
    });
    if (!alternating) failures.push('trot_diagonal_events_do_not_alternate');
    if (!eventSpacingAccepted) failures.push('trot_diagonal_event_spacing');
    if (suspensionFraction > maximumSuspensionFraction) failures.push('trot_excess_suspension');

    const uniqueFailures = [...new Set(failures)];
    const publicLimbs = Object.fromEntries(Object.entries(limbs).map(([foot, detail]) => [foot, {
        visibleFraction: detail.visibleFraction,
        groundY: detail.groundY,
        maximumLiftPx: detail.maximumLiftPx,
        swingThresholdPx: detail.swingThresholdPx,
        contactThresholdPx: detail.contactThresholdPx,
        swingFrames: detail.swingFrames,
        contactFrames: detail.contactFrames,
        swingIntervals: detail.swingIntervals,
        swingCenterPhases: detail.swingCenterPhases,
        accepted: detail.accepted,
    }]));
    return {
        schema: 'autorig-horse-trot-contact-gait-qa.v1',
        profile: {
            id: 'horse.diagonal_pair_trot.v1',
            gait: 'diagonal_pair_trot',
            diagonalPairs: HORSE_TROT_DIAGONAL_PAIRS.map(({ id, feet }) => ({ id, feet: [...feet] })),
            distinctFromWalkProfile: true,
        },
        accepted: uniqueFailures.length === 0,
        status: uniqueFailures.length ? 'FAIL' : 'PASS',
        sourceFrameCount,
        uniqueFrameCount: frameCount,
        loopEndpointDuplicated,
        limbs: publicLimbs,
        pairs,
        events,
        eventGaps,
        alternating,
        eventSpacingAccepted,
        suspension: {
            frames: allSwingFrames,
            fraction: suspensionFraction,
            accepted: suspensionFraction <= maximumSuspensionFraction,
        },
        failures: uniqueFailures,
        gates: {
            minimumVisibleFraction,
            minimumLiftPx,
            relativeSwingThreshold,
            relativeContactThreshold,
            minimumContactHeightPx,
            minimumSwingFrames,
            maximumSwingIntervals,
            minimumDiagonalSwingDice,
            minimumDiagonalContactDice,
            minimumDiagonalLiftCorrelation,
            maximumDiagonalLagPhase,
            maximumSuspensionFraction,
            minimumEventSpacingFactor,
            maximumEventSpacingFactor,
        },
    };
}

/**
 * Release gate for an in-place Horse idle with all four hooves planted.
 *
 * Unlike WALK/TROT this profile requires no swing event.  It verifies the
 * exact four bridged hoof tracks remain visible, confident and spatially
 * stationary, then checks the complete selected body-track set for a C0/C1
 * loop seam.  Passing this source-motion gate permits a contact-constrained
 * browser fit; it never grants visual or animation-library approval.
 */
export function assessHorsePlantedIdle(observations, options = {}) {
    if (!observations || observations.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const frameCount = Number(observations.frame_count);
    if (!Number.isInteger(frameCount) || frameCount < 8) {
        throw new Error('Horse planted idle QA requires at least eight frames');
    }
    const width = finiteNumber(observations.width, 'observations.width');
    const height = finiteNumber(observations.height, 'observations.height');
    if (width <= 0 || height <= 0) throw new Error('observations width and height must be positive');
    const diagonal = Math.hypot(width, height);
    const expectedFeet = options.expectedFeet || [...DEFAULT_LIMB_LABELS];
    if (!Array.isArray(expectedFeet) || expectedFeet.length !== 4 || new Set(expectedFeet).size !== 4) {
        throw new Error('expectedFeet must contain four unique limb labels');
    }
    const minimumVisibleFraction = clamp(finiteNumber(
        options.minimumVisibleFraction ?? 0.95,
        'minimumVisibleFraction',
    ), 0, 1);
    const minimumVisibleConfidence = clamp(finiteNumber(
        options.minimumVisibleConfidence ?? 0.7,
        'minimumVisibleConfidence',
    ), 0, 1);
    const maximumHorizontalRangePx = Math.max(0, finiteNumber(
        options.maximumHorizontalRangePx ?? 3,
        'maximumHorizontalRangePx',
    ));
    const maximumVerticalRangePx = Math.max(0, finiteNumber(
        options.maximumVerticalRangePx ?? 3,
        'maximumVerticalRangePx',
    ));
    const maximumDisplacementPx = Math.max(0, finiteNumber(
        options.maximumDisplacementPx ?? 3,
        'maximumDisplacementPx',
    ));
    const maximumEndpointDisplacementPx = Math.max(0, finiteNumber(
        options.maximumEndpointDisplacementPx ?? 2,
        'maximumEndpointDisplacementPx',
    ));
    const maximumP95SpeedPxPerFrame = Math.max(0, finiteNumber(
        options.maximumP95SpeedPxPerFrame ?? 1,
        'maximumP95SpeedPxPerFrame',
    ));
    const maximumVelocitySeamPxPerFrame = Math.max(0, finiteNumber(
        options.maximumVelocitySeamPxPerFrame ?? 1,
        'maximumVelocitySeamPxPerFrame',
    ));
    const maximumBodyEndpointMedianDiagonal = clamp(finiteNumber(
        options.maximumBodyEndpointMedianDiagonal ?? 0.005,
        'maximumBodyEndpointMedianDiagonal',
    ), 0, 1);
    const maximumBodyEndpointP95Px = Math.max(0, finiteNumber(
        options.maximumBodyEndpointP95Px ?? 1.5,
        'maximumBodyEndpointP95Px',
    ));
    const maximumBodyVelocitySeamP95PxPerFrame = Math.max(0, finiteNumber(
        options.maximumBodyVelocitySeamP95PxPerFrame ?? 1.5,
        'maximumBodyVelocitySeamP95PxPerFrame',
    ));

    const trackByAnchor = new Map((observations.tracks || []).map((track) => [track.anchor_id, track]));
    const failures = [];
    const feet = {};
    expectedFeet.forEach((foot) => {
        const track = trackByAnchor.get(`${foot}.hoof`);
        if (!track || !Array.isArray(track.points) || track.points.length !== frameCount) {
            failures.push(`${foot}:missing_hoof_track`);
            feet[foot] = { accepted: false, missing: true };
            return;
        }
        const points = Array.from({ length: frameCount }, (_, frame) => {
            const point = track.points.find((item) => Number(item?.frame) === frame) || track.points[frame];
            if (!point?.visible) return null;
            return {
                frame,
                x: finiteNumber(point.x, `${foot}.hoof[${frame}].x`),
                y: finiteNumber(point.y, `${foot}.hoof[${frame}].y`),
                confidence: finiteNumber(point.confidence ?? 1, `${foot}.hoof[${frame}].confidence`),
            };
        });
        const visible = points.filter(Boolean);
        const visibleFraction = visible.length / frameCount;
        const minimumConfidence = visible.length
            ? Math.min(...visible.map((point) => point.confidence))
            : 0;
        const xs = visible.map((point) => point.x);
        const ys = visible.map((point) => point.y);
        const first = points[0];
        const last = points.at(-1);
        const horizontalRangePx = xs.length ? Math.max(...xs) - Math.min(...xs) : Infinity;
        const verticalRangePx = ys.length ? Math.max(...ys) - Math.min(...ys) : Infinity;
        const displacements = first
            ? visible.map((point) => Math.hypot(point.x - first.x, point.y - first.y))
            : [Infinity];
        const speeds = [];
        for (let frame = 1; frame < frameCount; frame += 1) {
            if (points[frame - 1] && points[frame]) {
                speeds.push(Math.hypot(
                    points[frame].x - points[frame - 1].x,
                    points[frame].y - points[frame - 1].y,
                ));
            }
        }
        const endpointDisplacementPx = first && last
            ? Math.hypot(last.x - first.x, last.y - first.y)
            : Infinity;
        const startVelocity = points[0] && points[1]
            ? [points[1].x - points[0].x, points[1].y - points[0].y]
            : null;
        const endVelocity = points.at(-2) && points.at(-1)
            ? [points.at(-1).x - points.at(-2).x, points.at(-1).y - points.at(-2).y]
            : null;
        const velocitySeamPxPerFrame = startVelocity && endVelocity
            ? Math.hypot(startVelocity[0] - endVelocity[0], startVelocity[1] - endVelocity[1])
            : Infinity;
        const maximumObservedDisplacementPx = Math.max(...displacements);
        const p95SpeedPxPerFrame = speeds.length ? quantile(speeds, 0.95) : Infinity;
        const accepted = visibleFraction >= minimumVisibleFraction
            && minimumConfidence >= minimumVisibleConfidence
            && horizontalRangePx <= maximumHorizontalRangePx
            && verticalRangePx <= maximumVerticalRangePx
            && maximumObservedDisplacementPx <= maximumDisplacementPx
            && endpointDisplacementPx <= maximumEndpointDisplacementPx
            && p95SpeedPxPerFrame <= maximumP95SpeedPxPerFrame
            && velocitySeamPxPerFrame <= maximumVelocitySeamPxPerFrame;
        feet[foot] = {
            accepted,
            visibleFraction,
            minimumVisibleConfidence: minimumConfidence,
            horizontalRangePx,
            verticalRangePx,
            maximumDisplacementPx: maximumObservedDisplacementPx,
            endpointDisplacementPx,
            p95SpeedPxPerFrame,
            velocitySeamPxPerFrame,
            contactFrames: visible.map((point) => point.frame),
        };
        if (visibleFraction < minimumVisibleFraction) failures.push(`${foot}:visibility`);
        if (minimumConfidence < minimumVisibleConfidence) failures.push(`${foot}:confidence`);
        if (horizontalRangePx > maximumHorizontalRangePx) failures.push(`${foot}:horizontal_slide`);
        if (verticalRangePx > maximumVerticalRangePx) failures.push(`${foot}:hoof_lift`);
        if (maximumObservedDisplacementPx > maximumDisplacementPx) failures.push(`${foot}:maximum_displacement`);
        if (endpointDisplacementPx > maximumEndpointDisplacementPx) failures.push(`${foot}:endpoint_pose`);
        if (p95SpeedPxPerFrame > maximumP95SpeedPxPerFrame) failures.push(`${foot}:contact_speed`);
        if (velocitySeamPxPerFrame > maximumVelocitySeamPxPerFrame) failures.push(`${foot}:velocity_seam`);
    });

    const bodyEndpointPx = [];
    const bodyVelocitySeamPxPerFrame = [];
    (observations.tracks || []).forEach((track) => {
        if (!Array.isArray(track?.points) || track.points.length !== frameCount) return;
        const first = track.points[0];
        const second = track.points[1];
        const previous = track.points.at(-2);
        const last = track.points.at(-1);
        if (first?.visible && last?.visible) {
            bodyEndpointPx.push(Math.hypot(Number(last.x) - Number(first.x), Number(last.y) - Number(first.y)));
        }
        if (first?.visible && second?.visible && previous?.visible && last?.visible) {
            bodyVelocitySeamPxPerFrame.push(Math.hypot(
                (Number(second.x) - Number(first.x)) - (Number(last.x) - Number(previous.x)),
                (Number(second.y) - Number(first.y)) - (Number(last.y) - Number(previous.y)),
            ));
        }
    });
    const body = {
        trackCount: (observations.tracks || []).length,
        endpointSampleCount: bodyEndpointPx.length,
        velocitySeamSampleCount: bodyVelocitySeamPxPerFrame.length,
        endpointMedianPx: quantile(bodyEndpointPx, 0.5),
        endpointMedianDiagonal: quantile(bodyEndpointPx, 0.5) / diagonal,
        endpointP95Px: quantile(bodyEndpointPx, 0.95),
        velocitySeamP95PxPerFrame: quantile(bodyVelocitySeamPxPerFrame, 0.95),
    };
    if (!bodyEndpointPx.length || body.endpointMedianDiagonal > maximumBodyEndpointMedianDiagonal) {
        failures.push('body:endpoint_median');
    }
    if (!bodyEndpointPx.length || body.endpointP95Px > maximumBodyEndpointP95Px) {
        failures.push('body:endpoint_p95');
    }
    if (!bodyVelocitySeamPxPerFrame.length
        || body.velocitySeamP95PxPerFrame > maximumBodyVelocitySeamP95PxPerFrame) {
        failures.push('body:velocity_seam_p95');
    }

    const uniqueFailures = [...new Set(failures)];
    return {
        schema: 'autorig-horse-planted-idle-qa.v1',
        profile: {
            id: 'horse.all_hooves_planted_idle.v1',
            gait: 'planted_idle',
            distinctFromWalkAndTrotProfiles: true,
        },
        accepted: uniqueFailures.length === 0,
        status: uniqueFailures.length ? 'FAIL' : 'PASS',
        frameCount,
        feet,
        body,
        failures: uniqueFailures,
        gates: {
            minimumVisibleFraction,
            minimumVisibleConfidence,
            maximumHorizontalRangePx,
            maximumVerticalRangePx,
            maximumDisplacementPx,
            maximumEndpointDisplacementPx,
            maximumP95SpeedPxPerFrame,
            maximumVelocitySeamPxPerFrame,
            maximumBodyEndpointMedianDiagonal,
            maximumBodyEndpointP95Px,
            maximumBodyVelocitySeamP95PxPerFrame,
        },
    };
}

export const SEMANTIC_LIMB_LABELS = DEFAULT_LIMB_LABELS;
