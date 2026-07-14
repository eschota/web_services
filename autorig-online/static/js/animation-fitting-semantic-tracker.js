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

export const SEMANTIC_LIMB_LABELS = DEFAULT_LIMB_LABELS;
