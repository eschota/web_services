import { linearChannelToSrgbByte } from './animation-fitting-semantic-tracker.js';

export const HORSE_SEMANTIC_CAPTURE_SCHEMA = 'autorig.browser-semantic-reference.v1';

export const HORSE_SEMANTIC_CAPTURE_LABELS = Object.freeze([
    'fore_left',
    'fore_right',
    'hind_left',
    'hind_right',
]);

const REFERENCE_RESOLUTION = Object.freeze([768, 448]);
const DEFAULT_SEMANTIC_STROKE_WIDTH = 18;
const DEFAULT_UNDERLAY_STROKE_WIDTH = 30;
const DEFAULT_MINIMUM_SEGMENT_LENGTH = 1;
const DEFAULT_CHAIN_CONTINUITY_TOLERANCE = 0.5;
const DEFAULT_JPEG_QUALITY = 0.95;
const UNDERLAY_SRGB_BYTES = Object.freeze([178, 185, 195]);

function finiteNumber(value, field) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error(`${field} must be finite`);
    return number;
}

function positiveNumber(value, field) {
    const number = finiteNumber(value, field);
    if (number <= 0) throw new Error(`${field} must be positive`);
    return number;
}

function positiveDimension(value, field) {
    const number = positiveNumber(value, field);
    if (!Number.isInteger(number)) throw new Error(`${field} must be an integer`);
    return number;
}

function point2(value, field) {
    if (!Array.isArray(value) && !ArrayBuffer.isView(value)) {
        throw new Error(`${field} must be a two-component point`);
    }
    if (value.length !== 2) throw new Error(`${field} must contain exactly two components`);
    return [
        finiteNumber(value[0], `${field}[0]`),
        finiteNumber(value[1], `${field}[1]`),
    ];
}

function distance2(a, b) {
    return Math.hypot(a[0] - b[0], a[1] - b[1]);
}

function assertInsideReference(point, field) {
    if (
        point[0] < 0 || point[0] > REFERENCE_RESOLUTION[0]
        || point[1] < 0 || point[1] > REFERENCE_RESOLUTION[1]
    ) {
        throw new Error(`${field} is outside the 768x448 semantic reference`);
    }
}

function linearColor(value, field) {
    if (!Array.isArray(value) && !ArrayBuffer.isView(value)) {
        throw new Error(`${field} must be a linear RGB triplet`);
    }
    if (value.length !== 3) throw new Error(`${field} must contain exactly three channels`);
    return Array.from(value, (channel, index) => {
        const number = finiteNumber(channel, `${field}[${index}]`);
        if (number < 0 || number > 1) throw new Error(`${field}[${index}] must be inside [0, 1]`);
        return number;
    });
}

function rgbStyle(bytes) {
    return `rgb(${bytes[0]}, ${bytes[1]}, ${bytes[2]})`;
}

function paletteSource(profile) {
    return profile?.semantic_profile?.palette_linear
        || profile?.palette_linear
        || profile?.paletteLinear
        || null;
}

function profileId(profile) {
    return String(
        profile?.semantic_profile?.profile_id
        || profile?.semantic_profile?.profileId
        || profile?.profile_id
        || profile?.profileId
        || '',
    ).trim();
}

function normalizePalette(profile) {
    const source = paletteSource(profile);
    if (!source || typeof source !== 'object' || Array.isArray(source)) {
        throw new Error('semanticProfile.palette_linear is required');
    }
    const linear = {};
    const srgb = {};
    HORSE_SEMANTIC_CAPTURE_LABELS.forEach((label) => {
        linear[label] = linearColor(source[label], `semanticProfile.palette_linear.${label}`);
        srgb[label] = linear[label].map((channel) => linearChannelToSrgbByte(channel));
    });
    return { linear, srgb };
}

function assertProjectionResolution(skeleton) {
    const output = skeleton?.projection?.outputResolution;
    if (!Array.isArray(output) || output.length !== 2) {
        throw new Error('fittingSkeleton.projection.outputResolution must be [768, 448]');
    }
    if (Number(output[0]) !== REFERENCE_RESOLUTION[0] || Number(output[1]) !== REFERENCE_RESOLUTION[1]) {
        throw new Error('fittingSkeleton rest points must be projected at 768x448');
    }
}

function normalizePolylines(skeleton, options) {
    if (!skeleton || typeof skeleton !== 'object') throw new Error('fittingSkeleton is required');
    if (String(skeleton.rigType || '') !== 'HORSE_2') {
        throw new Error('fittingSkeleton.rigType must be HORSE_2');
    }
    assertProjectionResolution(skeleton);
    const minimumSegmentLength = positiveNumber(
        options.minimumSegmentLengthPx ?? DEFAULT_MINIMUM_SEGMENT_LENGTH,
        'minimumSegmentLengthPx',
    );
    const continuityTolerance = finiteNumber(
        options.chainContinuityTolerancePx ?? DEFAULT_CHAIN_CONTINUITY_TOLERANCE,
        'chainContinuityTolerancePx',
    );
    if (continuityTolerance < 0) throw new Error('chainContinuityTolerancePx must be non-negative');

    const polylines = {};
    HORSE_SEMANTIC_CAPTURE_LABELS.forEach((label) => {
        const joints = skeleton?.limbs?.[label]?.joints;
        if (!Array.isArray(joints) || !joints.length) {
            throw new Error(`fittingSkeleton is missing the ${label} chain`);
        }
        const points = [];
        joints.forEach((joint, index) => {
            const start = point2(joint?.restStart, `fittingSkeleton.limbs.${label}.joints[${index}].restStart`);
            const end = point2(joint?.restEnd, `fittingSkeleton.limbs.${label}.joints[${index}].restEnd`);
            assertInsideReference(start, `${label} segment ${index} start`);
            assertInsideReference(end, `${label} segment ${index} end`);
            if (distance2(start, end) < minimumSegmentLength) {
                throw new Error(`${label} segment ${index} is shorter than minimumSegmentLengthPx`);
            }
            if (index > 0 && distance2(points.at(-1), start) > continuityTolerance) {
                throw new Error(`${label} chain is discontinuous before segment ${index}`);
            }
            if (index === 0) points.push(start);
            points.push(end);
        });
        polylines[label] = points;
    });
    return polylines;
}

function stableGeometryNumber(value) {
    if (Math.abs(value) < 1e-10) return 0;
    return Math.round(value * 1e12) / 1e12;
}

function containTransform(sourceWidth, sourceHeight) {
    const scale = Math.min(
        REFERENCE_RESOLUTION[0] / sourceWidth,
        REFERENCE_RESOLUTION[1] / sourceHeight,
    );
    const drawWidth = stableGeometryNumber(sourceWidth * scale);
    const drawHeight = stableGeometryNumber(sourceHeight * scale);
    return {
        scale: stableGeometryNumber(scale),
        drawWidth,
        drawHeight,
        offsetX: stableGeometryNumber((REFERENCE_RESOLUTION[0] - drawWidth) / 2),
        offsetY: stableGeometryNumber((REFERENCE_RESOLUTION[1] - drawHeight) / 2),
    };
}

function createOutputCanvas(options) {
    let canvas = null;
    if (typeof options.canvasFactory === 'function') {
        canvas = options.canvasFactory(REFERENCE_RESOLUTION[0], REFERENCE_RESOLUTION[1]);
    } else {
        const documentRef = options.document || globalThis.document;
        canvas = documentRef?.createElement?.('canvas') || null;
    }
    if (!canvas) throw new Error('A canvasFactory or browser document is required');
    canvas.width = REFERENCE_RESOLUTION[0];
    canvas.height = REFERENCE_RESOLUTION[1];
    return canvas;
}

function drawPolyline(context, points) {
    context.beginPath();
    context.moveTo(points[0][0], points[0][1]);
    for (let index = 1; index < points.length; index += 1) {
        context.lineTo(points[index][0], points[index][1]);
    }
    context.stroke();
}

function deepFreeze(value) {
    if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
    Object.getOwnPropertyNames(value).forEach((key) => deepFreeze(value[key]));
    return Object.freeze(value);
}

/**
 * Capture an actionless Horse_2 semantic fitting reference without mutating the
 * source renderer canvas or the Three.js scene. Skeleton rest points must
 * already use the canonical 768x448 reference coordinate system.
 */
export function captureHorse2SemanticReference(options = {}) {
    const sourceCanvas = options.sourceCanvas;
    if (!sourceCanvas || typeof sourceCanvas !== 'object') throw new Error('sourceCanvas is required');
    const sourceWidth = positiveDimension(sourceCanvas.width, 'sourceCanvas.width');
    const sourceHeight = positiveDimension(sourceCanvas.height, 'sourceCanvas.height');
    const profile = options.semanticProfile;
    const id = profileId(profile);
    if (!id) throw new Error('semanticProfile.profile_id is required');
    const palette = normalizePalette(profile);
    const polylines = normalizePolylines(options.fittingSkeleton, options);

    const semanticStrokeWidth = positiveNumber(
        options.semanticStrokeWidthPx ?? DEFAULT_SEMANTIC_STROKE_WIDTH,
        'semanticStrokeWidthPx',
    );
    const underlayStrokeWidth = positiveNumber(
        options.underlayStrokeWidthPx ?? DEFAULT_UNDERLAY_STROKE_WIDTH,
        'underlayStrokeWidthPx',
    );
    if (underlayStrokeWidth <= semanticStrokeWidth) {
        throw new Error('underlayStrokeWidthPx must be greater than semanticStrokeWidthPx');
    }
    const jpegQuality = finiteNumber(options.jpegQuality ?? DEFAULT_JPEG_QUALITY, 'jpegQuality');
    if (jpegQuality <= 0 || jpegQuality > 1) throw new Error('jpegQuality must be inside (0, 1]');

    const canvas = createOutputCanvas(options);
    if (canvas === sourceCanvas) throw new Error('semantic capture output canvas must differ from sourceCanvas');
    const context = canvas.getContext?.('2d', { alpha: false, colorSpace: 'srgb' });
    if (!context) throw new Error('A writable sRGB 2D canvas context is required');
    const contain = containTransform(sourceWidth, sourceHeight);

    context.globalCompositeOperation = 'source-over';
    context.globalAlpha = 1;
    context.fillStyle = '#000000';
    context.fillRect(0, 0, REFERENCE_RESOLUTION[0], REFERENCE_RESOLUTION[1]);
    context.imageSmoothingEnabled = true;
    context.imageSmoothingQuality = 'high';
    context.drawImage(
        sourceCanvas,
        contain.offsetX,
        contain.offsetY,
        contain.drawWidth,
        contain.drawHeight,
    );

    context.lineCap = 'round';
    context.lineJoin = 'round';
    context.globalAlpha = 1;
    context.strokeStyle = rgbStyle(UNDERLAY_SRGB_BYTES);
    context.lineWidth = underlayStrokeWidth;
    HORSE_SEMANTIC_CAPTURE_LABELS.forEach((label) => drawPolyline(context, polylines[label]));

    context.lineWidth = semanticStrokeWidth;
    HORSE_SEMANTIC_CAPTURE_LABELS.forEach((label) => {
        context.strokeStyle = rgbStyle(palette.srgb[label]);
        drawPolyline(context, polylines[label]);
    });

    if (typeof canvas.toDataURL !== 'function') throw new Error('semantic capture canvas.toDataURL is required');
    const dataUrl = canvas.toDataURL('image/jpeg', jpegQuality);
    if (typeof dataUrl !== 'string' || !dataUrl.startsWith('data:image/jpeg')) {
        throw new Error('semantic capture did not produce a JPEG data URL');
    }

    const metadata = deepFreeze({
        schema: HORSE_SEMANTIC_CAPTURE_SCHEMA,
        profile_id_string: id,
        rig_type_string: 'HORSE_2',
        composition_string: 'canonical_rgb_contain_with_semantic_bone_overlay',
        source_resolution_array: [sourceWidth, sourceHeight],
        reference_resolution_array: [...REFERENCE_RESOLUTION],
        viewer_contain_object: {
            scale_float: contain.scale,
            offset_x_float: contain.offsetX,
            offset_y_float: contain.offsetY,
            draw_width_float: contain.drawWidth,
            draw_height_float: contain.drawHeight,
        },
        semantic_legend_object: {
            color_space_string: 'linear_rgb',
            labels_array: [...HORSE_SEMANTIC_CAPTURE_LABELS],
            palette_linear_object: Object.fromEntries(
                HORSE_SEMANTIC_CAPTURE_LABELS.map((label) => [label, [...palette.linear[label]]]),
            ),
            palette_srgb_byte_object: Object.fromEntries(
                HORSE_SEMANTIC_CAPTURE_LABELS.map((label) => [label, [...palette.srgb[label]]]),
            ),
        },
        overlay_object: {
            underlay_srgb_byte_array: [...UNDERLAY_SRGB_BYTES],
            underlay_width_px_float: underlayStrokeWidth,
            semantic_width_px_float: semanticStrokeWidth,
            line_cap_string: 'round',
            line_join_string: 'round',
            jpeg_quality_float: jpegQuality,
            polylines_object: Object.fromEntries(
                HORSE_SEMANTIC_CAPTURE_LABELS.map((label) => [
                    label,
                    polylines[label].map((point) => [...point]),
                ]),
            ),
        },
    });

    return Object.freeze({
        frame_jpeg_data_url_string: dataUrl,
        metadata_object: metadata,
    });
}
