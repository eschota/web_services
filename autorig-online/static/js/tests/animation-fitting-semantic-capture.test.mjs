import assert from 'node:assert/strict';
import test from 'node:test';

import {
    HORSE_SEMANTIC_CAPTURE_LABELS,
    HORSE_SEMANTIC_CAPTURE_SCHEMA,
    captureHorse2SemanticReference,
} from '../animation-fitting-semantic-capture.js';
import { linearChannelToSrgbByte } from '../animation-fitting-semantic-tracker.js';

const PALETTE = {
    fore_left: [0, 0.85, 1],
    fore_right: [0.12, 0.22, 1],
    hind_left: [1, 0.72, 0.02],
    hind_right: [1, 0.08, 0.55],
};

const PROFILE = {
    profile_id: 'horse_2.semantic_limbs.v1',
    palette_linear: PALETTE,
};

function fittingSkeleton() {
    const xByLabel = {
        fore_left: 180,
        fore_right: 300,
        hind_left: 470,
        hind_right: 590,
    };
    return {
        schema: 'autorig-browser-fitting-skeleton.v1',
        rigType: 'HORSE_2',
        projection: {
            outputResolution: [768, 448],
        },
        limbs: Object.fromEntries(Object.entries(xByLabel).map(([label, x]) => [label, {
            joints: [
                { restStart: [x, 130], restEnd: [x + 4, 205] },
                { restStart: [x + 4, 205], restEnd: [x - 3, 285] },
                { restStart: [x - 3, 285], restEnd: [x + 1, 375] },
            ],
        }])),
    };
}

class FakeContext {
    constructor() {
        this.operations = [];
        this.path = [];
        this.fillStyle = '';
        this.strokeStyle = '';
        this.lineWidth = 0;
        this.lineCap = '';
        this.lineJoin = '';
        this.globalAlpha = 0;
        this.globalCompositeOperation = '';
        this.imageSmoothingEnabled = false;
        this.imageSmoothingQuality = '';
    }

    fillRect(...args) {
        this.operations.push({ type: 'fillRect', args, fillStyle: this.fillStyle });
    }

    drawImage(...args) {
        this.operations.push({ type: 'drawImage', args });
    }

    beginPath() {
        this.path = [];
    }

    moveTo(x, y) {
        this.path.push(['moveTo', x, y]);
    }

    lineTo(x, y) {
        this.path.push(['lineTo', x, y]);
    }

    stroke() {
        this.operations.push({
            type: 'stroke',
            path: this.path.map((item) => [...item]),
            strokeStyle: this.strokeStyle,
            lineWidth: this.lineWidth,
            lineCap: this.lineCap,
            lineJoin: this.lineJoin,
            globalAlpha: this.globalAlpha,
        });
    }
}

class FakeCanvas {
    constructor(width = 1, height = 1) {
        this.width = width;
        this.height = height;
        this.context = new FakeContext();
        this.getContextCalls = [];
        this.toDataUrlCalls = [];
    }

    getContext(...args) {
        this.getContextCalls.push(args);
        return this.context;
    }

    toDataURL(...args) {
        this.toDataUrlCalls.push(args);
        return 'data:image/jpeg;base64,semantic-fixture';
    }
}

function deepFrozen(value) {
    if (!value || typeof value !== 'object') return true;
    return Object.isFrozen(value) && Object.values(value).every(deepFrozen);
}

test('captures a contained canonical canvas plus four opaque semantic polylines', () => {
    const source = new FakeCanvas(1280, 720);
    const sourceSnapshot = { width: source.width, height: source.height, calls: source.getContextCalls.length };
    let output = null;
    const result = captureHorse2SemanticReference({
        sourceCanvas: source,
        fittingSkeleton: fittingSkeleton(),
        semanticProfile: PROFILE,
        canvasFactory(width, height) {
            output = new FakeCanvas(width, height);
            return output;
        },
    });

    assert.equal(output.width, 768);
    assert.equal(output.height, 448);
    assert.deepEqual(output.getContextCalls, [['2d', { alpha: false, colorSpace: 'srgb' }]]);
    assert.deepEqual(output.toDataUrlCalls, [['image/jpeg', 0.95]]);
    assert.equal(result.frame_jpeg_data_url_string, 'data:image/jpeg;base64,semantic-fixture');

    const draw = output.context.operations.find((operation) => operation.type === 'drawImage');
    assert.equal(draw.args[0], source);
    assert.deepEqual(draw.args.slice(1), [0, 8, 768, 432]);

    const strokes = output.context.operations.filter((operation) => operation.type === 'stroke');
    assert.equal(strokes.length, 8);
    assert.deepEqual(strokes.slice(0, 4).map((stroke) => stroke.lineWidth), [30, 30, 30, 30]);
    assert.equal(new Set(strokes.slice(0, 4).map((stroke) => stroke.strokeStyle)).size, 1);
    assert.deepEqual(strokes.slice(4).map((stroke) => stroke.lineWidth), [18, 18, 18, 18]);
    assert.deepEqual(strokes.slice(4).map((stroke) => stroke.strokeStyle), HORSE_SEMANTIC_CAPTURE_LABELS.map((label) => {
        const bytes = PALETTE[label].map(linearChannelToSrgbByte);
        return `rgb(${bytes[0]}, ${bytes[1]}, ${bytes[2]})`;
    }));
    strokes.forEach((stroke) => {
        assert.equal(stroke.lineCap, 'round');
        assert.equal(stroke.lineJoin, 'round');
        assert.equal(stroke.globalAlpha, 1);
    });
    assert.deepEqual(strokes[4].path, [
        ['moveTo', 180, 130],
        ['lineTo', 184, 205],
        ['lineTo', 177, 285],
        ['lineTo', 181, 375],
    ]);

    assert.deepEqual(
        { width: source.width, height: source.height, calls: source.getContextCalls.length },
        sourceSnapshot,
    );
});

test('returns deterministic deeply immutable metadata for transforms, legend and geometry', () => {
    let output = null;
    const result = captureHorse2SemanticReference({
        sourceCanvas: new FakeCanvas(400, 800),
        fittingSkeleton: fittingSkeleton(),
        semanticProfile: PROFILE,
        semanticStrokeWidthPx: 20,
        underlayStrokeWidthPx: 34,
        jpegQuality: 1,
        canvasFactory(width, height) {
            output = new FakeCanvas(width, height);
            return output;
        },
    });
    const metadata = result.metadata_object;
    assert.equal(metadata.schema, HORSE_SEMANTIC_CAPTURE_SCHEMA);
    assert.equal(metadata.profile_id_string, PROFILE.profile_id);
    assert.equal(metadata.rig_type_string, 'HORSE_2');
    assert.deepEqual(metadata.source_resolution_array, [400, 800]);
    assert.deepEqual(metadata.reference_resolution_array, [768, 448]);
    assert.equal(metadata.viewer_contain_object.scale_float, 0.56);
    assert.equal(metadata.viewer_contain_object.offset_x_float, 272);
    assert.equal(metadata.viewer_contain_object.offset_y_float, 0);
    assert.equal(metadata.viewer_contain_object.draw_width_float, 224);
    assert.equal(metadata.viewer_contain_object.draw_height_float, 448);
    assert.deepEqual(metadata.semantic_legend_object.labels_array, HORSE_SEMANTIC_CAPTURE_LABELS);
    assert.deepEqual(metadata.semantic_legend_object.palette_linear_object, PALETTE);
    assert.deepEqual(
        metadata.semantic_legend_object.palette_srgb_byte_object.fore_left,
        PALETTE.fore_left.map(linearChannelToSrgbByte),
    );
    assert.deepEqual(metadata.overlay_object.polylines_object.hind_right, [
        [590, 130], [594, 205], [587, 285], [591, 375],
    ]);
    assert.equal(metadata.overlay_object.semantic_width_px_float, 20);
    assert.equal(metadata.overlay_object.underlay_width_px_float, 34);
    assert.equal(metadata.overlay_object.jpeg_quality_float, 1);
    assert.equal(deepFrozen(result), true);

    const draw = output.context.operations.find((operation) => operation.type === 'drawImage');
    assert.equal(draw.args[1], 272);
    assert.equal(draw.args[2], 0);
    assert.equal(draw.args[3], 224);
    assert.equal(draw.args[4], 448);
});

test('fails closed for missing chains, wrong projection, out-of-bounds and short segments', () => {
    const run = (skeleton, options = {}) => captureHorse2SemanticReference({
        sourceCanvas: new FakeCanvas(768, 448),
        fittingSkeleton: skeleton,
        semanticProfile: PROFILE,
        canvasFactory: () => new FakeCanvas(),
        ...options,
    });

    {
        const skeleton = fittingSkeleton();
        delete skeleton.limbs.fore_left;
        assert.throws(() => run(skeleton), /missing the fore_left chain/);
    }
    {
        const skeleton = fittingSkeleton();
        skeleton.projection.outputResolution = [512, 320];
        assert.throws(() => run(skeleton), /projected at 768x448/);
    }
    {
        const skeleton = fittingSkeleton();
        skeleton.limbs.hind_right.joints[2].restEnd = [769, 375];
        assert.throws(() => run(skeleton), /outside the 768x448 semantic reference/);
    }
    {
        const skeleton = fittingSkeleton();
        skeleton.limbs.fore_right.joints[1].restEnd = [304.1, 205];
        assert.throws(() => run(skeleton), /shorter than minimumSegmentLengthPx/);
    }
    {
        const source = new FakeCanvas(768, 448);
        assert.throws(
            () => run(fittingSkeleton(), { sourceCanvas: source, canvasFactory: () => source }),
            /must differ from sourceCanvas/,
        );
    }
});
