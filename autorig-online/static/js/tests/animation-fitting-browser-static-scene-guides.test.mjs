import assert from 'node:assert/strict';
import test from 'node:test';
import zlib from 'node:zlib';

import {
    analyzeStaticSceneGuideFrames,
    decodeOpaqueRgbPng,
} from '../../../tools/animation_fitting/author_browser_horse_swing_guides.mjs';

const WIDTH = 768;
const HEIGHT = 448;
const GUIDE_FRAMES = [0, 6, 18, 30, 42, 48];

function chunk(type, data) {
    const result = Buffer.alloc(12 + data.length);
    result.writeUInt32BE(data.length, 0);
    result.write(type, 4, 4, 'ascii');
    data.copy(result, 8);
    // The production decoder validates structure and zlib scanlines while the
    // immutable SHA pin protects bytes. CRC bytes are deliberately irrelevant
    // to this small decoder test fixture.
    return result;
}

function rgbaPng(width, height, rgba) {
    const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
    const ihdr = Buffer.alloc(13);
    ihdr.writeUInt32BE(width, 0);
    ihdr.writeUInt32BE(height, 4);
    ihdr[8] = 8;
    ihdr[9] = 6;
    const rows = [];
    for (let y = 0; y < height; y += 1) {
        rows.push(Buffer.from([0]), rgba.subarray(y * width * 4, (y + 1) * width * 4));
    }
    return Buffer.concat([
        signature,
        chunk('IHDR', ihdr),
        chunk('IDAT', zlib.deflateSync(Buffer.concat(rows))),
        chunk('IEND', Buffer.alloc(0)),
    ]);
}

function decodedGuide(frameIndex, centerValue = 230, endpointBytes = null) {
    const rgb = Buffer.alloc(WIDTH * HEIGHT * 3);
    for (let pixel = 0; pixel < WIDTH * HEIGHT; pixel += 1) {
        rgb[pixel * 3] = 200;
        rgb[pixel * 3 + 1] = 210;
        rgb[pixel * 3 + 2] = 220;
    }
    for (let y = 210; y < 230; y += 1) {
        for (let x = 370; x < 390; x += 1) {
            const offset = (y * WIDTH + x) * 3;
            rgb[offset] = centerValue;
            rgb[offset + 1] = centerValue;
            rgb[offset + 2] = centerValue;
        }
    }
    return {
        frameIndex,
        buffer: endpointBytes || Buffer.from(`guide-${frameIndex}`),
        decoded: { width: WIDTH, height: HEIGHT, rgb },
    };
}

test('RGB PNG decoder reads opaque Chrome-style RGBA scanlines and rejects alpha', () => {
    const opaque = rgbaPng(2, 1, Buffer.from([10, 20, 30, 255, 40, 50, 60, 255]));
    const decoded = decodeOpaqueRgbPng(opaque, 'opaque fixture');
    assert.deepEqual([decoded.width, decoded.height], [2, 1]);
    assert.deepEqual([...decoded.rgb], [10, 20, 30, 40, 50, 60]);
    const transparent = rgbaPng(1, 1, Buffer.from([10, 20, 30, 254]));
    assert.throws(() => decodeOpaqueRgbPng(transparent, 'alpha fixture'), /fully opaque/);
});

test('unified browser guide QA accepts static border/exposure and byte-identical endpoints', () => {
    const endpointBytes = Buffer.from('same browser endpoint PNG bytes');
    const frames = GUIDE_FRAMES.map((frameIndex, index) => decodedGuide(
        frameIndex,
        index === 0 || index === GUIDE_FRAMES.length - 1 ? 230 : 228 + index,
        index === 0 || index === GUIDE_FRAMES.length - 1 ? endpointBytes : null,
    ));
    const report = analyzeStaticSceneGuideFrames(frames);
    assert.equal(report.status, 'PASS');
    assert.equal(report.decoded_rgb_statistics_bool, true);
    assert.equal(report.endpoint_byte_identical_bool, true);
    assert.equal(report.maximum_background_channel_delta_int, 0);
    assert.equal(report.background_mean_luma_range_float, 0);
    assert.ok(report.full_frame_mean_luma_range_float < 0.5);
});

test('unified browser guide QA fails closed on a changed background/exposure plate', () => {
    const endpointBytes = Buffer.from('same browser endpoint PNG bytes');
    const frames = GUIDE_FRAMES.map((frameIndex, index) => decodedGuide(
        frameIndex,
        230,
        index === 0 || index === GUIDE_FRAMES.length - 1 ? endpointBytes : null,
    ));
    frames[2].decoded.rgb[0] += 1;
    const report = analyzeStaticSceneGuideFrames(frames, { failClosed: false });
    assert.equal(report.status, 'FAIL');
    assert.equal(report.maximum_background_channel_delta_int, 1);
    assert.throws(() => analyzeStaticSceneGuideFrames(frames), /static-scene QA failed/);
});
