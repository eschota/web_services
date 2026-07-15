import assert from 'node:assert/strict';
import test from 'node:test';

import {
    HORSE_V12_RECOVERY_GUIDE_PLAN,
    analyzeStaticSceneGuideFrames,
    browserGuideSceneProfile,
    buildHorseV12ContactCueVisibilityPlan,
    buildHorseV14ContactCueVisibilityPlan,
} from '../../../tools/animation_fitting/author_browser_horse_swing_guides.mjs';

const WIDTH = 768;
const HEIGHT = 448;
const FRAME_INDICES = [0, 6, 12, 18, 24, 30, 36, 42, 48];
const RECOVERY_INDICES = [12, 24, 36];

function decodedGuide(frameIndex, endpointBytes, centerOffset = 0) {
    const rgb = Buffer.alloc(WIDTH * HEIGHT * 3);
    for (let pixel = 0; pixel < WIDTH * HEIGHT; pixel += 1) {
        rgb[pixel * 3] = 200;
        rgb[pixel * 3 + 1] = 210;
        rgb[pixel * 3 + 2] = 220;
    }
    for (let y = 210; y < 220; y += 1) {
        for (let x = 370; x < 380; x += 1) {
            const offset = (y * WIDTH + x) * 3;
            rgb[offset] += centerOffset;
            rgb[offset + 1] += centerOffset;
            rgb[offset + 2] += centerOffset;
        }
    }
    const isStance = frameIndex === 0 || frameIndex === 48 || RECOVERY_INDICES.includes(frameIndex);
    return {
        frameIndex,
        buffer: isStance ? endpointBytes : Buffer.from(`swing-${frameIndex}`),
        decoded: { width: WIDTH, height: HEIGHT, rgb },
    };
}

test('v12 browser guide plan alternates four single-hoof swings with explicit four-hoof recovery', () => {
    assert.deepEqual(HORSE_V12_RECOVERY_GUIDE_PLAN.map((guide) => guide.frameIndex), FRAME_INDICES);
    assert.deepEqual(
        HORSE_V12_RECOVERY_GUIDE_PLAN.map((guide) => guide.strength),
        [0.8, 0.7, 0.85, 0.7, 0.85, 0.7, 0.85, 0.7, 0.8],
    );
    assert.deepEqual(
        HORSE_V12_RECOVERY_GUIDE_PLAN.filter((guide) => guide.swingLimb).map((guide) => guide.swingLimb),
        ['hind_left', 'fore_left', 'hind_right', 'fore_right'],
    );
    assert.deepEqual(
        HORSE_V12_RECOVERY_GUIDE_PLAN.filter((guide) => guide.role.startsWith('four_hoof_recovery')).map((guide) => guide.frameIndex),
        RECOVERY_INDICES,
    );
});

test('v12 scene profile is unified browser-only with deterministic cues and no shadow map', () => {
    const profile = browserGuideSceneProfile('v12_unified_browser_recovery_guides_v1');
    assert.equal(profile.unifiedBrowserScene, true);
    assert.equal(profile.recoveryGuides, true);
    assert.equal(profile.deterministicContactCues, true);
    assert.equal(profile.shadowsEnabled, false);
    assert.deepEqual(profile.guideFrames, FRAME_INDICES);
    assert.deepEqual(profile.recoveryFrames, RECOVERY_INDICES);
    assert.throws(() => browserGuideSceneProfile('v12_unknown'), /scene-contract must be/);
});

test('v12 per-guide cue visibility hides only the airborne hoof and exposes all stance contacts', () => {
    const rows = buildHorseV12ContactCueVisibilityPlan();
    assert.deepEqual(rows.map((row) => row.frameIndex), FRAME_INDICES);
    for (const row of rows) {
        const guide = HORSE_V12_RECOVERY_GUIDE_PLAN.find((value) => value.frameIndex === row.frameIndex);
        if (guide.swingLimb) {
            assert.equal(row.visibleCueCount, 3);
            assert.equal(row.hiddenCueCount, 1);
            assert.deepEqual(row.hiddenLimbs, [guide.swingLimb]);
            assert.equal(row.visibleLimbs.includes(guide.swingLimb), false);
        } else {
            assert.equal(row.visibleCueCount, 4);
            assert.equal(row.hiddenCueCount, 0);
            assert.deepEqual(row.hiddenLimbs, []);
            assert.deepEqual(row.visibleLimbs, ['hind_left', 'fore_left', 'hind_right', 'fore_right']);
        }
    }
});

test('v14 scene profile and contact cues cover one contiguous 49-frame interval', () => {
    const profile = browserGuideSceneProfile('v14_unified_browser_interval_guide_v1');
    assert.equal(profile.unifiedBrowserScene, true);
    assert.equal(profile.recoveryGuides, false);
    assert.equal(profile.intervalGuide, true);
    assert.equal(profile.deterministicContactCues, true);
    assert.equal(profile.shadowsEnabled, false);
    assert.deepEqual(profile.guideFrames, Array.from({ length: 49 }, (_, frameIndex) => frameIndex));

    const rows = buildHorseV14ContactCueVisibilityPlan();
    assert.equal(rows.length, 49);
    assert.deepEqual(rows.map((row) => row.frameIndex), profile.guideFrames);
    for (const frameIndex of [0, 12, 24, 36, 48]) {
        assert.equal(rows[frameIndex].visibleCueCount, 4);
        assert.equal(rows[frameIndex].hiddenCueCount, 0);
    }
    for (const [start, end, limb] of [
        [1, 11, 'hind_left'],
        [13, 23, 'fore_left'],
        [25, 35, 'hind_right'],
        [37, 47, 'fore_right'],
    ]) {
        for (let frameIndex = start; frameIndex <= end; frameIndex += 1) {
            assert.deepEqual(rows[frameIndex].hiddenLimbs, [limb]);
            assert.equal(rows[frameIndex].visibleCueCount, 3);
        }
    }
});

test('v12 nine-frame static-scene QA accepts byte-identical actionless recovery and endpoints', () => {
    const endpointBytes = Buffer.from('same actionless browser frame bytes');
    const frames = FRAME_INDICES.map((frameIndex, index) => decodedGuide(
        frameIndex,
        endpointBytes,
        [6, 18, 30, 42].includes(frameIndex) ? index : 0,
    ));
    const report = analyzeStaticSceneGuideFrames(frames, { expectedFrameIndices: FRAME_INDICES });
    assert.equal(report.status, 'PASS');
    assert.equal(report.endpoint_byte_identical_bool, true);
    assert.deepEqual(report.expected_frame_indices_array, FRAME_INDICES);
    assert.equal(report.maximum_background_channel_delta_int, 0);
    assert.equal(report.background_mean_luma_range_float, 0);
    assert.ok(report.full_frame_mean_luma_range_float < 0.5);
    for (const frameIndex of RECOVERY_INDICES) {
        assert.equal(frames.find((frame) => frame.frameIndex === frameIndex).buffer.equals(endpointBytes), true);
    }
});
