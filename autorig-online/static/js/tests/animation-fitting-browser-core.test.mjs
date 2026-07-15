import assert from 'node:assert/strict';
import test from 'node:test';

import {
    BROWSER_FITTING_SCHEMAS,
    fitBrowserAnimation,
    fittedTracksToThreeClip,
} from '../animation-fitting-browser-core.js';

const LABELS = ['fore_left', 'fore_right', 'hind_left', 'hind_right'];

function skeleton({ tightLimits = false, rootMotion = false, positionMappings = false } = {}) {
    const limbs = {};
    LABELS.forEach((label, index) => {
        const x = index * 2;
        limbs[label] = {
            joints: [
                {
                    bone: `${label}_upper`,
                    restStart: [x, 0],
                    restEnd: [x, 1],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: tightLimits ? -0.15 : -1.35,
                    maxAngle: tightLimits ? 0.15 : 1.35,
                    positionMapping: positionMappings ? {
                        restPosition: [x, 0, 0],
                        xAxisPerPixel: [0.01, 0, 0],
                        yAxisPerPixel: [0, -0.01, 0],
                        motionScale: 1,
                    } : undefined,
                },
                {
                    bone: `${label}_lower`,
                    restStart: [x, 1],
                    restEnd: [x, 2],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: tightLimits ? -0.2 : -1.7,
                    maxAngle: tightLimits ? 0.2 : 1.7,
                    positionMapping: positionMappings ? {
                        restPosition: [x, 1, 0],
                        xAxisPerPixel: [0.01, 0, 0],
                        yAxisPerPixel: [0, -0.01, 0],
                        motionScale: 1,
                    } : undefined,
                },
            ],
            trackedJointIndex: 1,
        };
    });
    return {
        schema: BROWSER_FITTING_SCHEMAS.skeleton,
        limbs,
        root: rootMotion ? {
            bone: 'HorseRoot',
            restPosition: [0, 0, 0],
            xAxisPerPixel: [0.01, 0, 0],
            yAxisPerPixel: [0, -0.01, 0],
            motionScale: 1,
        } : undefined,
    };
}

function track(anchorId, points) {
    return {
        anchor_id: anchorId,
        points: points.map(([x, y], frame) => ({
            frame,
            x,
            y,
            visible: true,
            confidence: 1,
        })),
    };
}

function observations({ frameCount = 7, loopMismatch = false, contact = false, extreme = false } = {}) {
    const tracks = [];
    LABELS.forEach((label, index) => {
        const x = index * 2;
        const proximal = [];
        const joint = [];
        const hoof = [];
        for (let frame = 0; frame < frameCount; frame += 1) {
            const phase = (frame / (frameCount - 1)) * Math.PI * 2 + index * Math.PI / 2;
            const targetX = extreme
                ? x + 2
                : x + Math.sin(phase) * 0.48 + (loopMismatch && frame === frameCount - 1 ? 0.35 : 0);
            const targetY = extreme ? 0 : 1.82 + Math.cos(phase) * 0.08;
            proximal.push([x, 0]);
            joint.push([x + Math.sin(phase) * 0.3, 0.95]);
            hoof.push([targetX, targetY]);
        }
        if (contact && label === 'hind_right') {
            hoof[2] = [x + 0.2, 1.98];
            hoof[3] = [x + 0.22, 1.99];
            hoof[4] = [x + 0.18, 1.97];
        }
        tracks.push(track(`${label}.proximal`, proximal));
        tracks.push(track(`${label}.joint`, joint));
        tracks.push(track(`${label}.hoof`, hoof));
    });
    return {
        schema: BROWSER_FITTING_SCHEMAS.observations,
        frame_count: frameCount,
        width: 512,
        height: 320,
        fps: 30,
        tracks,
        contacts: contact ? [{
            anchor_id: 'hind_right.hoof',
            frames: [2, 3, 4],
            weight: 1,
        }] : [],
    };
}

function orderedSkeleton() {
    const limbs = {};
    LABELS.forEach((label, labelIndex) => {
        const x = labelIndex * 10;
        limbs[label] = {
            joints: Array.from({ length: 6 }, (_, jointIndex) => ({
                bone: `${label}_segment_${jointIndex}`,
                restStart: [x, jointIndex],
                restEnd: [x, jointIndex + 1],
                restQuaternion: [0, 0, 0, 1],
                rotationAxis: [0, 0, 1],
                minAngle: -Math.PI,
                maxAngle: Math.PI,
            })),
            trackedJointIndex: 3,
        };
    });
    return { schema: BROWSER_FITTING_SCHEMAS.skeleton, limbs };
}

function orderedObservations({ frameCount = 9, legacyOnly = false } = {}) {
    const tracks = [];
    LABELS.forEach((label, labelIndex) => {
        const x = labelIndex * 10;
        const frames = Array.from({ length: frameCount }, (_, frame) => {
            const phase = frame / (frameCount - 1) * Math.PI * 2 + labelIndex * 0.4;
            const points = [[x, 0]];
            let parentAngle = 0;
            for (let segment = 0; segment < 6; segment += 1) {
                const localAngle = Math.PI / 2 + Math.sin(phase + segment * 0.7) * (0.12 + segment * 0.025);
                const worldAngle = segment === 0 ? localAngle : parentAngle + localAngle - Math.PI / 2;
                points.push([
                    points.at(-1)[0] + Math.cos(worldAngle),
                    points.at(-1)[1] + Math.sin(worldAngle),
                ]);
                parentAngle = worldAngle;
            }
            return points;
        });
        const semanticIds = Array.from({ length: 7 }, (_, headIndex) => {
            if (headIndex === 0) return `${label}.proximal`;
            if (headIndex === 3) return `${label}.joint`;
            if (headIndex === 6) return `${label}.hoof`;
            return `${label}.deformHead.${headIndex}`;
        });
        semanticIds.forEach((anchorId, headIndex) => {
            if (legacyOnly && ![0, 3, 6].includes(headIndex)) return;
            tracks.push(track(anchorId, frames.map((points) => points[headIndex])));
        });
    });
    return {
        schema: BROWSER_FITTING_SCHEMAS.observations,
        frame_count: frameCount,
        width: 512,
        height: 320,
        fps: 30,
        tracks,
        contacts: [],
    };
}

function orderedTargetMean(fitted, allObservations) {
    const targetById = new Map(allObservations.tracks.map((item) => [item.anchor_id, item.points]));
    let sum = 0;
    let samples = 0;
    fitted.frames.forEach((frame, frameIndex) => {
        LABELS.forEach((label) => {
            const ids = Array.from({ length: 7 }, (_, headIndex) => {
                if (headIndex === 0) return `${label}.proximal`;
                if (headIndex === 3) return `${label}.joint`;
                if (headIndex === 6) return `${label}.hoof`;
                return `${label}.deformHead.${headIndex}`;
            });
            ids.forEach((id, headIndex) => {
                const actual = frame.limbs[label].points[headIndex];
                const expected = targetById.get(id)[frameIndex];
                sum += Math.hypot(actual[0] - expected.x, actual[1] - expected.y);
                samples += 1;
            });
        });
    });
    return sum / samples;
}

test('browser solver reduces semantic hoof target error without Blender or Three.js', () => {
    const fitted = fitBrowserAnimation({
        skeleton: skeleton(),
        observations: observations(),
        options: { loop: false, smoothingRadius: 0, jointAttraction: 0, iterations: 64, tolerance: 1e-5 },
    });
    assert.equal(fitted.schema, BROWSER_FITTING_SCHEMAS.fitted);
    assert.equal(fitted.frameCount, 7);
    assert.equal(fitted.tracks.length, 8);
    assert.ok(fitted.qa.initialMeanTargetErrorPx > 0.2);
    assert.ok(fitted.qa.finalMeanTargetErrorPx < fitted.qa.initialMeanTargetErrorPx * 0.02);
});

test('ordered deform-head fitting uses every chain head and materially beats the legacy three-track solve', () => {
    const allTargets = orderedObservations();
    const legacy = fitBrowserAnimation({
        skeleton: orderedSkeleton(),
        observations: orderedObservations({ legacyOnly: true }),
        options: { loop: false, smoothingRadius: 0, iterations: 64, tolerance: 1e-6 },
    });
    const ordered = fitBrowserAnimation({
        skeleton: orderedSkeleton(),
        observations: allTargets,
        options: { loop: false, smoothingRadius: 0, iterations: 64, tolerance: 1e-6 },
    });
    assert.equal(legacy.qa.targetMode, 'legacy_three_track');
    assert.equal(ordered.qa.targetMode, 'ordered_deform_heads');
    assert.equal(ordered.qa.targetSamples, 4 * 7 * ordered.frameCount);
    assert.ok(orderedTargetMean(ordered, allTargets) < orderedTargetMean(legacy, allTargets) * 0.35);
    assert.ok(ordered.qa.maximumTargetErrorPx < 0.05);
});

test('ordered deform-head CCD never publishes a target regression across deterministic randomized targets', () => {
    let state = 123456789;
    const random = () => {
        state = (1664525 * state + 1013904223) >>> 0;
        return state / 2 ** 32;
    };
    const label = 'leg';
    const jointCount = 6;
    const rig = {
        schema: BROWSER_FITTING_SCHEMAS.skeleton,
        limbs: {
            [label]: {
                joints: Array.from({ length: jointCount }, (_, index) => ({
                    bone: `random_segment_${index}`,
                    restStart: [0, index],
                    restEnd: [0, index + 1],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: -Math.PI,
                    maxAngle: Math.PI,
                })),
                trackedJointIndex: 3,
            },
        },
    };
    const semanticIds = Array.from({ length: jointCount + 1 }, (_, headIndex) => {
        if (headIndex === 0) return `${label}.proximal`;
        if (headIndex === 3) return `${label}.joint`;
        if (headIndex === jointCount) return `${label}.hoof`;
        return `${label}.deformHead.${headIndex}`;
    });
    for (let sample = 0; sample < 1300; sample += 1) {
        const targets = Array.from({ length: jointCount + 1 }, (_, headIndex) => (
            headIndex === 0
                ? [0, 0]
                : [(random() - 0.5) * 12, (random() - 0.5) * 12]
        ));
        const input = {
            schema: BROWSER_FITTING_SCHEMAS.observations,
            frame_count: 2,
            fps: 30,
            tracks: semanticIds.map((anchorId, headIndex) => track(
                anchorId,
                [targets[headIndex], targets[headIndex]],
            )),
            contacts: [],
        };
        const fitted = fitBrowserAnimation({
            skeleton: rig,
            observations: input,
            options: { loop: false, smoothingRadius: 0, iterations: 64, tolerance: 1e-8 },
        });
        assert.ok(
            fitted.qa.finalMeanTargetErrorPx <= fitted.qa.initialMeanTargetErrorPx + 1e-12,
            `sample ${sample} regressed from ${fitted.qa.initialMeanTargetErrorPx} to ${fitted.qa.finalMeanTargetErrorPx}`,
        );
    }
});

test('partial ordered deform-head observations fail closed instead of falling back to three tracks', () => {
    const partial = orderedObservations();
    partial.tracks = partial.tracks.filter((item) => item.anchor_id !== 'fore_left.deformHead.2');
    assert.throws(
        () => fitBrowserAnimation({ skeleton: orderedSkeleton(), observations: partial }),
        /partial ordered deform-head chain for limb fore_left.*deformHead\.2/,
    );
});

test('published final target error matches postprocessed debug-frame hoof targets', () => {
    const input = observations({ loopMismatch: true, contact: true });
    const fitted = fitBrowserAnimation({
        skeleton: skeleton(),
        observations: input,
        options: {
            loop: true,
            smoothingRadius: 2,
            loopBlendFrames: 3,
            jointAttraction: 0.12,
            iterations: 64,
            tolerance: 1e-6,
        },
    });
    const tracks = new Map(input.tracks.map((item) => [item.anchor_id, item]));
    let expectedErrorSum = 0;
    let samples = 0;
    fitted.frames.forEach((frame, frameIndex) => {
        LABELS.forEach((label) => {
            const hoof = frame.limbs[label].points.at(-1);
            const observed = tracks.get(`${label}.hoof`).points[frameIndex];
            const target = label === 'hind_right' && frameIndex >= 2 && frameIndex <= 4
                ? [6.2, 1.98]
                : [observed.x, observed.y];
            expectedErrorSum += Math.hypot(hoof[0] - target[0], hoof[1] - target[1]);
            samples += 1;
        });
    });
    const expectedMean = expectedErrorSum / samples;
    assert.ok(Math.abs(fitted.qa.finalMeanTargetErrorPx - expectedMean) < 1e-12);
});

test('FABRIK output preserves every projected segment length', () => {
    const fitted = fitBrowserAnimation({
        skeleton: skeleton(),
        observations: observations(),
        options: { loop: false, smoothingRadius: 0, jointAttraction: 0, iterations: 64, tolerance: 1e-6 },
    });
    assert.ok(fitted.qa.maximumBoneLengthErrorPx < 1e-9);
    fitted.frames.forEach((frame) => Object.values(frame.limbs).forEach(({ points }) => {
        assert.ok(Math.abs(Math.hypot(points[1][0] - points[0][0], points[1][1] - points[0][1]) - 1) < 1e-9);
        assert.ok(Math.abs(Math.hypot(points[2][0] - points[1][0], points[2][1] - points[1][1]) - 1) < 1e-9);
    }));
});

test('joint limits fail closed and report zero post-clamp violation', () => {
    const fitted = fitBrowserAnimation({
        skeleton: skeleton({ tightLimits: true }),
        observations: observations({ extreme: true }),
        options: { loop: false, smoothingRadius: 0, jointAttraction: 0 },
    });
    assert.equal(fitted.qa.maximumJointLimitViolationRad, 0);
    assert.ok(fitted.tracks.every((track) => track.values.every(Number.isFinite)));
    assert.ok(fitted.qa.finalMeanTargetErrorPx > 0, 'unreachable target must not bypass limits');
});

test('contact intervals pin the hoof and bound slide', () => {
    const fitted = fitBrowserAnimation({
        skeleton: skeleton(),
        observations: observations({ contact: true }),
        options: { loop: false, smoothingRadius: 0, jointAttraction: 0, iterations: 80, tolerance: 1e-7 },
    });
    assert.ok(fitted.qa.maximumContactSlidePx < 1e-5);
});

test('loop closure makes quaternion and root endpoints identical', () => {
    const fitted = fitBrowserAnimation({
        skeleton: skeleton({ rootMotion: true }),
        observations: observations({ loopMismatch: true }),
        options: { loop: true, smoothingRadius: 1, loopBlendFrames: 3 },
    });
    assert.equal(fitted.qa.loopEndpointError, 0);
    fitted.tracks.forEach((track) => {
        assert.deepEqual(track.values.slice(0, 4), track.values.slice(-4));
    });
    assert.deepEqual(fitted.rootTrack.values.slice(0, 3), fitted.rootTrack.values.slice(-3));
});

test('Three.js adapter creates one shared AnimationClip without a mixer', () => {
    const fitted = fitBrowserAnimation({
        skeleton: skeleton({ rootMotion: true }),
        observations: observations(),
        options: { loop: true },
    });
    class QuaternionKeyframeTrack {
        constructor(name, times, values) { Object.assign(this, { name, times, values, kind: 'quaternion' }); }
    }
    class VectorKeyframeTrack {
        constructor(name, times, values) { Object.assign(this, { name, times, values, kind: 'vector' }); }
    }
    class AnimationClip {
        constructor(name, duration, tracks) { Object.assign(this, { name, duration, tracks }); }
    }
    const clip = fittedTracksToThreeClip(fitted, {
        QuaternionKeyframeTrack,
        VectorKeyframeTrack,
        AnimationClip,
    }, 'Horse_Walk_LTX');
    assert.equal(clip.name, 'Horse_Walk_LTX');
    assert.equal(clip.tracks.length, fitted.tracks.length + 1);
    assert.ok(clip.tracks.every((item) => item.kind === 'quaternion' || item.kind === 'vector'));
    assert.equal('mixer' in clip, false);
});

test('independently parented deform bones receive browser-solved position tracks', () => {
    const fitted = fitBrowserAnimation({
        skeleton: skeleton({ positionMappings: true }),
        observations: observations(),
        options: { loop: true, smoothingRadius: 0 },
    });
    assert.equal(fitted.positionTracks.length, 8);
    assert.equal(fitted.qa.loopEndpointError, 0);
    fitted.positionTracks.forEach((track) => {
        assert.equal(track.values.length, fitted.frameCount * 3);
        assert.deepEqual(track.values.slice(0, 3), track.values.slice(-3));
    });
});

test('invalid contracts are rejected before fitting', () => {
    assert.throws(
        () => fitBrowserAnimation({ skeleton: { ...skeleton(), schema: 'wrong' }, observations: observations() }),
        /skeleton.schema/,
    );
    const missing = observations();
    missing.tracks = missing.tracks.filter((item) => item.anchor_id !== 'fore_left.hoof');
    assert.throws(
        () => fitBrowserAnimation({ skeleton: skeleton(), observations: missing }),
        /missing semantic tracks for limb fore_left/,
    );
});
