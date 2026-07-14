const SKELETON_SCHEMA = 'autorig-browser-fitting-skeleton.v1';
const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';
const FITTED_SCHEMA = 'autorig-browser-fitted-animation.v1';

const EPSILON = 1e-9;

function finite(value, field) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error(`${field} must be finite`);
    return number;
}

function positive(value, field) {
    const number = finite(value, field);
    if (number <= 0) throw new Error(`${field} must be positive`);
    return number;
}

function clamp(value, minimum, maximum) {
    return Math.min(maximum, Math.max(minimum, value));
}

function vec2(value, field) {
    if (!Array.isArray(value) || value.length !== 2) {
        throw new Error(`${field} must be a two-component array`);
    }
    return [finite(value[0], `${field}[0]`), finite(value[1], `${field}[1]`)];
}

function vec3(value, field) {
    if (!Array.isArray(value) || value.length !== 3) {
        throw new Error(`${field} must be a three-component array`);
    }
    return value.map((item, index) => finite(item, `${field}[${index}]`));
}

function add2(a, b) {
    return [a[0] + b[0], a[1] + b[1]];
}

function sub2(a, b) {
    return [a[0] - b[0], a[1] - b[1]];
}

function scale2(a, scale) {
    return [a[0] * scale, a[1] * scale];
}

function length2(value) {
    return Math.hypot(value[0], value[1]);
}

function distance2(a, b) {
    return length2(sub2(a, b));
}

function normalized2(value, fallback = [1, 0]) {
    const length = length2(value);
    return length > EPSILON ? scale2(value, 1 / length) : [...fallback];
}

function angle2(value) {
    return Math.atan2(value[1], value[0]);
}

function fromAngle(angle, length) {
    return [Math.cos(angle) * length, Math.sin(angle) * length];
}

function wrapAngle(value) {
    let angle = value;
    while (angle > Math.PI) angle -= Math.PI * 2;
    while (angle < -Math.PI) angle += Math.PI * 2;
    return angle;
}

function angleDelta(from, to) {
    return wrapAngle(to - from);
}

function lerpAngle(from, to, alpha) {
    return wrapAngle(from + angleDelta(from, to) * alpha);
}

function median(values) {
    if (!values.length) return 0;
    const ordered = [...values].sort((a, b) => a - b);
    const middle = Math.floor(ordered.length / 2);
    return ordered.length % 2
        ? ordered[middle]
        : (ordered[middle - 1] + ordered[middle]) / 2;
}

function normalizeQuaternion(value, field) {
    if (!Array.isArray(value) || value.length !== 4) {
        throw new Error(`${field} must be an xyzw quaternion`);
    }
    const quaternion = value.map((item, index) => finite(item, `${field}[${index}]`));
    const length = Math.hypot(...quaternion);
    if (length <= EPSILON) throw new Error(`${field} must not be zero`);
    return quaternion.map((item) => item / length);
}

function multiplyQuaternions(a, b) {
    const [ax, ay, az, aw] = a;
    const [bx, by, bz, bw] = b;
    return [
        aw * bx + ax * bw + ay * bz - az * by,
        aw * by - ax * bz + ay * bw + az * bx,
        aw * bz + ax * by - ay * bx + az * bw,
        aw * bw - ax * bx - ay * by - az * bz,
    ];
}

function axisAngleQuaternion(axisValue, angle) {
    const axis = vec3(axisValue, 'rotationAxis');
    const length = Math.hypot(...axis);
    if (length <= EPSILON) throw new Error('rotationAxis must not be zero');
    const half = angle / 2;
    const scale = Math.sin(half) / length;
    return [axis[0] * scale, axis[1] * scale, axis[2] * scale, Math.cos(half)];
}

function normalizePoint(point, field) {
    if (!point || typeof point !== 'object') throw new Error(`${field} must be an object`);
    return {
        frame: Number.isInteger(point.frame) ? point.frame : null,
        x: finite(point.x, `${field}.x`),
        y: finite(point.y, `${field}.y`),
        visible: Boolean(point.visible),
        confidence: clamp(finite(point.confidence ?? 1, `${field}.confidence`), 0, 1),
    };
}

function normalizeObservations(value) {
    if (!value || typeof value !== 'object' || value.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const frameCount = Number(value.frame_count);
    if (!Number.isInteger(frameCount) || frameCount < 2) {
        throw new Error('observations.frame_count must be an integer of at least two');
    }
    const fps = positive(value.fps, 'observations.fps');
    if (!Array.isArray(value.tracks)) throw new Error('observations.tracks must be an array');
    const tracks = new Map();
    value.tracks.forEach((track, trackIndex) => {
        const anchorId = track?.anchor_id;
        if (typeof anchorId !== 'string' || !anchorId) {
            throw new Error(`observations.tracks[${trackIndex}].anchor_id is required`);
        }
        if (tracks.has(anchorId)) throw new Error(`duplicate observation track ${anchorId}`);
        if (!Array.isArray(track.points)) throw new Error(`track ${anchorId} points must be an array`);
        const points = new Array(frameCount).fill(null);
        track.points.forEach((point, pointIndex) => {
            const normalized = normalizePoint(point, `track ${anchorId}.points[${pointIndex}]`);
            const frame = normalized.frame ?? pointIndex;
            if (!Number.isInteger(frame) || frame < 0 || frame >= frameCount) {
                throw new Error(`track ${anchorId} contains an invalid frame`);
            }
            if (points[frame]) throw new Error(`track ${anchorId} repeats frame ${frame}`);
            points[frame] = normalized;
        });
        tracks.set(anchorId, points);
    });

    const contacts = Array.isArray(value.contacts) ? value.contacts.map((contact, index) => {
        if (!contact || typeof contact !== 'object' || typeof contact.anchor_id !== 'string') {
            throw new Error(`observations.contacts[${index}] is invalid`);
        }
        if (!Array.isArray(contact.frames)) throw new Error(`contact ${contact.anchor_id} frames must be an array`);
        const frames = [...new Set(contact.frames.map((frame) => {
            const result = Number(frame);
            if (!Number.isInteger(result) || result < 0 || result >= frameCount) {
                throw new Error(`contact ${contact.anchor_id} contains an invalid frame`);
            }
            return result;
        }))].sort((a, b) => a - b);
        return { anchorId: contact.anchor_id, frames };
    }) : [];
    return { frameCount, fps, tracks, contacts };
}

function normalizeSkeleton(value) {
    if (!value || typeof value !== 'object' || value.schema !== SKELETON_SCHEMA) {
        throw new Error(`skeleton.schema must be ${SKELETON_SCHEMA}`);
    }
    if (!value.limbs || typeof value.limbs !== 'object' || Array.isArray(value.limbs)) {
        throw new Error('skeleton.limbs must be an object');
    }
    const usedBones = new Set();
    const limbs = {};
    Object.entries(value.limbs).forEach(([label, limbValue]) => {
        if (!limbValue || typeof limbValue !== 'object') throw new Error(`limb ${label} must be an object`);
        const joints = limbValue.joints;
        if (!Array.isArray(joints) || joints.length < 2) {
            throw new Error(`limb ${label} must contain at least two joints`);
        }
        const normalizedJoints = joints.map((joint, index) => {
            const bone = joint?.bone;
            if (typeof bone !== 'string' || !bone) throw new Error(`limb ${label} joint ${index} needs a bone`);
            if (usedBones.has(bone)) throw new Error(`bone ${bone} is assigned to more than one limb`);
            usedBones.add(bone);
            const restStart = vec2(joint.restStart, `limb ${label}.${bone}.restStart`);
            const restEnd = vec2(joint.restEnd, `limb ${label}.${bone}.restEnd`);
            const length = distance2(restStart, restEnd);
            if (length <= EPSILON) throw new Error(`limb ${label}.${bone} has zero rest length`);
            const minimum = finite(joint.minAngle ?? -Math.PI, `limb ${label}.${bone}.minAngle`);
            const maximum = finite(joint.maxAngle ?? Math.PI, `limb ${label}.${bone}.maxAngle`);
            if (minimum > maximum) throw new Error(`limb ${label}.${bone} angle limits are reversed`);
            let positionMapping = null;
            if (joint.positionMapping) {
                positionMapping = {
                    restPosition: vec3(
                        joint.positionMapping.restPosition,
                        `limb ${label}.${bone}.positionMapping.restPosition`,
                    ),
                    xAxisPerPixel: vec3(
                        joint.positionMapping.xAxisPerPixel,
                        `limb ${label}.${bone}.positionMapping.xAxisPerPixel`,
                    ),
                    yAxisPerPixel: vec3(
                        joint.positionMapping.yAxisPerPixel,
                        `limb ${label}.${bone}.positionMapping.yAxisPerPixel`,
                    ),
                    motionScale: finite(
                        joint.positionMapping.motionScale ?? 1,
                        `limb ${label}.${bone}.positionMapping.motionScale`,
                    ),
                };
            }
            return {
                bone,
                restStart,
                restEnd,
                length,
                restQuaternion: normalizeQuaternion(joint.restQuaternion ?? [0, 0, 0, 1], `limb ${label}.${bone}.restQuaternion`),
                rotationAxis: vec3(joint.rotationAxis ?? [0, 0, 1], `limb ${label}.${bone}.rotationAxis`),
                minimum,
                maximum,
                positionMapping,
            };
        });
        for (let index = 1; index < normalizedJoints.length; index += 1) {
            if (distance2(normalizedJoints[index - 1].restEnd, normalizedJoints[index].restStart) > 1e-5) {
                throw new Error(`limb ${label} rest chain is disconnected at joint ${index}`);
            }
        }
        limbs[label] = {
            label,
            joints: normalizedJoints,
            proximalTrack: limbValue.proximalTrack ?? `${label}.proximal`,
            jointTrack: limbValue.jointTrack ?? `${label}.joint`,
            hoofTrack: limbValue.hoofTrack ?? `${label}.hoof`,
            trackedJointIndex: clamp(Math.trunc(finite(limbValue.trackedJointIndex ?? 1, `limb ${label}.trackedJointIndex`)), 1, normalizedJoints.length - 1),
        };
    });
    if (!Object.keys(limbs).length) throw new Error('skeleton.limbs must not be empty');

    let root = null;
    if (value.root) {
        if (typeof value.root.bone !== 'string' || !value.root.bone) throw new Error('skeleton.root.bone is required');
        root = {
            bone: value.root.bone,
            restPosition: vec3(value.root.restPosition ?? [0, 0, 0], 'skeleton.root.restPosition'),
            xAxisPerPixel: vec3(value.root.xAxisPerPixel ?? [0, 0, 0], 'skeleton.root.xAxisPerPixel'),
            yAxisPerPixel: vec3(value.root.yAxisPerPixel ?? [0, 0, 0], 'skeleton.root.yAxisPerPixel'),
            motionScale: finite(value.root.motionScale ?? 0, 'skeleton.root.motionScale'),
        };
    }
    return { limbs, root };
}

function pointFor(points, frame, fallback) {
    const point = points?.[frame];
    return point?.visible ? [point.x, point.y] : [...fallback];
}

function contiguousRuns(frames) {
    if (!frames.length) return [];
    const runs = [[frames[0]]];
    for (let index = 1; index < frames.length; index += 1) {
        if (frames[index] === frames[index - 1] + 1) runs[runs.length - 1].push(frames[index]);
        else runs.push([frames[index]]);
    }
    return runs;
}

function contactPins(observations, limb, loop) {
    const pins = new Map();
    let runs = observations.contacts
        .filter((contact) => contact.anchorId === limb.hoofTrack)
        .flatMap((contact) => contiguousRuns(contact.frames));
    if (loop && runs.length > 1) {
        const first = runs.find((run) => run[0] === 0);
        const last = runs.find((run) => run.at(-1) === observations.frameCount - 1);
        if (first && last && first !== last) {
            runs = runs.filter((run) => run !== first && run !== last);
            runs.push([...last, ...first]);
        }
    }
    runs.forEach((run) => {
            const points = run
                .map((frame) => observations.tracks.get(limb.hoofTrack)?.[frame])
                .filter((point) => point?.visible);
            if (!points.length) return;
            const pin = [median(points.map((point) => point.x)), median(points.map((point) => point.y))];
            run.forEach((frame) => pins.set(frame, pin));
        });
    return pins;
}

function initialRestPoints(limb) {
    const points = [[...limb.joints[0].restStart]];
    limb.joints.forEach((joint) => points.push([...joint.restEnd]));
    return points;
}

function solveFabrik(root, target, lengths, initial, options) {
    const totalLength = lengths.reduce((sum, value) => sum + value, 0);
    const points = initial.map((point) => [...point]);
    const rootToTarget = distance2(root, target);
    if (rootToTarget >= totalLength - EPSILON) {
        const direction = normalized2(sub2(target, root));
        points[0] = [...root];
        for (let index = 0; index < lengths.length; index += 1) {
            points[index + 1] = add2(points[index], scale2(direction, lengths[index]));
        }
        return points;
    }

    // The semantic tracker exposes the anatomical middle joint explicitly.
    // For the common upper/lower limb pair, the circle intersection is both
    // exact and deterministic; the observed joint selects the correct bend
    // side instead of allowing FABRIK to flip at a near-straight pose.
    if (lengths.length === 2 && options.jointTarget && rootToTarget > EPSILON) {
        const direction = normalized2(sub2(target, root));
        const perpendicular = [-direction[1], direction[0]];
        const along = (
            lengths[0] ** 2 - lengths[1] ** 2 + rootToTarget ** 2
        ) / (2 * rootToTarget);
        const height = Math.sqrt(Math.max(lengths[0] ** 2 - along ** 2, 0));
        const center = add2(root, scale2(direction, along));
        const candidates = [
            add2(center, scale2(perpendicular, height)),
            add2(center, scale2(perpendicular, -height)),
        ];
        const joint = candidates.sort((a, b) => (
            distance2(a, options.jointTarget) - distance2(b, options.jointTarget)
        ))[0];
        return [[...root], joint, [...target]];
    }

    const iterations = options.iterations;
    const tolerance = options.tolerance;
    for (let iteration = 0; iteration < iterations; iteration += 1) {
        points[points.length - 1] = [...target];
        for (let index = points.length - 2; index >= 0; index -= 1) {
            const direction = normalized2(sub2(points[index], points[index + 1]));
            points[index] = add2(points[index + 1], scale2(direction, lengths[index]));
        }
        points[0] = [...root];
        for (let index = 0; index < lengths.length; index += 1) {
            const direction = normalized2(sub2(points[index + 1], points[index]));
            points[index + 1] = add2(points[index], scale2(direction, lengths[index]));
        }
        if (options.jointTarget) {
            const index = options.trackedJointIndex;
            points[index] = add2(
                scale2(points[index], 1 - options.jointAttraction),
                scale2(options.jointTarget, options.jointAttraction),
            );
            for (let item = index - 1; item >= 0; item -= 1) {
                const direction = normalized2(sub2(points[item], points[item + 1]));
                points[item] = add2(points[item + 1], scale2(direction, lengths[item]));
            }
            points[0] = [...root];
            for (let item = 0; item < lengths.length; item += 1) {
                const direction = normalized2(sub2(points[item + 1], points[item]));
                points[item + 1] = add2(points[item], scale2(direction, lengths[item]));
            }
        }
        if (distance2(points[points.length - 1], target) <= tolerance) break;
    }
    return points;
}

function restLocalAngles(limb) {
    const world = limb.joints.map((joint) => angle2(sub2(joint.restEnd, joint.restStart)));
    return world.map((angle, index) => index === 0 ? angle : wrapAngle(angle - world[index - 1]));
}

function pointsToLocalDeltas(points, limb) {
    const rest = restLocalAngles(limb);
    const world = limb.joints.map((_, index) => angle2(sub2(points[index + 1], points[index])));
    return world.map((angle, index) => {
        const local = index === 0 ? angle : wrapAngle(angle - world[index - 1]);
        return wrapAngle(local - rest[index]);
    });
}

function clampLocalDeltas(deltas, limb) {
    return deltas.map((angle, index) => clamp(angle, limb.joints[index].minimum, limb.joints[index].maximum));
}

function localDeltasToPoints(root, deltas, limb) {
    const rest = restLocalAngles(limb);
    const points = [[...root]];
    let parentWorld = 0;
    deltas.forEach((delta, index) => {
        const local = rest[index] + delta;
        const world = index === 0 ? local : parentWorld + local;
        points.push(add2(points[index], fromAngle(world, limb.joints[index].length)));
        parentWorld = world;
    });
    return points;
}

function smoothAngles(frames, radius) {
    if (radius <= 0 || frames.length < 3) return frames.map((frame) => [...frame]);
    return frames.map((frame, frameIndex) => frame.map((angle, jointIndex) => {
        let x = 0;
        let y = 0;
        let weightSum = 0;
        for (let offset = -radius; offset <= radius; offset += 1) {
            const index = clamp(frameIndex + offset, 0, frames.length - 1);
            const weight = radius + 1 - Math.abs(offset);
            x += Math.cos(frames[index][jointIndex]) * weight;
            y += Math.sin(frames[index][jointIndex]) * weight;
            weightSum += weight;
        }
        return Math.atan2(y / weightSum, x / weightSum);
    }));
}

function closeLoopAngles(frames, blendFrames) {
    if (!frames.length) return frames;
    const result = frames.map((frame) => [...frame]);
    const last = result.length - 1;
    const count = Math.min(blendFrames, Math.max(1, Math.floor(result.length / 2)));
    for (let joint = 0; joint < result[0].length; joint += 1) {
        const seam = lerpAngle(result[0][joint], result[last][joint], 0.5);
        for (let offset = 0; offset < count; offset += 1) {
            const alpha = 1 - offset / count;
            result[offset][joint] = lerpAngle(result[offset][joint], seam, alpha);
            result[last - offset][joint] = lerpAngle(result[last - offset][joint], seam, alpha);
        }
        result[last][joint] = result[0][joint];
    }
    return result;
}

function closeLoopVectors(frames, blendFrames) {
    if (!frames.length) return frames;
    const result = frames.map((frame) => [...frame]);
    const last = result.length - 1;
    const count = Math.min(blendFrames, Math.max(1, Math.floor(result.length / 2)));
    const seam = result[0].map((value, index) => (value + result[last][index]) / 2);
    for (let offset = 0; offset < count; offset += 1) {
        const alpha = 1 - offset / count;
        [offset, last - offset].forEach((index) => {
            result[index] = result[index].map((value, axis) => value + (seam[axis] - value) * alpha);
        });
    }
    result[last] = [...result[0]];
    return result;
}

function averageRoots(frameRoots, labels, restRoots) {
    return frameRoots.map((roots) => {
        const displacements = labels.map((label) => sub2(roots[label], restRoots[label]));
        return [
            displacements.reduce((sum, value) => sum + value[0], 0) / displacements.length,
            displacements.reduce((sum, value) => sum + value[1], 0) / displacements.length,
        ];
    });
}

function rootPositions(rootContract, displacements) {
    if (!rootContract) return null;
    return displacements.map(([x, y]) => rootContract.restPosition.map((base, axis) => base + rootContract.motionScale * (
        x * rootContract.xAxisPerPixel[axis] + y * rootContract.yAxisPerPixel[axis]
    )));
}

function maximumContactSlide(pointsByFrame, pins) {
    let maximum = 0;
    const runs = contiguousRuns([...pins.keys()].sort((a, b) => a - b));
    runs.forEach((run) => {
        for (let index = 1; index < run.length; index += 1) {
            maximum = Math.max(maximum, distance2(
                pointsByFrame[run[index - 1]].at(-1),
                pointsByFrame[run[index]].at(-1),
            ));
        }
    });
    return maximum;
}

/**
 * Fit semantic video observations to a browser-normalized quadruped skeleton.
 *
 * This solver deliberately works on plain arrays. The viewer adapter projects
 * the Three.js rest skeleton to the canonical LTX camera and converts the
 * resulting quaternion tracks back into THREE.AnimationClip objects.
 */
export function fitBrowserAnimation({ skeleton: skeletonValue, observations: observationValue, options = {} }) {
    const skeleton = normalizeSkeleton(skeletonValue);
    const observations = normalizeObservations(observationValue);
    const iterations = Math.max(1, Math.trunc(finite(options.iterations ?? 24, 'options.iterations')));
    const tolerance = positive(options.tolerance ?? 0.05, 'options.tolerance');
    const jointAttraction = clamp(finite(options.jointAttraction ?? 0.15, 'options.jointAttraction'), 0, 0.5);
    const smoothingRadius = Math.max(0, Math.trunc(finite(options.smoothingRadius ?? 1, 'options.smoothingRadius')));
    const loop = options.loop !== false;
    const loopBlendFrames = Math.max(1, Math.trunc(finite(options.loopBlendFrames ?? 4, 'options.loopBlendFrames')));
    const labels = Object.keys(skeleton.limbs);
    const frameRoots = Array.from({ length: observations.frameCount }, () => ({}));
    const restRoots = {};
    const solved = {};
    let initialTargetErrorSum = 0;
    let finalTargetErrorSum = 0;
    let targetSamples = 0;

    labels.forEach((label) => {
        const limb = skeleton.limbs[label];
        const proximalPoints = observations.tracks.get(limb.proximalTrack);
        const jointPoints = observations.tracks.get(limb.jointTrack);
        const hoofPoints = observations.tracks.get(limb.hoofTrack);
        if (!proximalPoints || !jointPoints || !hoofPoints) {
            throw new Error(`observations are missing semantic tracks for limb ${label}`);
        }
        const rest = initialRestPoints(limb);
        const pins = contactPins(observations, limb, loop);
        const rawAngles = [];
        const rawRoots = [];
        let previous = rest;
        for (let frame = 0; frame < observations.frameCount; frame += 1) {
            const root = pointFor(proximalPoints, frame, previous[0]);
            const observedHoof = pointFor(hoofPoints, frame, previous.at(-1));
            const target = pins.get(frame) ?? observedHoof;
            const observedJoint = pointFor(jointPoints, frame, previous[limb.trackedJointIndex]);
            const translatedRest = rest.map((point) => add2(point, sub2(root, rest[0])));
            const initial = previous.map((point) => add2(point, sub2(root, previous[0])));
            const baseline = frame ? initial : translatedRest;
            initialTargetErrorSum += distance2(baseline.at(-1), target);
            const fabrik = solveFabrik(root, target, limb.joints.map((joint) => joint.length), baseline, {
                iterations,
                tolerance,
                jointTarget: observedJoint,
                trackedJointIndex: limb.trackedJointIndex,
                jointAttraction,
            });
            const angles = clampLocalDeltas(pointsToLocalDeltas(fabrik, limb), limb);
            const points = localDeltasToPoints(root, angles, limb);
            targetSamples += 1;
            rawAngles.push(angles);
            rawRoots.push(root);
            frameRoots[frame][label] = root;
            previous = points;
        }
        let angles = smoothAngles(rawAngles, smoothingRadius)
            .map((frame) => clampLocalDeltas(frame, limb));
        if (loop) angles = closeLoopAngles(angles, loopBlendFrames)
            .map((frame) => clampLocalDeltas(frame, limb));
        const roots = loop ? closeLoopVectors(rawRoots, loopBlendFrames) : rawRoots;
        pins.forEach((pin, frame) => {
            const current = localDeltasToPoints(roots[frame], angles[frame], limb);
            const observedJoint = pointFor(
                jointPoints,
                frame,
                current[limb.trackedJointIndex],
            );
            const contactSolved = solveFabrik(
                roots[frame],
                pin,
                limb.joints.map((joint) => joint.length),
                current,
                {
                    iterations,
                    tolerance,
                    jointTarget: observedJoint,
                    trackedJointIndex: limb.trackedJointIndex,
                    jointAttraction: 0,
                },
            );
            angles[frame] = clampLocalDeltas(pointsToLocalDeltas(contactSolved, limb), limb);
        });
        if (loop) {
            angles[angles.length - 1] = [...angles[0]];
            roots[roots.length - 1] = [...roots[0]];
        }
        const points = angles.map((frame, index) => localDeltasToPoints(roots[index], frame, limb));
        solved[label] = { limb, angles, roots, points, pins };
    });

    let displacements = averageRoots(frameRoots, labels, Object.fromEntries(labels.map((label) => [
        label,
        skeleton.limbs[label].joints[0].restStart,
    ])));
    if (loop) displacements = closeLoopVectors(displacements, loopBlendFrames);
    let fittedRootPositions = rootPositions(skeleton.root, displacements);
    if (fittedRootPositions && loop) fittedRootPositions = closeLoopVectors(fittedRootPositions, loopBlendFrames);

    const times = Array.from({ length: observations.frameCount }, (_, frame) => frame / observations.fps);
    const tracks = [];
    const positionTracks = [];
    labels.forEach((label) => {
        const { limb, angles, points } = solved[label];
        limb.joints.forEach((joint, jointIndex) => {
            const values = [];
            angles.forEach((frame) => {
                const delta = axisAngleQuaternion(joint.rotationAxis, frame[jointIndex]);
                values.push(...normalizeQuaternion(
                    multiplyQuaternions(joint.restQuaternion, delta),
                    `${joint.bone} fitted quaternion`,
                ));
            });
            tracks.push({
                bone: joint.bone,
                name: `${joint.bone}.quaternion`,
                type: 'quaternion',
                times: [...times],
                values,
            });
            if (joint.positionMapping) {
                const positionValues = [];
                points.forEach((framePoints) => {
                    const displacement = sub2(framePoints[jointIndex], joint.restStart);
                    const mapping = joint.positionMapping;
                    for (let axis = 0; axis < 3; axis += 1) {
                        positionValues.push(mapping.restPosition[axis] + mapping.motionScale * (
                            displacement[0] * mapping.xAxisPerPixel[axis]
                            + displacement[1] * mapping.yAxisPerPixel[axis]
                        ));
                    }
                });
                positionTracks.push({
                    bone: joint.bone,
                    name: `${joint.bone}.position`,
                    type: 'vector',
                    times: [...times],
                    values: positionValues,
                });
            }
        });
    });

    const rootTrack = fittedRootPositions ? {
        bone: skeleton.root.bone,
        name: `${skeleton.root.bone}.position`,
        type: 'vector',
        times: [...times],
        values: fittedRootPositions.flat(),
    } : null;

    let maximumLengthError = 0;
    let maximumJointLimitViolation = 0;
    let maximumSlide = 0;
    let loopEndpointError = 0;
    const debugFrames = Array.from({ length: observations.frameCount }, (_, frame) => ({ frame, limbs: {} }));
    labels.forEach((label) => {
        const { limb, angles, points, pins } = solved[label];
        const hoofPoints = observations.tracks.get(limb.hoofTrack);
        points.forEach((framePoints, frame) => {
            const target = pins.get(frame) ?? pointFor(hoofPoints, frame, framePoints.at(-1));
            finalTargetErrorSum += distance2(framePoints.at(-1), target);
            framePoints.slice(1).forEach((point, index) => {
                maximumLengthError = Math.max(maximumLengthError, Math.abs(
                    distance2(framePoints[index], point) - limb.joints[index].length,
                ));
                maximumJointLimitViolation = Math.max(maximumJointLimitViolation,
                    Math.max(limb.joints[index].minimum - angles[frame][index], 0),
                    Math.max(angles[frame][index] - limb.joints[index].maximum, 0));
            });
            debugFrames[frame].limbs[label] = { points: framePoints.map((point) => [...point]) };
        });
        maximumSlide = Math.max(maximumSlide, maximumContactSlide(points, pins));
        if (loop) {
            angles[0].forEach((value, index) => {
                loopEndpointError = Math.max(loopEndpointError, Math.abs(angleDelta(value, angles.at(-1)[index])));
            });
        }
    });
    if (rootTrack && loop) {
        for (let axis = 0; axis < 3; axis += 1) {
            loopEndpointError = Math.max(loopEndpointError, Math.abs(
                fittedRootPositions[0][axis] - fittedRootPositions.at(-1)[axis],
            ));
        }
    }
    if (loop) {
        positionTracks.forEach((track) => {
            for (let axis = 0; axis < 3; axis += 1) {
                loopEndpointError = Math.max(loopEndpointError, Math.abs(
                    track.values[axis] - track.values.at(-3 + axis),
                ));
            }
        });
    }

    return {
        schema: FITTED_SCHEMA,
        frameCount: observations.frameCount,
        fps: observations.fps,
        durationSeconds: (observations.frameCount - 1) / observations.fps,
        loop,
        tracks,
        positionTracks,
        rootTrack,
        qa: {
            targetSamples,
            initialMeanTargetErrorPx: initialTargetErrorSum / Math.max(targetSamples, 1),
            finalMeanTargetErrorPx: finalTargetErrorSum / Math.max(targetSamples, 1),
            maximumBoneLengthErrorPx: maximumLengthError,
            maximumJointLimitViolationRad: maximumJointLimitViolation,
            maximumContactSlidePx: maximumSlide,
            loopEndpointError: loopEndpointError,
        },
        frames: debugFrames,
    };
}

export function fittedTracksToThreeClip(fitted, THREE, name = 'LTX_Fitted') {
    if (!fitted || fitted.schema !== FITTED_SCHEMA) throw new Error(`fitted.schema must be ${FITTED_SCHEMA}`);
    if (!THREE?.AnimationClip || !THREE?.QuaternionKeyframeTrack || !THREE?.VectorKeyframeTrack) {
        throw new Error('THREE AnimationClip and keyframe track constructors are required');
    }
    const tracks = fitted.tracks.map((track) => new THREE.QuaternionKeyframeTrack(
        track.name,
        track.times,
        track.values,
    ));
    (fitted.positionTracks || []).forEach((track) => {
        tracks.push(new THREE.VectorKeyframeTrack(track.name, track.times, track.values));
    });
    if (fitted.rootTrack) {
        tracks.push(new THREE.VectorKeyframeTrack(
            fitted.rootTrack.name,
            fitted.rootTrack.times,
            fitted.rootTrack.values,
        ));
    }
    return new THREE.AnimationClip(name, fitted.durationSeconds, tracks);
}

export const BROWSER_FITTING_SCHEMAS = Object.freeze({
    skeleton: SKELETON_SCHEMA,
    observations: OBSERVATION_SCHEMA,
    fitted: FITTED_SCHEMA,
});
