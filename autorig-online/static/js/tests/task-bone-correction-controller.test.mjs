import assert from 'node:assert/strict';
import test from 'node:test';

import {
    TaskBoneCorrectionController,
    applyCorrectionToPose,
    buildStableBonePath,
    classifyBoneRole,
    computeSkeletonSignature,
    deserializeBoneCorrectionState,
    enumerateDeformBones,
    matchesBoneSearch,
    mergeBoneCorrections,
    normalizeBoneCorrectionState,
    quaternionFromEulerDegrees,
    resolvedBoneCorrection,
    serializeBoneCorrectionState,
} from '../task-bone-correction-controller.js';

function closeArray(actual, expected, epsilon = 1e-6) {
    assert.equal(actual.length, expected.length);
    actual.forEach((value, index) => assert.ok(
        Math.abs(value - expected[index]) <= epsilon,
        `${value} != ${expected[index]} at ${index}`,
    ));
}

function closeQuaternion(actual, expected, epsilon = 1e-6) {
    const direct = actual.every((value, index) => Math.abs(value - expected[index]) <= epsilon);
    const negated = actual.every((value, index) => Math.abs(value + expected[index]) <= epsilon);
    assert.ok(direct || negated, `${actual.join(',')} does not represent ${expected.join(',')}`);
}

function vector3(x = 0, y = 0, z = 0) {
    return {
        x, y, z,
        set(nx, ny, nz) {
            this.x = nx;
            this.y = ny;
            this.z = nz;
        },
    };
}

function quaternion(x = 0, y = 0, z = 0, w = 1) {
    return {
        x, y, z, w,
        set(nx, ny, nz, nw) {
            this.x = nx;
            this.y = ny;
            this.z = nz;
            this.w = nw;
        },
    };
}

function node(name, options = {}) {
    return {
        name,
        type: options.type || (options.isBone ? 'Bone' : 'Object3D'),
        isBone: options.isBone === true,
        isSkinnedMesh: options.isSkinnedMesh === true,
        position: vector3(),
        quaternion: quaternion(),
        scale: vector3(1, 1, 1),
        parent: null,
        children: [],
        updateMatrix() {},
        updateMatrixWorld() {},
        add(child) {
            child.parent = this;
            this.children.push(child);
            return this;
        },
        traverse(callback) {
            const visit = (current) => {
                callback(current);
                current.children.forEach(visit);
            };
            visit(this);
        },
    };
}

function rigFixture() {
    const model = node('Model');
    const armature = node('Armature');
    const left = node('leg.L', { isBone: true });
    const right = node('leg.R', { isBone: true });
    const head = node('head', { isBone: true });
    armature.add(left).add(right).add(head);
    const mesh = node('Body', { isSkinnedMesh: true });
    mesh.skeleton = { bones: [left, right, head, left] };
    model.add(armature).add(mesh);
    return { model, armature, left, right, head };
}

test('schema normalization validates, clamps and preserves sparse clip overrides', () => {
    const state = normalizeBoneCorrectionState({
        schemaVersion: 99,
        skeletonSignature: `  ${'s'.repeat(140)}  `,
        global: {
            head: {
                rotationDeg: ['10', 900, 'bad'],
                positionPct: [-200, 2, 3],
                motionScale: 4,
                enabled: false,
                ignored: true,
            },
            invalid: null,
        },
        clips: {
            run: { head: { motionScale: '0.5' } },
            '': { head: { motionScale: 1 } },
            idle: {},
        },
    });

    assert.deepEqual(state, {
        schemaVersion: 1,
        global: {
            head: {
                rotationDeg: [10, 180, 0],
                positionPct: [-100, 2, 3],
                motionScale: 2,
                enabled: false,
            },
        },
        clips: { run: { head: { motionScale: 0.5 } } },
        skeletonSignature: 's'.repeat(128),
    });
    assert.deepEqual(deserializeBoneCorrectionState(serializeBoneCorrectionState(state)), state);
    assert.deepEqual(deserializeBoneCorrectionState('{bad json'), {
        schemaVersion: 1,
        global: {},
        clips: {},
    });
});

test('clip correction inherits unspecified global fields and overrides explicit fields', () => {
    const global = { rotationDeg: [10, 0, 0], positionPct: [1, 2, 3], enabled: true };
    const clip = { motionScale: 0.25, enabled: false };
    assert.deepEqual(mergeBoneCorrections(global, clip), {
        rotationDeg: [10, 0, 0],
        positionPct: [1, 2, 3],
        motionScale: 0.25,
        enabled: false,
    });

    const state = normalizeBoneCorrectionState({ global: { head: global }, clips: { run: { head: clip } } });
    assert.equal(resolvedBoneCorrection(state, 'head', 'idle').motionScale, 1);
    assert.equal(resolvedBoneCorrection(state, 'head', 'run').motionScale, 0.25);
    assert.equal(resolvedBoneCorrection(state, 'head', 'run').enabled, false);
});

test('motion scale 0, 1 and 2 scales rest-to-animation translation and rotation', () => {
    const rest = { position: [1, 2, 3], quaternion: [0, 0, 0, 1], scale: [1, 1, 1] };
    const animated = {
        position: [3, 4, 5],
        quaternion: quaternionFromEulerDegrees([0, 0, 90]),
        scale: [1, 1, 1],
    };

    const frozen = applyCorrectionToPose({ rest, animated, correction: { motionScale: 0 } });
    closeArray(frozen.position, [1, 2, 3]);
    closeQuaternion(frozen.quaternion, quaternionFromEulerDegrees([0, 0, 0]));

    const original = applyCorrectionToPose({ rest, animated, correction: { motionScale: 1 } });
    closeArray(original.position, [3, 4, 5]);
    closeQuaternion(original.quaternion, quaternionFromEulerDegrees([0, 0, 90]));

    const doubled = applyCorrectionToPose({ rest, animated, correction: { motionScale: 2 } });
    closeArray(doubled.position, [5, 6, 7]);
    closeQuaternion(doubled.quaternion, quaternionFromEulerDegrees([0, 0, 180]));
});

test('rotation and model-height-normalized position offsets are applied after animation', () => {
    const corrected = applyCorrectionToPose({
        rest: { position: [0, 0, 0], quaternion: [0, 0, 0, 1] },
        animated: { position: [0, 0, 0], quaternion: [0, 0, 0, 1] },
        correction: { rotationDeg: [20, 0, 0], positionPct: [10, -5, 0] },
        modelHeight: 2,
    });
    closeArray(corrected.position, [0.2, -0.1, 0]);
    closeQuaternion(corrected.quaternion, quaternionFromEulerDegrees([20, 0, 0]));
});

test('stable paths distinguish duplicate names and collect each deform bone once', () => {
    const model = node('Model');
    const armature = node('Armature');
    const first = node('leg', { isBone: true });
    const second = node('leg', { isBone: true });
    armature.add(first).add(second);
    const mesh = node('Body', { isSkinnedMesh: true });
    mesh.skeleton = { bones: [first, second, first] };
    model.add(armature).add(mesh);

    assert.equal(buildStableBonePath(first, model), 'Armature/leg[0]');
    assert.equal(buildStableBonePath(second, model), 'Armature/leg[1]');
    const paths = enumerateDeformBones(model).map((entry) => entry.path);
    assert.deepEqual(paths, [
        'Armature/leg[0]',
        'Armature/leg[1]',
    ]);
    assert.equal(computeSkeletonSignature(paths), computeSkeletonSignature(paths));
    assert.notEqual(computeSkeletonSignature(paths), computeSkeletonSignature([...paths].reverse()));
});

test('controller application is idempotent and two-phase mixer integration does not accumulate offsets', () => {
    const { model, head } = rigFixture();
    const controller = new TaskBoneCorrectionController();
    const bones = controller.configure({ model, modelHeight: 2 });
    const headPath = bones.find((entry) => entry.bone === head).path;
    controller.setCorrection(headPath, { rotationDeg: [0, 0, 15], positionPct: [0, 10, 0] });

    assert.equal(controller.applyAfterMixerUpdate(), true);
    const firstQuaternion = [head.quaternion.x, head.quaternion.y, head.quaternion.z, head.quaternion.w];
    const firstPosition = [head.position.x, head.position.y, head.position.z];
    controller.applyAfterMixerUpdate();
    closeQuaternion([head.quaternion.x, head.quaternion.y, head.quaternion.z, head.quaternion.w], firstQuaternion);
    closeArray([head.position.x, head.position.y, head.position.z], firstPosition);

    controller.prepareForMixerUpdate();
    closeQuaternion([head.quaternion.x, head.quaternion.y, head.quaternion.z, head.quaternion.w], [0, 0, 0, 1]);
    closeArray([head.position.x, head.position.y, head.position.z], [0, 0, 0]);
    const animated = quaternionFromEulerDegrees([0, 10, 0]);
    head.quaternion.set(...animated);
    controller.applyAfterMixerUpdate();
    closeArray([head.position.x, head.position.y, head.position.z], firstPosition);
});

test('active clip switching changes the resolved correction without mutating global state', () => {
    const { model, head } = rigFixture();
    const controller = new TaskBoneCorrectionController();
    const headPath = controller.configure({ model }).find((entry) => entry.bone === head).path;
    controller.setCorrection(headPath, { rotationDeg: [10, 0, 0], motionScale: 1 });
    controller.setCorrection(headPath, { motionScale: 0 }, { scope: 'clip', clipId: 'idle' });
    controller.setCorrection(headPath, { rotationDeg: [0, 20, 0] }, { scope: 'clip', clipId: 'run' });

    controller.setActiveClip('idle');
    assert.deepEqual(controller.getResolvedCorrection(headPath), {
        rotationDeg: [10, 0, 0], positionPct: [0, 0, 0], motionScale: 0, enabled: true,
    });
    controller.setActiveClip('run');
    assert.deepEqual(controller.getResolvedCorrection(headPath), {
        rotationDeg: [0, 20, 0], positionPct: [0, 0, 0], motionScale: 1, enabled: true,
    });
    assert.deepEqual(controller.getState().global[headPath].rotationDeg, [10, 0, 0]);
});

test('mirror, reset, undo and redo operate on one correction scope', () => {
    const { model, left, right } = rigFixture();
    const controller = new TaskBoneCorrectionController();
    const bones = controller.configure({ model });
    const leftPath = bones.find((entry) => entry.bone === left).path;
    const rightPath = bones.find((entry) => entry.bone === right).path;

    assert.equal(controller.findMirrorPath(leftPath), rightPath);
    controller.setCorrection(leftPath, {
        rotationDeg: [10, 20, 30],
        positionPct: [1, 2, 3],
        motionScale: 0.75,
    });
    assert.equal(controller.mirrorBone(leftPath), true);
    assert.deepEqual(controller.getState().global[rightPath], {
        rotationDeg: [10, -20, -30],
        positionPct: [-1, 2, 3],
        motionScale: 0.75,
    });

    assert.equal(controller.undo(), true);
    assert.equal(controller.getState().global[rightPath], undefined);
    assert.equal(controller.redo(), true);
    assert.ok(controller.getState().global[rightPath]);
    assert.equal(controller.resetBone(rightPath), true);
    assert.equal(controller.getState().global[rightPath], undefined);
    assert.equal(controller.resetAll(), true);
    assert.deepEqual(controller.getState(), {
        schemaVersion: 1,
        global: {},
        clips: {},
        skeletonSignature: controller.skeletonSignature,
    });
});

test('a correction batch produces one notification and one undo snapshot', () => {
    const { model, head } = rigFixture();
    const notifications = [];
    const controller = new TaskBoneCorrectionController({
        onChange(_state, context) { notifications.push(context.reason); },
    });
    const headPath = controller.configure({ model }).find((entry) => entry.bone === head).path;

    controller.beginBatch();
    controller.setCorrection(headPath, { rotationDeg: [1, 0, 0] });
    controller.setCorrection(headPath, { rotationDeg: [2, 0, 0] });
    controller.setCorrection(headPath, { rotationDeg: [3, 0, 0] });
    assert.deepEqual(notifications, []);
    assert.equal(controller.endBatch(), true);
    assert.deepEqual(notifications, ['batch']);
    assert.deepEqual(controller.getState().global[headPath].rotationDeg, [3, 0, 0]);

    assert.equal(controller.undo(), true);
    assert.equal(controller.getState().global[headPath], undefined);
    assert.equal(controller.undo(), false);
});

test('role classification and search helpers support quick filters', () => {
    assert.equal(classifyBoneRole('c_tail_04.x'), 'tail');
    assert.equal(classifyBoneRole('front_left_hoof'), 'front_leg');
    assert.equal(classifyBoneRole('rear-leg.R'), 'rear_leg');
    assert.equal(classifyBoneRole('neck.x'), 'spine');
    assert.equal(matchesBoneSearch({ path: 'Armature/head', name: 'head', role: 'head' }, 'arm head'), true);
    assert.equal(matchesBoneSearch({ path: 'Armature/head', name: 'head', role: 'head' }, 'tail'), false);
});
