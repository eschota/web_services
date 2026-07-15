import assert from 'node:assert/strict';
import test from 'node:test';

import {
    HORSE_2_SEMANTIC_PROFILE,
    bakeFittedAnimationToThreeHierarchyClip,
    buildHorse2BrowserFittingSkeleton,
    createViewerToLtxProjection,
    horseDeformChainNames,
} from '../animation-fitting-three-adapter.js';

class Vector3 {
    constructor(x = 0, y = 0, z = 0) { this.set(x, y, z); }
    set(x, y, z) { Object.assign(this, { x: Number(x), y: Number(y), z: Number(z) }); return this; }
    copy(value) { return this.set(value.x, value.y, value.z); }
    clone() { return new Vector3(this.x, this.y, this.z); }
    add(value) { this.x += value.x; this.y += value.y; this.z += value.z; return this; }
    sub(value) { this.x -= value.x; this.y -= value.y; this.z -= value.z; return this; }
    multiplyScalar(value) { this.x *= value; this.y *= value; this.z *= value; return this; }
    dot(value) { return this.x * value.x + this.y * value.y + this.z * value.z; }
    lengthSq() { return this.dot(this); }
    length() { return Math.sqrt(this.lengthSq()); }
    normalize() { return this.multiplyScalar(1 / (this.length() || 1)); }
    distanceTo(value) { return this.clone().sub(value).length(); }
    applyQuaternion(q) {
        const { x, y, z } = this;
        const ix = q.w * x + q.y * z - q.z * y;
        const iy = q.w * y + q.z * x - q.x * z;
        const iz = q.w * z + q.x * y - q.y * x;
        const iw = -q.x * x - q.y * y - q.z * z;
        this.x = ix * q.w + iw * -q.x + iy * -q.z - iz * -q.y;
        this.y = iy * q.w + iw * -q.y + iz * -q.x - ix * -q.z;
        this.z = iz * q.w + iw * -q.z + ix * -q.y - iy * -q.x;
        return this;
    }
    project(camera) { return camera.projectVector(this); }
    unproject(camera) { return camera.unprojectVector(this); }
}

class Quaternion {
    constructor(x = 0, y = 0, z = 0, w = 1) { Object.assign(this, { x, y, z, w }); }
    clone() { return new Quaternion(this.x, this.y, this.z, this.w); }
    copy(value) { Object.assign(this, { x: value.x, y: value.y, z: value.z, w: value.w }); return this; }
    normalize() {
        const length = Math.hypot(this.x, this.y, this.z, this.w) || 1;
        this.x /= length; this.y /= length; this.z /= length; this.w /= length;
        return this;
    }
    invert() { this.x *= -1; this.y *= -1; this.z *= -1; return this.normalize(); }
    multiply(value) {
        const ax = this.x; const ay = this.y; const az = this.z; const aw = this.w;
        const bx = value.x; const by = value.y; const bz = value.z; const bw = value.w;
        this.x = ax * bw + aw * bx + ay * bz - az * by;
        this.y = ay * bw + aw * by + az * bx - ax * bz;
        this.z = az * bw + aw * bz + ax * by - ay * bx;
        this.w = aw * bw - ax * bx - ay * by - az * bz;
        return this;
    }
    setFromUnitVectors(fromValue, toValue) {
        const from = fromValue.clone().normalize();
        const to = toValue.clone().normalize();
        let scalar = from.dot(to) + 1;
        if (scalar < 1e-8) {
            scalar = 0;
            if (Math.abs(from.x) > Math.abs(from.z)) this.set(-from.y, from.x, 0, scalar);
            else this.set(0, -from.z, from.y, scalar);
        } else {
            this.set(
                from.y * to.z - from.z * to.y,
                from.z * to.x - from.x * to.z,
                from.x * to.y - from.y * to.x,
                scalar,
            );
        }
        return this.normalize();
    }
    set(x, y, z, w) { Object.assign(this, { x, y, z, w }); return this; }
}

class Object3D {
    constructor(name = '') {
        this.name = name; this.type = 'Object3D'; this.isBone = false;
        this.parent = null; this.children = []; this.userData = {};
        this.position = new Vector3(); this.quaternion = new Quaternion(); this.scale = new Vector3(1, 1, 1);
    }
    add(child) { child.parent = this; this.children.push(child); return this; }
    traverse(callback) { const visit = (node) => { callback(node); node.children.forEach(visit); }; visit(this); }
    updateWorldMatrix() {}
    getWorldQuaternion(target) {
        if (!this.parent) return target.copy(this.quaternion);
        return target.copy(this.parent.getWorldQuaternion(new Quaternion())).multiply(this.quaternion);
    }
    getWorldPosition(target) {
        if (!this.parent) return target.copy(this.position);
        return target.copy(this.position)
            .applyQuaternion(this.parent.getWorldQuaternion(new Quaternion()))
            .add(this.parent.getWorldPosition(new Vector3()));
    }
    worldToLocal(point) {
        return point.sub(this.getWorldPosition(new Vector3()))
            .applyQuaternion(this.getWorldQuaternion(new Quaternion()).invert());
    }
}

class Bone extends Object3D {
    constructor(name) { super(name); this.type = 'Bone'; this.isBone = true; this.userData.use_deform = true; }
}

class Camera extends Object3D {
    projectVector(value) { value.x /= 10; value.y /= 10; value.z /= 10; return value; }
    unprojectVector(value) { value.x *= 10; value.y *= 10; value.z *= 10; return value; }
    getWorldDirection(target) { return target.set(0, 0, -1); }
    updateProjectionMatrix() {}
}

class QuaternionKeyframeTrack { constructor(name, times, values) { Object.assign(this, { name, times, values }); } }
class VectorKeyframeTrack { constructor(name, times, values) { Object.assign(this, { name, times, values }); } }
class AnimationClip {
    constructor(name, duration, tracks) { Object.assign(this, { name, duration, tracks }); }
    validate() { return this.tracks.every((track) => track.values.every(Number.isFinite)); }
}

const THREE = { Vector3, Quaternion, QuaternionKeyframeTrack, VectorKeyframeTrack, AnimationClip };

function helperParentFixture() {
    const model = new Object3D('model');
    const sharedRoot = new Bone('c_pos');
    model.add(sharedRoot);
    const chainByLabel = {};
    Object.entries({ fore_left: -3, fore_right: -1, hind_left: 1, hind_right: 3 }).forEach(([label, x]) => {
        const chain = horseDeformChainNames(HORSE_2_SEMANTIC_PROFILE, label).map((name) => new Bone(name));
        const upperHelper = new Bone(`${label}_upper_helper`); upperHelper.userData.use_deform = false;
        const hoofHelper = new Bone(`${label}_hoof_helper`); hoofHelper.userData.use_deform = false;
        chain[0].position.set(x, 3, 0); sharedRoot.add(chain[0]);
        chain[1].position.set(0, -1, 0); chain[0].add(chain[1]);
        upperHelper.position.set(x, 1, 0); sharedRoot.add(upperHelper); upperHelper.add(chain[2]);
        chain[3].position.set(0, -1, 0); chain[2].add(chain[3]);
        chain[4].position.set(0, -1, 0); chain[3].add(chain[4]);
        hoofHelper.position.set(x, -2, 0); sharedRoot.add(hoofHelper); hoofHelper.add(chain[5]);
        chain[6].position.set(0, -1, 0); chain[5].add(chain[6]);
        chainByLabel[label] = chain;
    });
    return { model, chainByLabel, camera: new Camera('camera') };
}

function projectedFrames(skeleton, projection, frameCount = 3) {
    const angles = [0, 0.2, -0.1];
    return angles.slice(0, frameCount).map((angle, frame) => ({
        frame,
        limbs: Object.fromEntries(Object.entries(skeleton.limbs).map(([label, limb]) => {
            const root = limb.joints[0].restStart;
            const points = [root];
            limb.joints.forEach((joint, index) => {
                const start = joint.restStart;
                const end = joint.restEnd;
                const dx = end[0] - start[0];
                const dy = end[1] - start[1];
                const cos = Math.cos(angle); const sin = Math.sin(angle);
                points.push([points[index][0] + dx * cos - dy * sin, points[index][1] + dx * sin + dy * cos]);
            });
            return [label, { points }];
        })),
    }));
}

test('hierarchy bake preserves helper-interrupted Horse chains and padded projection', () => {
    const { model, chainByLabel, camera } = helperParentFixture();
    const skeleton = buildHorse2BrowserFittingSkeleton({
        THREE, model, camera,
        sourceViewport: [1280, 720],
        referenceResolution: [768, 448],
        outputResolution: [512, 320],
        includePositionMappings: false,
    });
    const projection = createViewerToLtxProjection({
        sourceViewport: [1280, 720], referenceResolution: [768, 448], outputResolution: [512, 320],
    });
    assert.ok(projection.capture.pad[1] > 0);
    assert.ok(projection.ltx.pad[1] > 0);
    const frames = projectedFrames(skeleton, projection);
    const fitted = {
        schema: 'autorig-browser-fitted-animation.v1',
        frameCount: frames.length,
        fps: 30,
        durationSeconds: (frames.length - 1) / 30,
        frames,
    };
    const restPositions = new Map(Object.values(chainByLabel).flat().map((bone) => [bone, bone.position.clone()]));
    const { clip, qa } = bakeFittedAnimationToThreeHierarchyClip({ THREE, model, camera, skeleton, fitted });
    assert.equal(clip.validate(), true);
    assert.equal(clip.tracks.length, 56);
    assert.equal(qa.animatedBones, 28);
    assert.ok(qa.maximumSegmentLengthDriftWorld < 1e-9);
    assert.ok(qa.maximumHierarchyBakeReprojectionErrorPx < 1e-9);
    assert.ok(qa.maximumRequestedFittedPointErrorPx < 1e-9);
    assert.equal(qa.unreachablePixelRays, 0);
    restPositions.forEach((position, bone) => assert.ok(bone.position.distanceTo(position) < 1e-12));

    const frameIndex = 1;
    clip.tracks.forEach((track) => {
        const split = track.name.lastIndexOf('.');
        const name = track.name.slice(0, split); const field = track.name.slice(split + 1);
        const bone = Object.values(chainByLabel).flat().find((item) => item.name === name);
        const width = field === 'quaternion' ? 4 : 3;
        const values = track.values.slice(frameIndex * width, (frameIndex + 1) * width);
        if (field === 'quaternion') bone.quaternion.set(...values);
        else bone.position.set(...values);
    });
    Object.entries(skeleton.limbs).forEach(([label, limb]) => {
        const chain = chainByLabel[label];
        chain.slice(0, -1).forEach((bone, index) => {
            assert.ok(Math.abs(bone.getWorldPosition(new Vector3()).distanceTo(
                chain[index + 1].getWorldPosition(new Vector3()),
            ) - 1) < 1e-9);
        });
        chain.forEach((bone, index) => {
            const ndc = bone.getWorldPosition(new Vector3()).project(camera);
            const actual = projection.ndcToOutput([ndc.x, ndc.y, ndc.z]);
            const expected = frames[frameIndex].limbs[label].points[index];
            assert.ok(Math.hypot(actual[0] - expected[0], actual[1] - expected[1]) < 1e-9);
        });
    });
});

test('hierarchy bake fails closed when fitted frames omit a real chain point', () => {
    const { model, camera } = helperParentFixture();
    const skeleton = buildHorse2BrowserFittingSkeleton({ THREE, model, camera, includePositionMappings: false });
    const frames = projectedFrames(skeleton);
    frames[0].limbs.fore_left.points.pop();
    assert.throws(
        () => bakeFittedAnimationToThreeHierarchyClip({
            THREE, model, camera, skeleton,
            fitted: { schema: 'autorig-browser-fitted-animation.v1', frameCount: 3, fps: 30, durationSeconds: 2 / 30, frames },
        }),
        /must contain 7 points/,
    );
});
