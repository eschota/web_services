import assert from 'node:assert/strict';
import test from 'node:test';

import { fitBrowserAnimation } from '../animation-fitting-browser-core.js';
import {
    HORSE_2_SEMANTIC_PROFILE,
    HORSE_2_FULL_BODY_CHAINS,
    HORSE_2_FULL_BODY_JOINT_LIMITS,
    HORSE_2_FULL_BODY_SOURCE_ANCHORS,
    buildHorse2BrowserFittingSkeleton,
    computeContainScaleAndPad,
    computeLongDimensionScaleAndPad,
    createViewerToLtxProjection,
    horseDeformChainNames,
} from '../animation-fitting-three-adapter.js';

class Vector3 {
    constructor(x = 0, y = 0, z = 0) {
        this.set(x, y, z);
    }

    set(x, y, z) {
        this.x = Number(x);
        this.y = Number(y);
        this.z = Number(z);
        return this;
    }

    clone() {
        return new Vector3(this.x, this.y, this.z);
    }

    normalize() {
        const length = Math.hypot(this.x, this.y, this.z) || 1;
        this.x /= length;
        this.y /= length;
        this.z /= length;
        return this;
    }

    applyQuaternion(quaternion) {
        const { x, y, z } = this;
        const qx = quaternion.x;
        const qy = quaternion.y;
        const qz = quaternion.z;
        const qw = quaternion.w;
        const ix = qw * x + qy * z - qz * y;
        const iy = qw * y + qz * x - qx * z;
        const iz = qw * z + qx * y - qy * x;
        const iw = -qx * x - qy * y - qz * z;
        this.x = ix * qw + iw * -qx + iy * -qz - iz * -qy;
        this.y = iy * qw + iw * -qy + iz * -qx - ix * -qz;
        this.z = iz * qw + iw * -qz + ix * -qy - iy * -qx;
        return this;
    }

    project(camera) {
        return camera.projectVector(this);
    }

    unproject(camera) {
        return camera.unprojectVector(this);
    }
}

class Quaternion {
    constructor(x = 0, y = 0, z = 0, w = 1) {
        Object.assign(this, { x, y, z, w });
    }

    clone() {
        return new Quaternion(this.x, this.y, this.z, this.w);
    }

    invert() {
        this.x *= -1;
        this.y *= -1;
        this.z *= -1;
        return this;
    }
}

class Object3D {
    constructor(name = '') {
        this.name = name;
        this.type = 'Object3D';
        this.isBone = false;
        this.parent = null;
        this.children = [];
        this.position = new Vector3();
        this.quaternion = new Quaternion();
        this.userData = {};
    }

    add(child) {
        child.parent?.remove?.(child);
        child.parent = this;
        this.children.push(child);
        return this;
    }

    remove(child) {
        this.children = this.children.filter((item) => item !== child);
        if (child.parent === this) child.parent = null;
    }

    traverse(callback) {
        const visit = (node) => {
            callback(node);
            node.children.forEach(visit);
        };
        visit(this);
    }

    updateWorldMatrix() {}

    worldPosition() {
        const result = this.position.clone();
        let parent = this.parent;
        while (parent) {
            result.x += parent.position.x;
            result.y += parent.position.y;
            result.z += parent.position.z;
            parent = parent.parent;
        }
        return result;
    }

    getWorldPosition(target) {
        const value = this.worldPosition();
        return target.set(value.x, value.y, value.z);
    }

    getWorldQuaternion(target) {
        return Object.assign(target, { x: 0, y: 0, z: 0, w: 1 });
    }

    worldToLocal(point) {
        const origin = this.worldPosition();
        point.x -= origin.x;
        point.y -= origin.y;
        point.z -= origin.z;
        return point;
    }
}

class Bone extends Object3D {
    constructor(name) {
        super(name);
        this.type = 'Bone';
        this.isBone = true;
        this.userData.use_deform = true;
    }
}

class Camera extends Object3D {
    projectVector(vector) {
        vector.x /= 10;
        vector.y /= 10;
        vector.z = 0;
        return vector;
    }

    unprojectVector(vector) {
        vector.x *= 10;
        vector.y *= 10;
        vector.z = 0;
        return vector;
    }

    getWorldDirection(target) {
        return target.set(0, 0, -1);
    }

    updateProjectionMatrix() {}
}

const THREE = { Vector3, Quaternion };

function horseFixture({ fullBody = false } = {}) {
    const model = new Object3D('Model');
    const sharedRoot = new Bone('c_pos');
    model.add(sharedRoot);
    const xByLabel = {
        fore_left: -3,
        fore_right: -1,
        hind_left: 1,
        hind_right: 3,
    };
    const chainByLabel = {};
    Object.entries(xByLabel).forEach(([label, x]) => {
        const names = horseDeformChainNames(HORSE_2_SEMANTIC_PROFILE, label);
        const chain = names.map((name) => new Bone(name));
        chain[0].position.set(x, 3, 0);
        sharedRoot.add(chain[0]);
        for (let index = 1; index < chain.length; index += 1) {
            chain[index].position.set(0, -1, 0);
            chain[index - 1].add(chain[index]);
        }
        chainByLabel[label] = chain;
    });
    if (fullBody) {
        const body = HORSE_2_FULL_BODY_CHAINS.body_neck_head.map((name) => new Bone(name));
        body[0].position.set(-2, 0, 0);
        sharedRoot.add(body[0]);
        for (let index = 1; index < body.length; index += 1) {
            body[index].position.set(0.6, 0.25, 0);
            body[index - 1].add(body[index]);
        }
        const head = body.at(-1);
        const leftEar = HORSE_2_FULL_BODY_CHAINS.head_left_ear.slice(1).map((name) => new Bone(name));
        leftEar[0].position.set(0.2, 0.5, 0);
        leftEar[1].position.set(0, 0.5, 0);
        head.add(leftEar[0]);
        leftEar[0].add(leftEar[1]);
        const rightEar = HORSE_2_FULL_BODY_CHAINS.ear_right.map((name) => new Bone(name));
        rightEar[0].position.set(-0.2, 0.5, 0);
        rightEar[1].position.set(0, 0.5, 0);
        head.add(rightEar[0]);
        rightEar[0].add(rightEar[1]);
        const tail = HORSE_2_FULL_BODY_CHAINS.tail_base.map((name) => new Bone(name));
        tail[0].position.set(-2.5, 0.25, 0);
        sharedRoot.add(tail[0]);
        for (let index = 1; index < tail.length; index += 1) {
            tail[index].position.set(-0.35, -0.2, 0);
            tail[index - 1].add(tail[index]);
        }
        chainByLabel.body_neck_head = body;
        chainByLabel.head_left_ear = [head, ...leftEar];
        chainByLabel.ear_right = rightEar;
        chainByLabel.tail_base = tail;
    }
    return { model, sharedRoot, chainByLabel, camera: new Camera('Camera') };
}

function reparentPreserveWorld(child, parent) {
    const world = child.worldPosition();
    const parentWorld = parent.worldPosition();
    parent.add(child);
    child.position.set(
        world.x - parentWorld.x,
        world.y - parentWorld.y,
        world.z - parentWorld.z,
    );
}

function close(actual, expected, epsilon = 1e-9) {
    assert.ok(Math.abs(actual - expected) <= epsilon, `${actual} != ${expected}`);
}

test('viewer capture contain and LTX long-dimension transforms compose exactly', () => {
    const capture = computeContainScaleAndPad([1280, 720], [768, 448]);
    close(capture.scale, 0.6);
    assert.deepEqual(capture.scaled, [768, 432]);
    assert.deepEqual(capture.pad, [0, 8]);

    const ltx = computeLongDimensionScaleAndPad([768, 448], [512, 320]);
    close(ltx.scale, 2 / 3);
    close(ltx.scaled[0], 512);
    close(ltx.scaled[1], 448 * 2 / 3);
    close(ltx.pad[0], 0);
    close(ltx.pad[1], 32 / 3);

    const projection = createViewerToLtxProjection({
        sourceViewport: [1280, 720],
        referenceResolution: [768, 448],
        outputResolution: [512, 320],
    });
    const center = projection.ndcToOutput([0, 0, 0]);
    close(center[0], 256);
    close(center[1], 160);
    close(projection.sourceToOutputScale, 0.4);
    const pixelDelta = projection.outputPixelToNdcDelta();
    close(pixelDelta[0], 1 / 256);
    close(pixelDelta[1], -1 / 144);
});

test('pinned center-crop geometry uses OpenCV half-pixel centers in both directions', () => {
    const geometryTransform = {
        mode: 'center_crop_cover',
        coordinate_transform: 'half_pixel_centers',
        source_resolution: [768, 448],
        target_resolution: [384, 256],
        crop_pixels: { x: 48, y: 0, width: 672, height: 448 },
        scale_xy: [384 / 672, 256 / 448],
        rgb_interpolation: 'opencv_bilinear',
        mask_interpolation: 'opencv_nearest',
    };
    const projection = createViewerToLtxProjection({
        sourceViewport: [768, 448],
        referenceResolution: [768, 448],
        outputResolution: [384, 256],
        geometryTransform,
    });
    assert.equal(projection.projectionMode, 'pinned_reference_geometry_transform');
    assert.deepEqual(projection.geometryTransform, geometryTransform);
    const cropTopLeftNdc = [2 * 48 / 768 - 1, 1, 0.25];
    const cropTopLeftOutput = projection.ndcToOutput(cropTopLeftNdc);
    close(cropTopLeftOutput[0], -3 / 14);
    close(cropTopLeftOutput[1], -3 / 14);
    const roundTrip = projection.outputPixelToNdc(cropTopLeftOutput, 0.25);
    roundTrip.forEach((value, index) => close(value, cropTopLeftNdc[index]));
    const pixelDelta = projection.outputPixelToNdcDelta();
    close(pixelDelta[0], 7 / 1536);
    close(pixelDelta[1], -1 / 128);
});

test('Horse profile skips clavicles and exposes the exact seven ordered deform heads', () => {
    const fore = horseDeformChainNames(HORSE_2_SEMANTIC_PROFILE, 'fore_left');
    assert.equal(fore.length, 7);
    assert.equal(fore[0], 'c_thigh_b_dupli_001.l');
    assert.equal(fore.at(-1), 'toes_01_dupli_001.l');
    assert.equal(fore.includes('clavicle.l'), false);
    assert.deepEqual(horseDeformChainNames(HORSE_2_SEMANTIC_PROFILE, 'hind_right'), [
        'c_thigh_b.r',
        'thigh_twist.r',
        'thigh_stretch.r',
        'leg_stretch.r',
        'leg_twist.r',
        'foot.r',
        'toes_01.r',
    ]);
});

test('Three adapter auto-maps only chain roots in directly connected chains', () => {
    const { model, camera } = horseFixture();
    const skeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model,
        camera,
        semanticProfile: HORSE_2_SEMANTIC_PROFILE,
        sourceViewport: [100, 80],
        referenceResolution: [100, 80],
        outputResolution: [50, 50],
    });
    assert.equal(skeleton.schema, 'autorig-browser-fitting-skeleton.v1');
    assert.equal(skeleton.rigType, 'HORSE_2');
    assert.deepEqual(Object.keys(skeleton.limbs), [
        'fore_left', 'fore_right', 'hind_left', 'hind_right',
    ]);
    Object.values(skeleton.limbs).forEach((limb) => {
        assert.equal(limb.joints.length, 6);
        assert.equal(limb.sourceBoneChain.length, 7);
        assert.equal(limb.trackedJointIndex, 3);
        assert.equal(limb.joints.at(-1).bone.startsWith('foot'), true);
        for (let index = 1; index < limb.joints.length; index += 1) {
            assert.deepEqual(limb.joints[index - 1].restEnd, limb.joints[index].restStart);
        }
        limb.joints.forEach((joint, index) => {
            assert.deepEqual(joint.restQuaternion, [0, 0, 0, 1]);
            assert.deepEqual(joint.rotationAxis, [0, 0, -1]);
            assert.equal('positionMapping' in joint, index === 0);
            if (joint.positionMapping) {
                close(joint.positionMapping.xAxisPerPixel[0], 0.4);
                close(joint.positionMapping.xAxisPerPixel[1], 0);
                close(joint.positionMapping.yAxisPerPixel[0], 0);
                close(joint.positionMapping.yAxisPerPixel[1], -0.5);
            }
            assert.equal(joint.minAngle < 0, true);
            assert.equal(joint.maxAngle > 0, true);
        });
    });
    assert.equal(skeleton.provenance.sharedBoneRoot, 'c_pos');
    assert.equal(skeleton.provenance.terminalPolicy, 'seven_bone_heads_six_segments_to_toes_head');
    assert.equal(skeleton.provenance.positionMappings, 'auto_chain_roots_and_parent_breaks');
    close(skeleton.projection.sourceToOutputScale, 0.5);
    close(skeleton.projection.ltxCenterPad[1], 5);
    assert.equal('auxiliaryChains' in skeleton, false);
    assert.equal(skeleton.provenance.fullBody.enabled, false);
    assert.equal(skeleton.provenance.fullBody.selectedChainCount, 4);
});

test('opt-in Horse_2 full-body adapter keeps four locomotion limbs and exposes exact auxiliary chains', () => {
    const { model, camera, chainByLabel } = horseFixture({ fullBody: true });
    // The head's declared tail belongs to the axial head bone, not to either
    // ear branch. Only the exact versioned head -> left-ear connector may
    // bypass the ordinary parent-chain tail continuity check.
    chainByLabel.body_neck_head.at(-1).userData.tailWorld = [99, 99, 99];
    const skeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model,
        camera,
        sourceViewport: [100, 80],
        referenceResolution: [100, 80],
        outputResolution: [50, 50],
        includeFullBody: true,
    });
    assert.deepEqual(Object.keys(skeleton.limbs), [
        'fore_left', 'fore_right', 'hind_left', 'hind_right',
    ]);
    assert.deepEqual(Object.keys(skeleton.auxiliaryChains), [
        'body_neck_head', 'head_left_ear', 'ear_right', 'tail_base',
    ]);
    Object.entries(HORSE_2_FULL_BODY_CHAINS).forEach(([label, names]) => {
        const chain = skeleton.auxiliaryChains[label];
        assert.deepEqual(chain.sourceBoneChain, names);
        assert.deepEqual(chain.sourceAnchorIds, HORSE_2_FULL_BODY_SOURCE_ANCHORS[label]);
        chain.joints.forEach((joint) => {
            assert.deepEqual(
                [joint.minAngle, joint.maxAngle],
                HORSE_2_FULL_BODY_JOINT_LIMITS[joint.bone],
            );
        });
    });
    assert.equal(skeleton.auxiliaryChains.ear_right.joints.length, 1);
    assert.equal(skeleton.auxiliaryChains.ear_right.trackedJointIndex, null);
    assert.deepEqual(skeleton.auxiliaryChains.head_left_ear.branchConnector, {
        schema: 'autorig-browser-fitting-branch-connector.v1',
        bone: 'head.x',
        fromChain: 'body_neck_head',
        fromHeadIndex: 8,
        toHeadIndex: 0,
    });
    assert.deepEqual(skeleton.provenance.fullBody, {
        schema: 'horse_2.semantic_full_body.v1',
        enabled: true,
        locomotionChainCount: 4,
        auxiliaryChainCount: 4,
        selectedChainCount: 8,
        selectedSourceBoneCount: 47,
        selectedAnimatedBoneCount: 40,
        auxiliaryChainLabels: ['body_neck_head', 'head_left_ear', 'ear_right', 'tail_base'],
    });
});

function constantObservations(skeleton, frameCount = 5) {
    const tracks = [];
    Object.entries(skeleton.limbs).forEach(([label, limb]) => {
        const proximal = limb.joints[0].restStart;
        const joint = limb.joints[limb.trackedJointIndex].restStart;
        const hoof = limb.joints.at(-1).restEnd;
        for (const [role, point] of Object.entries({ proximal, joint, hoof })) {
            tracks.push({
                anchor_id: `${label}.${role}`,
                points: Array.from({ length: frameCount }, (_, frame) => ({
                    frame,
                    x: point[0],
                    y: point[1],
                    visible: true,
                    confidence: 1,
                })),
            });
        }
    });
    return {
        schema: 'autorig-fitting-observations.v1',
        frame_count: frameCount,
        width: 50,
        height: 50,
        fps: 30,
        tracks,
        contacts: [],
    };
}

test('adapter output is accepted by browser core without child position double transforms', () => {
    const { model, camera } = horseFixture();
    const skeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model,
        camera,
        sourceViewport: [100, 80],
        referenceResolution: [100, 80],
        outputResolution: [50, 50],
    });
    const fitted = fitBrowserAnimation({
        skeleton,
        observations: constantObservations(skeleton),
        options: { loop: true, smoothingRadius: 0 },
    });
    assert.equal(fitted.tracks.length, 24);
    assert.equal(fitted.positionTracks.length, 4);
    assert.equal(fitted.qa.maximumBoneLengthErrorPx < 1e-9, true);
    assert.equal(fitted.qa.loopEndpointError, 0);
});

test('auto position mappings include mixed-parent chain breaks but exclude connected children', () => {
    const { model, sharedRoot, chainByLabel, camera } = horseFixture();
    Object.values(chainByLabel).forEach((chain) => {
        reparentPreserveWorld(chain[2], sharedRoot);
        reparentPreserveWorld(chain[5], sharedRoot);
    });
    const skeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model,
        camera,
        sourceViewport: [100, 80],
        referenceResolution: [100, 80],
        outputResolution: [50, 50],
        includePositionMappings: 'auto',
    });

    Object.values(skeleton.limbs).forEach((limb) => {
        const mappedIndices = limb.joints
            .map((joint, index) => ('positionMapping' in joint ? index : -1))
            .filter((index) => index >= 0);
        assert.deepEqual(mappedIndices, [0, 2, 5]);
    });
    assert.equal(skeleton.provenance.positionMappings, 'auto_chain_roots_and_parent_breaks');

    const fitted = fitBrowserAnimation({
        skeleton,
        observations: constantObservations(skeleton),
        options: { loop: true, smoothingRadius: 0 },
    });
    assert.equal(fitted.positionTracks.length, 12);
    assert.deepEqual(
        fitted.positionTracks.map((track) => track.bone),
        Object.values(skeleton.limbs).flatMap((limb) => [0, 2, 5].map((index) => limb.joints[index].bone)),
    );
});

test('position mappings can be disabled without changing rest-chain geometry', () => {
    const { model, camera } = horseFixture();
    const skeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model,
        camera,
        sourceViewport: [100, 80],
        referenceResolution: [100, 80],
        outputResolution: [50, 50],
        includePositionMappings: false,
    });
    Object.values(skeleton.limbs).forEach((limb) => limb.joints.forEach((joint) => {
        assert.equal('positionMapping' in joint, false);
    }));
    assert.equal(skeleton.provenance.positionMappings, 'disabled');
});

test('adapter fails closed for missing, zero-length, declared-tail and root disconnections', () => {
    {
        const { model, camera, chainByLabel } = horseFixture();
        const foot = chainByLabel.hind_left.find((bone) => bone.name === 'foot.l');
        foot.parent.remove(foot);
        assert.throws(
            () => buildHorse2BrowserFittingSkeleton({ THREE, model, camera }),
            /deform bone is missing: foot\.l/,
        );
    }
    {
        const { model, camera, chainByLabel } = horseFixture();
        chainByLabel.hind_left[1].position.set(0, 0, 0);
        assert.throws(
            () => buildHorse2BrowserFittingSkeleton({ THREE, model, camera }),
            /zero-length at c_thigh_b\.l/,
        );
    }
    {
        const { model, camera, chainByLabel } = horseFixture();
        chainByLabel.fore_left[0].userData.tailWorld = [9, 9, 9];
        assert.throws(
            () => buildHorse2BrowserFittingSkeleton({ THREE, model, camera }),
            /declared tail does not connect/,
        );
    }
    {
        const { model, camera, chainByLabel } = horseFixture({ fullBody: true });
        chainByLabel.ear_right[0].userData.tailWorld = [9, 9, 9];
        assert.throws(
            () => buildHorse2BrowserFittingSkeleton({ THREE, model, camera, includeFullBody: true }),
            /ear_right declared tail does not connect/,
        );
    }
    {
        const { model, camera, sharedRoot, chainByLabel } = horseFixture();
        const separateRoot = new Bone('separate-root');
        model.add(separateRoot);
        sharedRoot.remove(chainByLabel.fore_right[0]);
        separateRoot.add(chainByLabel.fore_right[0]);
        assert.throws(
            () => buildHorse2BrowserFittingSkeleton({ THREE, model, camera }),
            /disconnected across Bone roots/,
        );
    }
});

test('malformed profile chain order and cropping projection are rejected', () => {
    const profile = structuredClone(HORSE_2_SEMANTIC_PROFILE);
    [profile.limb_groups.hind_left[1], profile.limb_groups.hind_left[2]] = [
        profile.limb_groups.hind_left[2],
        profile.limb_groups.hind_left[1],
    ];
    assert.throws(
        () => horseDeformChainNames(profile, 'hind_left'),
        /chain order does not match/,
    );
    assert.throws(
        () => computeLongDimensionScaleAndPad([320, 512], [512, 320]),
        /would crop/,
    );
});
