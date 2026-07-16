import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

import {
    TASK_ANIMATION_FITTING_BROWSER_CONTROLLER,
    runTaskAnimationFittingInBrowser,
} from '../task-animation-fitting-browser-controller.js';
import {
    HORSE_2_SEMANTIC_PROFILE,
    horseDeformChainNames,
} from '../animation-fitting-three-adapter.js';
import { RGB_OBSERVATION_BRIDGE_CONTRACT } from '../animation-fitting-rgb-observation-bridge.js';

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
    set(x, y, z, w) { Object.assign(this, { x, y, z, w }); return this; }
    clone() { return new Quaternion(this.x, this.y, this.z, this.w); }
    copy(value) { return this.set(value.x, value.y, value.z, value.w); }
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

class QuaternionKeyframeTrack {
    constructor(name, times, values) { Object.assign(this, { name, times, values }); }
}
class VectorKeyframeTrack {
    constructor(name, times, values) { Object.assign(this, { name, times, values }); }
}
class AnimationClip {
    constructor(name, duration, tracks) { Object.assign(this, { name, duration, tracks }); }
    validate() { return this.tracks.every((track) => track.values.every(Number.isFinite)); }
    static toJSON(clip) {
        return {
            name: clip.name,
            duration: clip.duration,
            tracks: clip.tracks.map((track) => ({
                name: track.name,
                times: Array.from(track.times),
                values: Array.from(track.values),
            })),
        };
    }
}

const THREE = {
    REVISION: '160-test',
    Vector3,
    Quaternion,
    QuaternionKeyframeTrack,
    VectorKeyframeTrack,
    AnimationClip,
};

function horseFixture(rootName = 'c_pos') {
    const model = new Object3D('current-model');
    const root = new Bone(rootName);
    model.add(root);
    const chains = {};
    Object.entries({ fore_left: -3, fore_right: -1, hind_left: 1, hind_right: 3 })
        .forEach(([label, x]) => {
            const chain = horseDeformChainNames(HORSE_2_SEMANTIC_PROFILE, label)
                .map((name) => new Bone(name));
            chain[0].position.set(x, 3, 0);
            root.add(chain[0]);
            for (let index = 1; index < chain.length; index += 1) {
                chain[index].position.set(0, -1, 0);
                chain[index - 1].add(chain[index]);
            }
            chains[label] = chain;
        });
    return { model, camera: new Camera('camera'), chains };
}

const HASHES = Object.freeze({
    immutableBundleSha256: '1'.repeat(64),
    fittingBundleSha256: '2'.repeat(64),
    observationsSha256: '3'.repeat(64),
    sourceModelSha256: '4'.repeat(64),
    threeModuleSha256: '5'.repeat(64),
    contactScheduleSha256: null,
});

function rgbObservations(chains, schema = RGB_OBSERVATION_BRIDGE_CONTRACT.observations) {
    let vertex = 100;
    const tracks = [];
    Object.entries(chains).forEach(([label, chain], labelIndex) => {
        chain.forEach((bone, headIndex) => {
            tracks.push({
                id: `tap_${label}_${headIndex}`,
                anchor_id: `${bone.name}:${vertex++}`,
                query_frame: 0,
                points: Array.from({ length: 3 }, (_, frame) => ({
                    frame,
                    x: labelIndex * 20 + headIndex,
                    y: 100 + headIndex,
                    visible: true,
                    confidence: 0.99,
                })),
            });
        });
    });
    return {
        schema,
        frame_count: 3,
        width: 512,
        height: 320,
        fps: 30,
        tracks,
        contacts: [],
        provenance: {
            bundle_sha256: HASHES.fittingBundleSha256,
            immutable_manifest_sha256: HASHES.immutableBundleSha256,
            source_video_sha256: '6'.repeat(64),
            tracker: { backend: RGB_OBSERVATION_BRIDGE_CONTRACT.trackerBackend },
        },
    };
}

function diagnosticInput(overrides = {}) {
    const fixture = horseFixture();
    return {
        mode: 'diagnostic',
        semanticId: 'horse.walk.diagnostic',
        clipName: 'Horse_Walk_Browser_Diagnostic',
        THREE,
        model: fixture.model,
        camera: fixture.camera,
        observations: rgbObservations(fixture.chains),
        pins: { ...HASHES },
        pinnedContactSchedule: null,
        skeletonOptions: { includePositionMappings: false },
        fitOptions: { loop: true, smoothingRadius: 0 },
        applyClips: () => true,
        ...overrides,
    };
}

test('diagnostic mode bridges, fits, hierarchy-bakes, serializes and applies one clip without a mixer', async () => {
    const input = diagnosticInput();
    const originalObservations = structuredClone(input.observations);
    const calls = [];
    input.applyClips = async (...args) => { calls.push(args); return true; };

    const result = await runTaskAnimationFittingInBrowser(input);

    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0][0], [result.clip]);
    assert.equal(calls[0][1], result.evidence);
    assert.equal(result.clip.name, input.clipName);
    assert.equal(result.clip.validate(), true);
    assert.equal(result.clip.tracks.length, 56);
    assert.equal(result.evidence.schema, TASK_ANIMATION_FITTING_BROWSER_CONTROLLER.evidenceSchema);
    assert.equal(result.evidence.status, 'APPLIED');
    assert.equal(result.evidence.mode, 'diagnostic');
    assert.equal(result.evidence.browserOnly, true);
    assert.equal(result.evidence.blenderUsed, false);
    assert.equal(result.evidence.mixerCreated, false);
    assert.equal(result.evidence.skeleton.sharedBoneRoot, 'c_pos');
    assert.equal(result.evidence.animationClip.name, input.clipName);
    assert.equal(result.evidence.animationClip.tracks.length, result.clip.tracks.length);
    assert.deepEqual(input.observations, originalObservations, 'controller does not mutate pinned observations');

    const source = fs.readFileSync(
        new URL('../task-animation-fitting-browser-controller.js', import.meta.url),
        'utf8',
    );
    assert.doesNotMatch(source, /new\s+THREE\.AnimationMixer|new\s+AnimationMixer/);
});

test('controller fails closed before apply on missing model, schema, hashes, or shared root', async () => {
    const cases = [
        [diagnosticInput({ model: null }), /current model root is required/],
        (() => {
            const input = diagnosticInput();
            input.observations.schema = 'wrong';
            return [input, /observations\.schema must be autorig-fitting-observations\.v1/];
        })(),
        (() => {
            const input = diagnosticInput();
            delete input.pins.threeModuleSha256;
            return [input, /pins\.threeModuleSha256 must be a non-empty string/];
        })(),
        (() => {
            const fixture = horseFixture('');
            return [diagnosticInput({
                model: fixture.model,
                camera: fixture.camera,
                observations: rgbObservations(fixture.chains),
            }), /Horse_2 shared bone root must be a non-empty string/];
        })(),
    ];
    for (const [input, pattern] of cases) {
        let applied = false;
        input.applyClips = () => { applied = true; return true; };
        await assert.rejects(runTaskAnimationFittingInBrowser(input), pattern);
        assert.equal(applied, false);
    }
});

test('mode and contact pin contract are explicit and fail closed', async () => {
    await assert.rejects(
        runTaskAnimationFittingInBrowser(diagnosticInput({
            pins: { ...HASHES, contactScheduleSha256: '7'.repeat(64) },
        })),
        /must be null in diagnostic mode/,
    );
    await assert.rejects(
        runTaskAnimationFittingInBrowser(diagnosticInput({
            mode: 'pinned-contact',
            pins: { ...HASHES, contactScheduleSha256: '7'.repeat(64) },
            pinnedContactSchedule: {
                sha256: '8'.repeat(64),
                schedule: {},
                pins: {},
            },
        })),
        /does not match its controller pin/,
    );
    await assert.rejects(
        runTaskAnimationFittingInBrowser(diagnosticInput({ mode: 'automatic' })),
        /mode must be diagnostic or pinned-contact/,
    );
});

test('a shared apply callback rejection leaves no false APPLIED evidence', async () => {
    let callbackEvidence;
    await assert.rejects(
        runTaskAnimationFittingInBrowser(diagnosticInput({
            applyClips: (_clips, evidence) => { callbackEvidence = evidence; return false; },
        })),
        /applyClips rejected/,
    );
    assert.equal(callbackEvidence.status, 'READY_TO_APPLY');
});
