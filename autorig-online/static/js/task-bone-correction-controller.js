export const BONE_CORRECTION_SCHEMA_VERSION = 1;

export const IDENTITY_BONE_CORRECTION = Object.freeze({
    rotationDeg: Object.freeze([0, 0, 0]),
    positionPct: Object.freeze([0, 0, 0]),
    motionScale: 1,
    enabled: true,
});

const EPSILON = 1e-7;
const ROLE_ORDER = Object.freeze(['head', 'tail', 'spine', 'front_leg', 'rear_leg', 'limb', 'other']);

function finiteNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function clamp(value, minimum, maximum) {
    return Math.min(maximum, Math.max(minimum, value));
}

function normalizeVector(value, limit, partial) {
    if (!Array.isArray(value) || value.length < 3) return partial ? undefined : [0, 0, 0];
    const result = value.slice(0, 3).map((item) => {
        const number = finiteNumber(item);
        return number === null ? 0 : clamp(number, -limit, limit);
    });
    return result;
}

function hasOwn(value, key) {
    return Object.prototype.hasOwnProperty.call(value || {}, key);
}

function deepClone(value) {
    return JSON.parse(JSON.stringify(value));
}

export function normalizeClipId(value) {
    return String(value ?? '').trim();
}

export function normalizeBoneCorrection(value, options = {}) {
    const partial = options.partial === true;
    const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    const result = {};

    if (!partial || hasOwn(source, 'rotationDeg')) {
        const rotationDeg = normalizeVector(source.rotationDeg, 180, partial);
        if (rotationDeg) result.rotationDeg = rotationDeg;
    }
    if (!partial || hasOwn(source, 'positionPct')) {
        const positionPct = normalizeVector(source.positionPct, 100, partial);
        if (positionPct) result.positionPct = positionPct;
    }
    if (!partial || hasOwn(source, 'motionScale')) {
        const motionScale = finiteNumber(source.motionScale);
        if (motionScale !== null) result.motionScale = clamp(motionScale, 0, 2);
        else if (!partial) result.motionScale = IDENTITY_BONE_CORRECTION.motionScale;
    }
    if (!partial || hasOwn(source, 'enabled')) {
        result.enabled = hasOwn(source, 'enabled')
            ? source.enabled !== false
            : IDENTITY_BONE_CORRECTION.enabled;
    }

    return result;
}

function normalizeCorrectionMap(value) {
    const result = {};
    if (!value || typeof value !== 'object' || Array.isArray(value)) return result;
    Object.entries(value).forEach(([rawPath, correction]) => {
        const bonePath = String(rawPath || '').trim();
        if (!bonePath || !correction || typeof correction !== 'object' || Array.isArray(correction)) return;
        const normalized = normalizeBoneCorrection(correction, { partial: true });
        if (Object.keys(normalized).length) result[bonePath] = normalized;
    });
    return result;
}

export function normalizeBoneCorrectionState(value = {}) {
    const source = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    const clips = {};
    if (source.clips && typeof source.clips === 'object' && !Array.isArray(source.clips)) {
        Object.entries(source.clips).forEach(([rawClipId, corrections]) => {
            const clipId = normalizeClipId(rawClipId);
            if (!clipId) return;
            const normalized = normalizeCorrectionMap(corrections);
            if (Object.keys(normalized).length) clips[clipId] = normalized;
        });
    }
    const result = {
        schemaVersion: BONE_CORRECTION_SCHEMA_VERSION,
        global: normalizeCorrectionMap(source.global),
        clips,
    };
    if (typeof source.skeletonSignature === 'string' && source.skeletonSignature.trim()) {
        result.skeletonSignature = source.skeletonSignature.trim().slice(0, 128);
    }
    return result;
}

export function serializeBoneCorrectionState(value) {
    return JSON.stringify(normalizeBoneCorrectionState(value));
}

export function deserializeBoneCorrectionState(value) {
    if (typeof value !== 'string') return normalizeBoneCorrectionState(value);
    try {
        return normalizeBoneCorrectionState(JSON.parse(value));
    } catch (_error) {
        return normalizeBoneCorrectionState();
    }
}

export function mergeBoneCorrections(globalCorrection, clipCorrection) {
    return normalizeBoneCorrection({
        ...(globalCorrection && typeof globalCorrection === 'object' ? globalCorrection : {}),
        ...(clipCorrection && typeof clipCorrection === 'object' ? clipCorrection : {}),
    });
}

export function resolvedBoneCorrection(state, bonePath, clipId = '') {
    const normalized = normalizeBoneCorrectionState(state);
    const key = String(bonePath || '').trim();
    const activeClip = normalizeClipId(clipId);
    return mergeBoneCorrections(
        normalized.global[key],
        activeClip ? normalized.clips[activeClip]?.[key] : null,
    );
}

function vec3(value, fallback = [0, 0, 0]) {
    if (Array.isArray(value)) return value.slice(0, 3).map((item, index) => finiteNumber(item) ?? fallback[index]);
    if (value && typeof value === 'object') {
        return [finiteNumber(value.x) ?? fallback[0], finiteNumber(value.y) ?? fallback[1], finiteNumber(value.z) ?? fallback[2]];
    }
    return [...fallback];
}

function quat(value, fallback = [0, 0, 0, 1]) {
    if (Array.isArray(value)) return value.slice(0, 4).map((item, index) => finiteNumber(item) ?? fallback[index]);
    if (value && typeof value === 'object') {
        return [
            finiteNumber(value.x) ?? fallback[0],
            finiteNumber(value.y) ?? fallback[1],
            finiteNumber(value.z) ?? fallback[2],
            finiteNumber(value.w) ?? fallback[3],
        ];
    }
    return [...fallback];
}

function quatNormalize(value) {
    const q = quat(value);
    const length = Math.hypot(q[0], q[1], q[2], q[3]);
    if (length < EPSILON) return [0, 0, 0, 1];
    return q.map((item) => item / length);
}

function quatInverse(value) {
    const q = quatNormalize(value);
    return [-q[0], -q[1], -q[2], q[3]];
}

function quatMultiply(left, right) {
    const a = quat(left);
    const b = quat(right);
    return quatNormalize([
        a[3] * b[0] + a[0] * b[3] + a[1] * b[2] - a[2] * b[1],
        a[3] * b[1] - a[0] * b[2] + a[1] * b[3] + a[2] * b[0],
        a[3] * b[2] + a[0] * b[1] - a[1] * b[0] + a[2] * b[3],
        a[3] * b[3] - a[0] * b[0] - a[1] * b[1] - a[2] * b[2],
    ]);
}

function quatPow(value, exponent) {
    let q = quatNormalize(value);
    if (q[3] < 0) q = q.map((item) => -item);
    const halfAngle = Math.acos(clamp(q[3], -1, 1));
    const sinHalfAngle = Math.sin(halfAngle);
    if (Math.abs(sinHalfAngle) < EPSILON || Math.abs(exponent) < EPSILON) return [0, 0, 0, 1];
    const scaledHalfAngle = halfAngle * exponent;
    const factor = Math.sin(scaledHalfAngle) / sinHalfAngle;
    return quatNormalize([
        q[0] * factor,
        q[1] * factor,
        q[2] * factor,
        Math.cos(scaledHalfAngle),
    ]);
}

export function quaternionFromEulerDegrees(rotationDeg) {
    const [x, y, z] = vec3(rotationDeg).map((degrees) => degrees * Math.PI / 180);
    const c1 = Math.cos(x / 2);
    const c2 = Math.cos(y / 2);
    const c3 = Math.cos(z / 2);
    const s1 = Math.sin(x / 2);
    const s2 = Math.sin(y / 2);
    const s3 = Math.sin(z / 2);
    return quatNormalize([
        s1 * c2 * c3 + c1 * s2 * s3,
        c1 * s2 * c3 - s1 * c2 * s3,
        c1 * c2 * s3 + s1 * s2 * c3,
        c1 * c2 * c3 - s1 * s2 * s3,
    ]);
}

export function applyCorrectionToPose({ rest, animated, correction, modelHeight = 1 } = {}) {
    const normalized = normalizeBoneCorrection(correction);
    const restPosition = vec3(rest?.position);
    const animatedPosition = vec3(animated?.position, restPosition);
    const restQuaternion = quatNormalize(rest?.quaternion);
    const animatedQuaternion = quatNormalize(animated?.quaternion || restQuaternion);
    const height = Math.max(EPSILON, Math.abs(finiteNumber(modelHeight) ?? 1));

    if (!normalized.enabled) {
        return {
            position: animatedPosition,
            quaternion: animatedQuaternion,
            scale: vec3(animated?.scale, vec3(rest?.scale, [1, 1, 1])),
        };
    }

    const animatedDelta = quatMultiply(quatInverse(restQuaternion), animatedQuaternion);
    const scaledDelta = quatPow(animatedDelta, normalized.motionScale);
    const offsetQuaternion = quaternionFromEulerDegrees(normalized.rotationDeg);
    const position = restPosition.map((restValue, index) => (
        restValue
        + (animatedPosition[index] - restValue) * normalized.motionScale
        + normalized.positionPct[index] * height / 100
    ));

    return {
        position,
        quaternion: quatMultiply(quatMultiply(restQuaternion, scaledDelta), offsetQuaternion),
        scale: vec3(animated?.scale, vec3(rest?.scale, [1, 1, 1])),
    };
}

function nodeBaseName(node) {
    const name = String(node?.name || '').trim();
    if (name) return name.replaceAll('/', '%2F');
    return String(node?.type || (node?.isBone ? 'Bone' : 'Object3D'));
}

function stableNodeSegment(node) {
    const base = nodeBaseName(node);
    const siblings = Array.isArray(node?.parent?.children)
        ? node.parent.children.filter((candidate) => nodeBaseName(candidate) === base)
        : [];
    if (siblings.length <= 1) return base;
    return `${base}[${Math.max(0, siblings.indexOf(node))}]`;
}

export function buildStableBonePath(bone, root = null) {
    if (!bone) return '';
    const segments = [];
    let current = bone;
    while (current && current !== root) {
        segments.push(stableNodeSegment(current));
        current = current.parent || null;
    }
    return segments.reverse().join('/');
}

function traverseObject(root, callback) {
    if (!root) return;
    if (typeof root.traverse === 'function') {
        root.traverse(callback);
        return;
    }
    const visit = (node) => {
        callback(node);
        (Array.isArray(node?.children) ? node.children : []).forEach(visit);
    };
    visit(root);
}

export function collectDeformBones(model) {
    const bones = [];
    const seen = new Set();
    traverseObject(model, (object) => {
        const skeletonBones = Array.isArray(object?.skeleton?.bones) ? object.skeleton.bones : [];
        skeletonBones.forEach((bone) => {
            if (!bone || seen.has(bone)) return;
            seen.add(bone);
            bones.push(bone);
        });
    });
    return bones;
}

function normalizedBoneText(value) {
    return String(value || '')
        .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, ' ');
}

export function classifyBoneRole(boneOrName, path = '') {
    const name = typeof boneOrName === 'string' ? boneOrName : boneOrName?.name;
    const text = normalizedBoneText(`${path} ${name || ''}`);
    if (/\b(head|skull|muzzle|jaw|cranium)\b/.test(text)) return 'head';
    if (/\b(tail|cauda)\b/.test(text)) return 'tail';
    if (/\b(spine|neck|chest|pelvis|hip|body|torso)\b/.test(text)) return 'spine';
    const isLeg = /\b(leg|limb|thigh|calf|shin|foot|paw|hoof|arm|hand)\b/.test(text);
    if (isLeg && /\b(front|fore)\b/.test(text)) return 'front_leg';
    if (isLeg && /\b(back|rear|hind)\b/.test(text)) return 'rear_leg';
    if (isLeg) return 'limb';
    return 'other';
}

export function matchesBoneSearch(metadata, query) {
    const terms = normalizedBoneText(query).split(/\s+/).filter(Boolean);
    if (!terms.length) return true;
    const haystack = normalizedBoneText(`${metadata?.path || ''} ${metadata?.name || metadata?.bone?.name || ''} ${metadata?.role || ''}`);
    return terms.every((term) => haystack.includes(term));
}

export function enumerateDeformBones(model) {
    const records = collectDeformBones(model).map((bone) => {
        const path = buildStableBonePath(bone, model);
        return {
            bone,
            path,
            name: String(bone?.name || stableNodeSegment(bone)),
            role: classifyBoneRole(bone, path),
        };
    });
    const pathCounts = new Map();
    records.forEach((record) => pathCounts.set(record.path, (pathCounts.get(record.path) || 0) + 1));
    const seenPaths = new Map();
    records.forEach((record) => {
        if ((pathCounts.get(record.path) || 0) <= 1) return;
        const occurrence = seenPaths.get(record.path) || 0;
        seenPaths.set(record.path, occurrence + 1);
        record.path = `${record.path}#${occurrence}`;
    });
    return records;
}

export function computeSkeletonSignature(bonesOrMetadata = []) {
    const paths = (Array.isArray(bonesOrMetadata) ? bonesOrMetadata : [])
        .map((entry) => String(entry?.path || entry || ''))
        .filter(Boolean);
    let hash = 0x811c9dc5;
    for (const character of paths.join('\n')) {
        hash ^= character.charCodeAt(0);
        hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return `bones-v1:${paths.length}:${hash.toString(16).padStart(8, '0')}`;
}

function readLocalTransform(bone) {
    return {
        position: vec3(bone?.position),
        quaternion: quatNormalize(bone?.quaternion),
        scale: vec3(bone?.scale, [1, 1, 1]),
    };
}

function writeVector(target, values) {
    if (!target) return;
    if (typeof target.set === 'function') target.set(values[0], values[1], values[2]);
    else {
        target.x = values[0];
        target.y = values[1];
        target.z = values[2];
    }
}

function writeQuaternion(target, values) {
    if (!target) return;
    if (typeof target.set === 'function') target.set(values[0], values[1], values[2], values[3]);
    else {
        target.x = values[0];
        target.y = values[1];
        target.z = values[2];
        target.w = values[3];
    }
}

function writeLocalTransform(bone, transform) {
    writeVector(bone?.position, transform.position);
    writeQuaternion(bone?.quaternion, transform.quaternion);
    writeVector(bone?.scale, transform.scale);
    bone?.updateMatrix?.();
}

function transformEquals(left, right, epsilon = EPSILON) {
    if (!left || !right) return false;
    return [...left.position, ...left.quaternion, ...left.scale].every((value, index) => {
        const other = [...right.position, ...right.quaternion, ...right.scale][index];
        return Math.abs(value - other) <= epsilon;
    });
}

function calculateModelHeight(model, THREE) {
    if (!model || !THREE?.Box3) return 1;
    try {
        const box = new THREE.Box3().setFromObject(model);
        const height = Number(box.max?.y) - Number(box.min?.y);
        return Number.isFinite(height) && Math.abs(height) > EPSILON ? Math.abs(height) : 1;
    } catch (_error) {
        return 1;
    }
}

function mirroredPathKey(value) {
    return String(value || '')
        .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
        .toLowerCase()
        .replace(/\bleft\b/g, '{side}')
        .replace(/\bright\b/g, '{side}')
        .replace(/(^|[._\- ])l(?=$|[._\- ])/g, '$1{side}')
        .replace(/(^|[._\- ])r(?=$|[._\- ])/g, '$1{side}');
}

export function mirrorCorrection(correction) {
    const normalized = normalizeBoneCorrection(correction, { partial: true });
    const result = { ...normalized };
    if (normalized.rotationDeg) {
        result.rotationDeg = [normalized.rotationDeg[0], -normalized.rotationDeg[1], -normalized.rotationDeg[2]];
    }
    if (normalized.positionPct) {
        result.positionPct = [-normalized.positionPct[0], normalized.positionPct[1], normalized.positionPct[2]];
    }
    return result;
}

export class TaskBoneCorrectionController {
    constructor(options = {}) {
        this.THREE = options.THREE || null;
        this.maxHistory = Math.max(1, Number(options.maxHistory) || 50);
        this.onChange = typeof options.onChange === 'function' ? options.onChange : () => {};
        this.state = normalizeBoneCorrectionState(options.initialState);
        this.activeClipId = normalizeClipId(options.activeClipId);
        this.enabled = options.enabled !== false;
        this.model = null;
        this.modelHeight = 1;
        this.bones = [];
        this.boneByPath = new Map();
        this.past = [];
        this.future = [];
        this.batchDepth = 0;
        this.batchSnapshot = null;
        this.batchChanged = false;
        this.batchNotification = null;
        this.skeletonSignature = '';
    }

    configure({ model = null, modelHeight = null } = {}) {
        this.restoreAnimatedPose();
        this.model = model;
        this.modelHeight = Math.max(EPSILON, Math.abs(finiteNumber(modelHeight) ?? calculateModelHeight(model, this.THREE)));
        this.bones = enumerateDeformBones(model).map((metadata) => ({
            ...metadata,
            rest: readLocalTransform(metadata.bone),
            lastRaw: null,
            lastCorrected: null,
        }));
        this.boneByPath = new Map(this.bones.map((metadata) => [metadata.path, metadata]));
        this.skeletonSignature = computeSkeletonSignature(this.bones);
        if (!this.state.skeletonSignature && this.bones.length) {
            this.state.skeletonSignature = this.skeletonSignature;
        }
        return this.bones;
    }

    destroy() {
        this.restoreAnimatedPose();
        this.model = null;
        this.bones = [];
        this.boneByPath.clear();
    }

    setActiveClip(clipId) {
        this.activeClipId = normalizeClipId(clipId);
        return this.activeClipId;
    }

    setEnabled(enabled) {
        const next = enabled !== false;
        if (this.enabled === next) return this.enabled;
        this.enabled = next;
        if (!next) this.restoreAnimatedPose();
        return this.enabled;
    }

    getResolvedCorrection(bonePath, clipId = this.activeClipId) {
        return resolvedBoneCorrection(this.state, bonePath, clipId);
    }

    listBones(options = {}) {
        const role = ROLE_ORDER.includes(options.role) ? options.role : '';
        return this.bones.filter((metadata) => (
            (!role || metadata.role === role) && matchesBoneSearch(metadata, options.query)
        ));
    }

    _scopeMap(scope, clipId, create = false) {
        if (scope === 'clip') {
            const key = normalizeClipId(clipId || this.activeClipId);
            if (!key) return null;
            if (create && !this.state.clips[key]) this.state.clips[key] = {};
            return this.state.clips[key] || null;
        }
        return this.state.global;
    }

    _recordHistory() {
        if (this.batchDepth > 0) return;
        this.past.push(deepClone(this.state));
        if (this.past.length > this.maxHistory) this.past.shift();
        this.future = [];
    }

    _notify(reason, detail = {}) {
        if (this.batchDepth > 0) {
            this.batchChanged = true;
            this.batchNotification = { reason, ...detail };
            return;
        }
        this.onChange(this.getState(), { reason, ...detail });
    }

    beginBatch() {
        if (this.batchDepth === 0) {
            this.batchSnapshot = deepClone(this.state);
            this.batchChanged = false;
            this.batchNotification = null;
        }
        this.batchDepth += 1;
        return this.batchDepth;
    }

    endBatch() {
        if (this.batchDepth <= 0) return false;
        this.batchDepth -= 1;
        if (this.batchDepth > 0) return false;
        const snapshot = this.batchSnapshot;
        const changed = this.batchChanged;
        const notification = this.batchNotification;
        this.batchSnapshot = null;
        this.batchChanged = false;
        this.batchNotification = null;
        if (!changed || !snapshot) return false;
        this.past.push(snapshot);
        if (this.past.length > this.maxHistory) this.past.shift();
        this.future = [];
        this.onChange(this.getState(), {
            ...notification,
            reason: 'batch',
            lastReason: notification?.reason || '',
        });
        return true;
    }

    cancelBatch() {
        if (this.batchDepth <= 0 || !this.batchSnapshot) return false;
        this.state = normalizeBoneCorrectionState(this.batchSnapshot);
        this.batchDepth = 0;
        this.batchSnapshot = null;
        this.batchChanged = false;
        this.batchNotification = null;
        this.onChange(this.getState(), { reason: 'batch-cancel' });
        return true;
    }

    setCorrection(bonePath, patch, options = {}) {
        const path = String(bonePath || '').trim();
        if (!path || !patch || typeof patch !== 'object') return false;
        const normalizedPatch = normalizeBoneCorrection(patch, { partial: true });
        if (!Object.keys(normalizedPatch).length) return false;
        if (options.scope === 'clip' && !normalizeClipId(options.clipId || this.activeClipId)) return false;
        this._recordHistory();
        const map = this._scopeMap(options.scope, options.clipId, true);
        if (!map) return false;
        map[path] = { ...(map[path] || {}), ...normalizedPatch };
        this._notify('set-correction', { bonePath: path, scope: options.scope === 'clip' ? 'clip' : 'global' });
        return true;
    }

    resetBone(bonePath, options = {}) {
        const path = String(bonePath || '').trim();
        const map = this._scopeMap(options.scope, options.clipId, false);
        if (!path || !map || !hasOwn(map, path)) return false;
        this._recordHistory();
        delete map[path];
        if (options.scope === 'clip') {
            const clipId = normalizeClipId(options.clipId || this.activeClipId);
            if (clipId && !Object.keys(this.state.clips[clipId] || {}).length) delete this.state.clips[clipId];
        }
        this._notify('reset-bone', { bonePath: path });
        return true;
    }

    resetAll(options = {}) {
        const scope = options.scope || 'all';
        const hasChanges = scope === 'all'
            ? Object.keys(this.state.global).length > 0 || Object.keys(this.state.clips).length > 0
            : Boolean(this._scopeMap(scope, options.clipId, false) && Object.keys(this._scopeMap(scope, options.clipId, false)).length);
        if (!hasChanges) return false;
        this._recordHistory();
        if (scope === 'all') {
            this.state = normalizeBoneCorrectionState({ skeletonSignature: this.state.skeletonSignature });
        } else if (scope === 'clip') {
            delete this.state.clips[normalizeClipId(options.clipId || this.activeClipId)];
        } else {
            this.state.global = {};
        }
        this._notify('reset-all', { scope });
        return true;
    }

    findMirrorPath(bonePath) {
        const source = this.boneByPath.get(String(bonePath || ''));
        if (!source) return '';
        const key = mirroredPathKey(source.path);
        const matches = this.bones.filter((candidate) => candidate !== source && mirroredPathKey(candidate.path) === key);
        return matches.length === 1 ? matches[0].path : '';
    }

    mirrorBone(bonePath, options = {}) {
        const sourcePath = String(bonePath || '').trim();
        const targetPath = String(options.targetPath || this.findMirrorPath(sourcePath)).trim();
        const sourceMap = this._scopeMap(options.scope, options.clipId, false);
        if (!sourceMap?.[sourcePath] || !targetPath) return false;
        return this.setCorrection(targetPath, mirrorCorrection(sourceMap[sourcePath]), options);
    }

    undo() {
        if (this.batchDepth > 0 || !this.past.length) return false;
        this.future.push(deepClone(this.state));
        this.state = normalizeBoneCorrectionState(this.past.pop());
        this._notify('undo');
        return true;
    }

    redo() {
        if (this.batchDepth > 0 || !this.future.length) return false;
        this.past.push(deepClone(this.state));
        this.state = normalizeBoneCorrectionState(this.future.pop());
        this._notify('redo');
        return true;
    }

    loadState(value, options = {}) {
        if (this.batchDepth > 0) this.cancelBatch();
        if (options.recordHistory === true) this._recordHistory();
        this.state = deserializeBoneCorrectionState(value);
        if (!this.state.skeletonSignature && this.skeletonSignature) {
            this.state.skeletonSignature = this.skeletonSignature;
        }
        if (options.recordHistory !== true) {
            this.past = [];
            this.future = [];
        }
        this._notify('load');
        return this.getState();
    }

    getState() {
        return deepClone(this.state);
    }

    serialize() {
        return serializeBoneCorrectionState(this.state);
    }

    prepareForMixerUpdate() {
        this.bones.forEach((metadata) => {
            if (!metadata.lastRaw || !metadata.lastCorrected) return;
            const current = readLocalTransform(metadata.bone);
            if (transformEquals(current, metadata.lastCorrected)) writeLocalTransform(metadata.bone, metadata.lastRaw);
            metadata.lastRaw = null;
            metadata.lastCorrected = null;
        });
    }

    applyAfterMixerUpdate() {
        if (!this.enabled) return false;
        let changed = false;
        this.bones.forEach((metadata) => {
            const current = readLocalTransform(metadata.bone);
            const animated = metadata.lastRaw && metadata.lastCorrected && transformEquals(current, metadata.lastCorrected)
                ? metadata.lastRaw
                : current;
            const correction = this.getResolvedCorrection(metadata.path);
            const corrected = applyCorrectionToPose({
                rest: metadata.rest,
                animated,
                correction,
                modelHeight: this.modelHeight,
            });
            metadata.lastRaw = animated;
            metadata.lastCorrected = corrected;
            if (!transformEquals(current, corrected)) {
                writeLocalTransform(metadata.bone, corrected);
                changed = true;
            }
        });
        if (changed) this.model?.updateMatrixWorld?.(true);
        return changed;
    }

    apply() {
        return this.applyAfterMixerUpdate();
    }

    restoreAnimatedPose() {
        this.bones.forEach((metadata) => {
            if (metadata.lastRaw) writeLocalTransform(metadata.bone, metadata.lastRaw);
            metadata.lastRaw = null;
            metadata.lastCorrected = null;
        });
        this.model?.updateMatrixWorld?.(true);
    }
}
