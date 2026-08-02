function attributeValue(attribute, vertexIndex, componentIndex) {
    const getters = ['getX', 'getY', 'getZ', 'getW'];
    const getter = getters[componentIndex];
    if (getter && typeof attribute?.[getter] === 'function') {
        return Number(attribute[getter](vertexIndex));
    }
    const itemSize = Number(attribute?.itemSize || 0);
    return Number(attribute?.array?.[(vertexIndex * itemSize) + componentIndex]);
}

function parseTrackBindingFallback(trackName) {
    const value = String(trackName || '');
    const boneMatch = value.match(/^\.?bones\[([^\]]+)\]\.(position|quaternion|scale)$/);
    if (boneMatch) return { targetName: boneMatch[1], propertyName: boneMatch[2] };
    const propertyMatch = value.match(/^(.*)\.(position|quaternion|scale|rotation|morphTargetInfluences)(?:\[.*\])?$/);
    return propertyMatch
        ? { targetName: propertyMatch[1], propertyName: propertyMatch[2] }
        : { targetName: '', propertyName: '' };
}

function trackBinding(track, parseTrackName) {
    try {
        const parsed = typeof parseTrackName === 'function'
            ? parseTrackName(String(track?.name || ''))
            : null;
        if (parsed?.nodeName) {
            return {
                targetName: String(parsed.nodeName),
                propertyName: String(parsed.propertyName || ''),
            };
        }
    } catch (_) {
        // A malformed/unsupported binding must remain in the clip.
    }
    return parseTrackBindingFallback(track?.name);
}

function addNamedObject(namedObjects, object) {
    const name = String(object?.name || '');
    if (!name) return;
    if (!namedObjects.has(name)) namedObjects.set(name, new Set());
    namedObjects.get(name).add(object);
}

function isRigidRenderable(object) {
    return !!(
        object?.isMesh
        || object?.isSkinnedMesh
        || object?.isSprite
        || object?.isLine
        || object?.isPoints
        || object?.isLight
    );
}

function collectSkinUsage(model, weightEpsilon) {
    const meshes = [];
    const allBones = new Set();
    const namedObjects = new Map();
    const rigidRenderableBones = new Set();
    model?.traverse?.((object) => {
        addNamedObject(namedObjects, object);
        if (object?.isBone) allBones.add(object);
        if (object?.isSkinnedMesh) meshes.push(object);
        if (isRigidRenderable(object)) {
            let parent = object?.parent;
            const visited = new Set();
            while (parent && !visited.has(parent)) {
                visited.add(parent);
                if (parent.isBone) rigidRenderableBones.add(parent);
                parent = parent.parent;
            }
        }
    });
    if (!meshes.length) return { reliable: false, reason: 'no-skinned-mesh' };

    meshes.forEach((mesh) => {
        (mesh?.skeleton?.bones || []).forEach((bone) => {
            allBones.add(bone);
            addNamedObject(namedObjects, bone);
        });
    });

    const requiredBones = new Set();
    rigidRenderableBones.forEach((bone) => requiredBones.add(bone));
    for (const mesh of meshes) {
        const bones = mesh?.skeleton?.bones;
        const skinIndex = mesh?.geometry?.attributes?.skinIndex;
        const skinWeight = mesh?.geometry?.attributes?.skinWeight;
        const indexItemSize = Number(skinIndex?.itemSize || 0);
        const weightItemSize = Number(skinWeight?.itemSize || 0);
        const vertexCount = Number(skinIndex?.count || 0);
        if (
            !Array.isArray(bones)
            || !bones.length
            || !skinIndex
            || !skinWeight
            || vertexCount <= 0
            || Number(skinWeight.count || 0) !== vertexCount
            || indexItemSize <= 0
            || weightItemSize <= 0
            || indexItemSize !== weightItemSize
        ) {
            return { reliable: false, reason: 'invalid-skin-attributes' };
        }

        for (let vertex = 0; vertex < vertexCount; vertex += 1) {
            for (let component = 0; component < weightItemSize; component += 1) {
                const weight = attributeValue(skinWeight, vertex, component);
                if (!Number.isFinite(weight)) return { reliable: false, reason: 'invalid-skin-weight' };
                if (weight <= weightEpsilon) continue;
                const boneIndex = attributeValue(skinIndex, vertex, component);
                if (!Number.isInteger(boneIndex) || boneIndex < 0 || boneIndex >= bones.length || !bones[boneIndex]) {
                    return { reliable: false, reason: 'invalid-skin-index' };
                }
                requiredBones.add(bones[boneIndex]);
            }
        }
    }
    if (!requiredBones.size) return { reliable: false, reason: 'no-weighted-bones' };

    [...requiredBones].forEach((bone) => {
        let parent = bone?.parent;
        const visited = new Set();
        while (parent?.isBone && !visited.has(parent)) {
            visited.add(parent);
            requiredBones.add(parent);
            parent = parent.parent;
        }
    });

    return { reliable: true, requiredBones, allBones, namedObjects };
}

function valuesNearlyEqual(left, right, epsilon = 1e-6) {
    return Math.abs(Number(left) - Number(right)) <= epsilon;
}

function constantTrackInfo(track, propertyName, target, epsilon = 1e-6) {
    if (!['position', 'quaternion', 'scale'].includes(propertyName)) return null;
    const times = track?.times;
    const values = track?.values;
    const keyCount = Number(times?.length || 0);
    const valueSize = Number(track?.getValueSize?.() || (keyCount ? values?.length / keyCount : 0));
    const expectedSize = propertyName === 'quaternion' ? 4 : 3;
    if (
        !keyCount
        || valueSize !== expectedSize
        || Number(values?.length || 0) !== keyCount * valueSize
    ) return null;

    for (let key = 1; key < keyCount; key += 1) {
        for (let component = 0; component < valueSize; component += 1) {
            if (!valuesNearlyEqual(values[component], values[(key * valueSize) + component], epsilon)) {
                return null;
            }
        }
    }

    const rest = target?.[propertyName];
    const restValues = propertyName === 'quaternion'
        ? [rest?.x, rest?.y, rest?.z, rest?.w]
        : [rest?.x, rest?.y, rest?.z];
    const equalsRest = restValues.length === valueSize
        && restValues.every((value, index) => Number.isFinite(Number(value))
            && valuesNearlyEqual(values[index], value, epsilon));
    return { valueSize, equalsRest };
}

function collapseTrackToSingleKey(track, valueSize) {
    if (typeof track?.clone !== 'function') return null;
    const clone = track.clone();
    const TimesConstructor = track.times?.constructor || Float32Array;
    const ValuesConstructor = track.values?.constructor || Float32Array;
    try {
        clone.times = new TimesConstructor([Number(track.times[0]) || 0]);
        clone.values = new ValuesConstructor(Array.from(track.values).slice(0, valueSize));
        return clone;
    } catch (_) {
        return null;
    }
}

function cloneClipWithTracks(clip, tracks) {
    if (typeof clip?.clone !== 'function') return null;
    const clone = clip.clone();
    clone.tracks = tracks;
    return clone;
}

function clonePositionTrackWithInPlaceXZ(track, epsilon = 1e-6) {
    const times = track?.times;
    const values = track?.values;
    const keyCount = Number(times?.length || 0);
    const valueSize = Number(track?.getValueSize?.() || (keyCount ? values?.length / keyCount : 0));
    if (
        typeof track?.clone !== 'function'
        || !keyCount
        || valueSize !== 3
        || Number(values?.length || 0) !== keyCount * valueSize
    ) return null;

    const firstX = Number(values[0]);
    const firstZ = Number(values[2]);
    if (!Number.isFinite(firstX) || !Number.isFinite(firstZ)) return null;

    let changed = false;
    for (let key = 0; key < keyCount; key += 1) {
        const offset = key * valueSize;
        const x = Number(values[offset]);
        const y = Number(values[offset + 1]);
        const z = Number(values[offset + 2]);
        if (![x, y, z].every(Number.isFinite)) return null;
        if (!valuesNearlyEqual(x, firstX, epsilon) || !valuesNearlyEqual(z, firstZ, epsilon)) {
            changed = true;
        }
    }
    if (!changed) return track;

    const clone = track.clone();
    try {
        const copiedValues = Array.isArray(values)
            ? [...values]
            : new (values.constructor || Float32Array)(values);
        for (let key = 0; key < keyCount; key += 1) {
            const offset = key * valueSize;
            copiedValues[offset] = firstX;
            copiedValues[offset + 2] = firstZ;
        }
        clone.values = copiedValues;
        return clone;
    } catch (_) {
        return null;
    }
}

export function makeRootMotionInPlace(clip, rootBoneNames, options = {}) {
    const names = rootBoneNames instanceof Set
        ? rootBoneNames
        : new Set(Array.isArray(rootBoneNames) ? rootBoneNames : []);
    const sourceTracks = Array.isArray(clip?.tracks) ? clip.tracks : [];
    if (!names.size || !sourceTracks.length) return clip;

    let changed = false;
    const tracks = sourceTracks.map((track) => {
        const binding = trackBinding(track, options.parseTrackName);
        if (binding.propertyName !== 'position' || !names.has(binding.targetName)) return track;
        const inPlaceTrack = clonePositionTrackWithInPlaceXZ(
            track,
            Math.max(0, Number(options.valueEpsilon) || 1e-6),
        );
        if (inPlaceTrack && inPlaceTrack !== track) changed = true;
        return inPlaceTrack || track;
    });

    if (!changed) return clip;
    return cloneClipWithTracks(clip, tracks) || clip;
}

export function optimizeAnimationClipsForViewer(clips, model, options = {}) {
    const sourceClips = Array.isArray(clips) ? clips : [];
    if (!sourceClips.length || !model?.traverse) {
        return { clips: sourceClips, reliable: false, reason: 'missing-input', droppedTracks: 0 };
    }

    try {
        const usage = collectSkinUsage(model, Math.max(0, Number(options.weightEpsilon) || 1e-6));
        if (!usage.reliable) {
            return { clips: sourceClips, reliable: false, reason: usage.reason, droppedTracks: 0 };
        }

        let droppedTracks = 0;
        let restTracksDropped = 0;
        let constantTracksCollapsed = 0;
        let rootMotionTracksLocked = 0;
        const requiredRootNames = new Set();
        usage.requiredBones.forEach((bone) => {
            if (bone?.parent?.isBone && usage.requiredBones.has(bone.parent)) return;
            const name = String(bone?.name || '');
            if (name && usage.namedObjects.get(name)?.size === 1) requiredRootNames.add(name);
        });
        const optimizedClips = sourceClips.map((clip) => {
            const inPlaceClip = options.inPlaceRootMotion === false
                ? clip
                : makeRootMotionInPlace(clip, requiredRootNames, options);
            const sourceTracks = Array.isArray(inPlaceClip?.tracks) ? inPlaceClip.tracks : [];
            if (!sourceTracks.length) return clip;
            if (inPlaceClip !== clip) {
                rootMotionTracksLocked += sourceTracks.reduce(
                    (count, track, index) => count + (track !== clip.tracks[index] ? 1 : 0),
                    0,
                );
            }
            const keptTracks = [];
            let clipChanged = inPlaceClip !== clip;
            sourceTracks.forEach((track) => {
                const binding = trackBinding(track, options.parseTrackName);
                if (!binding.targetName) {
                    keptTracks.push(track);
                    return;
                }
                const candidates = usage.namedObjects.get(binding.targetName);
                if (!candidates || candidates.size !== 1) {
                    keptTracks.push(track);
                    return;
                }
                const [target] = candidates;
                if (!target?.isBone || !usage.allBones.has(target)) {
                    keptTracks.push(track);
                    return;
                }
                if (!usage.requiredBones.has(target)) {
                    droppedTracks += 1;
                    clipChanged = true;
                    return;
                }
                const constant = constantTrackInfo(
                    track,
                    binding.propertyName,
                    target,
                    Math.max(0, Number(options.valueEpsilon) || 1e-6),
                );
                if (constant?.equalsRest) {
                    droppedTracks += 1;
                    restTracksDropped += 1;
                    clipChanged = true;
                    return;
                }
                if (constant && Number(track?.times?.length || 0) > 1) {
                    const collapsed = collapseTrackToSingleKey(track, constant.valueSize);
                    if (collapsed) {
                        keptTracks.push(collapsed);
                        constantTracksCollapsed += 1;
                        clipChanged = true;
                        return;
                    }
                }
                keptTracks.push(track);
            });
            if (!clipChanged) return clip;
            return cloneClipWithTracks(clip, keptTracks) || clip;
        });

        return {
            clips: optimizedClips,
            reliable: true,
            reason: (droppedTracks || constantTracksCollapsed) ? 'optimized' : 'nothing-to-drop',
            droppedTracks,
            restTracksDropped,
            constantTracksCollapsed,
            rootMotionTracksLocked,
            requiredBoneCount: usage.requiredBones.size,
            totalBoneCount: usage.allBones.size,
        };
    } catch (_) {
        return { clips: sourceClips, reliable: false, reason: 'optimizer-error', droppedTracks: 0 };
    }
}
