const SKELETON_SCHEMA = 'autorig-browser-fitting-skeleton.v1';
const REQUIRED_LIMB_LABELS = Object.freeze([
    'fore_left',
    'fore_right',
    'hind_left',
    'hind_right',
]);
const CHAIN_STEMS = Object.freeze([
    'c_thigh_b',
    'thigh_twist',
    'thigh_stretch',
    'leg_stretch',
    'leg_twist',
    'foot',
    'toes_01',
]);

export const HORSE_2_SEMANTIC_PROFILE = Object.freeze({
    profile_id: 'horse_2.semantic_limbs.v1',
    reference_resolution: Object.freeze([768, 448]),
    output_resolution: Object.freeze([512, 320]),
    palette_linear: Object.freeze({
        fore_left: Object.freeze([0.0, 0.85, 1.0]),
        fore_right: Object.freeze([0.12, 0.22, 1.0]),
        hind_left: Object.freeze([1.0, 0.72, 0.02]),
        hind_right: Object.freeze([1.0, 0.08, 0.55]),
    }),
    limb_groups: Object.freeze({
        fore_left: Object.freeze([
            'clavicle.l',
            'c_thigh_b_dupli_001.l',
            'thigh_twist_dupli_001.l',
            'thigh_stretch_dupli_001.l',
            'leg_stretch_dupli_001.l',
            'leg_twist_dupli_001.l',
            'foot_dupli_001.l',
            'toes_01_dupli_001.l',
        ]),
        fore_right: Object.freeze([
            'clavicle.r',
            'c_thigh_b_dupli_001.r',
            'thigh_twist_dupli_001.r',
            'thigh_stretch_dupli_001.r',
            'leg_stretch_dupli_001.r',
            'leg_twist_dupli_001.r',
            'foot_dupli_001.r',
            'toes_01_dupli_001.r',
        ]),
        hind_left: Object.freeze([
            'c_thigh_b.l',
            'thigh_twist.l',
            'thigh_stretch.l',
            'leg_stretch.l',
            'leg_twist.l',
            'foot.l',
            'toes_01.l',
        ]),
        hind_right: Object.freeze([
            'c_thigh_b.r',
            'thigh_twist.r',
            'thigh_stretch.r',
            'leg_stretch.r',
            'leg_twist.r',
            'foot.r',
            'toes_01.r',
        ]),
    }),
});

export const HORSE_2_JOINT_LIMITS = Object.freeze({
    c_thigh_b: Object.freeze([-1.4, 1.4]),
    thigh_twist: Object.freeze([-0.9, 0.9]),
    thigh_stretch: Object.freeze([-1.3, 1.3]),
    leg_stretch: Object.freeze([-1.65, 1.65]),
    leg_twist: Object.freeze([-1.25, 1.25]),
    foot: Object.freeze([-1.1, 1.1]),
});

function finite(value, field) {
    const number = Number(value);
    if (!Number.isFinite(number)) throw new Error(`${field} must be finite`);
    return number;
}

function resolution(value, field) {
    if (!Array.isArray(value) || value.length !== 2) {
        throw new Error(`${field} must be [width, height]`);
    }
    const width = finite(value[0], `${field}[0]`);
    const height = finite(value[1], `${field}[1]`);
    if (width <= 0 || height <= 0) throw new Error(`${field} dimensions must be positive`);
    return [width, height];
}

function array3(value, field) {
    if (!value || typeof value !== 'object') throw new Error(`${field} must be a vector`);
    const result = Array.isArray(value)
        ? value.slice(0, 3)
        : [value.x, value.y, value.z];
    if (result.length !== 3) throw new Error(`${field} must have three components`);
    return result.map((item, index) => finite(item, `${field}[${index}]`));
}

function array4(value, field) {
    if (!value || typeof value !== 'object') throw new Error(`${field} must be a quaternion`);
    const result = Array.isArray(value)
        ? value.slice(0, 4)
        : [value.x, value.y, value.z, value.w];
    if (result.length !== 4) throw new Error(`${field} must have four components`);
    const normalized = result.map((item, index) => finite(item, `${field}[${index}]`));
    const length = Math.hypot(...normalized);
    if (length <= 1e-12) throw new Error(`${field} has zero length`);
    return normalized.map((item) => item / length);
}

function distance3(a, b) {
    return Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z);
}

function distance2(a, b) {
    return Math.hypot(a[0] - b[0], a[1] - b[1]);
}

export function computeContainScaleAndPad(sourceValue, targetValue) {
    const source = resolution(sourceValue, 'source resolution');
    const target = resolution(targetValue, 'target resolution');
    const scale = Math.min(target[0] / source[0], target[1] / source[1]);
    const scaled = [source[0] * scale, source[1] * scale];
    return {
        source,
        target,
        scale,
        scaled,
        pad: [(target[0] - scaled[0]) / 2, (target[1] - scaled[1]) / 2],
    };
}

export function computeLongDimensionScaleAndPad(referenceValue, outputValue) {
    const reference = resolution(referenceValue, 'reference resolution');
    const output = resolution(outputValue, 'output resolution');
    const scale = Math.max(...output) / Math.max(...reference);
    const scaled = [reference[0] * scale, reference[1] * scale];
    const pad = [(output[0] - scaled[0]) / 2, (output[1] - scaled[1]) / 2];
    if (pad.some((item) => item < -1e-7)) {
        throw new Error('long-dimension scaling would crop instead of center-pad the reference');
    }
    return { reference, output, scale, scaled, pad };
}

/** Compose task.html viewer contain-capture with Comfy long-dimension scaling. */
export function createViewerToLtxProjection(options = {}) {
    const sourceViewport = resolution(options.sourceViewport || options.referenceResolution || [768, 448], 'sourceViewport');
    const referenceResolution = resolution(options.referenceResolution || [768, 448], 'referenceResolution');
    const outputResolution = resolution(options.outputResolution || [512, 320], 'outputResolution');
    const capture = computeContainScaleAndPad(sourceViewport, referenceResolution);
    const ltx = computeLongDimensionScaleAndPad(referenceResolution, outputResolution);
    const sourceToOutputScale = capture.scale * ltx.scale;
    return {
        sourceViewport,
        referenceResolution,
        outputResolution,
        capture,
        ltx,
        sourceToOutputScale,
        ndcToOutput(ndcValue) {
            const ndc = array3(ndcValue, 'ndc');
            const sourcePixel = [
                ((ndc[0] + 1) / 2) * sourceViewport[0],
                ((1 - ndc[1]) / 2) * sourceViewport[1],
            ];
            const referencePixel = [
                sourcePixel[0] * capture.scale + capture.pad[0],
                sourcePixel[1] * capture.scale + capture.pad[1],
            ];
            return [
                referencePixel[0] * ltx.scale + ltx.pad[0],
                referencePixel[1] * ltx.scale + ltx.pad[1],
            ];
        },
        outputPixelToNdcDelta() {
            return [
                2 / (sourceViewport[0] * sourceToOutputScale),
                -2 / (sourceViewport[1] * sourceToOutputScale),
            ];
        },
    };
}

function semanticProfile(value) {
    return value?.semantic_profile || value || HORSE_2_SEMANTIC_PROFILE;
}

function expectedChainNames(label) {
    const isFore = label.startsWith('fore_');
    const side = label.endsWith('_left') ? '.l' : '.r';
    return CHAIN_STEMS.map((stem) => `${stem}${isFore ? '_dupli_001' : ''}${side}`);
}

export function horseDeformChainNames(profileValue, label) {
    if (!REQUIRED_LIMB_LABELS.includes(label)) throw new Error(`unsupported Horse limb label: ${label}`);
    const profile = semanticProfile(profileValue);
    const groups = profile?.limb_groups || profile?.limbGroups;
    const group = groups?.[label];
    if (!Array.isArray(group)) throw new Error(`Horse semantic profile is missing limb group ${label}`);
    const expected = expectedChainNames(label);
    const start = group.indexOf(expected[0]);
    if (start < 0) throw new Error(`${label} does not start a c_thigh_b deform chain`);
    const actual = group.slice(start, start + expected.length);
    if (actual.length !== expected.length || actual.some((name, index) => name !== expected[index])) {
        throw new Error(`${label} deform chain order does not match Horse_2`);
    }
    return [...actual];
}

function traverse(root, callback) {
    if (typeof root?.traverse === 'function') {
        root.traverse(callback);
        return;
    }
    const visit = (node) => {
        if (!node) return;
        callback(node);
        (node.children || []).forEach(visit);
    };
    visit(root);
}

function requiredBones(model, chains) {
    const requiredNames = new Set(Object.values(chains).flat());
    const matches = new Map([...requiredNames].map((name) => [name, []]));
    traverse(model, (node) => {
        if (!(node?.isBone === true || node?.type === 'Bone')) return;
        if (matches.has(node.name)) matches.get(node.name).push(node);
    });
    const result = new Map();
    matches.forEach((items, name) => {
        if (!items.length) throw new Error(`Horse_2 deform bone is missing: ${name}`);
        if (items.length > 1) throw new Error(`Horse_2 deform bone is duplicated: ${name}`);
        if (items[0].userData?.use_deform === false || items[0].userData?.useDeform === false) {
            throw new Error(`Horse_2 chain bone is marked non-deform: ${name}`);
        }
        result.set(name, items[0]);
    });
    return result;
}

function topBoneRoot(bone) {
    let root = bone;
    while (root.parent?.isBone === true || root.parent?.type === 'Bone') root = root.parent;
    return root;
}

function assertSharedBoneRoot(bones) {
    const roots = new Set([...bones.values()].map(topBoneRoot));
    if (roots.size !== 1) throw new Error('Horse_2 deform chains are disconnected across Bone roots');
    return [...roots][0];
}

function worldHead(THREE, bone) {
    if (typeof THREE?.Vector3 !== 'function' || typeof bone?.getWorldPosition !== 'function') {
        throw new Error('THREE.Vector3 and Bone.getWorldPosition are required');
    }
    const result = bone.getWorldPosition(new THREE.Vector3());
    array3(result, `${bone.name} world head`);
    return result;
}

function declaredTailWorld(THREE, bone) {
    const raw = bone?.userData?.tailWorld || bone?.userData?.tail_world || null;
    if (!raw) return null;
    const values = array3(raw, `${bone.name} declared tailWorld`);
    return new THREE.Vector3(values[0], values[1], values[2]);
}

function projectWorldPoint(world, camera, projection, options = {}) {
    const ndc = world.clone().project(camera);
    const values = array3(ndc, 'projected bone point');
    if (options.requireVisible !== false && (
        values[0] < -1.0001 || values[0] > 1.0001
        || values[1] < -1.0001 || values[1] > 1.0001
        || values[2] < -1.0001 || values[2] > 1.0001
    )) {
        throw new Error('Horse_2 rest chain projects outside the canonical camera frustum');
    }
    return { ndc, pixel: projection.ndcToOutput(values) };
}

function invertQuaternion(quaternion) {
    if (typeof quaternion.invert === 'function') return quaternion.invert();
    if (typeof quaternion.inverse === 'function') return quaternion.inverse();
    throw new Error('THREE.Quaternion invert() is required');
}

function localCameraPlaneAxis(THREE, bone, camera) {
    if (typeof THREE?.Quaternion !== 'function' || typeof camera?.getWorldDirection !== 'function' || typeof bone?.getWorldQuaternion !== 'function') {
        throw new Error('THREE quaternion and world-direction APIs are required');
    }
    const axis = camera.getWorldDirection(new THREE.Vector3()).normalize();
    const inverseWorld = invertQuaternion(bone.getWorldQuaternion(new THREE.Quaternion()).clone());
    axis.applyQuaternion(inverseWorld).normalize();
    const result = array3(axis, `${bone.name} camera-plane rotation axis`);
    if (Math.hypot(...result) <= 1e-9) throw new Error(`${bone.name} camera-plane rotation axis is zero`);
    return result;
}

function parentLocalVector(THREE, bone, worldStart, worldEnd, field) {
    const parent = bone.parent;
    if (!parent || typeof parent.worldToLocal !== 'function') {
        throw new Error(`${bone.name} parent.worldToLocal is required for ${field}`);
    }
    const localStart = parent.worldToLocal(worldStart.clone());
    const localEnd = parent.worldToLocal(worldEnd.clone());
    const result = [
        localEnd.x - localStart.x,
        localEnd.y - localStart.y,
        localEnd.z - localStart.z,
    ].map((item, index) => finite(item, `${bone.name}.${field}[${index}]`));
    if (Math.hypot(...result) <= 1e-12) throw new Error(`${bone.name}.${field} is zero`);
    return result;
}

function positionMappingForBone(THREE, bone, camera, projection, projectedHead, motionScale) {
    if (typeof projectedHead.ndc?.clone !== 'function' || typeof projectedHead.ndc?.unproject !== 'function') {
        throw new Error('THREE.Vector3.unproject is required for position mapping');
    }
    const [deltaX, deltaY] = projection.outputPixelToNdcDelta();
    const xWorld = projectedHead.ndc.clone();
    xWorld.x += deltaX;
    xWorld.unproject(camera);
    const yWorld = projectedHead.ndc.clone();
    yWorld.y += deltaY;
    yWorld.unproject(camera);
    return {
        restPosition: array3(bone.position, `${bone.name}.position`),
        xAxisPerPixel: parentLocalVector(THREE, bone, projectedHead.world, xWorld, 'xAxisPerPixel'),
        yAxisPerPixel: parentLocalVector(THREE, bone, projectedHead.world, yWorld, 'yAxisPerPixel'),
        motionScale,
    };
}

function positionMappingPolicy(selection) {
    if (selection === false || selection === 'disabled' || selection === 'none') return 'disabled';
    if (selection == null || selection === 'auto') return 'auto_chain_roots_and_parent_breaks';
    if (selection === true || selection === 'all') return 'parent_local_per_bone';
    if (typeof selection === 'function' || Array.isArray(selection) || selection instanceof Set) {
        return 'explicit_selector';
    }
    throw new Error(`unsupported includePositionMappings policy: ${String(selection)}`);
}

function includePositionMapping(options, bone, label, index, previousBone) {
    const selection = options.includePositionMappings;
    const policy = positionMappingPolicy(selection);
    if (policy === 'disabled') return false;
    if (policy === 'auto_chain_roots_and_parent_breaks') {
        return index === 0 || !previousBone || bone.parent !== previousBone;
    }
    if (selection === true || selection === 'all') return true;
    if (typeof selection === 'function') return Boolean(selection({ bone, label, index }));
    if (Array.isArray(selection)) return selection.includes(bone.name);
    if (selection instanceof Set) return selection.has(bone.name);
    return false;
}

function defaultLimitStem(boneName) {
    return CHAIN_STEMS.find((stem) => boneName.startsWith(stem)) || '';
}

function limitValue(raw, field) {
    if (Array.isArray(raw)) {
        if (raw.length !== 2) throw new Error(`${field} must contain min and max`);
        return [finite(raw[0], `${field}[0]`), finite(raw[1], `${field}[1]`)];
    }
    if (!raw || typeof raw !== 'object') throw new Error(`${field} must be a limit object`);
    if ('minDegrees' in raw || 'maxDegrees' in raw || 'min_degrees' in raw || 'max_degrees' in raw) {
        const minimum = finite(raw.minDegrees ?? raw.min_degrees, `${field}.minDegrees`) * Math.PI / 180;
        const maximum = finite(raw.maxDegrees ?? raw.max_degrees, `${field}.maxDegrees`) * Math.PI / 180;
        return [minimum, maximum];
    }
    return [
        finite(raw.minAngle ?? raw.min_angle_rad ?? raw.minimum, `${field}.minAngle`),
        finite(raw.maxAngle ?? raw.max_angle_rad ?? raw.maximum, `${field}.maxAngle`),
    ];
}

function jointLimits(profile, options, boneName) {
    const stem = defaultLimitStem(boneName);
    const optionLimits = options.jointLimits || {};
    const profileLimits = profile.joint_limits || profile.jointLimits || {};
    const raw = optionLimits[boneName]
        || optionLimits[stem]
        || profileLimits[boneName]
        || profileLimits[stem]
        || HORSE_2_JOINT_LIMITS[stem]
        || [-Math.PI, Math.PI];
    const [minimum, maximum] = limitValue(raw, `joint limits for ${boneName}`);
    if (minimum > maximum) throw new Error(`joint limits are reversed for ${boneName}`);
    return [minimum, maximum];
}

function projectionOptions(options, profile) {
    return createViewerToLtxProjection({
        sourceViewport: options.sourceViewport || profile.source_viewport || profile.sourceViewport,
        referenceResolution: options.referenceResolution
            || profile.reference_resolution
            || profile.referenceResolution
            || [768, 448],
        outputResolution: options.outputResolution
            || profile.output_resolution
            || profile.outputResolution
            || [512, 320],
    });
}

/**
 * Build the pure browser fitting skeleton from the current actionless Three.js
 * rest pose and the same Perspective camera used by canonical LTX capture.
 */
export function buildHorse2BrowserFittingSkeleton(options = {}) {
    const { THREE, model, camera } = options;
    if (!THREE || !model || !camera) throw new Error('THREE, model and camera are required');
    const profile = semanticProfile(options.semanticProfile || HORSE_2_SEMANTIC_PROFILE);
    const chains = Object.fromEntries(REQUIRED_LIMB_LABELS.map((label) => [
        label,
        horseDeformChainNames(profile, label),
    ]));
    model.updateWorldMatrix?.(true, true);
    camera.updateProjectionMatrix?.();
    camera.updateWorldMatrix?.(true, false);
    const bones = requiredBones(model, chains);
    const boneRoot = assertSharedBoneRoot(bones);
    const projection = projectionOptions(options, profile);
    const worldByBone = new Map();
    const projectedByBone = new Map();
    bones.forEach((bone, name) => {
        const world = worldHead(THREE, bone);
        const projected = projectWorldPoint(world, camera, projection, options);
        worldByBone.set(name, world);
        projectedByBone.set(name, { ...projected, world });
    });

    const minimumWorldLength = finite(options.minimumWorldSegmentLength ?? 1e-7, 'minimumWorldSegmentLength');
    const minimumPixelLength = finite(options.minimumProjectedSegmentLengthPx ?? 0.25, 'minimumProjectedSegmentLengthPx');
    const tailTolerance = finite(options.connectionToleranceWorld ?? 1e-4, 'connectionToleranceWorld');
    const motionScale = finite(options.positionMotionScale ?? profile.position_motion_scale ?? 1, 'positionMotionScale');
    const limbs = {};
    REQUIRED_LIMB_LABELS.forEach((label) => {
        const names = chains[label];
        const joints = [];
        // Seven ordered bone heads define six non-fabricated deform segments;
        // toes_01 is the terminal hoof target because GLTF does not encode tail length.
        for (let index = 0; index < names.length - 1; index += 1) {
            const name = names[index];
            const nextName = names[index + 1];
            const bone = bones.get(name);
            const worldStart = worldByBone.get(name);
            const worldEnd = worldByBone.get(nextName);
            const restStart = projectedByBone.get(name).pixel;
            const restEnd = projectedByBone.get(nextName).pixel;
            if (distance3(worldStart, worldEnd) <= minimumWorldLength) {
                throw new Error(`${label} rest chain is disconnected or zero-length at ${name}`);
            }
            if (distance2(restStart, restEnd) <= minimumPixelLength) {
                throw new Error(`${label} projected rest chain is disconnected at ${name}`);
            }
            const declaredTail = declaredTailWorld(THREE, bone);
            if (declaredTail && distance3(declaredTail, worldEnd) > tailTolerance) {
                throw new Error(`${label} declared tail does not connect ${name} to ${nextName}`);
            }
            const [minAngle, maxAngle] = jointLimits(profile, options, name);
            const joint = {
                bone: name,
                restStart: [...restStart],
                restEnd: [...restEnd],
                restQuaternion: array4(bone.quaternion, `${name}.quaternion`),
                rotationAxis: localCameraPlaneAxis(THREE, bone, camera),
                minAngle,
                maxAngle,
            };
            const previousBone = index > 0 ? bones.get(names[index - 1]) : null;
            if (includePositionMapping(options, bone, label, index, previousBone)) {
                joint.positionMapping = positionMappingForBone(
                    THREE,
                    bone,
                    camera,
                    projection,
                    projectedByBone.get(name),
                    motionScale,
                );
            }
            joints.push(joint);
        }
        limbs[label] = {
            joints,
            proximalTrack: `${label}.proximal`,
            jointTrack: `${label}.joint`,
            hoofTrack: `${label}.hoof`,
            trackedJointIndex: Math.max(1, Math.floor(joints.length / 2)),
            sourceBoneChain: [...names],
            terminalBone: names.at(-1),
        };
    });

    let root = null;
    const rootBoneName = String(options.rootBoneName || profile.root_bone || profile.rootBone || '').trim();
    if (rootBoneName) {
        const matches = [];
        traverse(model, (node) => {
            if ((node?.isBone === true || node?.type === 'Bone') && node.name === rootBoneName) matches.push(node);
        });
        if (matches.length !== 1) throw new Error(`root bone ${rootBoneName} must exist exactly once`);
        if (topBoneRoot(matches[0]) !== boneRoot) throw new Error(`root bone ${rootBoneName} is disconnected from Horse_2 chains`);
        const world = worldHead(THREE, matches[0]);
        const projected = projectWorldPoint(world, camera, projection, options);
        const mapping = positionMappingForBone(
            THREE,
            matches[0],
            camera,
            projection,
            { ...projected, world },
            finite(options.rootMotionScale ?? 1, 'rootMotionScale'),
        );
        root = {
            bone: rootBoneName,
            restPosition: mapping.restPosition,
            xAxisPerPixel: mapping.xAxisPerPixel,
            yAxisPerPixel: mapping.yAxisPerPixel,
            motionScale: mapping.motionScale,
        };
    }

    return {
        schema: SKELETON_SCHEMA,
        rigType: 'HORSE_2',
        limbs,
        ...(root ? { root } : {}),
        projection: {
            sourceViewport: [...projection.sourceViewport],
            referenceResolution: [...projection.referenceResolution],
            outputResolution: [...projection.outputResolution],
            viewerContainScale: projection.capture.scale,
            viewerContainPad: [...projection.capture.pad],
            ltxLongDimensionScale: projection.ltx.scale,
            ltxCenterPad: [...projection.ltx.pad],
            sourceToOutputScale: projection.sourceToOutputScale,
        },
        provenance: {
            source: 'three_actionless_rest_pose',
            semanticProfileId: String(profile.profile_id || profile.profileId || 'horse_2.semantic_limbs.v1'),
            terminalPolicy: 'seven_bone_heads_six_segments_to_toes_head',
            sharedBoneRoot: String(boneRoot.name || ''),
            positionMappings: positionMappingPolicy(options.includePositionMappings),
        },
    };
}

export const THREE_FITTING_ADAPTER_SCHEMA = SKELETON_SCHEMA;
