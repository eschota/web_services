const SKELETON_SCHEMA = 'autorig-browser-fitting-skeleton.v1';
const FITTED_SCHEMA = 'autorig-browser-fitted-animation.v1';
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

function referenceGeometryTransform(value, referenceResolution, outputResolution) {
    if (value == null) return null;
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error('geometryTransform must be an object');
    }
    if (value.mode !== 'center_crop_cover') {
        throw new Error('geometryTransform.mode must be center_crop_cover');
    }
    if (value.coordinate_transform !== 'half_pixel_centers') {
        throw new Error('geometryTransform.coordinate_transform must be half_pixel_centers');
    }
    const source = resolution(value.source_resolution, 'geometryTransform.source_resolution');
    const target = resolution(value.target_resolution, 'geometryTransform.target_resolution');
    if (source.some((item, index) => Math.abs(item - referenceResolution[index]) > 1e-7)) {
        throw new Error('geometryTransform.source_resolution does not match referenceResolution');
    }
    if (target.some((item, index) => Math.abs(item - outputResolution[index]) > 1e-7)) {
        throw new Error('geometryTransform.target_resolution does not match outputResolution');
    }
    const cropValue = value.crop_pixels;
    if (!cropValue || typeof cropValue !== 'object' || Array.isArray(cropValue)) {
        throw new Error('geometryTransform.crop_pixels must be an object');
    }
    const crop = {
        x: finite(cropValue.x, 'geometryTransform.crop_pixels.x'),
        y: finite(cropValue.y, 'geometryTransform.crop_pixels.y'),
        width: finite(cropValue.width, 'geometryTransform.crop_pixels.width'),
        height: finite(cropValue.height, 'geometryTransform.crop_pixels.height'),
    };
    if (crop.x < 0 || crop.y < 0 || crop.width <= 0 || crop.height <= 0
        || crop.x + crop.width > source[0] + 1e-7
        || crop.y + crop.height > source[1] + 1e-7) {
        throw new Error('geometryTransform.crop_pixels is outside source_resolution');
    }
    const scale = resolution(value.scale_xy, 'geometryTransform.scale_xy');
    const expectedScale = [target[0] / crop.width, target[1] / crop.height];
    if (scale.some((item, index) => Math.abs(item - expectedScale[index]) > 1e-6)) {
        throw new Error('geometryTransform.scale_xy does not match crop and target resolutions');
    }
    if (value.rgb_interpolation !== 'opencv_bilinear') {
        throw new Error('geometryTransform.rgb_interpolation must be opencv_bilinear');
    }
    if (value.mask_interpolation !== 'opencv_nearest') {
        throw new Error('geometryTransform.mask_interpolation must be opencv_nearest');
    }
    return {
        mode: value.mode,
        coordinate_transform: value.coordinate_transform,
        source_resolution: [...source],
        target_resolution: [...target],
        crop_pixels: crop,
        scale_xy: [...scale],
        rgb_interpolation: value.rgb_interpolation,
        mask_interpolation: value.mask_interpolation,
    };
}

/** Compose task.html viewer contain-capture with Comfy long-dimension scaling. */
export function createViewerToLtxProjection(options = {}) {
    const sourceViewport = resolution(options.sourceViewport || options.referenceResolution || [768, 448], 'sourceViewport');
    const referenceResolution = resolution(options.referenceResolution || [768, 448], 'referenceResolution');
    const outputResolution = resolution(options.outputResolution || [512, 320], 'outputResolution');
    const capture = computeContainScaleAndPad(sourceViewport, referenceResolution);
    const ltx = computeLongDimensionScaleAndPad(referenceResolution, outputResolution);
    const geometryTransform = referenceGeometryTransform(
        options.geometryTransform || options.referenceGeometryTransform || null,
        referenceResolution,
        outputResolution,
    );
    const referenceToOutputScale = geometryTransform
        ? [...geometryTransform.scale_xy]
        : [ltx.scale, ltx.scale];
    const referenceToOutputOffset = geometryTransform
        ? [
            (0.5 - geometryTransform.crop_pixels.x) * referenceToOutputScale[0] - 0.5,
            (0.5 - geometryTransform.crop_pixels.y) * referenceToOutputScale[1] - 0.5,
        ]
        : [...ltx.pad];
    const sourceToOutputScaleXY = referenceToOutputScale.map((scale) => capture.scale * scale);
    const sourceToOutputScale = sourceToOutputScaleXY[0];
    const referencePixelToOutput = (referencePixel) => [
        referencePixel[0] * referenceToOutputScale[0] + referenceToOutputOffset[0],
        referencePixel[1] * referenceToOutputScale[1] + referenceToOutputOffset[1],
    ];
    const outputPixelToSource = (outputPixel) => {
        const referencePixel = [
            (outputPixel[0] - referenceToOutputOffset[0]) / referenceToOutputScale[0],
            (outputPixel[1] - referenceToOutputOffset[1]) / referenceToOutputScale[1],
        ];
        return [
            (referencePixel[0] - capture.pad[0]) / capture.scale,
            (referencePixel[1] - capture.pad[1]) / capture.scale,
        ];
    };
    return {
        sourceViewport,
        referenceResolution,
        outputResolution,
        capture,
        ltx,
        geometryTransform,
        projectionMode: geometryTransform ? 'pinned_reference_geometry_transform' : 'legacy_ltx_long_dimension_pad',
        referenceToOutputScale,
        referenceToOutputOffset,
        sourceToOutputScale,
        sourceToOutputScaleXY,
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
            return referencePixelToOutput(referencePixel);
        },
        outputPixelToNdc(pixelValue, z = 0) {
            if (!Array.isArray(pixelValue) || pixelValue.length !== 2) {
                throw new Error('output pixel must be [x, y]');
            }
            const pixel = pixelValue.map((item, index) => finite(item, `output pixel[${index}]`));
            const sourcePixel = outputPixelToSource(pixel);
            return [
                2 * sourcePixel[0] / sourceViewport[0] - 1,
                1 - 2 * sourcePixel[1] / sourceViewport[1],
                finite(z, 'output pixel ndc z'),
            ];
        },
        outputPixelToNdcDelta() {
            return [
                2 / (sourceViewport[0] * sourceToOutputScaleXY[0]),
                -2 / (sourceViewport[1] * sourceToOutputScaleXY[1]),
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

function namedBones(model, names) {
    const wanted = new Set(names);
    const matches = new Map([...wanted].map((name) => [name, []]));
    traverse(model, (node) => {
        if (!(node?.isBone === true || node?.type === 'Bone') || !matches.has(node.name)) return;
        matches.get(node.name).push(node);
    });
    const result = new Map();
    matches.forEach((items, name) => {
        if (items.length !== 1) throw new Error(`fitted hierarchy bone ${name} must exist exactly once`);
        result.set(name, items[0]);
    });
    return result;
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
        geometryTransform: options.geometryTransform || options.referenceGeometryTransform || null,
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
            projectionMode: projection.projectionMode,
            viewerContainScale: projection.capture.scale,
            viewerContainPad: [...projection.capture.pad],
            ltxLongDimensionScale: projection.ltx.scale,
            ltxCenterPad: [...projection.ltx.pad],
            sourceToOutputScale: projection.sourceToOutputScale,
            sourceToOutputScaleXY: [...projection.sourceToOutputScaleXY],
            ...(projection.geometryTransform ? {
                geometryTransform: {
                    ...projection.geometryTransform,
                    source_resolution: [...projection.geometryTransform.source_resolution],
                    target_resolution: [...projection.geometryTransform.target_resolution],
                    crop_pixels: { ...projection.geometryTransform.crop_pixels },
                    scale_xy: [...projection.geometryTransform.scale_xy],
                },
            } : {}),
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

function multiplyQuaternion(left, right) {
    if (typeof left?.multiply !== 'function') throw new Error('THREE.Quaternion.multiply() is required');
    return left.multiply(right);
}

function setQuaternionFromUnitVectors(quaternion, from, to) {
    if (typeof quaternion?.setFromUnitVectors !== 'function') {
        throw new Error('THREE.Quaternion.setFromUnitVectors() is required');
    }
    return quaternion.setFromUnitVectors(from, to);
}

function assignVector(target, source, field) {
    if (typeof target?.copy !== 'function') throw new Error(`${field}.copy() is required`);
    target.copy(source);
}

function assignQuaternion(target, source, field) {
    if (typeof target?.copy !== 'function') throw new Error(`${field}.copy() is required`);
    target.copy(source);
}

function objectDepth(object) {
    let depth = 0;
    let cursor = object?.parent;
    while (cursor) {
        depth += 1;
        cursor = cursor.parent;
    }
    return depth;
}

function outputPixelToNdc(pixel, projection, z = 0) {
    return projection.outputPixelToNdc([
        finite(pixel[0], 'fitted pixel x'),
        finite(pixel[1], 'fitted pixel y'),
    ], z);
}

function unprojectPixel(THREE, camera, pixel, projection, z) {
    if (typeof THREE?.Vector3 !== 'function') throw new Error('THREE.Vector3 is required');
    const ndc = outputPixelToNdc(pixel, projection, z);
    const value = new THREE.Vector3(ndc[0], ndc[1], ndc[2]);
    if (typeof value.unproject !== 'function') throw new Error('THREE.Vector3.unproject() is required');
    return value.unproject(camera);
}

function projectedPixel(world, camera, projection) {
    if (typeof world?.clone !== 'function') throw new Error('world point clone() is required');
    const ndc = world.clone().project(camera);
    return projection.ndcToOutput([ndc.x, ndc.y, ndc.z]);
}

function raySpherePoint(THREE, camera, pixel, projection, center, radius, preferredDirection) {
    const near = unprojectPixel(THREE, camera, pixel, projection, -1);
    const far = unprojectPixel(THREE, camera, pixel, projection, 1);
    const direction = far.clone().sub(near).normalize();
    const offset = near.clone().sub(center);
    const b = offset.dot(direction);
    const discriminant = b * b - (offset.dot(offset) - radius * radius);
    if (discriminant >= 0) {
        const root = Math.sqrt(discriminant);
        const candidates = [-b - root, -b + root]
            .filter((distance) => Number.isFinite(distance) && distance >= 0)
            .map((distance) => near.clone().add(direction.clone().multiplyScalar(distance)));
        if (candidates.length) {
            candidates.sort((left, right) => (
                right.clone().sub(center).normalize().dot(preferredDirection)
                - left.clone().sub(center).normalize().dot(preferredDirection)
            ));
            return { point: candidates[0], usedFallback: false };
        }
    }

    // A clamped 2D solve can make a pixel ray geometrically unreachable by a
    // fixed 3D segment. Keep the chain continuous and preserve its exact world
    // length while retaining the requested screen-space direction.
    const centerNdc = center.clone().project(camera);
    const planeTarget = unprojectPixel(THREE, camera, pixel, projection, centerNdc.z);
    const fallbackDirection = planeTarget.sub(center);
    if (fallbackDirection.lengthSq() <= 1e-18) fallbackDirection.copy(preferredDirection);
    return {
        point: center.clone().add(fallbackDirection.normalize().multiplyScalar(radius)),
        usedFallback: true,
    };
}

function normalizedFittedFrames(fitted, skeleton) {
    if (!fitted || fitted.schema !== FITTED_SCHEMA) throw new Error(`fitted.schema must be ${FITTED_SCHEMA}`);
    if (!Number.isInteger(fitted.frameCount) || fitted.frameCount < 2) throw new Error('fitted.frameCount must be at least 2');
    if (!Array.isArray(fitted.frames) || fitted.frames.length !== fitted.frameCount) {
        throw new Error('fitted.frames must contain every fitted frame');
    }
    const labels = Object.keys(skeleton?.limbs || {});
    if (!labels.length) throw new Error('skeleton.limbs must not be empty');
    fitted.frames.forEach((frame, frameIndex) => labels.forEach((label) => {
        const expected = skeleton.limbs[label].sourceBoneChain?.length;
        const points = frame?.limbs?.[label]?.points;
        if (!Number.isInteger(expected) || expected < 2) {
            throw new Error(`skeleton limb ${label} is missing sourceBoneChain`);
        }
        if (!Array.isArray(points) || points.length !== expected) {
            throw new Error(`fitted frame ${frameIndex} limb ${label} must contain ${expected} points`);
        }
        points.forEach((point, pointIndex) => {
            if (!Array.isArray(point) || point.length !== 2 || !point.every(Number.isFinite)) {
                throw new Error(`fitted frame ${frameIndex} limb ${label} point ${pointIndex} is invalid`);
            }
        });
    }));
    return labels;
}

/**
 * Bake the pure 2D browser solve through the real Three.js hierarchy.
 *
 * Horse_2 deform names form an anatomical chain, but exported helper/control
 * parents can interrupt the Object3D parent chain. Directly assigning local
 * quaternion tracks therefore moves logical children independently and breaks
 * their 3D segment lengths. This bake reconstructs each fitted limb as one
 * continuous world-space chain, then resolves every bone transform against its
 * actual animated parent. The result remains a regular browser AnimationClip;
 * Blender is not involved.
 */
export function bakeFittedAnimationToThreeHierarchyClip(options = {}) {
    const { THREE, model, camera, skeleton, fitted } = options;
    if (!THREE || !model || !camera || !skeleton || !fitted) {
        throw new Error('THREE, model, camera, skeleton and fitted are required');
    }
    if (skeleton.schema !== SKELETON_SCHEMA) throw new Error(`skeleton.schema must be ${SKELETON_SCHEMA}`);
    if (!THREE.AnimationClip || !THREE.QuaternionKeyframeTrack || !THREE.VectorKeyframeTrack) {
        throw new Error('THREE animation clip and keyframe track constructors are required');
    }
    const labels = normalizedFittedFrames(fitted, skeleton);
    const outputResolution = resolution(
        options.outputResolution || skeleton.projection?.outputResolution,
        'outputResolution',
    );
    const projection = createViewerToLtxProjection({
        sourceViewport: skeleton.projection?.sourceViewport,
        referenceResolution: skeleton.projection?.referenceResolution,
        outputResolution,
        geometryTransform: skeleton.projection?.geometryTransform || null,
    });
    model.updateWorldMatrix?.(true, true);
    camera.updateProjectionMatrix?.();
    camera.updateWorldMatrix?.(true, false);

    const chainNames = labels.flatMap((label) => skeleton.limbs[label].sourceBoneChain);
    const bones = namedBones(model, chainNames);
    const uniqueBones = [...new Set(chainNames)].map((name) => bones.get(name));
    const snapshots = new Map(uniqueBones.map((bone) => [bone, {
        position: bone.position.clone(),
        quaternion: bone.quaternion.clone(),
        scale: bone.scale?.clone?.() || null,
    }]));
    const rest = new Map(uniqueBones.map((bone) => [bone.name, {
        head: worldHead(THREE, bone),
        quaternion: bone.getWorldQuaternion(new THREE.Quaternion()).clone(),
    }]));
    const perLimb = new Map(labels.map((label) => {
        const names = skeleton.limbs[label].sourceBoneChain;
        const segments = names.slice(0, -1).map((name, index) => {
            const start = rest.get(name).head;
            const end = rest.get(names[index + 1]).head;
            const vector = end.clone().sub(start);
            return { length: vector.length(), direction: vector.normalize() };
        });
        return [label, { names, segments }];
    }));
    const times = Array.from({ length: fitted.frameCount }, (_, frame) => frame / finite(fitted.fps, 'fitted.fps'));
    const quaternionValues = new Map(uniqueBones.map((bone) => [bone.name, []]));
    const positionValues = new Map(uniqueBones.map((bone) => [bone.name, []]));
    let maximumSegmentLengthDriftWorld = 0;
    let maximumHierarchyBakeReprojectionErrorPx = 0;
    let maximumRequestedFittedPointErrorPx = 0;
    let unreachablePixelRays = 0;

    const restore = () => {
        snapshots.forEach((snapshot, bone) => {
            assignVector(bone.position, snapshot.position, `${bone.name}.position`);
            assignQuaternion(bone.quaternion, snapshot.quaternion, `${bone.name}.quaternion`);
            if (snapshot.scale && bone.scale) assignVector(bone.scale, snapshot.scale, `${bone.name}.scale`);
        });
        model.updateWorldMatrix?.(true, true);
    };

    try {
        fitted.frames.forEach((frame) => {
            restore();
            const desired = new Map();
            labels.forEach((label) => {
                const { names, segments } = perLimb.get(label);
                const pixels = frame.limbs[label].points;
                const restRoot = rest.get(names[0]).head;
                const rootNdcZ = restRoot.clone().project(camera).z;
                const points = [unprojectPixel(THREE, camera, pixels[0], projection, rootNdcZ)];
                segments.forEach((segment, index) => {
                    const result = raySpherePoint(
                        THREE,
                        camera,
                        pixels[index + 1],
                        projection,
                        points[index],
                        segment.length,
                        segment.direction,
                    );
                    if (result.usedFallback) unreachablePixelRays += 1;
                    points.push(result.point);
                });
                names.forEach((name, index) => {
                    const restQuaternion = rest.get(name).quaternion;
                    const segmentIndex = Math.min(index, segments.length - 1);
                    const desiredDirection = points[segmentIndex + 1].clone().sub(points[segmentIndex]).normalize();
                    const delta = setQuaternionFromUnitVectors(
                        new THREE.Quaternion(),
                        segments[segmentIndex].direction,
                        desiredDirection,
                    );
                    desired.set(name, {
                        head: points[index],
                        pixel: pixels[index],
                        quaternion: multiplyQuaternion(delta, restQuaternion.clone()).normalize(),
                    });
                });
            });

            uniqueBones.sort((left, right) => objectDepth(left) - objectDepth(right)).forEach((bone) => {
                model.updateWorldMatrix?.(true, true);
                const target = desired.get(bone.name);
                if (!target) throw new Error(`missing hierarchy target for ${bone.name}`);
                if (!bone.parent || typeof bone.parent.worldToLocal !== 'function') {
                    throw new Error(`${bone.name} parent.worldToLocal() is required`);
                }
                const localPosition = bone.parent.worldToLocal(target.head.clone());
                const inverseParent = invertQuaternion(
                    bone.parent.getWorldQuaternion(new THREE.Quaternion()).clone(),
                );
                const localQuaternion = multiplyQuaternion(inverseParent, target.quaternion.clone()).normalize();
                assignVector(bone.position, localPosition, `${bone.name}.position`);
                assignQuaternion(bone.quaternion, localQuaternion, `${bone.name}.quaternion`);
            });
            model.updateWorldMatrix?.(true, true);

            uniqueBones.forEach((bone) => {
                const quaternion = array4(bone.quaternion, `${bone.name}.bakedQuaternion`);
                const position = array3(bone.position, `${bone.name}.bakedPosition`);
                quaternionValues.get(bone.name).push(...quaternion);
                positionValues.get(bone.name).push(...position);
                const target = desired.get(bone.name);
                const actual = worldHead(THREE, bone);
                const actualPixel = projectedPixel(actual, camera, projection);
                maximumHierarchyBakeReprojectionErrorPx = Math.max(
                    maximumHierarchyBakeReprojectionErrorPx,
                    distance2(
                        actualPixel,
                        projectedPixel(target.head, camera, projection),
                    ),
                );
                maximumRequestedFittedPointErrorPx = Math.max(
                    maximumRequestedFittedPointErrorPx,
                    distance2(
                        actualPixel,
                        target.pixel,
                    ),
                );
            });
            labels.forEach((label) => {
                const { names, segments } = perLimb.get(label);
                names.slice(0, -1).forEach((name, index) => {
                    const actualLength = worldHead(THREE, bones.get(name)).distanceTo(
                        worldHead(THREE, bones.get(names[index + 1])),
                    );
                    maximumSegmentLengthDriftWorld = Math.max(
                        maximumSegmentLengthDriftWorld,
                        Math.abs(actualLength - segments[index].length),
                    );
                });
            });
        });
    } finally {
        restore();
    }

    const tracks = [];
    uniqueBones.forEach((bone) => {
        tracks.push(new THREE.QuaternionKeyframeTrack(
            `${bone.name}.quaternion`,
            times,
            quaternionValues.get(bone.name),
        ));
        tracks.push(new THREE.VectorKeyframeTrack(
            `${bone.name}.position`,
            times,
            positionValues.get(bone.name),
        ));
    });
    const clip = new THREE.AnimationClip(
        options.name || 'LTX_Fitted_HierarchyBake',
        finite(fitted.durationSeconds, 'fitted.durationSeconds'),
        tracks,
    );
    return {
        clip,
        qa: {
            frameCount: fitted.frameCount,
            animatedBones: uniqueBones.length,
            maximumSegmentLengthDriftWorld,
            maximumHierarchyBakeReprojectionErrorPx,
            maximumRequestedFittedPointErrorPx,
            unreachablePixelRays,
        },
    };
}

export const THREE_FITTING_ADAPTER_SCHEMA = SKELETON_SCHEMA;
