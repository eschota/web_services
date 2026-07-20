const SKELETON_SCHEMA = 'autorig-browser-fitting-skeleton.v1';
const FITTED_SCHEMA = 'autorig-browser-fitted-animation.v1';
const HOOF_GROUND_ORIENTATION_SCHEMA = 'autorig-browser-hoof-ground-orientation.v1';
const THREE_WORLD_UP = Object.freeze([0, 1, 0]);
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

const FULL_BODY_PROFILE_ID = 'horse_2.semantic_full_body.v1';
const FULL_BODY_CHAIN_LABELS = Object.freeze([
    'body_neck_head',
    'head_left_ear',
    'ear_right',
    'tail_base',
]);

const HORSE_2_HEAD_LEFT_EAR_BRANCH = Object.freeze({
    schema: 'autorig-browser-fitting-branch-connector.v1',
    bone: 'head.x',
    fromChain: 'body_neck_head',
    fromHeadIndex: 8,
    toHeadIndex: 0,
});

export const HORSE_2_FULL_BODY_CHAINS = Object.freeze({
    body_neck_head: Object.freeze([
        'spine_01.x',
        'spine_02.x',
        'spine_03.x',
        'c_subneck_1.x',
        'c_subneck_2.x',
        'c_subneck_3.x',
        'c_subneck_4.x',
        'neck.x',
        'head.x',
    ]),
    head_left_ear: Object.freeze([
        'head.x',
        'c_ear_01.l',
        'c_ear_02.l',
    ]),
    ear_right: Object.freeze([
        'c_ear_01.r',
        'c_ear_02.r',
    ]),
    tail_base: Object.freeze([
        'c_tail_00.x',
        'c_tail_01.x',
        'c_tail_02.x',
        'c_tail_03.x',
        'c_tail_04.x',
        'c_tail_05.x',
    ]),
});

export const HORSE_2_FULL_BODY_SOURCE_ANCHORS = Object.freeze({
    body_neck_head: Object.freeze([
        'spine_01.x:82',
        'spine_02.x:117',
        'spine_03.x:185',
        'c_subneck_1.x:148',
        'c_subneck_2.x:312',
        'c_subneck_3.x:321',
        'c_subneck_4.x:329',
        'neck.x:337',
        'head.x:5',
    ]),
    head_left_ear: Object.freeze([
        'head.x:5',
        'c_ear_01.l:4',
        'c_ear_02.l:8',
    ]),
    ear_right: Object.freeze([
        'c_ear_01.r:56',
        'c_ear_02.r:60',
    ]),
    tail_base: Object.freeze([
        'c_tail_00.x:195',
        'c_tail_01.x:197',
        'c_tail_02.x:85',
        'c_tail_03.x:2',
        'c_tail_04.x:291',
        'c_tail_05.x:301',
    ]),
});

// Planar limits are deliberately conservative. Every animated full-body bone
// has an exact entry so opting in can never fall through to the generic +/-pi
// browser-solver default.
export const HORSE_2_FULL_BODY_JOINT_LIMITS = Object.freeze({
    'spine_01.x': Object.freeze([-0.13962634015954636, 0.13962634015954636]), // +/-8 deg
    'spine_02.x': Object.freeze([-0.17453292519943295, 0.17453292519943295]), // +/-10 deg
    'spine_03.x': Object.freeze([-0.17453292519943295, 0.17453292519943295]),
    'c_subneck_1.x': Object.freeze([-0.20943951023931953, 0.20943951023931953]), // +/-12 deg
    'c_subneck_2.x': Object.freeze([-0.20943951023931953, 0.20943951023931953]),
    'c_subneck_3.x': Object.freeze([-0.20943951023931953, 0.20943951023931953]),
    'c_subneck_4.x': Object.freeze([-0.20943951023931953, 0.20943951023931953]),
    'neck.x': Object.freeze([-0.2617993877991494, 0.2617993877991494]), // +/-15 deg
    'head.x': Object.freeze([-0.3141592653589793, 0.3141592653589793]), // +/-18 deg
    'c_ear_01.l': Object.freeze([-0.4363323129985824, 0.4363323129985824]), // +/-25 deg
    'c_ear_01.r': Object.freeze([-0.4363323129985824, 0.4363323129985824]),
    'c_tail_00.x': Object.freeze([-0.3141592653589793, 0.3141592653589793]),
    'c_tail_01.x': Object.freeze([-0.3490658503988659, 0.3490658503988659]), // +/-20 deg
    'c_tail_02.x': Object.freeze([-0.3839724354387525, 0.3839724354387525]), // +/-22 deg
    'c_tail_03.x': Object.freeze([-0.41887902047863906, 0.41887902047863906]), // +/-24 deg
    'c_tail_04.x': Object.freeze([-0.4537856055185257, 0.4537856055185257]), // +/-26 deg
    'c_tail_05.x': Object.freeze([-0.4886921905584123, 0.4886921905584123]), // +/-28 deg
});

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

function normalizedVector3(THREE, value, field) {
    if (typeof THREE?.Vector3 !== 'function') throw new Error('THREE.Vector3 is required');
    const components = array3(value, field);
    const length = Math.hypot(...components);
    if (length <= 1e-12) throw new Error(`${field} has zero length`);
    return new THREE.Vector3(...components).normalize();
}

function hoofGroundOrientationContract(THREE, terminalBone, groundNormalValue) {
    if (!terminalBone || typeof terminalBone.getWorldQuaternion !== 'function') {
        throw new Error('terminal hoof bone getWorldQuaternion() is required');
    }
    if (typeof THREE?.Quaternion !== 'function') throw new Error('THREE.Quaternion is required');
    const groundNormalWorld = normalizedVector3(
        THREE,
        groundNormalValue ?? THREE_WORLD_UP,
        'groundPlaneNormalWorld',
    );
    const restWorldQuaternion = terminalBone.getWorldQuaternion(new THREE.Quaternion());
    if (typeof restWorldQuaternion?.clone !== 'function'
        || typeof restWorldQuaternion.clone().invert !== 'function') {
        throw new Error('terminal hoof world quaternion clone().invert() is required');
    }
    // Define the sole normal in terminal-bone local space from the canonical
    // actionless planted pose.  This preserves each hoof's authored roll/yaw,
    // while giving contact IK one exact normal that must map back to the
    // canonical ground-plane normal.
    const soleNormalLocal = groundNormalWorld.clone()
        .applyQuaternion(restWorldQuaternion.clone().invert())
        .normalize();
    return {
        schema: HOOF_GROUND_ORIENTATION_SCHEMA,
        terminalBone: terminalBone.name,
        soleNormalLocal: array3(soleNormalLocal, `${terminalBone.name}.soleNormalLocal`),
        groundNormalWorld: array3(groundNormalWorld, 'groundNormalWorld'),
        source: groundNormalValue == null
            ? 'three_world_y_up_actionless_planted_pose'
            : 'declared_ground_plane_actionless_planted_pose',
    };
}

function distance3(a, b) {
    return Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z);
}

function sameDirection3(leftValue, rightValue, leftField, rightField) {
    const left = array3(leftValue, leftField);
    const right = array3(rightValue, rightField);
    const leftLength = Math.hypot(...left);
    const rightLength = Math.hypot(...right);
    if (leftLength <= 1e-12 || rightLength <= 1e-12) return false;
    const dot = left.reduce((sum, value, index) => (
        sum + (value / leftLength) * (right[index] / rightLength)
    ), 0);
    return dot >= 1 - 1e-12;
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
        || HORSE_2_FULL_BODY_JOINT_LIMITS[boneName]
        || HORSE_2_JOINT_LIMITS[stem]
        || [-Math.PI, Math.PI];
    const [minimum, maximum] = limitValue(raw, `joint limits for ${boneName}`);
    if (minimum > maximum) throw new Error(`joint limits are reversed for ${boneName}`);
    return [minimum, maximum];
}

function fullBodyChainContracts(enabled) {
    if (!enabled) return {};
    const contracts = Object.fromEntries(FULL_BODY_CHAIN_LABELS.map((label) => [
        label,
        [...HORSE_2_FULL_BODY_CHAINS[label]],
    ]));
    FULL_BODY_CHAIN_LABELS.forEach((label) => {
        const anchors = HORSE_2_FULL_BODY_SOURCE_ANCHORS[label];
        if (!Array.isArray(anchors) || anchors.length !== contracts[label].length) {
            throw new Error(`Horse_2 full-body source anchors do not match ${label}`);
        }
        anchors.forEach((anchorId, index) => {
            if (!anchorId.startsWith(`${contracts[label][index]}:`)) {
                throw new Error(`Horse_2 full-body source anchor order does not match ${label}`);
            }
        });
    });
    const occurrences = new Map();
    Object.entries(contracts).forEach(([label, names]) => names.forEach((name, headIndex) => {
        if (!occurrences.has(name)) occurrences.set(name, []);
        occurrences.get(name).push({ label, headIndex });
    }));
    occurrences.forEach((items, name) => {
        if (items.length === 1) return;
        const allowed = name === 'head.x'
            && items.length === 2
            && items.some((item) => item.label === 'body_neck_head'
                && item.headIndex === contracts.body_neck_head.length - 1)
            && items.some((item) => item.label === 'head_left_ear' && item.headIndex === 0);
        if (!allowed) throw new Error(`unsupported Horse_2 full-body chain overlap at ${name}`);
    });
    return contracts;
}

function semanticChainContract({ THREE, options, profile, label, names, bones, worldByBone, projectedByBone }) {
    const minimumWorldLength = finite(options.minimumWorldSegmentLength ?? 1e-7, 'minimumWorldSegmentLength');
    const minimumPixelLength = finite(options.minimumProjectedSegmentLengthPx ?? 0.25, 'minimumProjectedSegmentLengthPx');
    const tailTolerance = finite(options.connectionToleranceWorld ?? 1e-4, 'connectionToleranceWorld');
    const motionScale = finite(options.positionMotionScale ?? profile.position_motion_scale ?? 1, 'positionMotionScale');
    const joints = [];
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
        const connector = options.branchConnector;
        const declaredBranchEdge = label === 'head_left_ear'
            && index === HORSE_2_HEAD_LEFT_EAR_BRANCH.toHeadIndex
            && name === HORSE_2_HEAD_LEFT_EAR_BRANCH.bone
            && nextName === 'c_ear_01.l'
            && connector?.schema === HORSE_2_HEAD_LEFT_EAR_BRANCH.schema
            && connector?.bone === HORSE_2_HEAD_LEFT_EAR_BRANCH.bone
            && connector?.fromChain === HORSE_2_HEAD_LEFT_EAR_BRANCH.fromChain
            && connector?.fromHeadIndex === HORSE_2_HEAD_LEFT_EAR_BRANCH.fromHeadIndex
            && connector?.toHeadIndex === HORSE_2_HEAD_LEFT_EAR_BRANCH.toHeadIndex;
        if (declaredTail && distance3(declaredTail, worldEnd) > tailTolerance && !declaredBranchEdge) {
            throw new Error(`${label} declared tail does not connect ${name} to ${nextName}`);
        }
        const [minAngle, maxAngle] = jointLimits(profile, options, name);
        const joint = {
            bone: name,
            restStart: [...restStart],
            restEnd: [...restEnd],
            restQuaternion: array4(bone.quaternion, `${name}.quaternion`),
            rotationAxis: localCameraPlaneAxis(THREE, bone, options.camera),
            minAngle,
            maxAngle,
        };
        const previousBone = index > 0 ? bones.get(names[index - 1]) : null;
        if (includePositionMapping(options, bone, label, index, previousBone)) {
            joint.positionMapping = positionMappingForBone(
                THREE,
                bone,
                options.camera,
                options.projection,
                projectedByBone.get(name),
                motionScale,
            );
        }
        joints.push(joint);
    }
    const oneJoint = joints.length === 1;
    const terminalRole = REQUIRED_LIMB_LABELS.includes(label) ? 'hoof' : 'terminal';
    const contactOrientation = REQUIRED_LIMB_LABELS.includes(label)
        ? hoofGroundOrientationContract(
            THREE,
            bones.get(names.at(-1)),
            options.groundPlaneNormalWorld,
        )
        : null;
    return {
        joints,
        proximalTrack: `${label}.proximal`,
        jointTrack: `${label}.joint`,
        hoofTrack: `${label}.${terminalRole}`,
        trackedJointIndex: oneJoint ? null : Math.max(1, Math.floor(joints.length / 2)),
        sourceBoneChain: [...names],
        terminalBone: names.at(-1),
        ...(contactOrientation ? { contactOrientation } : {}),
    };
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
    if (options.includeFullBody != null && typeof options.includeFullBody !== 'boolean') {
        throw new Error('includeFullBody must be a boolean');
    }
    const fullBodyEnabled = options.includeFullBody === true;
    const profile = semanticProfile(options.semanticProfile || HORSE_2_SEMANTIC_PROFILE);
    const limbChains = Object.fromEntries(REQUIRED_LIMB_LABELS.map((label) => [
        label,
        horseDeformChainNames(profile, label),
    ]));
    const auxiliaryChainNames = fullBodyChainContracts(fullBodyEnabled);
    const chains = { ...limbChains, ...auxiliaryChainNames };
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

    const limbs = {};
    REQUIRED_LIMB_LABELS.forEach((label) => {
        limbs[label] = semanticChainContract({
            THREE,
            options: { ...options, camera, projection },
            profile,
            label,
            names: limbChains[label],
            bones,
            worldByBone,
            projectedByBone,
        });
    });
    const auxiliaryChains = {};
    Object.entries(auxiliaryChainNames).forEach(([label, names]) => {
        const branchConnector = label === 'head_left_ear'
            ? { ...HORSE_2_HEAD_LEFT_EAR_BRANCH }
            : null;
        auxiliaryChains[label] = semanticChainContract({
            THREE,
            options: { ...options, camera, projection, branchConnector },
            profile,
            label,
            names,
            bones,
            worldByBone,
            projectedByBone,
        });
        auxiliaryChains[label].sourceAnchorIds = [...HORSE_2_FULL_BODY_SOURCE_ANCHORS[label]];
        if (branchConnector) auxiliaryChains[label].branchConnector = branchConnector;
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
        ...(fullBodyEnabled ? { auxiliaryChains } : {}),
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
            groundPlaneNormalWorld: [...limbs.fore_left.contactOrientation.groundNormalWorld],
            groundPlaneNormalSource: options.groundPlaneNormalWorld == null
                ? 'three_world_y_up_default'
                : 'explicit_caller_contract',
            semanticProfileId: String(profile.profile_id || profile.profileId || 'horse_2.semantic_limbs.v1'),
            terminalPolicy: 'seven_bone_heads_six_segments_to_toes_head',
            sharedBoneRoot: String(boneRoot.name || ''),
            positionMappings: positionMappingPolicy(options.includePositionMappings),
            fullBody: {
                schema: FULL_BODY_PROFILE_ID,
                enabled: fullBodyEnabled,
                locomotionChainCount: REQUIRED_LIMB_LABELS.length,
                auxiliaryChainCount: fullBodyEnabled ? FULL_BODY_CHAIN_LABELS.length : 0,
                selectedChainCount: REQUIRED_LIMB_LABELS.length
                    + (fullBodyEnabled ? FULL_BODY_CHAIN_LABELS.length : 0),
                selectedSourceBoneCount: new Set(Object.values(chains).flat()).size,
                selectedAnimatedBoneCount: new Set([
                    ...Object.values(limbs).flatMap((limb) => limb.joints.map((joint) => joint.bone)),
                    ...Object.values(auxiliaryChains).flatMap((chain) => chain.joints.map((joint) => joint.bone)),
                ]).size,
                auxiliaryChainLabels: fullBodyEnabled ? [...FULL_BODY_CHAIN_LABELS] : [],
            },
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

/**
 * Rotate one fitted hoof terminal orientation so its declared sole normal is
 * parallel to the ground-plane normal.  The correction is applied in world
 * space and pre-multiplied onto the fitted world quaternion; contact position
 * and the already solved chain heads therefore stay unchanged.
 */
export function alignHoofSoleNormalToGround({
    THREE,
    worldQuaternion,
    soleNormalLocal,
    groundNormalWorld,
} = {}) {
    if (typeof THREE?.Quaternion !== 'function' || typeof THREE?.Vector3 !== 'function') {
        throw new Error('THREE Quaternion and Vector3 are required');
    }
    if (!worldQuaternion || typeof worldQuaternion.clone !== 'function') {
        throw new Error('worldQuaternion.clone() is required');
    }
    const sole = normalizedVector3(THREE, soleNormalLocal, 'soleNormalLocal');
    const ground = normalizedVector3(THREE, groundNormalWorld, 'groundNormalWorld');
    const before = sole.clone().applyQuaternion(worldQuaternion).normalize();
    const beforeDot = Math.min(1, Math.max(-1, before.dot(ground)));
    const correction = setQuaternionFromUnitVectors(new THREE.Quaternion(), before, ground);
    const correctionW = Math.min(1, Math.max(-1, Math.abs(finite(correction.w, 'correction.w'))));
    const corrected = multiplyQuaternion(correction.clone(), worldQuaternion.clone()).normalize();
    const after = sole.clone().applyQuaternion(corrected).normalize();
    const afterDot = Math.min(1, Math.max(-1, after.dot(ground)));
    return {
        quaternion: corrected,
        qa: {
            schema: HOOF_GROUND_ORIENTATION_SCHEMA,
            beforeErrorRad: Math.acos(beforeDot),
            afterErrorRad: Math.acos(afterDot),
            correctionAngleRad: 2 * Math.acos(correctionW),
            soleNormalWorldBefore: array3(before, 'soleNormalWorldBefore'),
            soleNormalWorldAfter: array3(after, 'soleNormalWorldAfter'),
            groundNormalWorld: array3(ground, 'groundNormalWorld'),
        },
    };
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
    const chains = [
        ...Object.entries(skeleton?.limbs || {}).map(([label, contract]) => ({
            key: `limbs:${label}`, collection: 'limbs', label, contract,
        })),
        ...Object.entries(skeleton?.auxiliaryChains || {}).map(([label, contract]) => ({
            key: `auxiliaryChains:${label}`, collection: 'auxiliaryChains', label, contract,
        })),
    ];
    if (!Object.keys(skeleton?.limbs || {}).length) throw new Error('skeleton.limbs must not be empty');
    fitted.frames.forEach((frame, frameIndex) => chains.forEach(({ collection, label, contract }) => {
        const expected = contract.sourceBoneChain?.length;
        const fittedChain = frame?.[collection]?.[label];
        const points = fittedChain?.points;
        if (!Number.isInteger(expected) || expected < 2) {
            throw new Error(`skeleton chain ${label} is missing sourceBoneChain`);
        }
        if (!Array.isArray(points) || points.length !== expected) {
            throw new Error(`fitted frame ${frameIndex} chain ${label} must contain ${expected} points`);
        }
        points.forEach((point, pointIndex) => {
            if (!Array.isArray(point) || point.length !== 2 || !point.every(Number.isFinite)) {
                throw new Error(`fitted frame ${frameIndex} chain ${label} point ${pointIndex} is invalid`);
            }
        });
        const contactOrientation = fittedChain?.contactOrientation;
        if (contactOrientation != null) {
            const declared = contract.contactOrientation;
            if (!declared || collection !== 'limbs'
                || contactOrientation.schema !== HOOF_GROUND_ORIENTATION_SCHEMA
                || contactOrientation.apply !== true
                || contactOrientation.terminalBone !== declared.terminalBone
                || !sameDirection3(
                    contactOrientation.soleNormalLocal,
                    declared.soleNormalLocal,
                    `fitted frame ${frameIndex} ${label} soleNormalLocal`,
                    `skeleton ${label} soleNormalLocal`,
                )
                || !sameDirection3(
                    contactOrientation.groundNormalWorld,
                    declared.groundNormalWorld,
                    `fitted frame ${frameIndex} ${label} groundNormalWorld`,
                    `skeleton ${label} groundNormalWorld`,
                )) {
                throw new Error(`fitted frame ${frameIndex} chain ${label} has invalid hoof ground orientation`);
            }
        }
    }));
    return chains;
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
    const chains = normalizedFittedFrames(fitted, skeleton);
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

    const chainNames = chains.flatMap(({ contract }) => contract.sourceBoneChain);
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
    const perChain = new Map(chains.map(({ key, contract }) => {
        const names = contract.sourceBoneChain;
        const segments = names.slice(0, -1).map((name, index) => {
            const start = rest.get(name).head;
            const end = rest.get(names[index + 1]).head;
            const vector = end.clone().sub(start);
            return { length: vector.length(), direction: vector.normalize() };
        });
        return [key, { names, segments }];
    }));
    const times = Array.from({ length: fitted.frameCount }, (_, frame) => frame / finite(fitted.fps, 'fitted.fps'));
    const quaternionValues = new Map(uniqueBones.map((bone) => [bone.name, []]));
    const positionValues = new Map(uniqueBones.map((bone) => [bone.name, []]));
    let maximumSegmentLengthDriftWorld = 0;
    let maximumHierarchyBakeReprojectionErrorPx = 0;
    let maximumRequestedFittedPointErrorPx = 0;
    let unreachablePixelRays = 0;
    let hoofGroundOrientationContactFrameCount = 0;
    let maximumHoofNormalErrorBeforeRad = 0;
    let maximumHoofNormalErrorAfterRad = 0;
    let maximumHoofOrientationCorrectionRad = 0;
    const expectedHoofGroundOrientationContactFrameCount = Number.isInteger(
        fitted.qa?.hoofGroundOrientationContactFrameCount,
    ) && fitted.qa.hoofGroundOrientationContactFrameCount >= 0
        ? fitted.qa.hoofGroundOrientationContactFrameCount
        : null;

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
            chains.forEach(({ key, collection, label, contract }) => {
                const { names, segments } = perChain.get(key);
                const fittedChain = frame[collection][label];
                const pixels = fittedChain.points;
                const restRoot = rest.get(names[0]).head;
                const rootNdcZ = restRoot.clone().project(camera).z;
                let rootPoint = unprojectPixel(THREE, camera, pixels[0], projection, rootNdcZ);
                if (contract.branchConnector) {
                    const connector = contract.branchConnector;
                    if (connector.bone !== names[0] || connector.toHeadIndex !== 0) {
                        throw new Error(`unsupported hierarchy branch connector on ${label}`);
                    }
                    const connectedTarget = desired.get(connector.bone);
                    if (!connectedTarget) {
                        throw new Error(`hierarchy branch source ${connector.fromChain} must precede ${label}`);
                    }
                    rootPoint = connectedTarget.head.clone();
                }
                const points = [rootPoint];
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
                if (fittedChain.contactOrientation?.apply === true) {
                    const orientation = fittedChain.contactOrientation;
                    const terminal = desired.get(orientation.terminalBone);
                    if (!terminal) {
                        throw new Error(`missing hoof orientation terminal ${orientation.terminalBone}`);
                    }
                    const aligned = alignHoofSoleNormalToGround({
                        THREE,
                        worldQuaternion: terminal.quaternion,
                        soleNormalLocal: orientation.soleNormalLocal,
                        groundNormalWorld: orientation.groundNormalWorld,
                    });
                    terminal.quaternion = aligned.quaternion;
                    hoofGroundOrientationContactFrameCount += 1;
                    maximumHoofNormalErrorBeforeRad = Math.max(
                        maximumHoofNormalErrorBeforeRad,
                        aligned.qa.beforeErrorRad,
                    );
                    maximumHoofNormalErrorAfterRad = Math.max(
                        maximumHoofNormalErrorAfterRad,
                        aligned.qa.afterErrorRad,
                    );
                    maximumHoofOrientationCorrectionRad = Math.max(
                        maximumHoofOrientationCorrectionRad,
                        aligned.qa.correctionAngleRad,
                    );
                }
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
            chains.forEach(({ key }) => {
                const { names, segments } = perChain.get(key);
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
            hoofGroundOrientation: {
                schema: HOOF_GROUND_ORIENTATION_SCHEMA,
                method: 'world_space_shortest_arc_sole_normal_to_ground_normal',
                positionPreserved: true,
                contactFrameCount: hoofGroundOrientationContactFrameCount,
                expectedContactFrameCount: expectedHoofGroundOrientationContactFrameCount,
                maximumNormalErrorBeforeRad: maximumHoofNormalErrorBeforeRad,
                maximumNormalErrorAfterRad: maximumHoofNormalErrorAfterRad,
                maximumCorrectionRad: maximumHoofOrientationCorrectionRad,
                passed: expectedHoofGroundOrientationContactFrameCount > 0
                    && hoofGroundOrientationContactFrameCount
                        === expectedHoofGroundOrientationContactFrameCount
                    && maximumHoofNormalErrorAfterRad <= 1e-6,
            },
        },
    };
}

export const THREE_FITTING_ADAPTER_SCHEMA = SKELETON_SCHEMA;
