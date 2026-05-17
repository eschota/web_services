import RAPIER from 'https://cdn.jsdelivr.net/npm/@dimforge/rapier3d-compat@0.12.0/rapier.es.js';

let rapierReadyPromise = null;

export const RAGDOLL_BODY_SEGMENTS = [
    { key: 'torso', fromKey: 'hips', toKey: 'spine', driveKey: 'hips', radius: 0.13, parentKey: null },
    { key: 'head', fromKey: 'spine', toKey: 'head', driveKey: 'head', radius: 0.09, parentKey: 'torso' },
    { key: 'leftUpperArm', fromKey: 'leftUpperArm', toKey: 'leftLowerArm', driveKey: 'leftUpperArm', radius: 0.055, parentKey: 'torso' },
    { key: 'leftLowerArm', fromKey: 'leftLowerArm', toKey: 'leftHand', driveKey: 'leftLowerArm', radius: 0.045, parentKey: 'leftUpperArm' },
    { key: 'rightUpperArm', fromKey: 'rightUpperArm', toKey: 'rightLowerArm', driveKey: 'rightUpperArm', radius: 0.055, parentKey: 'torso' },
    { key: 'rightLowerArm', fromKey: 'rightLowerArm', toKey: 'rightHand', driveKey: 'rightLowerArm', radius: 0.045, parentKey: 'rightUpperArm' },
    { key: 'leftUpperLeg', fromKey: 'leftUpperLeg', toKey: 'leftLowerLeg', driveKey: 'leftUpperLeg', radius: 0.07, parentKey: 'torso' },
    { key: 'leftLowerLeg', fromKey: 'leftLowerLeg', toKey: 'leftFoot', driveKey: 'leftLowerLeg', radius: 0.055, parentKey: 'leftUpperLeg' },
    { key: 'rightUpperLeg', fromKey: 'rightUpperLeg', toKey: 'rightLowerLeg', driveKey: 'rightUpperLeg', radius: 0.07, parentKey: 'torso' },
    { key: 'rightLowerLeg', fromKey: 'rightLowerLeg', toKey: 'rightFoot', driveKey: 'rightLowerLeg', radius: 0.055, parentKey: 'rightUpperLeg' },
];

export async function ensureRapierReady() {
    if (!rapierReadyPromise) {
        rapierReadyPromise = RAPIER.init().then(() => RAPIER);
    }
    return rapierReadyPromise;
}

export function makeRapierVector(R, x = 0, y = 0, z = 0) {
    return new R.Vector3(Number(x) || 0, Number(y) || 0, Number(z) || 0);
}

export function computeScreenSpaceDragTarget({
    THREE,
    camera,
    domElement,
    event,
    startClientX,
    startClientY,
    startTranslation,
}) {
    if (!THREE || !camera || !domElement || !event || !startTranslation) return null;

    const rect = domElement.getBoundingClientRect();
    const height = Math.max(1, rect.height || domElement.clientHeight || 1);
    const dx = Number(event.clientX - startClientX) || 0;
    const dy = Number(startClientY - event.clientY) || 0;

    const start = new THREE.Vector3(
        Number(startTranslation.x) || 0,
        Number(startTranslation.y) || 0,
        Number(startTranslation.z) || 0
    );
    const camPos = new THREE.Vector3();
    const camQuat = new THREE.Quaternion();
    camera.getWorldPosition(camPos);
    camera.getWorldQuaternion(camQuat);

    const cameraRight = new THREE.Vector3(1, 0, 0).applyQuaternion(camQuat).normalize();
    const cameraUp = new THREE.Vector3(0, 1, 0).applyQuaternion(camQuat).normalize();
    const distance = Math.max(0.25, camPos.distanceTo(start));
    const fov = THREE.MathUtils?.degToRad ? THREE.MathUtils.degToRad(camera.fov || 50) : ((camera.fov || 50) * Math.PI / 180);
    const worldUnitsPerPixel = camera.isPerspectiveCamera
        ? (2 * Math.tan(fov * 0.5) * distance) / height
        : ((camera.top - camera.bottom) || 2) / Math.max(1, camera.zoom || 1) / height;

    const target = start
        .addScaledVector(cameraRight, dx * worldUnitsPerPixel)
        .addScaledVector(cameraUp, dy * worldUnitsPerPixel);

    return { x: target.x, y: target.y, z: target.z };
}
