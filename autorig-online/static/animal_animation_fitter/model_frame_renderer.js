/**
 * Renders skinned mesh pose to ImageData for comparison (fixed camera copy).
 */

function flipPixelsY(rgba, width, height) {
    const rowBytes = width * 4;
    const half = height >> 1;
    for (let y = 0; y < half; y++) {
        const yTop = y * rowBytes;
        const yBot = (height - 1 - y) * rowBytes;
        const tmp = rgba.slice(yTop, yTop + rowBytes);
        rgba.copyWithin(yTop, yBot, yBot + rowBytes);
        rgba.set(tmp, yBot);
    }
}

export class ModelFrameRenderer {
    /**
     * @param {import('three')} THREE
     * @param {{
     *   scene: import('three').Scene,
     *   camera: import('three').Camera,
     *   renderer: import('three').WebGLRenderer,
     *   skinnedMesh: import('three').SkinnedMesh,
     * }} ctx
     */
    constructor(THREE, ctx) {
        this.THREE = THREE;
        this.scene = ctx.scene;
        this.camera = ctx.camera;
        this.renderer = ctx.renderer;
        this.skinnedMesh = ctx.skinnedMesh;
        this._bones = ctx.skinnedMesh?.skeleton?.bones || [];
        this._backupQuats = new Map();
    }

    _snapshotQuaternions() {
        this._backupQuats.clear();
        for (const bone of this._bones) {
            this._backupQuats.set(bone.uuid, bone.quaternion.clone());
        }
    }

    _restoreQuaternions() {
        for (const bone of this._bones) {
            const q = this._backupQuats.get(bone.uuid);
            if (q) bone.quaternion.copy(q);
        }
        if (this.skinnedMesh) this.skinnedMesh.updateMatrixWorld(true);
    }

    /**
     * @param {Record<string, { rx:number, ry:number, rz:number }>} eulerMap bone name -> euler radians XYZ
     */
    setPoseFromEulerMap(eulerMap) {
        const THREE = this.THREE;
        const eul = new THREE.Euler(0, 0, 0, 'XYZ');
        for (const bone of this._bones) {
            const row = eulerMap[bone.name];
            if (!row) continue;
            eul.set(row.rx || 0, row.ry || 0, row.rz || 0);
            bone.quaternion.setFromEuler(eul);
        }
        if (this.skinnedMesh) this.skinnedMesh.updateMatrixWorld(true);
    }

    resetPose() {
        this._restoreQuaternions();
    }

    /** @returns {ImageData} */
    renderCurrentPoseToImageData(width_int, height_int) {
        const prevSize = new this.THREE.Vector2();
        this.renderer.getSize(prevSize);
        const prevPixelRatio = this.renderer.getPixelRatio();
        this.renderer.setPixelRatio(1);
        this.renderer.setSize(width_int, height_int, false);
        this.renderer.render(this.scene, this.camera);

        const gl = this.renderer.getContext();
        const w = width_int;
        const h = height_int;
        const pixels = new Uint8Array(w * h * 4);
        gl.readPixels(0, 0, w, h, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
        flipPixelsY(pixels, w, h);

        this.renderer.setSize(prevSize.x, prevSize.y, false);
        this.renderer.setPixelRatio(prevPixelRatio);

        return new ImageData(new Uint8ClampedArray(pixels.buffer), w, h);
    }

    renderPoseToImageData(pose_state, width_int, height_int) {
        this._snapshotQuaternions();
        const eulerMap = {};
        const bones = pose_state?.bones || {};
        for (const [name, br] of Object.entries(bones)) {
            eulerMap[name] = {
                rx: br.rotation_x_float || 0,
                ry: br.rotation_y_float || 0,
                rz: br.rotation_z_float || 0,
            };
        }
        this.setPoseFromEulerMap(eulerMap);
        const img = this.renderCurrentPoseToImageData(width_int, height_int);
        this.resetPose();
        return img;
    }
}
