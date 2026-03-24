/**
 * Client-side sprite sheet capture (Three.js) — MVP.
 * Renders N frames of one AnimationClip into a PNG atlas using an orthographic camera.
 */

/** @typedef {'front'|'back'|'left'|'right'|'front_right'} ViewPreset */

export const VIEW_PRESETS = /** @type {const} */ ([
  { id: 'front', label: 'Front' },
  { id: 'back', label: 'Back' },
  { id: 'left', label: 'Left' },
  { id: 'right', label: 'Right' },
  { id: 'front_right', label: 'Front-right (45°)' },
]);

export const SPRITE_SHEET_LIMITS = {
  minFrames: 1,
  maxFrames: 64,
  minFrameSize: 64,
  maxFrameSize: 512,
};

/**
 * @param {number} frameCount
 * @returns {{ cols: number, rows: number }}
 */
export function computeAtlasGrid(frameCount) {
  const n = Math.max(1, Math.floor(frameCount));
  const cols = Math.ceil(Math.sqrt(n));
  const rows = Math.ceil(n / cols);
  return { cols, rows };
}

/**
 * @param {import('three').Vector3} center
 * @param {number} distance
 * @param {ViewPreset} preset
 * @param {import('three').Vector3} out
 */
function viewDirection(preset, out) {
  switch (preset) {
    case 'front':
      out.set(0, 0, 1);
      break;
    case 'back':
      out.set(0, 0, -1);
      break;
    case 'left':
      out.set(-1, 0, 0);
      break;
    case 'right':
      out.set(1, 0, 0);
      break;
    case 'front_right':
      out.set(1, 0, 1).normalize();
      break;
    default:
      out.set(0, 0, 1);
  }
}

/**
 * Flip rows vertically (WebGL readPixels is bottom-up).
 * @param {Uint8Array} src RGBA
 * @param {number} w
 * @param {number} h
 * @param {Uint8Array} dst
 */
function flipY(src, w, h, dst) {
  const row = w * 4;
  for (let y = 0; y < h; y++) {
    const srcRow = (h - 1 - y) * row;
    const dstRow = y * row;
    dst.set(src.subarray(srcRow, srcRow + row), dstRow);
  }
}

/**
 * @param {object} opts
 * @param {typeof import('three')} opts.THREE
 * @param {import('three').WebGLRenderer} opts.renderer
 * @param {import('three').Scene} opts.scene
 * @param {import('three').Object3D} opts.rootObject
 * @param {import('three').AnimationMixer} opts.mixer
 * @param {import('three').AnimationClip} opts.clip
 * @param {number} opts.frameCount
 * @param {number} opts.frameWidth
 * @param {number} opts.frameHeight
 * @param {ViewPreset} opts.viewPreset
 * @param {boolean} [opts.transparentBackground]
 * @param {import('three').Object3D | null} [opts.groundObject] set .visible = false during capture
 * @param {(progress01: number) => void} [opts.onProgress]
 * @returns {Promise<{ pngBlob: Blob, meta: object }>}
 */
export async function captureSpriteSheetAtlas(opts) {
  const {
    THREE,
    renderer,
    scene,
    rootObject,
    mixer,
    clip,
    frameCount: rawFrameCount,
    frameWidth: rawFw,
    frameHeight: rawFh,
    viewPreset,
    transparentBackground = true,
    groundObject = null,
    onProgress,
  } = opts;

  const frameCount = Math.min(
    SPRITE_SHEET_LIMITS.maxFrames,
    Math.max(SPRITE_SHEET_LIMITS.minFrames, Math.floor(Number(rawFrameCount) || 8))
  );
  const frameWidth = Math.min(
    SPRITE_SHEET_LIMITS.maxFrameSize,
    Math.max(SPRITE_SHEET_LIMITS.minFrameSize, Math.floor(Number(rawFw) || 256))
  );
  const frameHeight = Math.min(
    SPRITE_SHEET_LIMITS.maxFrameSize,
    Math.max(SPRITE_SHEET_LIMITS.minFrameSize, Math.floor(Number(rawFh) || 256))
  );

  if (!clip || !rootObject || !mixer) {
    throw new Error('Sprite sheet: missing clip, model, or mixer');
  }

  const { cols, rows } = computeAtlasGrid(frameCount);
  const atlasW = cols * frameWidth;
  const atlasH = rows * frameHeight;

  const box = new THREE.Box3().setFromObject(rootObject);
  if (box.isEmpty()) {
    throw new Error('Sprite sheet: empty bounding box');
  }
  const center = box.getCenter(new THREE.Vector3());
  const size = box.getSize(new THREE.Vector3());
  const maxDim = Math.max(size.x, size.y, size.z, 1e-6);
  const aspect = frameWidth / frameHeight;
  const frustumH = maxDim * 1.2;
  const frustumW = frustumH * aspect;

  const dir = new THREE.Vector3();
  viewDirection(viewPreset, dir);
  const distance = maxDim * 3;

  const orthoCam = new THREE.OrthographicCamera(
    -frustumW / 2,
    frustumW / 2,
    frustumH / 2,
    -frustumH / 2,
    0.01,
    maxDim * 40
  );
  orthoCam.position.copy(center).add(dir.multiplyScalar(distance));
  orthoCam.lookAt(center);
  orthoCam.updateProjectionMatrix();
  orthoCam.updateMatrixWorld(true);

  const prevGroundVis = groundObject ? groundObject.visible : null;
  if (groundObject) groundObject.visible = false;

  const prevBg = scene.background;
  const prevEnv = scene.environment;
  const prevEnvIntensity = 'environmentIntensity' in scene ? scene.environmentIntensity : undefined;
  if (transparentBackground) {
    scene.background = null;
    scene.environment = null;
    if (prevEnvIntensity !== undefined) {
      scene.environmentIntensity = 0;
    }
  }

  const prevClearAlpha = renderer.getClearAlpha();
  const prevClearColor = new THREE.Color();
  renderer.getClearColor(prevClearColor);
  renderer.setClearColor(0x000000, transparentBackground ? 0 : 1);

  const rt = new THREE.WebGLRenderTarget(frameWidth, frameHeight, {
    type: THREE.UnsignedByteType,
    format: THREE.RGBAFormat,
    depthBuffer: true,
    stencilBuffer: false,
  });

  const prevTarget = renderer.getRenderTarget();
  const prevAutoClear = renderer.autoClear;

  mixer.stopAllAction();
  const action = mixer.clipAction(clip);
  action.reset();
  action.clampWhenFinished = true;
  action.loop = THREE.LoopOnce;
  action.paused = true;
  action.enabled = true;
  action.play();

  const rawPixels = new Uint8Array(frameWidth * frameHeight * 4);
  const flipped = new Uint8Array(frameWidth * frameHeight * 4);

  const atlasCanvas = document.createElement('canvas');
  atlasCanvas.width = atlasW;
  atlasCanvas.height = atlasH;
  const atlasCtx = atlasCanvas.getContext('2d');
  if (!atlasCtx) {
    throw new Error('Sprite sheet: 2D canvas unsupported');
  }

  const duration = Math.max(clip.duration || 0, 1e-6);

  try {
    for (let i = 0; i < frameCount; i++) {
      const t = frameCount <= 1 ? 0 : (i / (frameCount - 1)) * duration;
      action.time = t;
      mixer.update(0);
      rootObject.updateMatrixWorld(true);

      renderer.setRenderTarget(rt);
      renderer.autoClear = true;
      renderer.clear(true, true, true);
      renderer.render(scene, orthoCam);

      renderer.readRenderTargetPixels(rt, 0, 0, frameWidth, frameHeight, rawPixels);
      flipY(rawPixels, frameWidth, frameHeight, flipped);

      const imgData = new ImageData(new Uint8ClampedArray(flipped), frameWidth, frameHeight);
      const col = i % cols;
      const row = Math.floor(i / cols);
      atlasCtx.putImageData(imgData, col * frameWidth, row * frameHeight);

      if (onProgress) onProgress((i + 1) / frameCount);
      await new Promise((r) => requestAnimationFrame(r));
    }
  } finally {
    renderer.setRenderTarget(prevTarget);
    renderer.autoClear = prevAutoClear;
    renderer.setClearColor(prevClearColor, prevClearAlpha);
    scene.background = prevBg;
    scene.environment = prevEnv;
    if (transparentBackground && prevEnvIntensity !== undefined) {
      scene.environmentIntensity = prevEnvIntensity;
    }
    if (groundObject && prevGroundVis !== null) groundObject.visible = prevGroundVis;
    rt.dispose();
    orthoCam.removeFromParent();
  }

  const durSec = clip.duration || 0;
  const effectiveFps = durSec > 1e-6 ? frameCount / durSec : frameCount;

  const meta = {
    version: 1,
    frameWidth,
    frameHeight,
    frameCount,
    columns: cols,
    rows,
    durationSeconds: durSec,
    fps: Math.round(effectiveFps * 1000) / 1000,
    view: viewPreset,
  };

  const pngBlob = await new Promise((resolve, reject) => {
    atlasCanvas.toBlob((b) => (b ? resolve(b) : reject(new Error('PNG encode failed'))), 'image/png');
  });

  return { pngBlob, meta };
}
