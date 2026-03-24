/**
 * Client-side sprite sheet capture (Three.js) — MVP.
 * Renders N frames of one AnimationClip into a PNG atlas using an orthographic camera
 * with frustum fitted to the model AABB in camera space (fixes “only legs” crop).
 */

/** @typedef {'front'|'back'|'left'|'right'|'top'|'top_right'|'front_right'} ViewPreset */

/** Standard pack: front, back, left, top, 45° top-right (XZ diagonal + elevation). */
export const STANDARD_SPRITE_VIEWS = /** @type {const} */ ([
  { id: 'front', label: 'Front' },
  { id: 'back', label: 'Back' },
  { id: 'left', label: 'Left' },
  { id: 'top', label: 'Top' },
  { id: 'top_right', label: 'Top-right 45°' },
]);

export const VIEW_PRESETS = STANDARD_SPRITE_VIEWS;

export const SPRITE_SHEET_LIMITS = {
  minFrames: 1,
  maxFrames: 64,
  minFrameSize: 64,
  maxFrameSize: 1024,
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
 * @param {ViewPreset} preset
 * @param {import('three').Vector3} out unit direction from center to camera (before distance)
 */
export function viewDirection(preset, out) {
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
    case 'top':
      out.set(0, 1, 0);
      break;
    case 'top_right':
      out.set(1, 0.85, 1).normalize();
      break;
    case 'front_right':
      out.set(1, 0, 1).normalize();
      break;
    default:
      out.set(0, 0, 1);
  }
}

/**
 * World-space bounds that include animated skinned meshes (bone hull + static fallback).
 * `Box3.setFromObject` alone uses rest-pose geometry bounds and misses deformed vertices.
 *
 * @param {typeof import('three')} THREE
 * @param {import('three').Object3D} rootObject
 * @returns {import('three').Box3}
 */
export function computeCaptureBoundingBox(THREE, rootObject) {
  const box = new THREE.Box3();
  const tmp = new THREE.Vector3();
  const cubeSize = new THREE.Vector3();

  rootObject.updateMatrixWorld(true);

  const fallback = new THREE.Box3().setFromObject(rootObject);
  if (!fallback.isEmpty()) {
    box.copy(fallback);
  }

  rootObject.traverse((obj) => {
    if (!obj.visible) return;

    if (obj.isSkinnedMesh && obj.skeleton && obj.geometry) {
      obj.skeleton.update();
      const geom = obj.geometry;
      if (!geom.boundingSphere) geom.computeBoundingSphere();
      const bs = geom.boundingSphere;
      const r = bs.radius > 0 ? bs.radius : 0.1;
      const sx = Math.abs(obj.scale.x);
      const sy = Math.abs(obj.scale.y);
      const sz = Math.abs(obj.scale.z);
      const scaleMax = Math.max(sx, sy, sz, 1e-6);
      const bonePad = Math.max(r * scaleMax * 0.4, 0.15);

      const bones = obj.skeleton.bones;
      for (let i = 0; i < bones.length; i++) {
        bones[i].getWorldPosition(tmp);
        cubeSize.set(bonePad * 2, bonePad * 2, bonePad * 2);
        const boneBox = new THREE.Box3().setFromCenterAndSize(tmp, cubeSize);
        box.union(boneBox);
      }
    } else if (obj.isMesh && obj.geometry && !obj.isSkinnedMesh) {
      const ob = new THREE.Box3().setFromObject(obj);
      if (!ob.isEmpty()) {
        box.union(ob);
      }
    }
  });

  if (box.isEmpty() && !fallback.isEmpty()) {
    return fallback;
  }
  return box;
}

/**
 * One stable zoom-extents box for the whole clip: union of skin-aware bounds at each
 * render time sample (same times as atlas frames). Camera stays fixed while frames play.
 *
 * @param {typeof import('three')} THREE
 * @param {import('three').Object3D} rootObject
 * @param {import('three').AnimationMixer} mixer
 * @param {import('three').AnimationAction} action
 * @param {import('three').AnimationClip} clip
 * @param {number} frameCount
 * @returns {{ box: import('three').Box3, center: import('three').Vector3 }}
 */
export function computeStableCaptureBoundsForClip(
  THREE,
  rootObject,
  mixer,
  action,
  clip,
  frameCount
) {
  const duration = Math.max(clip.duration || 0, 1e-6);
  const unionBox = new THREE.Box3();

  for (let i = 0; i < frameCount; i++) {
    const t = frameCount <= 1 ? 0 : (i / (frameCount - 1)) * duration;
    action.time = t;
    mixer.update(0);
    rootObject.updateMatrixWorld(true);

    const b = computeCaptureBoundingBox(THREE, rootObject);
    if (!b.isEmpty()) {
      unionBox.union(b);
    }
  }

  if (unionBox.isEmpty()) {
    throw new Error('Sprite sheet: empty bounding box');
  }

  const center = unionBox.getCenter(new THREE.Vector3());
  return { box: unionBox, center };
}

/**
 * Position orthographic camera and set left/right/top/bottom so the full AABB fits
 * for the current frame aspect ratio (fixes wrong crop when using max(x,y,z) only).
 * @param {typeof import('three')} THREE
 * @param {import('three').OrthographicCamera} orthoCam
 * @param {import('three').Box3} box
 * @param {import('three').Vector3} center
 * @param {ViewPreset} viewPreset
 * @param {number} frameAspect width / height
 */
export function fitOrthoCameraToBox(THREE, orthoCam, box, center, viewPreset, frameAspect) {
  const dir = new THREE.Vector3();
  viewDirection(viewPreset, dir);
  const size = box.getSize(new THREE.Vector3());
  const span = Math.max(size.x, size.y, size.z, 1e-6);
  const dist = span * 3;

  if (viewPreset === 'top') {
    orthoCam.up.set(0, 0, -1);
  } else {
    orthoCam.up.set(0, 1, 0);
  }

  orthoCam.position.copy(center).add(dir.clone().multiplyScalar(dist));
  orthoCam.lookAt(center);
  orthoCam.updateMatrixWorld(true);

  const corners = [
    new THREE.Vector3(box.min.x, box.min.y, box.min.z),
    new THREE.Vector3(box.max.x, box.min.y, box.min.z),
    new THREE.Vector3(box.min.x, box.max.y, box.min.z),
    new THREE.Vector3(box.max.x, box.max.y, box.min.z),
    new THREE.Vector3(box.min.x, box.min.y, box.max.z),
    new THREE.Vector3(box.max.x, box.min.y, box.max.z),
    new THREE.Vector3(box.min.x, box.max.y, box.max.z),
    new THREE.Vector3(box.max.x, box.max.y, box.max.z),
  ];

  let maxAbsX = 0;
  let maxAbsY = 0;
  const tmp = new THREE.Vector3();
  for (const c of corners) {
    tmp.copy(c).applyMatrix4(orthoCam.matrixWorldInverse);
    maxAbsX = Math.max(maxAbsX, Math.abs(tmp.x));
    maxAbsY = Math.max(maxAbsY, Math.abs(tmp.y));
  }

  const pad = 1.12;
  let halfW = Math.max(maxAbsX * pad, 1e-4);
  let halfH = Math.max(maxAbsY * pad, 1e-4);

  const ar = Math.max(frameAspect, 1e-6);
  const curAr = halfW / halfH;
  if (curAr < ar) {
    halfW = halfH * ar;
  } else {
    halfH = halfW / ar;
  }

  orthoCam.left = -halfW;
  orthoCam.right = halfW;
  orthoCam.top = halfH;
  orthoCam.bottom = -halfH;
  orthoCam.near = 0.01;
  orthoCam.far = dist * 6 + span * 2;
  orthoCam.updateProjectionMatrix();
}

/**
 * Flip rows vertically (WebGL readPixels is bottom-up).
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
 * @param {import('three').Object3D | null} [opts.groundObject]
 * @param {(progress01: number) => void} [opts.onProgress]
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
    Math.max(SPRITE_SHEET_LIMITS.minFrames, Math.floor(Number(rawFrameCount) || 30))
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

  const frameAspect = frameWidth / frameHeight;

  const orthoCam = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.01, 1000);

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
  const totalSteps = frameCount;

  const { box: stableBox, center: stableCenter } = computeStableCaptureBoundsForClip(
    THREE,
    rootObject,
    mixer,
    action,
    clip,
    frameCount
  );
  fitOrthoCameraToBox(THREE, orthoCam, stableBox, stableCenter, viewPreset, frameAspect);

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

      if (onProgress) onProgress((i + 1) / totalSteps);
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

/**
 * Renders STANDARD_SPRITE_VIEWS in order. onProgress(0..1) across all views and frames.
 * @returns {Promise<{ results: Array<{ view: string, pngBlob: Blob, meta: object }>, packMeta: object }>}
 */
export async function captureStandardSpriteSheetPack(opts) {
  const {
    views = STANDARD_SPRITE_VIEWS.map((v) => v.id),
    onProgress,
    ...rest
  } = opts;

  const results = [];
  const totalViews = Math.max(views.length, 1);

  for (let vi = 0; vi < views.length; vi++) {
    const viewPreset = views[vi];
    const { pngBlob, meta } = await captureSpriteSheetAtlas({
      ...rest,
      viewPreset,
      onProgress: (local01) => {
        if (onProgress) {
          onProgress((vi + local01) / totalViews);
        }
      },
    });
    results.push({ view: viewPreset, pngBlob, meta });
  }

  if (onProgress) onProgress(1);

  const packMeta = {
    version: 1,
    views: results.map((r) => ({
      view: r.view,
      frameWidth: r.meta.frameWidth,
      frameHeight: r.meta.frameHeight,
      frameCount: r.meta.frameCount,
      columns: r.meta.columns,
      rows: r.meta.rows,
      durationSeconds: r.meta.durationSeconds,
      fps: r.meta.fps,
    })),
  };

  return { results, packMeta };
}

/**
 * Start looping preview: draws atlas sub-rects onto canvas.
 * @returns {function} stop()
 */
export function startAtlasPreviewAnimation(canvas, imageUrl, meta, fps = 12) {
  const ctx = canvas.getContext('2d');
  if (!ctx) return () => {};

  const { frameWidth, frameHeight, frameCount, columns } = meta;
  const cols = columns || Math.ceil(Math.sqrt(frameCount));
  const rows = Math.ceil(frameCount / cols);
  const maxCell = Math.max(frameWidth, frameHeight, 1);
  const previewSize = Math.min(220, maxCell * 2);
  canvas.width = previewSize;
  canvas.height = previewSize;

  const img = new Image();
  let stopped = false;
  let raf = 0;
  let last = 0;
  let frameIdx = 0;

  img.onload = () => {
    const tick = (t) => {
      if (stopped) return;
      if (t - last >= 1000 / fps) {
        last = t;
        const fi = frameIdx % frameCount;
        const col = fi % cols;
        const row = Math.floor(fi / cols);
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.imageSmoothingEnabled = false;
        ctx.drawImage(
          img,
          col * frameWidth,
          row * frameHeight,
          frameWidth,
          frameHeight,
          0,
          0,
          canvas.width,
          canvas.height
        );
        frameIdx++;
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
  };
  img.onerror = () => {};
  img.src = imageUrl;

  return () => {
    stopped = true;
    if (raf) cancelAnimationFrame(raf);
  };
}
