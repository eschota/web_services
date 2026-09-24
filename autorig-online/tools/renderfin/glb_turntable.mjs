#!/usr/bin/env node
/**
 * GLB turntable renderer: 6-second orbit MP4 around a GLB model.
 *
 * Headless-Chrome-over-CDP + three.js harness forked from
 * tools/animation_fitting/browser_horse_visual_phase_qa.mjs (launchChrome,
 * harness server, frame capture, ffmpeg encode), with the fixed-camera QA
 * gates replaced by a bounding-sphere-fitted orbit camera and GLTFLoader.
 *
 * Usage:
 *   node glb_turntable.mjs --glb model.glb --output out.mp4 \
 *     [--seconds 6] [--fps 30] [--size 768] [--chrome path] [--ffmpeg ffmpeg]
 *
 * Still mode (PNG, no ffmpeg): one view written exactly to --output, or
 * several written as <output-stem>_<view>.png.
 *   node glb_turntable.mjs --glb model.glb --output front.png --still front
 *   node glb_turntable.mjs --glb model.glb --output out/model.png --views front,back,left,right
 *     [--size 1024] [--ortho 1] [--margin 0.08] [--background '#7f7f7f'|transparent] [--chrome path]
 *
 * Views match the browser preflight renderer (static/js/app.js): "front" looks
 * at the model from +Z (glTF's forward), "back" from -Z, "left" from -X,
 * "right" from +X, "top_side_45" from (+X, +Y, +Z). The camera is fitted to
 * the posed vertices so the silhouette is centred with --margin (a fraction
 * of the frame) left clear on every side. Animations are never played: a
 * rigged model renders in the rest pose the file stores.
 */
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const VENDOR = path.join(HERE, 'vendor');

// Node < 22 has no global WebSocket; fall back to the vendored dependency-free `ws`.
let WebSocketImpl = globalThis.WebSocket;
if (!WebSocketImpl) {
    const { createRequire } = await import('node:module');
    WebSocketImpl = createRequire(import.meta.url)(path.join(VENDOR, 'ws', 'index.js'));
}

function fail(message) {
    console.error(`[glb_turntable] ${message}`);
    process.exit(1);
}

// Once Chrome, the harness server or temp directories exist, failures are
// thrown instead: process.exit() would skip the cleanup in `finally` and leave
// Chrome running. main()'s catch prints the same `[glb_turntable] ...` line.
class RenderFailure extends Error {}

function bail(message) {
    throw new RenderFailure(message);
}

// Camera viewing direction + up per still view, as in static/js/app.js.
const STILL_VIEWS = {
    front: { forward: [0, 0, -1], up: [0, 1, 0] },
    back: { forward: [0, 0, 1], up: [0, 1, 0] },
    left: { forward: [1, 0, 0], up: [0, 1, 0] },
    right: { forward: [-1, 0, 0], up: [0, 1, 0] },
    top_side_45: { forward: [-1, -1, -1], up: [0, 1, 0] },
};
const TURNTABLE_ONLY_ARGS = ['--seconds', '--fps', '--ffmpeg'];
const STILL_ONLY_ARGS = ['--ortho', '--margin', '--background'];

function parseArgs(argv) {
    const args = { seconds: 6, fps: 30, size: 768, ffmpeg: 'ffmpeg', chrome: '' };
    const still = { ortho: '1', margin: 0.08, background: '#7f7f7f' };
    const given = new Set();
    for (let i = 2; i < argv.length; i += 2) {
        const key = argv[i];
        const value = argv[i + 1];
        if (value === undefined) fail(`missing value for ${key}`);
        given.add(key);
        switch (key) {
            case '--glb': args.glb = value; break;
            case '--output': args.output = value; break;
            case '--seconds': args.seconds = Number(value); break;
            case '--fps': args.fps = Number(value); break;
            case '--size': args.size = Number(value); break;
            case '--chrome': args.chrome = value; break;
            case '--ffmpeg': args.ffmpeg = value; break;
            case '--still': still.single = value; break;
            case '--views': still.list = value; break;
            case '--ortho': still.ortho = value; break;
            case '--margin': still.margin = Number(value); break;
            case '--background': still.background = value; break;
            default: fail(`unknown argument ${key}`);
        }
    }
    if (given.has('--still') || given.has('--views')) return parseStillArgs(args, still, given);
    const stillOnly = STILL_ONLY_ARGS.filter((key) => given.has(key));
    if (stillOnly.length) fail(`still-only argument(s) without --still/--views: ${stillOnly.join(', ')}`);
    if (!args.glb || !args.output) fail('usage: --glb <model.glb> --output <out.mp4>');
    if (!Number.isFinite(args.seconds) || args.seconds <= 0 || args.seconds > 60) fail('--seconds must be in (0, 60]');
    if (!Number.isFinite(args.fps) || args.fps < 1 || args.fps > 60) fail('--fps must be in [1, 60]');
    if (!Number.isInteger(args.size) || args.size < 64 || args.size > 2048 || args.size % 2 !== 0) fail('--size must be an even integer in [64, 2048]');
    return args;
}

function parseStillArgs(args, still, given) {
    if (given.has('--still') && given.has('--views')) fail('--still and --views are mutually exclusive');
    const turntableOnly = TURNTABLE_ONLY_ARGS.filter((key) => given.has(key));
    if (turntableOnly.length) fail(`turntable-only argument(s) with --still/--views: ${turntableOnly.join(', ')}`);
    if (!args.glb || !args.output) fail('usage: --glb <model.glb> --output <out.png> (--still <view> | --views <view,...>)');
    const single = given.has('--still');
    const views = single ? [still.single] : still.list.split(',').map((name) => name.trim());
    for (const view of views) {
        if (!Object.hasOwn(STILL_VIEWS, view)) fail(`unknown view '${view}' (expected ${Object.keys(STILL_VIEWS).join(', ')})`);
    }
    if (new Set(views).size !== views.length) fail('--views must not repeat a view');
    const size = given.has('--size') ? args.size : 1024;
    if (!Number.isInteger(size) || size < 64 || size > 2048) fail('--size must be an integer in [64, 2048]');
    const ortho = new Map([['1', true], ['true', true], ['0', false], ['false', false]]).get(String(still.ortho).trim().toLowerCase());
    if (ortho === undefined) fail('--ortho must be 1 or 0');
    if (!Number.isFinite(still.margin) || still.margin < 0 || still.margin > 0.4) fail('--margin must be in [0, 0.4]');
    const background = String(still.background).trim().toLowerCase();
    if (background !== 'transparent' && !/^#[0-9a-f]{6}$/.test(background)) fail("--background must be '#rrggbb' or 'transparent'");
    return { mode: 'still', glb: args.glb, output: args.output, chrome: args.chrome, single, views, size, ortho, margin: still.margin, background };
}

function findChrome(explicit) {
    const candidates = explicit ? [explicit] : [
        process.env.CHROME_PATH,
        process.env.CHROME_HEADLESS_SHELL,
        'C:/Program Files/Google/Chrome/Application/chrome.exe',
        'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable',
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/usr/bin/chrome-headless-shell',
        '/opt/chrome-headless-shell/chrome-headless-shell',
    ].filter(Boolean);
    for (const candidate of candidates) {
        try { if (fs.statSync(candidate).isFile()) return candidate; } catch { /* keep looking */ }
    }
    fail(`chrome executable not found (tried ${candidates.join(', ')}); pass --chrome`);
}

function harnessHtml() {
    return `<!doctype html><html><head><meta charset="utf-8"><style>
html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#717b86}canvas{display:block}
</style></head><body><script type="module">
window.addEventListener('unhandledrejection', (event) => { window.__AUTORIG_ERROR__ ||= String(event.reason?.stack || event.reason); });
window.addEventListener('error', (event) => { window.__AUTORIG_ERROR__ ||= String(event.error?.stack || event.message); });
try {
  const THREE = await import('/three.module.js');
  const { GLTFLoader } = await import('/GLTFLoader.js');
  const config = await (await fetch('/config.json', {cache:'no-store'})).json();
  const size = config.size;
  const renderer = new THREE.WebGLRenderer({antialias:true, alpha:false, preserveDrawingBuffer:true});
  renderer.setPixelRatio(1);
  renderer.setSize(size, size, false);
  renderer.setClearColor(0x717b86, 1);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.1;
  document.body.replaceChildren(renderer.domElement);

  const gltf = await new GLTFLoader().loadAsync('/model.glb');
  const model = gltf.scene;
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x717b86);
  scene.add(model);
  scene.add(new THREE.HemisphereLight(0xe9f1ff, 0x3f4650, 2.0));
  const key = new THREE.DirectionalLight(0xffffff, 2.8);
  key.position.set(4.5, 8.5, 5.5);
  scene.add(key); scene.add(key.target);
  const fill = new THREE.DirectionalLight(0xdfe8ff, 1.1);
  fill.position.set(-6, 3, -4);
  scene.add(fill);

  model.updateWorldMatrix(true, true);
  const box = new THREE.Box3().setFromObject(model);
  if (box.isEmpty()) throw new Error('model bounding box is empty');
  const center = box.getCenter(new THREE.Vector3());
  const sphere = box.getBoundingSphere(new THREE.Sphere());
  const radius = Math.max(sphere.radius, 1e-6);

  const camera = new THREE.PerspectiveCamera(40, 1, radius / 100, radius * 100);
  const distance = radius / Math.sin(THREE.MathUtils.degToRad(camera.fov / 2)) * 1.15;
  const elevation = radius * 0.35;

  let meshCount = 0, triangleCount = 0;
  model.traverse((node) => {
    if (node.isMesh) {
      meshCount += 1;
      const index = node.geometry?.getIndex();
      const positions = node.geometry?.getAttribute('position');
      triangleCount += Math.floor((index ? index.count : positions ? positions.count : 0) / 3);
    }
  });
  if (meshCount === 0) throw new Error('model contains no meshes');

  window.__renderTurntableFrame = async (frameIndex, frameCount) => {
    if (!Number.isInteger(frameIndex) || frameIndex < 0 || frameIndex >= frameCount) throw new Error('invalid frame index');
    const theta = (frameIndex / frameCount) * Math.PI * 2;
    camera.position.set(
      center.x + distance * Math.cos(theta),
      center.y + elevation,
      center.z + distance * Math.sin(theta),
    );
    camera.lookAt(center);
    camera.updateMatrixWorld(true);
    renderer.render(scene, camera);
    await new Promise((resolve) => requestAnimationFrame(resolve));
    renderer.render(scene, camera);
    return {
      frameIndex,
      width: renderer.domElement.width,
      height: renderer.domElement.height,
      dataUrl: renderer.domElement.toDataURL('image/png'),
    };
  };

  window.__AUTORIG_RESULT__ = {
    threeRevision: String(THREE.REVISION),
    meshCount,
    triangleCount,
    boundingRadius: radius,
    webgl2: renderer.capabilities.isWebGL2 === true,
  };
  window.__AUTORIG_READY__ = true;
} catch (error) {
  window.__AUTORIG_ERROR__ = String(error?.stack || error);
  console.error(error);
}
</script></body></html>`;
}

// Still-image harness. Same loader as the turntable (GLTFLoader handles
// KHR_texture_transform, multi-primitive meshes and COLOR_0 on its own), but a
// per-view camera fitted to the posed vertices and neutral, even lighting:
// soft key from the camera side, weaker fill, faint rim, a soft grey studio
// environment so metals do not render black, no shadows and no ground.
function stillHarnessHtml() {
    return `<!doctype html><html><head><meta charset="utf-8"><style>
html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#7f7f7f}canvas{display:block}
</style></head><body><script type="module">
window.addEventListener('unhandledrejection', (event) => { window.__AUTORIG_ERROR__ ||= String(event.reason?.stack || event.reason); });
window.addEventListener('error', (event) => { window.__AUTORIG_ERROR__ ||= String(event.error?.stack || event.message); });
try {
  const THREE = await import('/three.module.js');
  const { GLTFLoader } = await import('/GLTFLoader.js');
  const config = await (await fetch('/config.json', {cache:'no-store'})).json();
  const { size, ortho, margin, background, views } = config;
  const transparent = background === 'transparent';
  const renderer = new THREE.WebGLRenderer({antialias:true, alpha:transparent, preserveDrawingBuffer:true});
  renderer.setPixelRatio(1);
  renderer.setSize(size, size, false);
  if (transparent) renderer.setClearColor(0x000000, 0);
  else renderer.setClearColor(new THREE.Color(background), 1);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  // No tone mapping: the image-edit model should see texture colours as authored.
  renderer.toneMapping = THREE.NoToneMapping;
  document.body.replaceChildren(renderer.domElement);

  // gltf.animations are never bound to a mixer, so skinned and morphing
  // meshes keep the rest pose (node transforms, default weights) of the file.
  const gltf = await new GLTFLoader().loadAsync('/model.glb');
  const model = gltf.scene;
  const scene = new THREE.Scene();
  scene.add(model);

  // Equirect studio environment, row 0 = straight down: dim floor, brighter
  // sky, no hot spot, so every view gets the same soft fill.
  const envWidth = 64, envHeight = 32;
  const envData = new Uint16Array(envWidth * envHeight * 4);
  for (let row = 0; row < envHeight; row += 1) {
    const elevation = Math.sin(((row + 0.5) / envHeight - 0.5) * Math.PI);
    const radiance = elevation >= 0 ? 0.30 + 0.16 * elevation : 0.30 - 0.16 * -elevation;
    const value = THREE.DataUtils.toHalfFloat(radiance);
    for (let column = 0; column < envWidth; column += 1) {
      const offset = (row * envWidth + column) * 4;
      envData[offset] = value; envData[offset + 1] = value; envData[offset + 2] = value;
      envData[offset + 3] = THREE.DataUtils.toHalfFloat(1);
    }
  }
  const environment = new THREE.DataTexture(envData, envWidth, envHeight, THREE.RGBAFormat, THREE.HalfFloatType);
  environment.mapping = THREE.EquirectangularReflectionMapping;
  environment.colorSpace = THREE.LinearSRGBColorSpace;
  environment.magFilter = THREE.LinearFilter;
  environment.minFilter = THREE.LinearFilter;
  environment.generateMipmaps = false;
  environment.needsUpdate = true;
  scene.environment = environment;

  const key = new THREE.DirectionalLight(0xffffff, 1.7);
  const fill = new THREE.DirectionalLight(0xffffff, 0.45);
  const rim = new THREE.DirectionalLight(0xffffff, 0.8);
  for (const light of [key, fill, rim]) { scene.add(light); scene.add(light.target); }
  scene.updateMatrixWorld(true);

  let meshCount = 0, triangleCount = 0, skinnedMeshCount = 0;
  model.traverse((node) => {
    if (node.isMesh) {
      meshCount += 1;
      if (node.isSkinnedMesh) skinnedMeshCount += 1;
      const index = node.geometry?.getIndex();
      const positions = node.geometry?.getAttribute('position');
      triangleCount += Math.floor((index ? index.count : positions ? positions.count : 0) / 3);
    }
  });
  if (meshCount === 0) throw new Error('model contains no meshes');

  // World-space positions as drawn: getVertexPosition applies morph weights
  // and skinning, so the fit follows the pose rather than the raw buffers.
  const chunks = [];
  const vertex = new THREE.Vector3();
  const instanceMatrix = new THREE.Matrix4();
  model.traverseVisible((node) => {
    if (!node.isMesh && !node.isLine && !node.isPoints) return;
    const positions = node.geometry?.getAttribute('position');
    if (!positions || positions.count === 0) return;
    if (node.isInstancedMesh) {
      if (!node.geometry.boundingBox) node.geometry.computeBoundingBox();
      const { min, max } = node.geometry.boundingBox;
      const corners = new Float32Array(node.count * 24);
      let offset = 0;
      for (let instance = 0; instance < node.count; instance += 1) {
        node.getMatrixAt(instance, instanceMatrix);
        instanceMatrix.premultiply(node.matrixWorld);
        for (let corner = 0; corner < 8; corner += 1) {
          vertex.set(corner & 1 ? max.x : min.x, corner & 2 ? max.y : min.y, corner & 4 ? max.z : min.z).applyMatrix4(instanceMatrix);
          corners[offset++] = vertex.x; corners[offset++] = vertex.y; corners[offset++] = vertex.z;
        }
      }
      chunks.push(corners);
      return;
    }
    const points = new Float32Array(positions.count * 3);
    for (let index = 0; index < positions.count; index += 1) {
      if (node.isMesh) node.getVertexPosition(index, vertex); else vertex.fromBufferAttribute(positions, index);
      vertex.applyMatrix4(node.matrixWorld);
      points[index * 3] = vertex.x; points[index * 3 + 1] = vertex.y; points[index * 3 + 2] = vertex.z;
    }
    chunks.push(points);
  });
  const box = new THREE.Box3();
  for (const points of chunks) {
    for (let index = 0; index < points.length; index += 3) {
      if (Number.isFinite(points[index]) && Number.isFinite(points[index + 1]) && Number.isFinite(points[index + 2])) {
        box.expandByPoint(vertex.fromArray(points, index));
      }
    }
  }
  if (box.isEmpty()) throw new Error('model bounding box is empty');
  const center = box.getCenter(new THREE.Vector3());
  const radius = Math.max(box.getBoundingSphere(new THREE.Sphere()).radius, 1e-6);

  const fov = 40;
  const aim = (light, direction) => {
    light.position.copy(center).addScaledVector(direction.normalize(), radius * 4);
    light.target.position.copy(center);
    light.updateMatrixWorld(true);
    light.target.updateMatrixWorld(true);
  };

  window.__renderStill = async (viewId) => {
    const view = views[viewId];
    if (!view) throw new Error('unknown view ' + viewId);
    const forward = new THREE.Vector3().fromArray(view.forward).normalize();
    const up = new THREE.Vector3().fromArray(view.up).normalize();
    const right = new THREE.Vector3().crossVectors(forward, up).normalize();
    const screenUp = new THREE.Vector3().crossVectors(right, forward).normalize();
    const distance = ortho ? radius * 3 : radius / Math.sin(THREE.MathUtils.degToRad(fov / 2)) * 1.15;
    const eye = center.clone().addScaledVector(forward, -distance);

    // Image-plane extents of every vertex: camera-space x/y for the
    // orthographic camera, x/depth and y/depth (tangent units) in perspective.
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity, minDepth = Infinity, maxDepth = -Infinity;
    for (const points of chunks) {
      for (let index = 0; index < points.length; index += 3) {
        const dx = points[index] - eye.x, dy = points[index + 1] - eye.y, dz = points[index + 2] - eye.z;
        const depth = dx * forward.x + dy * forward.y + dz * forward.z;
        let x = dx * right.x + dy * right.y + dz * right.z;
        let y = dx * screenUp.x + dy * screenUp.y + dz * screenUp.z;
        if (!Number.isFinite(depth) || !Number.isFinite(x) || !Number.isFinite(y)) continue;
        if (!ortho) { x /= depth; y /= depth; }
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
        if (depth < minDepth) minDepth = depth;
        if (depth > maxDepth) maxDepth = depth;
      }
    }
    // Square frame around the silhouette's centre; its longer side spans
    // (1 - 2 * margin) of the frame, the shorter one is centred.
    const extent = Math.max(maxX - minX, maxY - minY, ortho ? radius * 1e-3 : 1e-3);
    const half = extent / (1 - 2 * margin) / 2;
    const centerX = (minX + maxX) / 2, centerY = (minY + maxY) / 2;
    const near = Math.max(minDepth * 0.5, distance * 1e-3);
    const far = maxDepth * 1.5 + radius * 0.01;
    const camera = ortho
      ? new THREE.OrthographicCamera(centerX - half, centerX + half, centerY + half, centerY - half, near, far)
      : new THREE.PerspectiveCamera(fov, 1, near, far);
    camera.position.copy(eye);
    camera.up.copy(up);
    camera.lookAt(eye.clone().add(forward));
    camera.updateMatrixWorld(true);
    if (!ortho) {
      // Off-axis (lens-shifted) frustum: fit and centre the projected
      // silhouette exactly instead of the bounding sphere.
      camera.projectionMatrix.makePerspective(near * (centerX - half), near * (centerX + half), near * (centerY + half), near * (centerY - half), near, far);
      camera.projectionMatrixInverse.copy(camera.projectionMatrix).invert();
    }

    const toCamera = forward.clone().negate();
    aim(key, toCamera.clone().addScaledVector(screenUp, 0.55).addScaledVector(right, -0.45));
    aim(fill, toCamera.clone().addScaledVector(screenUp, 0.1).addScaledVector(right, 0.8));
    aim(rim, forward.clone().addScaledVector(screenUp, 0.9));

    renderer.render(scene, camera);
    await new Promise((resolve) => requestAnimationFrame(resolve));
    renderer.render(scene, camera);
    return {
      view: viewId,
      width: renderer.domElement.width,
      height: renderer.domElement.height,
      dataUrl: renderer.domElement.toDataURL('image/png'),
    };
  };

  window.__AUTORIG_RESULT__ = {
    threeRevision: String(THREE.REVISION),
    meshCount,
    triangleCount,
    skinnedMeshCount,
    animationCount: gltf.animations.length,
    boundingRadius: radius,
    webgl2: renderer.capabilities.isWebGL2 === true,
  };
  window.__AUTORIG_READY__ = true;
} catch (error) {
  window.__AUTORIG_ERROR__ = String(error?.stack || error);
  console.error(error);
}
</script></body></html>`;
}

function startHarnessServer({ glbPath, size, html = harnessHtml(), config = { size } }) {
    const routes = new Map([
        ['/index.html', { buffer: Buffer.from(html, 'utf8'), type: 'text/html; charset=utf-8' }],
        ['/config.json', { buffer: Buffer.from(JSON.stringify(config), 'utf8'), type: 'application/json; charset=utf-8' }],
        ['/three.module.js', { filename: path.join(VENDOR, 'three.module.js'), type: 'text/javascript; charset=utf-8' }],
        ['/GLTFLoader.js', { filename: path.join(VENDOR, 'GLTFLoader.js'), type: 'text/javascript; charset=utf-8' }],
        ['/utils/BufferGeometryUtils.js', { filename: path.join(VENDOR, 'BufferGeometryUtils.js'), type: 'text/javascript; charset=utf-8' }],
        ['/model.glb', { filename: glbPath, type: 'model/gltf-binary' }],
    ]);
    for (const route of routes.values()) {
        if (route.filename) {
            if (!fs.existsSync(route.filename)) bail(`missing harness file ${route.filename}`);
            route.buffer = fs.readFileSync(route.filename);
            if (route.filename.endsWith('.js') && !route.filename.endsWith('three.module.js')) {
                // no importmap in the harness: point bare specifiers at our routes
                route.buffer = Buffer.from(
                    route.buffer.toString('utf8').replace("from 'three'", "from '/three.module.js'"),
                    'utf8',
                );
            }
        }
    }
    const server = http.createServer((request, response) => {
        const route = routes.get(new URL(request.url, 'http://127.0.0.1').pathname);
        response.setHeader('Cache-Control', 'no-store');
        if (!route) { response.writeHead(404); response.end('not found'); return; }
        response.writeHead(200, { 'Content-Type': route.type, 'Content-Length': route.buffer.length });
        response.end(route.buffer);
    });
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => resolve({ server, url: `http://127.0.0.1:${server.address().port}/index.html` }));
    });
}

class CdpClient {
    constructor(url) {
        this.socket = new WebSocketImpl(url);
        this.nextId = 1;
        this.pending = new Map();
        this.socket.onmessage = (event) => {
            const message = JSON.parse(event.data);
            if (!message.id) return;
            const pending = this.pending.get(message.id);
            if (!pending) return;
            this.pending.delete(message.id);
            if (message.error) pending.reject(new Error(message.error.message)); else pending.resolve(message.result || {});
        };
    }
    async open() {
        if (this.socket.readyState === WebSocketImpl.OPEN) return;
        await new Promise((resolve, reject) => { this.socket.onopen = resolve; this.socket.onerror = () => reject(new Error('CDP connection failed')); });
    }
    command(method, params = {}) {
        const id = this.nextId++;
        return new Promise((resolve, reject) => { this.pending.set(id, { resolve, reject }); this.socket.send(JSON.stringify({ id, method, params })); });
    }
    close() { try { this.socket.close(); } catch { /* already closed */ } }
}

function delay(milliseconds) { return new Promise((resolve) => setTimeout(resolve, milliseconds)); }

async function launchChrome(chromeExecutable) {
    const profileDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'renderfin-turntable-'));
    const child = spawn(chromeExecutable, [
        '--headless=new', '--use-angle=swiftshader', '--enable-webgl', '--ignore-gpu-blocklist',
        '--disable-background-networking', '--disable-component-update', '--disable-default-apps', '--disable-extensions',
        '--disable-sync', '--no-first-run', '--no-default-browser-check', '--no-sandbox',
        '--remote-debugging-address=127.0.0.1', '--remote-debugging-port=0',
        `--user-data-dir=${profileDirectory}`, 'about:blank',
    ], { stdio: ['ignore', 'ignore', 'pipe'], windowsHide: true });
    let stderr = '';
    let websocketUrl = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => { stderr += chunk; websocketUrl ||= stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)?.[1] || ''; });
    const runtime = { child, profileDirectory, pageWebSocketUrl: '', stderr: () => stderr };
    try {
        const started = Date.now();
        while (!websocketUrl && Date.now() - started < 20000) {
            if (child.exitCode != null) bail(`Chrome exited during startup (${child.exitCode}): ${stderr}`);
            await delay(50);
        }
        if (!websocketUrl) bail(`Chrome did not expose CDP: ${stderr}`);
        const endpoint = new URL(websocketUrl);
        const pages = await (await fetch(`http://${endpoint.host}/json/list`)).json();
        const page = pages.find((entry) => entry.type === 'page');
        if (!page?.webSocketDebuggerUrl) bail('Chrome did not expose a page target');
        runtime.pageWebSocketUrl = page.webSocketDebuggerUrl;
        return runtime;
    } catch (error) {
        // The caller never sees this runtime, so its `finally` cannot stop it.
        await stopChrome(runtime);
        throw error;
    }
}

async function stopChrome(runtime) {
    if (!runtime) return;
    try {
        if (runtime.child.exitCode == null) runtime.child.kill();
        await Promise.race([new Promise((resolve) => runtime.child.once('exit', resolve)), delay(3000)]);
        if (runtime.child.exitCode == null) {
            runtime.child.kill('SIGKILL');
            await Promise.race([new Promise((resolve) => runtime.child.once('exit', resolve)), delay(3000)]);
        }
    } finally {
        try {
            fs.rmSync(runtime.profileDirectory, {
                recursive: true,
                force: true,
                maxRetries: 5,
                retryDelay: 100,
            });
        } catch (error) {
            // Chrome can briefly recreate files while it is shutting down. A
            // disposable profile leak must not invalidate a verified MP4.
            console.warn(`[glb_turntable] temporary profile cleanup failed: ${error?.message || error}`);
        }
    }
}

async function evaluate(client, expression) {
    const result = await client.command('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
    if (result.exceptionDetails) throw new Error(`browser evaluation failed: ${result.exceptionDetails.exception?.description || result.exceptionDetails.text}`);
    return result.result?.value;
}

function encodeMp4({ ffmpeg, framesDirectory, outputPath, fps, frameCount }) {
    const result = spawnSync(ffmpeg, [
        '-hide_banner', '-loglevel', 'error', '-nostdin', '-y',
        '-framerate', String(fps), '-start_number', '0', '-i', path.join(framesDirectory, 'frame_%04d.png'),
        '-frames:v', String(frameCount), '-an', '-c:v', 'libx264', '-preset', 'medium', '-crf', '18',
        '-pix_fmt', 'yuv420p', '-movflags', '+faststart', outputPath,
    ], { encoding: 'utf8', windowsHide: true, maxBuffer: 16 * 1024 * 1024 });
    if (result.error || result.status !== 0) {
        bail(`ffmpeg encode failed: ${result.error?.message || String(result.stderr || '').trim() || `exit ${result.status}`}`);
    }
    if (!fs.existsSync(outputPath) || fs.statSync(outputPath).size < 1024) bail('ffmpeg produced no output');
}

async function waitForHarness(client, runtime) {
    const started = Date.now();
    while (Date.now() - started < 60000) {
        const state = await evaluate(client, `({ready:window.__AUTORIG_READY__===true,error:window.__AUTORIG_ERROR__||null,result:window.__AUTORIG_RESULT__||null})`);
        if (state?.error) bail(`harness failed: ${state.error}`);
        if (state?.ready) return state.result;
        await delay(100);
    }
    bail(`harness timed out: ${runtime.stderr()}`);
}

function stillOutputPath(outputPath, view) {
    const { dir, name } = path.parse(outputPath);
    return path.join(dir, `${name}_${view}.png`);
}

function pngFromDataUrl(rendered, size, view) {
    const prefix = 'data:image/png;base64,';
    if (!rendered?.dataUrl?.startsWith(prefix)) bail(`${view} render failed`);
    const png = Buffer.from(rendered.dataUrl.slice(prefix.length), 'base64');
    if (png.length < 33 || png.readUInt32BE(0) !== 0x89504e47 || png.toString('latin1', 12, 16) !== 'IHDR') bail(`${view} render did not return a PNG`);
    const width = png.readUInt32BE(16);
    const height = png.readUInt32BE(20);
    if (width !== size || height !== size) bail(`${view} render is ${width}x${height}, expected ${size}x${size}`);
    return png;
}

function writeFileAtomic(file, buffer) {
    const temporary = `${file}.${process.pid}.tmp`;
    try {
        fs.writeFileSync(temporary, buffer);
        fs.renameSync(temporary, file);
    } catch (error) {
        fs.rmSync(temporary, { force: true });
        throw error;
    }
}

async function renderStills(args) {
    const glbPath = path.resolve(args.glb);
    const outputPath = path.resolve(args.output);
    if (!fs.existsSync(glbPath)) fail(`glb not found: ${glbPath}`);
    const targets = args.views.map((view) => ({ view, file: args.single ? outputPath : stillOutputPath(outputPath, view) }));
    if (targets.some(({ file }) => file === glbPath)) fail(`refusing to overwrite the input glb ${glbPath}`);
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    const chrome = findChrome(args.chrome);
    const views = Object.fromEntries(args.views.map((view) => [view, STILL_VIEWS[view]]));

    let server;
    let runtime;
    let client;
    try {
        const harness = await startHarnessServer({
            glbPath,
            html: stillHarnessHtml(),
            config: { size: args.size, ortho: args.ortho, margin: args.margin, background: args.background, views },
        });
        server = harness.server;
        runtime = await launchChrome(chrome);
        client = new CdpClient(runtime.pageWebSocketUrl);
        await client.open();
        await client.command('Page.enable');
        await client.command('Runtime.enable');
        await client.command('Emulation.setDeviceMetricsOverride', { width: args.size, height: args.size, deviceScaleFactor: 1, mobile: false });
        await client.command('Page.navigate', { url: harness.url });

        const report = await waitForHarness(client, runtime);
        const pose = report.animationCount || report.skinnedMeshCount
            ? `; rest pose, ${report.animationCount} animation clip(s) not played`
            : '';
        console.log(`[glb_turntable] model loaded: ${report.meshCount} mesh(es), ${report.triangleCount} tris, radius ${Number(report.boundingRadius).toFixed(3)}${pose}`);
        const camera = args.ortho ? 'orthographic' : 'perspective';
        for (const { view, file } of targets) {
            const started = Date.now();
            const rendered = await evaluate(client, `window.__renderStill(${JSON.stringify(view)})`);
            writeFileAtomic(file, pngFromDataUrl(rendered, args.size, view));
            console.log(`[glb_turntable] wrote ${file} (${view}, ${args.size}px ${camera}, margin ${args.margin}, background ${args.background}, ${Date.now() - started} ms)`);
        }
    } finally {
        client?.close();
        await stopChrome(runtime);
        if (server) await new Promise((resolve) => server.close(resolve));
    }
}

async function main() {
    const args = parseArgs(process.argv);
    if (args.mode === 'still') {
        await renderStills(args);
        return;
    }
    const glbPath = path.resolve(args.glb);
    const outputPath = path.resolve(args.output);
    if (!fs.existsSync(glbPath)) fail(`glb not found: ${glbPath}`);
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    const chrome = findChrome(args.chrome);
    const frameCount = Math.round(args.seconds * args.fps);
    const framesDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'renderfin-frames-'));

    let server;
    let runtime;
    let client;
    try {
        const harness = await startHarnessServer({ glbPath, size: args.size });
        server = harness.server;
        runtime = await launchChrome(chrome);
        client = new CdpClient(runtime.pageWebSocketUrl);
        await client.open();
        await client.command('Page.enable');
        await client.command('Runtime.enable');
        await client.command('Emulation.setDeviceMetricsOverride', { width: args.size, height: args.size, deviceScaleFactor: 1, mobile: false });
        await client.command('Page.navigate', { url: harness.url });

        const report = await waitForHarness(client, runtime);
        console.log(`[glb_turntable] model loaded: ${report.meshCount} mesh(es), ${report.triangleCount} tris, radius ${Number(report.boundingRadius).toFixed(3)}`);

        for (let frameIndex = 0; frameIndex < frameCount; frameIndex += 1) {
            const rendered = await evaluate(client, `window.__renderTurntableFrame(${frameIndex}, ${frameCount})`);
            if (!rendered?.dataUrl?.startsWith('data:image/png;base64,')) bail(`frame ${frameIndex} render failed`);
            const png = Buffer.from(rendered.dataUrl.slice('data:image/png;base64,'.length), 'base64');
            fs.writeFileSync(path.join(framesDirectory, `frame_${String(frameIndex).padStart(4, '0')}.png`), png);
        }
        encodeMp4({ ffmpeg: args.ffmpeg, framesDirectory, outputPath, fps: args.fps, frameCount });
        console.log(`[glb_turntable] wrote ${outputPath} (${frameCount} frames @ ${args.fps} fps)`);
    } finally {
        client?.close();
        await stopChrome(runtime);
        if (server) await new Promise((resolve) => server.close(resolve));
        fs.rmSync(framesDirectory, { recursive: true, force: true });
    }
}

main().catch((error) => fail(error instanceof RenderFailure ? error.message : String(error?.stack || error)));
