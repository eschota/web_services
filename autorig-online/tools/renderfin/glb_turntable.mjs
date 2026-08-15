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

function parseArgs(argv) {
    const args = { seconds: 6, fps: 30, size: 768, ffmpeg: 'ffmpeg', chrome: '' };
    for (let i = 2; i < argv.length; i += 2) {
        const key = argv[i];
        const value = argv[i + 1];
        if (value === undefined) fail(`missing value for ${key}`);
        switch (key) {
            case '--glb': args.glb = value; break;
            case '--output': args.output = value; break;
            case '--seconds': args.seconds = Number(value); break;
            case '--fps': args.fps = Number(value); break;
            case '--size': args.size = Number(value); break;
            case '--chrome': args.chrome = value; break;
            case '--ffmpeg': args.ffmpeg = value; break;
            default: fail(`unknown argument ${key}`);
        }
    }
    if (!args.glb || !args.output) fail('usage: --glb <model.glb> --output <out.mp4>');
    if (!Number.isFinite(args.seconds) || args.seconds <= 0 || args.seconds > 60) fail('--seconds must be in (0, 60]');
    if (!Number.isFinite(args.fps) || args.fps < 1 || args.fps > 60) fail('--fps must be in [1, 60]');
    if (!Number.isInteger(args.size) || args.size < 64 || args.size > 2048 || args.size % 2 !== 0) fail('--size must be an even integer in [64, 2048]');
    return args;
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

function startHarnessServer({ glbPath, size }) {
    const routes = new Map([
        ['/index.html', { buffer: Buffer.from(harnessHtml(), 'utf8'), type: 'text/html; charset=utf-8' }],
        ['/config.json', { buffer: Buffer.from(JSON.stringify({ size }), 'utf8'), type: 'application/json; charset=utf-8' }],
        ['/three.module.js', { filename: path.join(VENDOR, 'three.module.js'), type: 'text/javascript; charset=utf-8' }],
        ['/GLTFLoader.js', { filename: path.join(VENDOR, 'GLTFLoader.js'), type: 'text/javascript; charset=utf-8' }],
        ['/utils/BufferGeometryUtils.js', { filename: path.join(VENDOR, 'BufferGeometryUtils.js'), type: 'text/javascript; charset=utf-8' }],
        ['/model.glb', { filename: glbPath, type: 'model/gltf-binary' }],
    ]);
    for (const route of routes.values()) {
        if (route.filename) {
            if (!fs.existsSync(route.filename)) fail(`missing harness file ${route.filename}`);
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
    const started = Date.now();
    while (!websocketUrl && Date.now() - started < 20000) {
        if (child.exitCode != null) fail(`Chrome exited during startup (${child.exitCode}): ${stderr}`);
        await delay(50);
    }
    if (!websocketUrl) fail(`Chrome did not expose CDP: ${stderr}`);
    const endpoint = new URL(websocketUrl);
    const pages = await (await fetch(`http://${endpoint.host}/json/list`)).json();
    const page = pages.find((entry) => entry.type === 'page');
    if (!page?.webSocketDebuggerUrl) fail('Chrome did not expose a page target');
    return { child, profileDirectory, pageWebSocketUrl: page.webSocketDebuggerUrl, stderr: () => stderr };
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
        fail(`ffmpeg encode failed: ${result.error?.message || String(result.stderr || '').trim() || `exit ${result.status}`}`);
    }
    if (!fs.existsSync(outputPath) || fs.statSync(outputPath).size < 1024) fail('ffmpeg produced no output');
}

async function main() {
    const args = parseArgs(process.argv);
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

        const started = Date.now();
        let report;
        while (Date.now() - started < 60000) {
            const state = await evaluate(client, `({ready:window.__AUTORIG_READY__===true,error:window.__AUTORIG_ERROR__||null,result:window.__AUTORIG_RESULT__||null})`);
            if (state?.error) fail(`harness failed: ${state.error}`);
            if (state?.ready) { report = state.result; break; }
            await delay(100);
        }
        if (!report) fail(`harness timed out: ${runtime.stderr()}`);
        console.log(`[glb_turntable] model loaded: ${report.meshCount} mesh(es), ${report.triangleCount} tris, radius ${Number(report.boundingRadius).toFixed(3)}`);

        for (let frameIndex = 0; frameIndex < frameCount; frameIndex += 1) {
            const rendered = await evaluate(client, `window.__renderTurntableFrame(${frameIndex}, ${frameCount})`);
            if (!rendered?.dataUrl?.startsWith('data:image/png;base64,')) fail(`frame ${frameIndex} render failed`);
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

main().catch((error) => fail(String(error?.stack || error)));
