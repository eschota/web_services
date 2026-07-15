#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

import { validateImmutableInputs } from './browser_fit_canary.mjs';

const TOOL_FILE = fileURLToPath(import.meta.url);
const TOOL_DIRECTORY = path.dirname(TOOL_FILE);
const AUTORIG_ONLINE = path.resolve(TOOL_DIRECTORY, '..', '..');
const ADAPTER_FILE = path.join(AUTORIG_ONLINE, 'static', 'js', 'animation-fitting-three-adapter.js');
const AUTHOR_FILE = path.join(AUTORIG_ONLINE, 'static', 'js', 'animation-fitting-horse-swing-guide-author.js');
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const WIDTH = 768;
const HEIGHT = 448;
const GUIDE_FRAMES = Object.freeze([0, 6, 18, 30, 42, 48]);
const SWING_FRAMES = Object.freeze([6, 18, 30, 42]);

function fail(message) {
    throw new Error(message);
}

function nonEmpty(value, field) {
    const result = String(value || '').trim();
    if (!result) fail(`${field} is required`);
    return result;
}

function existingFile(value, field) {
    const result = path.resolve(nonEmpty(value, field));
    if (!fs.statSync(result, { throwIfNoEntry: false })?.isFile()) fail(`${field} is not a file: ${result}`);
    return result;
}

function existingDirectory(value, field) {
    const result = path.resolve(nonEmpty(value, field));
    if (!fs.statSync(result, { throwIfNoEntry: false })?.isDirectory()) fail(`${field} is not a directory: ${result}`);
    return result;
}

function sha256Buffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function sha256File(filename) {
    return sha256Buffer(fs.readFileSync(filename));
}

function pinFile(filename, extra = {}) {
    const stats = fs.statSync(filename);
    return {
        filename: path.basename(filename),
        bytes: stats.size,
        sha256: sha256File(filename),
        ...extra,
    };
}

function readJson(filename, field) {
    try {
        return JSON.parse(fs.readFileSync(filename, 'utf8'));
    } catch (error) {
        fail(`${field} is not valid JSON: ${error.message}`);
    }
}

function readGzipJson(filename, field) {
    try {
        return JSON.parse(zlib.gunzipSync(fs.readFileSync(filename)).toString('utf8'));
    } catch (error) {
        fail(`${field} is not valid gzip JSON: ${error.message}`);
    }
}

function writeJson(filename, value) {
    const buffer = Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
    fs.writeFileSync(filename, buffer, { flag: 'wx' });
}

function outputDirectory(value) {
    const result = path.resolve(nonEmpty(value, 'output'));
    if (fs.existsSync(result)) fail(`output must not already exist: ${result}`);
    return result;
}

function parseArguments(argv) {
    const result = {};
    for (let index = 0; index < argv.length; index += 1) {
        const argument = argv[index];
        if (argument === '--synthetic-smoke') {
            result.syntheticSmoke = true;
            continue;
        }
        if (!argument.startsWith('--')) fail(`unexpected argument: ${argument}`);
        const key = argument.slice(2).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) fail(`${argument} requires a value`);
        result[key] = argv[++index];
    }
    return result;
}

function mime(filename) {
    if (filename.endsWith('.js')) return 'text/javascript; charset=utf-8';
    if (filename.endsWith('.json')) return 'application/json; charset=utf-8';
    if (filename.endsWith('.html')) return 'text/html; charset=utf-8';
    return 'application/octet-stream';
}

function harnessHtml() {
    return `<!doctype html>
<html><head><meta charset="utf-8"><style>
html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#717b86}canvas{display:block}
</style></head><body><script type="module">
import * as THREE from '/three.module.js';
import { buildHorse2BrowserFittingSkeleton, bakeFittedAnimationToThreeHierarchyClip, createViewerToLtxProjection } from '/adapter.js';
import { authorHorseV10SwingGuidePoses, verifyHorseV10PostBakeHoofProjections } from '/author.js';

const config = await (await fetch('/config.json', { cache: 'no-store' })).json();

function matrix4(values, field) {
    if (!Array.isArray(values) || values.length !== 16 || values.some((value) => !Number.isFinite(Number(value)))) {
        throw new Error(field + ' must contain 16 finite numbers');
    }
    return new THREE.Matrix4().set(...values.map(Number));
}

function buildBundleModel(sourceSkeleton) {
    if (!Array.isArray(sourceSkeleton.armatures) || sourceSkeleton.armatures.length !== 1) {
        throw new Error('source skeleton must contain one armature');
    }
    const armature = sourceSkeleton.armatures[0];
    const model = new THREE.Group();
    model.name = 'AutoRig_Browser_Horse_Guide_Model';
    const armatureMatrix = matrix4(armature.matrix_world, 'armature.matrix_world');
    armatureMatrix.decompose(model.position, model.quaternion, model.scale);
    const bones = new Map();
    for (const source of armature.bones) {
        if (bones.has(source.name)) throw new Error('duplicate source bone ' + source.name);
        const bone = new THREE.Bone();
        bone.name = source.name;
        bone.userData.use_deform = source.use_deform === true;
        bone.userData.tailWorld = new THREE.Vector3(...source.tail_local).applyMatrix4(armatureMatrix).toArray();
        matrix4(source.parent ? source.parent_relative_matrix : source.matrix_local, source.name + '.local')
            .decompose(bone.position, bone.quaternion, bone.scale);
        bones.set(source.name, bone);
    }
    for (const source of armature.bones) {
        const bone = bones.get(source.name);
        if (source.parent) {
            const parent = bones.get(source.parent);
            if (!parent) throw new Error('missing parent ' + source.parent);
            parent.add(bone);
        } else {
            model.add(bone);
        }
    }
    model.updateWorldMatrix(true, true);
    let maximumHeadReconstructionErrorWorld = 0;
    for (const source of armature.bones) {
        const expected = new THREE.Vector3(...source.head_local).applyMatrix4(armatureMatrix);
        const actual = bones.get(source.name).getWorldPosition(new THREE.Vector3());
        maximumHeadReconstructionErrorWorld = Math.max(maximumHeadReconstructionErrorWorld, actual.distanceTo(expected));
    }
    return { model, bones, sourceBones: armature.bones, maximumHeadReconstructionErrorWorld };
}

function buildBundleCamera(contract) {
    const [width, height] = contract.resolution;
    const { fx, fy, cx, cy } = contract.intrinsics;
    const near = 0.01;
    const far = 1000;
    const camera = new THREE.PerspectiveCamera();
    camera.matrixAutoUpdate = false;
    camera.matrix.copy(matrix4(contract.camera_to_world, 'camera_to_world'));
    camera.matrixWorld.copy(camera.matrix);
    camera.matrixWorldInverse.copy(matrix4(contract.world_to_camera, 'world_to_camera'));
    camera.projectionMatrix.set(
        2 * fx / width, 0, 1 - 2 * cx / width, 0,
        0, 2 * fy / height, 2 * cy / height - 1, 0,
        0, 0, (far + near) / (near - far), 2 * far * near / (near - far),
        0, 0, -1, 0,
    );
    camera.projectionMatrixInverse.copy(camera.projectionMatrix).invert();
    camera.updateProjectionMatrix = () => {};
    camera.updateWorldMatrix(true, false);
    return camera;
}

function buildSkinnedMesh(modelState, weights, topology) {
    if (!Array.isArray(weights.vertices) || weights.vertices.length !== 344) {
        throw new Error('skin weights must contain exactly 344 Horse_2 vertices');
    }
    const vertices = [...weights.vertices].sort((a, b) => a.vertex_index - b.vertex_index);
    vertices.forEach((vertex, index) => {
        if (vertex.vertex_index !== index || vertex.vertex_id !== index) throw new Error('skin vertices must be dense and ordered');
    });
    const boneOrder = modelState.sourceBones.map((source) => modelState.bones.get(source.name));
    const boneIndex = new Map(boneOrder.map((bone, index) => [bone.name, index]));
    const positions = [];
    const skinIndices = [];
    const skinWeights = [];
    for (const vertex of vertices) {
        positions.push(...vertex.local.map(Number));
        const influences = vertex.weights
            .filter((entry) => Number(entry.weight) > 0)
            .sort((a, b) => Number(b.weight) - Number(a.weight))
            .slice(0, 4);
        if (!influences.length) throw new Error('vertex ' + vertex.vertex_index + ' has no positive skin influence');
        const sum = influences.reduce((total, entry) => total + Number(entry.weight), 0);
        while (influences.length < 4) influences.push({ bone: influences[0].bone, weight: 0 });
        for (const influence of influences) {
            if (!boneIndex.has(influence.bone)) throw new Error('skin influence bone is missing: ' + influence.bone);
            skinIndices.push(boneIndex.get(influence.bone));
            skinWeights.push(Number(influence.weight) / sum);
        }
    }
    if (!Array.isArray(topology.faces) || topology.faces.length !== 258) {
        throw new Error('surface topology must contain exactly 258 Horse_2 faces');
    }
    const indices = [];
    for (const face of topology.faces) {
        const ids = face.vertex_ids;
        if (!Array.isArray(ids) || ids.length < 3) throw new Error('surface face has fewer than 3 vertices');
        for (let index = 1; index < ids.length - 1; index += 1) indices.push(ids[0], ids[index], ids[index + 1]);
    }
    const geometry = new THREE.BufferGeometry();
    geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
    geometry.setAttribute('skinIndex', new THREE.Uint16BufferAttribute(skinIndices, 4));
    geometry.setAttribute('skinWeight', new THREE.Float32BufferAttribute(skinWeights, 4));
    geometry.setIndex(indices);
    geometry.computeVertexNormals();
    geometry.computeBoundingSphere();
    const material = new THREE.MeshStandardMaterial({
        color: 0xe8e8e8,
        roughness: 0.74,
        metalness: 0,
        flatShading: true,
        side: THREE.DoubleSide,
    });
    const mesh = new THREE.SkinnedMesh(geometry, material);
    mesh.name = 'Horse_geo_browser_344v';
    // LTX pose guides must encode skeletal motion, not a second changing
    // signal from hard WebGL shadow-map wedges. The immutable endpoints retain
    // the canonical reference render and its matte shadow byte-for-byte.
    mesh.castShadow = false;
    mesh.receiveShadow = false;
    const skeleton = new THREE.Skeleton(boneOrder);
    modelState.model.add(mesh);
    modelState.model.updateWorldMatrix(true, true);
    mesh.bind(skeleton, new THREE.Matrix4());
    return { mesh, skeleton, triangleCount: indices.length / 3 };
}

function webglInfo(renderer) {
    const gl = renderer.getContext();
    const debug = gl.getExtension('WEBGL_debug_renderer_info');
    return {
        isWebGL2: renderer.capabilities.isWebGL2 === true,
        version: gl.getParameter(gl.VERSION),
        shadingLanguageVersion: gl.getParameter(gl.SHADING_LANGUAGE_VERSION),
        vendor: debug ? gl.getParameter(debug.UNMASKED_VENDOR_WEBGL) : gl.getParameter(gl.VENDOR),
        renderer: debug ? gl.getParameter(debug.UNMASKED_RENDERER_WEBGL) : gl.getParameter(gl.RENDERER),
        threeRevision: THREE.REVISION,
    };
}

function makeRenderer(width, height) {
    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, preserveDrawingBuffer: true });
    renderer.setPixelRatio(1);
    renderer.setSize(width, height, false);
    renderer.setClearColor(0x717b86, 1);
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.1;
    renderer.shadowMap.enabled = false;
    document.body.replaceChildren(renderer.domElement);
    return renderer;
}

function makeScene(model, groundHeight) {
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x717b86);
    scene.add(model);
    scene.add(new THREE.HemisphereLight(0xe9f1ff, 0x3f4650, 2.1));
    const key = new THREE.DirectionalLight(0xffffff, 3.5);
    key.position.set(4.5, -5.5, 8.5);
    key.castShadow = false;
    scene.add(key);
    scene.add(key.target);
    const ground = new THREE.Mesh(
        new THREE.PlaneGeometry(50, 50),
        new THREE.MeshStandardMaterial({ color: 0xb8c3cc, roughness: 0.92, metalness: 0 }),
    );
    ground.position.z = Number(groundHeight);
    ground.receiveShadow = false;
    scene.add(ground);
    return scene;
}

async function initializeReal() {
    const modelState = buildBundleModel(config.sourceSkeleton);
    const camera = buildBundleCamera(config.fittingBundle.camera);
    const skin = buildSkinnedMesh(modelState, config.skinWeights, config.surfaceTopology);
    const fittingSkeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model: modelState.model,
        camera,
        sourceViewport: config.fittingBundle.camera.resolution,
        referenceResolution: config.fittingBundle.camera.resolution,
        outputResolution: [${WIDTH}, ${HEIGHT}],
        includePositionMappings: 'auto',
    });
    const poseContract = authorHorseV10SwingGuidePoses({
        skeleton: fittingSkeleton,
        candidateA: config.candidateA,
        candidateB: config.candidateB,
    });
    const hierarchy = bakeFittedAnimationToThreeHierarchyClip({
        THREE,
        model: modelState.model,
        camera,
        skeleton: fittingSkeleton,
        fitted: poseContract.fitted,
        outputResolution: [${WIDTH}, ${HEIGHT}],
        name: 'Horse_Walk_v10_Browser_Swing_Guides',
    });
    if (hierarchy.clip.validate() !== true) throw new Error('Three hierarchy clip validation failed');
    const scene = makeScene(modelState.model, config.fittingBundle.ground_plane.height);
    const renderer = makeRenderer(${WIDTH}, ${HEIGHT});
    const mixer = new THREE.AnimationMixer(modelState.model);
    const action = mixer.clipAction(hierarchy.clip);
    action.setLoop(THREE.LoopOnce, 1);
    action.clampWhenFinished = true;
    action.play();
    const applyAuthoredTime = (timeSeconds) => {
        // Sampling frame 48 completes a LoopOnce action. Explicitly re-enable
        // it before every non-monotonic CDP render so later frame requests do
        // not silently reuse the endpoint pose.
        action.enabled = true;
        action.paused = false;
        action.setEffectiveWeight(1);
        mixer.setTime(timeSeconds);
    };
    const projection = createViewerToLtxProjection({
        sourceViewport: config.fittingBundle.camera.resolution,
        referenceResolution: config.fittingBundle.camera.resolution,
        outputResolution: [${WIDTH}, ${HEIGHT}],
    });
    const terminalBones = Object.fromEntries(Object.entries(fittingSkeleton.limbs).map(([limb, value]) => [
        limb,
        modelState.bones.get(value.terminalBone),
    ]));
    const sample = (guide) => {
        applyAuthoredTime(guide.authoredClipTimeSeconds);
        modelState.model.updateWorldMatrix(true, true);
        const hooves = Object.fromEntries(Object.entries(terminalBones).map(([limb, bone]) => {
            const world = bone.getWorldPosition(new THREE.Vector3());
            const ndc = world.project(camera);
            return [limb, projection.ndcToOutput([ndc.x, ndc.y, ndc.z])];
        }));
        return { frameIndex: guide.frameIndex, hooves };
    };
    const projectedHoovesByGuide = poseContract.guides.map(sample);
    const postBakeQa = verifyHorseV10PostBakeHoofProjections({
        poseContract,
        projectedHoovesByGuide,
        maximumStanceErrorPx: 1,
        // The actual canary worst-case requested terminal error is 2.27 px
        // (2.45 px across every fitted chain head). Three pixels is therefore
        // a strict, measured guard with no broad visual-error allowance.
        maximumRequestedErrorPx: 3,
        minimumSwingLiftPx: 5,
    });
    const info = webglInfo(renderer);
    window.__renderGuide = async (frameIndex) => {
        const guide = poseContract.guides.find((value) => value.frameIndex === frameIndex);
        if (!guide) throw new Error('unknown guide frame ' + frameIndex);
        applyAuthoredTime(guide.authoredClipTimeSeconds);
        modelState.model.updateWorldMatrix(true, true);
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
        mode: 'real',
        poseContract,
        hierarchyQa: hierarchy.qa,
        postBakeQa,
        webgl: info,
        model: {
            sourceBoneCount: modelState.sourceBones.length,
            vertexCount: skin.mesh.geometry.getAttribute('position').count,
            sourceFaceCount: config.surfaceTopology.faces.length,
            triangleCount: skin.triangleCount,
            skinBoneCount: skin.skeleton.bones.length,
            maximumHeadReconstructionErrorWorld: modelState.maximumHeadReconstructionErrorWorld,
        },
    };
}

async function initializeSmoke() {
    const renderer = makeRenderer(64, 64);
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x102030);
    const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 10);
    camera.position.z = 2;
    const geometry = new THREE.BufferGeometry();
    geometry.setAttribute('position', new THREE.Float32BufferAttribute([-0.8,-0.7,0, 0.8,-0.7,0, 0,0.8,0], 3));
    const triangle = new THREE.Mesh(geometry, new THREE.MeshBasicMaterial({ color: 0x50e090 }));
    scene.add(triangle);
    renderer.render(scene, camera);
    window.__renderGuide = async () => ({
        frameIndex: 0,
        width: renderer.domElement.width,
        height: renderer.domElement.height,
        dataUrl: renderer.domElement.toDataURL('image/png'),
    });
    window.__AUTORIG_RESULT__ = { mode: 'synthetic-smoke', webgl: webglInfo(renderer), vertexCount: 3 };
}

try {
    if (config.mode === 'synthetic-smoke') await initializeSmoke();
    else await initializeReal();
    window.__AUTORIG_READY__ = true;
} catch (error) {
    window.__AUTORIG_ERROR__ = String(error?.stack || error);
    console.error(error);
}
</script></body></html>`;
}

function startHarnessServer({ config, threeModule }) {
    const routes = new Map([
        ['/index.html', { type: 'buffer', body: Buffer.from(harnessHtml(), 'utf8'), contentType: 'text/html; charset=utf-8' }],
        ['/config.json', { type: 'buffer', body: Buffer.from(JSON.stringify(config), 'utf8'), contentType: 'application/json; charset=utf-8' }],
        ['/three.module.js', { type: 'file', filename: threeModule }],
        ['/adapter.js', { type: 'file', filename: ADAPTER_FILE }],
        ['/author.js', { type: 'file', filename: AUTHOR_FILE }],
    ]);
    const server = http.createServer((request, response) => {
        const route = routes.get(new URL(request.url, 'http://127.0.0.1').pathname);
        response.setHeader('Cache-Control', 'no-store');
        if (!route) {
            response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
            response.end('not found');
            return;
        }
        if (route.type === 'file') {
            response.writeHead(200, { 'Content-Type': mime(route.filename) });
            fs.createReadStream(route.filename).pipe(response);
        } else {
            response.writeHead(200, { 'Content-Type': route.contentType });
            response.end(route.body);
        }
    });
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => resolve({
            server,
            url: `http://127.0.0.1:${server.address().port}/index.html`,
        }));
    });
}

class CdpClient {
    constructor(url) {
        this.socket = new WebSocket(url);
        this.nextId = 1;
        this.pending = new Map();
        this.events = [];
        this.socket.onmessage = (event) => {
            const message = JSON.parse(event.data);
            if (message.id) {
                const pending = this.pending.get(message.id);
                if (!pending) return;
                this.pending.delete(message.id);
                if (message.error) pending.reject(new Error(message.error.message));
                else pending.resolve(message.result || {});
            } else {
                this.events.push(message);
            }
        };
    }

    async open() {
        if (this.socket.readyState === WebSocket.OPEN) return;
        await new Promise((resolve, reject) => {
            this.socket.onopen = resolve;
            this.socket.onerror = () => reject(new Error('CDP WebSocket connection failed'));
        });
    }

    command(method, params = {}) {
        const id = this.nextId++;
        return new Promise((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            this.socket.send(JSON.stringify({ id, method, params }));
        });
    }

    close() {
        this.socket.close();
    }
}

function delay(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function launchChrome(chromeExecutable) {
    const profileDirectory = fs.mkdtempSync(path.join(process.env.TEMP || process.cwd(), 'autorig-horse-guide-chrome-'));
    const args = [
        '--headless=new',
        '--use-angle=swiftshader',
        '--enable-webgl',
        '--ignore-gpu-blocklist',
        '--disable-background-networking',
        '--disable-component-update',
        '--disable-default-apps',
        '--disable-extensions',
        '--disable-sync',
        '--no-first-run',
        '--no-default-browser-check',
        '--remote-debugging-address=127.0.0.1',
        '--remote-debugging-port=0',
        `--user-data-dir=${profileDirectory}`,
        'about:blank',
    ];
    const child = spawn(chromeExecutable, args, { stdio: ['ignore', 'ignore', 'pipe'], windowsHide: true });
    let stderr = '';
    let websocketUrl = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => {
        stderr += chunk;
        const match = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/);
        if (match) websocketUrl = match[1];
    });
    const started = Date.now();
    while (!websocketUrl && Date.now() - started < 15000) {
        if (child.exitCode != null) fail(`Chrome exited before CDP startup (${child.exitCode}): ${stderr}`);
        await delay(50);
    }
    if (!websocketUrl) fail(`Chrome did not expose CDP within 15 seconds: ${stderr}`);
    const endpoint = new URL(websocketUrl);
    const pages = await (await fetch(`http://${endpoint.host}/json/list`)).json();
    const page = pages.find((value) => value.type === 'page');
    if (!page?.webSocketDebuggerUrl) fail('Chrome did not expose an initial page target');
    return { child, profileDirectory, stderr: () => stderr, pageWebSocketUrl: page.webSocketDebuggerUrl };
}

async function stopChrome(runtime) {
    if (!runtime) return;
    try {
        if (runtime.child.exitCode == null) runtime.child.kill();
        await Promise.race([
            new Promise((resolve) => runtime.child.once('exit', resolve)),
            delay(3000),
        ]);
        if (runtime.child.exitCode == null) runtime.child.kill('SIGKILL');
    } finally {
        fs.rmSync(runtime.profileDirectory, { recursive: true, force: true });
    }
}

async function evaluate(client, expression) {
    const result = await client.command('Runtime.evaluate', {
        expression,
        awaitPromise: true,
        returnByValue: true,
    });
    if (result.exceptionDetails) fail(`browser evaluation failed: ${result.exceptionDetails.text}`);
    return result.result?.value;
}

async function runHarnessInChrome({ chromeExecutable, url, guideFrames }) {
    let runtime;
    let client;
    try {
        runtime = await launchChrome(chromeExecutable);
        client = new CdpClient(runtime.pageWebSocketUrl);
        await client.open();
        await client.command('Page.enable');
        await client.command('Runtime.enable');
        await client.command('Emulation.setDeviceMetricsOverride', {
            width: WIDTH,
            height: HEIGHT,
            deviceScaleFactor: 1,
            mobile: false,
        });
        await client.command('Page.navigate', { url });
        const started = Date.now();
        let result;
        while (Date.now() - started < 30000) {
            const state = await evaluate(client, `({ready:window.__AUTORIG_READY__===true,error:window.__AUTORIG_ERROR__||null,result:window.__AUTORIG_RESULT__||null})`);
            if (state?.error) fail(`browser harness failed: ${state.error}`);
            if (state?.ready) {
                result = state.result;
                break;
            }
            await delay(100);
        }
        if (!result) fail(`browser harness did not become ready within 30 seconds: ${runtime.stderr()}`);
        const renders = [];
        for (const frameIndex of guideFrames) {
            const rendered = await evaluate(client, `window.__renderGuide(${Number(frameIndex)})`);
            if (!rendered?.dataUrl?.startsWith('data:image/png;base64,')) fail(`guide ${frameIndex} did not return PNG data`);
            renders.push(rendered);
        }
        const version = await client.command('Browser.getVersion');
        return { result, renders, browserVersion: version };
    } finally {
        client?.close();
        await stopChrome(runtime);
    }
}

function pngDimensions(buffer) {
    const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
    if (buffer.length < 24 || !buffer.subarray(0, 8).equals(signature)) fail('rendered output is not PNG');
    return [buffer.readUInt32BE(16), buffer.readUInt32BE(20)];
}

function sourceVideoPin(observations, label) {
    const filename = existingFile(observations.provenance?.source_video, `${label} source video`);
    const pin = pinFile(filename, { path: filename });
    const expected = String(observations.provenance?.source_video_sha256 || '').toLowerCase();
    if (!SHA256_PATTERN.test(expected) || pin.sha256 !== expected) fail(`${label} source video SHA-256 mismatch`);
    return pin;
}

function immutableEntry(manifest, filename) {
    const entry = manifest.files.find((value) => value.filename === filename);
    if (!entry) fail(`immutable manifest does not pin ${filename}`);
    return { filename, bytes: entry.bytes, sha256: entry.sha256 };
}

async function runSyntheticSmoke(config) {
    const output = outputDirectory(config.output);
    const chromeExecutable = existingFile(config.chrome, 'chrome');
    const threeModule = existingFile(config.three, 'three');
    const { server, url } = await startHarnessServer({ config: { mode: 'synthetic-smoke' }, threeModule });
    try {
        const browser = await runHarnessInChrome({ chromeExecutable, url, guideFrames: [0] });
        const render = browser.renders[0];
        const png = Buffer.from(render.dataUrl.slice('data:image/png;base64,'.length), 'base64');
        const dimensions = pngDimensions(png);
        if (dimensions[0] !== 64 || dimensions[1] !== 64) fail(`synthetic PNG dimensions are ${dimensions.join('x')}`);
        fs.mkdirSync(output, { recursive: false });
        const pngPath = path.join(output, 'synthetic-webgl-smoke.png');
        fs.writeFileSync(pngPath, png, { flag: 'wx' });
        const report = {
            schema: 'autorig-browser-webgl-synthetic-smoke.v1',
            status: 'PASS',
            browserOnly: true,
            blenderUsed: false,
            chrome: pinFile(chromeExecutable, { product: browser.browserVersion.product, protocolVersion: browser.browserVersion.protocolVersion }),
            three: pinFile(threeModule, { revision: browser.result.webgl.threeRevision }),
            webgl: browser.result.webgl,
            model: { vertexCount: browser.result.vertexCount },
            output: pinFile(pngPath, { width: dimensions[0], height: dimensions[1] }),
        };
        writeJson(path.join(output, 'report.json'), report);
        return { output, report };
    } finally {
        await new Promise((resolve) => server.close(resolve));
    }
}

async function runReal(config) {
    const output = outputDirectory(config.output);
    const bundleDirectory = existingDirectory(config.bundle, 'bundle');
    const candidateAPath = existingFile(config.candidateA, 'candidate-a');
    const candidateBPath = existingFile(config.candidateB, 'candidate-b');
    const chromeExecutable = existingFile(config.chrome, 'chrome');
    const threeModule = existingFile(config.three, 'three');
    const candidateAValidated = validateImmutableInputs({ bundleDirectory, observationsPath: candidateAPath });
    const candidateBValidated = validateImmutableInputs({ bundleDirectory, observationsPath: candidateBPath });
    if (candidateAValidated.integrity.fittingBundleSha256 !== candidateBValidated.integrity.fittingBundleSha256) {
        fail('candidate observations do not pin the same fitting bundle');
    }
    const manifest = candidateAValidated.immutableManifest;
    const topologyEntry = immutableEntry(manifest, 'surface_topology.json.gz');
    const weightsEntry = immutableEntry(manifest, 'skin_weights.json.gz');
    const referenceEntry = immutableEntry(manifest, 'reference_rgb.png');
    const topologyPath = path.join(bundleDirectory, topologyEntry.filename);
    const weightsPath = path.join(bundleDirectory, weightsEntry.filename);
    const referencePath = path.join(bundleDirectory, referenceEntry.filename);
    const sourceVideoA = sourceVideoPin(candidateAValidated.observations, 'candidate A');
    const sourceVideoB = sourceVideoPin(candidateBValidated.observations, 'candidate B');
    const harnessConfig = {
        mode: 'real',
        fittingBundle: candidateAValidated.fittingBundle,
        sourceSkeleton: candidateAValidated.skeleton,
        surfaceTopology: readGzipJson(topologyPath, 'surface topology'),
        skinWeights: readGzipJson(weightsPath, 'skin weights'),
        candidateA: candidateAValidated.observations,
        candidateB: candidateBValidated.observations,
    };
    const { server, url } = await startHarnessServer({ config: harnessConfig, threeModule });
    let browser;
    try {
        browser = await runHarnessInChrome({ chromeExecutable, url, guideFrames: SWING_FRAMES });
    } finally {
        await new Promise((resolve) => server.close(resolve));
    }
    if (browser.result.mode !== 'real') fail('browser returned the wrong harness mode');
    if (browser.result.webgl.threeRevision !== '160') fail(`renderer used Three r${browser.result.webgl.threeRevision}, expected r160`);
    if (browser.result.model.vertexCount !== 344) fail('browser did not render the 344-vertex Horse_2 mesh');
    if (browser.result.model.sourceFaceCount !== 258) fail('browser did not use all 258 source faces');
    if (browser.result.postBakeQa?.status !== 'PASS') fail('post-bake hoof QA did not pass');
    if (browser.result.postBakeQa.guides.filter((guide) => guide.swingLimb).some((guide) => guide.stanceHoofCount !== 3)) {
        fail('a swing guide did not retain exactly three stance hooves');
    }
    const staging = `${output}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
    fs.mkdirSync(staging, { recursive: false });
    try {
        const reference = fs.readFileSync(referencePath);
        const referenceSha = sha256Buffer(reference);
        if (reference.length !== referenceEntry.bytes || referenceSha !== referenceEntry.sha256) fail('reference RGB pin changed after validation');
        const renderByFrame = new Map(browser.renders.map((render) => [render.frameIndex, render]));
        const guidePins = [];
        for (const frameIndex of GUIDE_FRAMES) {
            const filename = `guide_${String(frameIndex).padStart(3, '0')}.png`;
            const destination = path.join(staging, filename);
            const buffer = frameIndex === 0 || frameIndex === 48
                ? reference
                : Buffer.from(renderByFrame.get(frameIndex).dataUrl.slice('data:image/png;base64,'.length), 'base64');
            const [width, height] = pngDimensions(buffer);
            if (width !== WIDTH || height !== HEIGHT) fail(`guide ${frameIndex} is ${width}x${height}, expected ${WIDTH}x${HEIGHT}`);
            fs.writeFileSync(destination, buffer, { flag: 'wx' });
            const guide = browser.result.poseContract.guides.find((value) => value.frameIndex === frameIndex);
            guidePins.push(pinFile(destination, {
                frameIndex,
                role: guide.role,
                swingLimb: guide.swingLimb,
                strength: guide.strength,
                width,
                height,
                byteIdenticalReferenceCopy: frameIndex === 0 || frameIndex === 48,
            }));
        }
        if (guidePins[0].sha256 !== referenceSha || guidePins.at(-1).sha256 !== referenceSha) {
            fail('frame 0 and frame 48 are not byte-identical reference copies');
        }
        const swingHashes = guidePins.filter((guide) => SWING_FRAMES.includes(guide.frameIndex)).map((guide) => guide.sha256);
        if (new Set(swingHashes).size !== SWING_FRAMES.length || swingHashes.includes(referenceSha)) {
            fail(`all four swing guide PNGs must be distinct from each other and the reference: ${JSON.stringify(
                guidePins.map((guide) => [guide.frameIndex, guide.sha256]),
            )}`);
        }
        const poseContract = {
            ...browser.result.poseContract,
            status: 'PASS_RENDERED_BROWSER_GUIDES',
            renderer: {
                implementation: 'chromium_webgl_three_r160',
                webgl: browser.result.webgl,
            },
            browserRendererRequired: false,
            postBakeQa: browser.result.postBakeQa,
            hierarchyQa: browser.result.hierarchyQa,
        };
        const posePath = path.join(staging, 'pose_contract.json');
        writeJson(posePath, poseContract);
        const framesArray = guidePins.map((guide) => ({
            frame_index_int: guide.frameIndex,
            filename_string: guide.filename,
            sha256_string: guide.sha256,
            bytes_int: guide.bytes,
            strength_float: guide.strength,
        }));
        const manifestValue = {
            schema: 'autorig-browser-ltx-guide-bundle.v1',
            status: 'PASS',
            approvedForAnimationLibrary: false,
            browserOnly: true,
            blenderUsed: false,
            rigType: 'HORSE_2',
            resolution: [WIDTH, HEIGHT],
            source_reference_sha256_string: referenceSha,
            cycle_frame_count_int: 49,
            guide_count_int: guidePins.length,
            renderer_object: {
                renderer_string: 'browser_threejs',
                blender_used_bool: false,
            },
            frames_array: framesArray,
            source: {
                sourceModelSha256: candidateAValidated.integrity.sourceModelSha256,
                immutableManifest: pinFile(candidateAValidated.immutableManifestPath),
                fittingBundle: pinFile(candidateAValidated.fittingBundlePath),
                skeleton: pinFile(candidateAValidated.skeletonPath),
                surfaceTopology: topologyEntry,
                skinWeights: weightsEntry,
                referenceRgb: referenceEntry,
            },
            observations: {
                candidateA: pinFile(candidateAPath, { sourceVideo: sourceVideoA }),
                candidateB: pinFile(candidateBPath, { sourceVideo: sourceVideoB }),
            },
            renderer: {
                chrome: pinFile(chromeExecutable, {
                    product: browser.browserVersion.product,
                    protocolVersion: browser.browserVersion.protocolVersion,
                    userAgent: browser.browserVersion.userAgent,
                }),
                three: pinFile(threeModule, { revision: browser.result.webgl.threeRevision }),
                adapter: pinFile(ADAPTER_FILE),
                author: pinFile(AUTHOR_FILE),
                cli: pinFile(TOOL_FILE),
                webgl: browser.result.webgl,
            },
            model: browser.result.model,
            hierarchyQa: browser.result.hierarchyQa,
            postBakeQa: browser.result.postBakeQa,
            poseContract: pinFile(posePath),
            guides: guidePins,
        };
        writeJson(path.join(staging, 'immutable_manifest.json'), manifestValue);
        fs.renameSync(staging, output);
        return { output, manifest: manifestValue };
    } catch (error) {
        fs.rmSync(staging, { recursive: true, force: true });
        throw error;
    }
}

export async function main(argv = process.argv.slice(2)) {
    const config = parseArguments(argv);
    return config.syntheticSmoke ? runSyntheticSmoke(config) : runReal(config);
}

if (path.resolve(process.argv[1] || '') === path.resolve(TOOL_FILE)) {
    main().then((result) => {
        process.stdout.write(`${JSON.stringify({ status: 'PASS', output: result.output }, null, 2)}\n`);
    }).catch((error) => {
        process.stderr.write(`${String(error?.stack || error)}\n`);
        process.exitCode = 1;
    });
}
