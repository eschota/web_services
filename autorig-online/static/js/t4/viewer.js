// AutoRig task viewer (2026-11-10): three.js, PBR (IBL studio environment, ACES tone mapping, soft shadows).
// One GLB in, everything else is computed here: the voxel build effect, the skeleton overlay, the weight colours.
import * as THREE from 'three';
import { GLTFLoader } from 'three/addons/loaders/GLTFLoader.js';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { RoomEnvironment } from 'three/addons/environments/RoomEnvironment.js';
import { MeshoptDecoder } from 'three/addons/libs/meshopt_decoder.module.js';

const INTERNAL_CLIPS = new Set(['rig_check']);
const VOXEL_RES = 46;            // cells along the model's longest side
const MAX_SAMPLES = 260000;      // vertices looked at when voxelizing

const boneColor = (i, out = new THREE.Color()) => out.setHSL((i * 0.61803398875 + 0.07) % 1, 0.78, 0.52);
const ease = (t) => (t <= 0 ? 0 : t >= 1 ? 1 : t * t * (3 - 2 * t));

function discTexture(inner, outer) {
  const c = document.createElement('canvas');
  c.width = c.height = 128;
  const g = c.getContext('2d');
  const grad = g.createRadialGradient(64, 64, 0, 64, 64, 64);
  grad.addColorStop(0, inner);
  grad.addColorStop(1, outer);
  g.fillStyle = grad;
  g.fillRect(0, 0, 128, 128);
  const t = new THREE.CanvasTexture(c);
  t.colorSpace = THREE.SRGBColorSpace;
  return t;
}

function dotTexture() {
  const c = document.createElement('canvas');
  c.width = c.height = 64;
  const g = c.getContext('2d');
  g.beginPath(); g.arc(32, 32, 26, 0, Math.PI * 2); g.fillStyle = '#fff'; g.fill();
  g.lineWidth = 6; g.strokeStyle = 'rgba(0,0,0,.55)'; g.stroke();
  return new THREE.CanvasTexture(c);
}

async function fetchBuffer(url, onProgress, signal) {
  const res = await fetch(url, { credentials: 'same-origin', signal });
  if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { status: res.status });
  const total = Number(res.headers.get('content-length')) || 0;
  if (!res.body || !total) return res.arrayBuffer();
  const reader = res.body.getReader();
  const out = new Uint8Array(total);
  let got = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (got + value.length > out.length) {           // a compressed transfer: the length was the wire size
      const rest = await new Response(new ReadableStream({
        start(ctl) { ctl.enqueue(value); },
        async pull(ctl) { const r = await reader.read(); if (r.done) ctl.close(); else ctl.enqueue(r.value); },
      })).arrayBuffer();
      const all = new Uint8Array(got + rest.byteLength);
      all.set(out.subarray(0, got)); all.set(new Uint8Array(rest), got);
      return all.buffer;
    }
    out.set(value, got);
    got += value.length;
    if (onProgress) onProgress(got / total, got, total);
  }
  return got === total ? out.buffer : out.buffer.slice(0, got);
}

// World position of a vertex as it is drawn: a skinned mesh is placed by its bones, not by its node (a model the
// pipeline stood upright carries the turn in a root node above the armature).
function worldPos(mesh, i, out) {
  if (mesh.isSkinnedMesh) mesh.getVertexPosition(i, out);
  else out.fromBufferAttribute(mesh.geometry.attributes.position, i);
  return out.applyMatrix4(mesh.matrixWorld);
}

function modelBox(meshes, box) {
  const v = new THREE.Vector3();
  box.makeEmpty();
  for (const m of meshes) {
    const n = m.geometry.attributes.position.count;
    const stride = Math.max(1, Math.ceil(n / 20000));
    for (let i = 0; i < n; i += stride) box.expandByPoint(worldPos(m, i, v));
  }
  return box;
}

// Surface voxels of the model in world space: cell centres, height 0..1 and the strongest bone of each cell.
function voxelize(meshes, box) {
  const size = box.getSize(new THREE.Vector3());
  const cell = Math.max(size.x, size.y, size.z, 1e-6) / VOXEL_RES;
  const nx = Math.max(1, Math.ceil(size.x / cell)), ny = Math.max(1, Math.ceil(size.y / cell)), nz = Math.max(1, Math.ceil(size.z / cell));
  const grid = new Int32Array(nx * ny * nz).fill(-1);
  const pos = [], bone = [], best = [];
  const v = new THREE.Vector3(), a = new THREE.Vector3(), b = new THREE.Vector3(), c = new THREE.Vector3();
  const put = (p, j, w) => {
    const ix = Math.min(nx - 1, Math.max(0, Math.floor((p.x - box.min.x) / cell)));
    const iy = Math.min(ny - 1, Math.max(0, Math.floor((p.y - box.min.y) / cell)));
    const iz = Math.min(nz - 1, Math.max(0, Math.floor((p.z - box.min.z) / cell)));
    const k = ix + nx * (iy + ny * iz);
    let id = grid[k];
    if (id < 0) {
      id = grid[k] = bone.length;
      pos.push(box.min.x + (ix + 0.5) * cell, box.min.y + (iy + 0.5) * cell, box.min.z + (iz + 0.5) * cell);
      bone.push(j); best.push(w);
    } else if (w > best[id]) { best[id] = w; bone[id] = j; }
  };
  let total = 0;
  for (const m of meshes) total += m.geometry.attributes.position.count;
  const stride = Math.max(1, Math.ceil(total / MAX_SAMPLES));
  let boneBase = 0;
  for (const m of meshes) {
    const g = m.geometry, P = g.attributes.position, J = g.attributes.skinIndex, W = g.attributes.skinWeight;
    const top = (i) => {
      if (!J || !W) return [-1, 0];
      let bj = 0, bw = -1;
      for (let k = 0; k < 4; k++) { const w = W.getComponent(i, k); if (w > bw) { bw = w; bj = J.getComponent(i, k); } }
      return [boneBase + bj, bw];
    };
    for (let i = 0; i < P.count; i += stride) {
      worldPos(m, i, v);
      const [j, w] = top(i);
      put(v, j, w);
    }
    // a low-poly model has triangles wider than a cell: fill them
    const idx = g.index;
    const tris = idx ? idx.count / 3 : P.count / 3;
    if (tris <= 60000) {
      for (let t = 0; t < tris; t++) {
        const i0 = idx ? idx.getX(t * 3) : t * 3, i1 = idx ? idx.getX(t * 3 + 1) : t * 3 + 1, i2 = idx ? idx.getX(t * 3 + 2) : t * 3 + 2;
        worldPos(m, i0, a); worldPos(m, i1, b); worldPos(m, i2, c);
        const n = Math.min(24, Math.ceil(Math.max(a.distanceTo(b), b.distanceTo(c), c.distanceTo(a)) / (cell * 0.7)));
        if (n < 2) continue;
        const [j, w] = top(i0);
        for (let p = 0; p <= n; p++) for (let q = 0; q <= n - p; q++) {
          const u = p / n, s = q / n, r = 1 - u - s;
          v.set(a.x * r + b.x * u + c.x * s, a.y * r + b.y * u + c.y * s, a.z * r + b.z * u + c.z * s);
          put(v, j, w * 0.5);
        }
      }
    }
    if (m.isSkinnedMesh) boneBase += 0;             // every mesh of a rig shares one skeleton
  }
  return { cell, pos: new Float32Array(pos), bone: Int32Array.from(bone), minY: box.min.y, height: Math.max(size.y, 1e-6) };
}

export function createViewer(host) {
  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: 'high-performance' });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.0;
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  renderer.localClippingEnabled = true;
  host.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  const pmrem = new THREE.PMREMGenerator(renderer);
  scene.environment = pmrem.fromScene(new RoomEnvironment(), 0.035).texture;
  scene.environmentIntensity = 0.85;
  pmrem.dispose();

  const camera = new THREE.PerspectiveCamera(32, 1, 0.01, 2000);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;
  controls.dampingFactor = 0.08;
  controls.autoRotateSpeed = 0.9;
  controls.zoomToCursor = true;

  const key = new THREE.DirectionalLight(0xfff2e2, 2.4);
  key.castShadow = true;
  key.shadow.mapSize.set(2048, 2048);
  key.shadow.bias = -0.0002;
  key.shadow.normalBias = 0.02;
  key.shadow.radius = 6;
  const rim = new THREE.DirectionalLight(0x8fb8ff, 1.1);
  const fill = new THREE.HemisphereLight(0xdfe8ff, 0x1a1c22, 0.35);
  scene.add(key, key.target, rim, fill);

  const ground = new THREE.Mesh(new THREE.CircleGeometry(1, 72), new THREE.ShadowMaterial({ opacity: 0.42 }));
  ground.rotation.x = -Math.PI / 2;
  ground.receiveShadow = true;
  const halo = new THREE.Mesh(new THREE.CircleGeometry(1, 72), new THREE.MeshBasicMaterial({
    map: discTexture('rgba(150,190,255,.16)', 'rgba(150,190,255,0)'), transparent: true, depthWrite: false, toneMapped: false,
  }));
  halo.rotation.x = -Math.PI / 2;
  scene.add(ground, halo);

  const loader = new GLTFLoader();
  loader.setMeshoptDecoder(MeshoptDecoder);

  const clip = new THREE.Plane(new THREE.Vector3(0, -1, 0), 1e9);
  const clay = new THREE.MeshStandardMaterial({ color: 0xb8bcc6, roughness: 0.62, metalness: 0.0 });
  const weightsMat = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.75, metalness: 0.0 });
  const dots = dotTexture();

  const st = {
    token: 0, abort: null, root: null, meshes: [], bones: [], mixer: null, clips: [], action: null, clipName: null,
    paused: false, mode: 'pbr', showBones: false, front: '+z', box: new THREE.Box3(), radius: 1, userMoved: false,
    vox: null, skel: null, fx: null, dirty: true, rigged: false, listeners: {},
  };
  controls.addEventListener('start', () => { st.userMoved = true; controls.autoRotate = false; });
  controls.addEventListener('change', () => { st.dirty = true; });
  const emit = (name, data) => { const f = st.listeners[name]; if (f) try { f(data); } catch (e) { console.error(e); } };

  function resize() {
    if (st.capture) return;
    const w = Math.max(1, host.clientWidth), h = Math.max(1, host.clientHeight);
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
    st.dirty = true;
  }
  new ResizeObserver(resize).observe(host);
  resize();

  function disposeTree(obj) {
    obj.traverse((o) => {
      if (o.geometry) o.geometry.dispose();
      const mats = o.userData.t4mats || (Array.isArray(o.material) ? o.material : o.material ? [o.material] : []);
      for (const m of mats) {
        if (!m || m === clay || m === weightsMat) continue;
        for (const k of Object.keys(m)) { const v = m[k]; if (v && v.isTexture) v.dispose(); }
        m.dispose();
      }
    });
  }

  function clearModel() {
    stopFx(true);
    if (st.mixer) { st.mixer.stopAllAction(); st.mixer = null; }
    if (st.root) { scene.remove(st.root); disposeTree(st.root); st.root = null; }
    if (st.skel) { scene.remove(st.skel.lines, st.skel.points); st.skel.lines.geometry.dispose(); st.skel.points.geometry.dispose(); st.skel = null; }
    if (st.vox) { scene.remove(st.vox.mesh); st.vox.mesh.geometry.dispose(); st.vox.mesh.material.dispose(); st.vox = null; }
    st.meshes = []; st.bones = []; st.clips = []; st.action = null; st.clipName = null;
  }

  function fit(force) {
    const size = st.box.getSize(new THREE.Vector3());
    const center = st.box.getCenter(new THREE.Vector3());
    st.radius = Math.max(size.length() / 2, 1e-4);
    const r = st.radius;
    ground.position.set(center.x, st.box.min.y, center.z);
    ground.scale.setScalar(r * 2.4);
    halo.position.set(center.x, st.box.min.y + r * 0.002, center.z);
    halo.scale.setScalar(r * 2.0);
    key.position.set(center.x + r * 1.6, st.box.max.y + r * 2.2, center.z + r * 1.8);
    key.target.position.copy(center);
    const sc = key.shadow.camera;
    sc.left = sc.bottom = -r * 1.4; sc.right = sc.top = r * 1.4; sc.near = r * 0.1; sc.far = r * 8;
    sc.updateProjectionMatrix();
    rim.position.set(center.x - r * 2, center.y + r * 1.2, center.z - r * 2.4);
    if (!st.userMoved || force) {
      const fov = (camera.fov * Math.PI) / 180;
      const fitH = (size.y / 2) / Math.tan(fov / 2);
      const fitW = (Math.max(size.x, size.z) / 2) / Math.tan(fov / 2) / Math.max(camera.aspect, 0.4);
      const dist = Math.max(fitH, fitW) * 1.28 + Math.max(size.x, size.z) / 2;
      const yaw = { '+z': 0, '-z': Math.PI, '+x': Math.PI / 2, '-x': -Math.PI / 2 }[st.front] || 0;   // face the model's front
      const dir = new THREE.Vector3(0.52, 0.22, 1).applyAxisAngle(new THREE.Vector3(0, 1, 0), yaw).normalize();
      controls.target.copy(center);
      camera.position.copy(center).addScaledVector(dir, dist);
      camera.near = dist / 200; camera.far = dist * 40;
      camera.updateProjectionMatrix();
      controls.minDistance = r * 0.15; controls.maxDistance = dist * 6;
      controls.update();
    }
  }

  // ---------------------------------------------------------------- materials
  function eachMesh(fn) { for (const m of st.meshes) fn(m); }
  function ensureWeightColors() {
    const col = new THREE.Color();
    eachMesh((m) => {
      const g = m.geometry;
      if (g.attributes.color && g.userData.t4weights) return;
      const J = g.attributes.skinIndex, W = g.attributes.skinWeight, n = g.attributes.position.count;
      const arr = new Float32Array(n * 3);
      const lut = [];
      for (let i = 0; i < n; i++) {
        let r = 0.5, gr = 0.5, b = 0.5;
        if (J && W) {
          r = gr = b = 0;
          for (let k = 0; k < 4; k++) {
            const w = W.getComponent(i, k);
            if (w <= 0) continue;
            const j = J.getComponent(i, k);
            const c = lut[j] || (lut[j] = boneColor(j, new THREE.Color()).convertSRGBToLinear());
            r += c.r * w; gr += c.g * w; b += c.b * w;
          }
        }
        arr[i * 3] = r; arr[i * 3 + 1] = gr; arr[i * 3 + 2] = b;
      }
      g.userData.t4orig = g.attributes.color || null;
      g.setAttribute('color', new THREE.BufferAttribute(arr, 3));
      g.userData.t4weights = true;
      void col;
    });
  }
  function applyMode() {
    if (st.mode === 'weights') ensureWeightColors();
    eachMesh((m) => {
      const orig = m.userData.t4mats;
      if (st.mode === 'pbr') {
        if (m.geometry.userData.t4weights) {
          if (m.geometry.userData.t4orig) m.geometry.setAttribute('color', m.geometry.userData.t4orig); else m.geometry.deleteAttribute('color');
          m.geometry.userData.t4weights = false;
        }
        m.material = orig.length === 1 ? orig[0] : orig;
      } else m.material = st.mode === 'clay' ? clay : weightsMat;
    });
    st.dirty = true;
  }
  function setClip(planes) {
    const set = (mat) => { if (mat) { mat.clippingPlanes = planes; mat.clipShadows = !!planes; mat.needsUpdate = true; } };
    eachMesh((m) => m.userData.t4mats.forEach(set));
    set(clay); set(weightsMat);
  }

  // ---------------------------------------------------------------- skeleton overlay
  function buildSkeleton() {
    const bones = st.bones;
    if (!bones.length) return;
    const index = new Map(bones.map((b, i) => [b, i]));
    const seg = [], depth = new Array(bones.length).fill(0);
    bones.forEach((b, i) => {
      const p = index.get(b.parent);
      if (p !== undefined) { seg.push([p, i]); }
    });
    const dof = (i) => { let d = 0, b = bones[i]; while (b.parent && index.has(b.parent)) { d++; b = b.parent; } return d; };
    bones.forEach((_, i) => { depth[i] = dof(i); });
    const maxDepth = Math.max(1, ...depth);
    const lg = new THREE.BufferGeometry();
    lg.setAttribute('position', new THREE.BufferAttribute(new Float32Array(seg.length * 6), 3));
    const lc = new Float32Array(seg.length * 6), c = new THREE.Color();
    seg.forEach(([, ch], s) => { boneColor(ch, c); lc.set([1, 1, 1, c.r, c.g, c.b], s * 6); });
    lg.setAttribute('color', new THREE.BufferAttribute(lc, 3));
    const lines = new THREE.LineSegments(lg, new THREE.LineBasicMaterial({ vertexColors: true, depthTest: false, transparent: true, opacity: 0.95, toneMapped: false }));
    const pg = new THREE.BufferGeometry();
    pg.setAttribute('position', new THREE.BufferAttribute(new Float32Array(bones.length * 3), 3));
    const pc = new Float32Array(bones.length * 3);
    bones.forEach((_, i) => { boneColor(i, c); pc.set([c.r, c.g, c.b], i * 3); });
    pg.setAttribute('color', new THREE.BufferAttribute(pc, 3));
    const points = new THREE.Points(pg, new THREE.PointsMaterial({ size: 9, sizeAttenuation: false, map: dots, alphaTest: 0.4, vertexColors: true, depthTest: false, transparent: true, toneMapped: false }));
    lines.renderOrder = 20; points.renderOrder = 21;
    lines.frustumCulled = points.frustumCulled = false;
    lines.visible = points.visible = false;
    scene.add(lines, points);
    st.skel = { lines, points, seg, depth, maxDepth, grow: 1 };
  }
  const _a = new THREE.Vector3(), _b = new THREE.Vector3();
  function updateSkeleton() {
    const sk = st.skel;
    if (!sk || !sk.lines.visible) return;
    const lp = sk.lines.geometry.attributes.position, pp = sk.points.geometry.attributes.position;
    const g = sk.grow * (sk.maxDepth + 1);
    const wp = st.bones.map((b) => new THREE.Vector3().setFromMatrixPosition(b.matrixWorld));
    sk.seg.forEach(([p, ch], s) => {
      const t = ease(g - sk.depth[ch] + 0.0);
      _a.copy(wp[p]); _b.copy(wp[p]).lerp(wp[ch], t);
      lp.setXYZ(s * 2, _a.x, _a.y, _a.z); lp.setXYZ(s * 2 + 1, _b.x, _b.y, _b.z);
    });
    st.bones.forEach((_, i) => {
      const shown = g - sk.depth[i] >= 0.98;
      const q = shown ? wp[i] : (wp[0] || wp[i]);
      pp.setXYZ(i, q.x, q.y, shown ? q.z : q.z);
    });
    lp.needsUpdate = pp.needsUpdate = true;
  }
  function setBonesVisible(on) {
    if (!st.skel) return;
    st.skel.lines.visible = st.skel.points.visible = !!on;
    st.dirty = true;
  }

  // ---------------------------------------------------------------- voxels
  function buildVoxels() {
    const data = voxelize(st.meshes, st.box);
    const n = data.bone.length;
    if (!n) return;
    const geo = new THREE.BoxGeometry(data.cell * 0.9, data.cell * 0.9, data.cell * 0.9);
    const aH = new Float32Array(n);
    for (let i = 0; i < n; i++) aH[i] = Math.min(1, Math.max(0, (data.pos[i * 3 + 1] - data.minY) / data.height + (Math.random() - 0.5) * 0.03));
    geo.setAttribute('aH', new THREE.InstancedBufferAttribute(aH, 1));
    const uniforms = { uIn: { value: 0 }, uOut: { value: 0 }, uScan: { value: 0 }, uBand: { value: 0 }, uGlow: { value: 0.25 } };
    const mat = new THREE.MeshStandardMaterial({ color: 0x8a8a8a, roughness: 0.55, metalness: 0.0, envMapIntensity: 0.6 });
    mat.onBeforeCompile = (sh) => {
      Object.assign(sh.uniforms, uniforms);
      sh.vertexShader = `attribute float aH;\nuniform float uIn;\nuniform float uOut;\nuniform float uScan;\nuniform float uBand;\n${sh.vertexShader}`
        .replace('#include <begin_vertex>', `float t4a = smoothstep(aH * 0.8, aH * 0.8 + 0.2, uIn) * (1.0 - smoothstep(aH * 0.8, aH * 0.8 + 0.2, uOut));
        float t4b = exp(-pow((aH - uScan) * 6.0, 2.0));
        vec3 transformed = position * mix(t4a * (1.0 + 0.35 * t4b), t4b * 1.2, uBand);`);
      sh.fragmentShader = `uniform float uGlow;\n${sh.fragmentShader}`
        .replace('#include <emissivemap_fragment>', '#include <emissivemap_fragment>\n#ifdef USE_COLOR\n totalEmissiveRadiance += vColor.rgb * uGlow;\n#endif');
    };
    const mesh = new THREE.InstancedMesh(geo, mat, n);
    const m4 = new THREE.Matrix4(), c = new THREE.Color();
    for (let i = 0; i < n; i++) {
      m4.makeTranslation(data.pos[i * 3], data.pos[i * 3 + 1], data.pos[i * 3 + 2]);
      mesh.setMatrixAt(i, m4);
      mesh.setColorAt(i, c.set(0x7fd8ff));
    }
    mesh.frustumCulled = false;
    mesh.visible = false;
    scene.add(mesh);
    st.vox = { mesh, data, uniforms, aH };
  }
  function voxColors(kind) {
    const vx = st.vox;
    if (!vx) return;
    const c = new THREE.Color();
    for (let i = 0; i < vx.data.bone.length; i++) {
      if (kind === 'bones' && vx.data.bone[i] >= 0) boneColor(vx.data.bone[i], c);
      else c.setHSL(0.44 + vx.aH[i] * 0.12, 0.85, 0.5);
      vx.mesh.setColorAt(i, c);
    }
    vx.mesh.instanceColor.needsUpdate = true;
  }

  // ---------------------------------------------------------------- the build effect
  function stopFx(silent) {
    const fx = st.fx;
    st.fx = null;
    if (st.vox) st.vox.mesh.visible = false;
    if (st.root) setClip(null);
    clip.constant = 1e9;
    if (st.skel) { st.skel.grow = 1; setBonesVisible(st.showBones); }
    if (fx && fx.kind === 'build' && !silent) { startDefaultClip(); emit('build', { done: true }); }
    st.dirty = true;
  }
  function startScan() {            // the model is still being rigged: a voxel wave runs over it
    if (!st.vox) return;
    voxColors('scan');
    const u = st.vox.uniforms;
    u.uBand.value = 1; u.uIn.value = 1; u.uOut.value = 0; u.uGlow.value = 0.45;
    st.vox.mesh.visible = true;
    st.fx = { kind: 'scan', t: 0 };
  }
  function startBuild(fast) {       // voxels by bone -> the skeleton grows -> the voxels melt into the mesh
    if (!st.vox) { startDefaultClip(); return; }
    voxColors(st.bones.length ? 'bones' : 'scan');
    const u = st.vox.uniforms;
    u.uBand.value = 0; u.uIn.value = fast ? 0.35 : 0; u.uOut.value = 0; u.uGlow.value = 0.1;
    st.vox.mesh.visible = true;
    clip.constant = st.box.min.y - st.radius;     // the mesh is hidden under the plane
    setClip([clip]);
    if (st.skel) { st.skel.grow = 0; setBonesVisible(true); }
    st.fx = { kind: 'build', t: 0, tin: fast ? 0.55 : 1.3, tsk: st.skel ? (fast ? 0.75 : 1.2) : 0.1, tout: fast ? 1.0 : 1.5 };
    emit('build', { done: false });
  }
  function tickFx(dt) {
    const fx = st.fx;
    if (!fx) return;
    fx.t += dt;
    const u = st.vox.uniforms;
    if (fx.kind === 'scan') {
      u.uScan.value = 0.5 - 0.62 * Math.cos(fx.t * 1.25);
      return;
    }
    const a = fx.t / fx.tin, b = (fx.t - fx.tin) / fx.tsk, c = (fx.t - fx.tin - fx.tsk) / fx.tout;
    u.uIn.value = Math.max(u.uIn.value, Math.min(1, a) * 1.02);
    u.uScan.value = Math.min(1.3, a * 1.2);
    if (st.skel) st.skel.grow = Math.min(1, Math.max(0, b));
    if (c > 0) {
      const p = ease(Math.min(1, c));
      u.uOut.value = p * 1.02;
      clip.constant = st.box.min.y + (st.box.max.y - st.box.min.y) * Math.min(1.05, (p - 0.08) / 0.8);
    }
    if (c >= 1) stopFx(false);
  }

  // ---------------------------------------------------------------- animation
  function playClip(name) {
    if (!st.mixer) return;
    const clipObj = st.clips.find((c) => c.name === name);
    if (!clipObj) return;
    const next = st.mixer.clipAction(clipObj);
    next.reset().setEffectiveWeight(1).play();
    if (st.action && st.action !== next) st.action.crossFadeTo(next, 0.25, false);
    st.action = next; st.clipName = name; st.paused = false; st.mixer.timeScale = 1;
    emit('clip', { name, paused: false });
  }
  function startDefaultClip() {
    if (!st.clips.length || st.action) return;
    const pick = st.clips.find((c) => /idle/i.test(c.name)) || st.clips.find((c) => /walk/i.test(c.name)) || st.clips[0];
    playClip(pick.name);
  }
  function setPaused(p) {
    st.paused = !!p;
    if (st.mixer) st.mixer.timeScale = st.paused ? 0 : 1;
    emit('clip', { name: st.clipName, paused: st.paused });
  }

  // ---------------------------------------------------------------- loading
  function setModel(gltf, { rigged, intro }) {
    clearModel();
    const root = gltf.scene;
    st.root = root; st.rigged = !!rigged;
    scene.add(root);
    const maxAniso = Math.min(8, renderer.capabilities.getMaxAnisotropy());
    const bones = new Set();
    let verts = 0, tris = 0;
    root.traverse((o) => {
      if (!o.isMesh) return;
      st.meshes.push(o);
      o.castShadow = true; o.receiveShadow = true;
      if (o.isSkinnedMesh) { o.frustumCulled = false; o.skeleton.bones.forEach((b) => bones.add(b)); }
      const mats = Array.isArray(o.material) ? o.material : [o.material];
      o.userData.t4mats = mats;
      for (const m of mats) for (const k of ['map', 'normalMap', 'roughnessMap', 'metalnessMap', 'emissiveMap', 'aoMap']) if (m[k]) m[k].anisotropy = maxAniso;
      for (const m of mats) {                               // a texture the file only points at (a path on the author's
        const as = gltf.parser.associations.get(m);         // disk) never loads: show the shape in neutral grey, not black
        const def = as && as.materials != null ? (gltf.parser.json.materials || [])[as.materials] : null;
        const wants = !!(def && def.pbrMetallicRoughness && def.pbrMetallicRoughness.baseColorTexture);
        let missing = wants && (!m.map || !m.map.image);
        if (wants && !missing && m.map.image.width <= 2 && m.map.image.height <= 2) {   // the viewer copy puts a black
          try {                                                                         // pixel where an image was missing
            const cv = document.createElement('canvas'); cv.width = cv.height = 1;
            const cx = cv.getContext('2d'); cx.drawImage(m.map.image, 0, 0, 1, 1);
            const px = cx.getImageData(0, 0, 1, 1).data;
            missing = px[0] + px[1] + px[2] < 12;
          } catch (e) { missing = false; }
        }
        if (missing) { m.map = null; m.color.setScalar(0.72); if (m.metalness > 0.3) m.metalness = 0.1; m.needsUpdate = true; }
      }
      verts += o.geometry.attributes.position.count;
      tris += (o.geometry.index ? o.geometry.index.count : o.geometry.attributes.position.count) / 3;
    });
    st.bones = [...bones];
    root.updateMatrixWorld(true);
    for (const m of st.meshes) if (m.isSkinnedMesh) m.skeleton.update();
    modelBox(st.meshes, st.box);
    if (st.box.isEmpty()) st.box.set(new THREE.Vector3(-1, 0, -1), new THREE.Vector3(1, 2, 1));
    fit(false);
    st.clips = (gltf.animations || []).filter((c) => !INTERNAL_CLIPS.has(c.name));
    st.mixer = st.clips.length ? new THREE.AnimationMixer(root) : null;
    buildSkeleton();
    buildVoxels();
    applyMode();
    const info = { rigged: !!rigged, verts, tris: Math.round(tris), bones: st.bones.length, clips: st.clips.map((c) => ({ name: c.name, duration: c.duration })) };
    emit('model', info);
    if (!rigged) startScan();
    else if (intro === 'none') { setBonesVisible(st.showBones); startDefaultClip(); }
    else startBuild(intro === 'fast');
    controls.autoRotate = !rigged && !st.userMoved;
    st.dirty = true;
    return info;
  }

  async function load(url, { rigged = true, intro = 'fast', onProgress } = {}) {
    const token = ++st.token;
    if (st.abort) st.abort.abort();
    st.abort = new AbortController();
    const buf = await fetchBuffer(url, onProgress, st.abort.signal);
    if (token !== st.token) return null;
    const gltf = await loader.parseAsync(buf, '');
    if (token !== st.token) { disposeTree(gltf.scene); return null; }
    return setModel(gltf, { rigged, intro });
  }

  // ---------------------------------------------------------------- loop
  const clock = new THREE.Clock();
  function frame() {
    requestAnimationFrame(frame);
    if (st.capture) return;                               // preview capture renders its own frames (shot)
    const dt = Math.min(clock.getDelta(), 0.1);
    const moving = !!st.fx || (st.mixer && st.action && !st.paused) || controls.autoRotate;
    if (st.mixer) st.mixer.update(dt);
    tickFx(dt);
    const changed = controls.update();
    if (!moving && !changed && !st.dirty) return;
    st.dirty = false;
    scene.updateMatrixWorld();
    updateSkeleton();
    renderer.render(scene, camera);
  }
  requestAnimationFrame(frame);

  // ---------------------------------------------------------------- preview capture (/static/capture4.html)
  // One deterministic frame: the clip time and the camera orbit are set per call, so a slow GPU never drops a frame
  // and a clip of n loops closes on itself. Returns a JPEG data URL of w x h.
  let capOut = null;
  function shot({ time = 0, yaw = 0, pitch = 9, w = 1080, h = 1920, clip = null, quality = 0.9, label = '' } = {}) {
    if (!st.root) return null;
    if (!st.capture) {
      st.capture = true;
      if (st.fx) stopFx(true);
      controls.enabled = false;
      const c = document.createElement('canvas');
      c.width = 4; c.height = 256;
      const g = c.getContext('2d');
      const gr = g.createLinearGradient(0, 0, 0, 256);
      gr.addColorStop(0, '#39424f'); gr.addColorStop(0.55, '#1c212b'); gr.addColorStop(1, '#0b0d11');
      g.fillStyle = gr; g.fillRect(0, 0, 4, 256);
      const bg = new THREE.CanvasTexture(c);
      bg.colorSpace = THREE.SRGBColorSpace;
      scene.background = bg;
      capOut = document.createElement('canvas');
      st.root.traverse((o) => {                           // a white model without textures burns out: light clay
        for (const m of (Array.isArray(o.material) ? o.material : [o.material])) {
          if (m && m.isMeshStandardMaterial && !m.map && m.color && Math.min(m.color.r, m.color.g, m.color.b) > 0.85) {
            m.color.setScalar(0.6); m.roughness = Math.max(m.roughness, 0.62); m.metalness = Math.min(m.metalness, 0.1);
          }
        }
      });
    }
    if (renderer.domElement.width !== w || renderer.domElement.height !== h) {
      renderer.setPixelRatio(1);
      renderer.setSize(w, h, false);
      camera.aspect = w / h;
      capOut.width = w; capOut.height = h;
    }
    if (st.mixer && clip && clip !== st.clipName) {
      const c = st.clips.find((x) => x.name === clip);
      if (c) {
        st.mixer.stopAllAction();
        st.action = st.mixer.clipAction(c);
        st.action.reset().setEffectiveWeight(1).play();
        st.clipName = clip; st.paused = false; st.mixer.timeScale = 1;
      }
    }
    if (st.capClip !== st.clipName || !st.capBox) {       // frame the clip's own extent, not the rest pose's arm span
      st.capClip = st.clipName;
      const all = new THREE.Box3(), one = new THREE.Box3();
      const dur = st.mixer && st.action ? st.action.getClip().duration : 0;
      const n = dur > 0 ? 8 : 1;
      for (let k = 0; k < n; k++) {
        if (dur > 0) st.mixer.setTime((dur * k) / n);
        scene.updateMatrixWorld(true);
        for (const m of st.meshes) if (m.isSkinnedMesh && m.skeleton) m.skeleton.update();
        all.union(modelBox(st.meshes, one));
      }
      st.capBox = all.isEmpty() ? st.box.clone() : all;
    }
    if (st.mixer) st.mixer.setTime(time);
    const size = st.capBox.getSize(new THREE.Vector3()), center = st.capBox.getCenter(new THREE.Vector3());
    const fov = (camera.fov * Math.PI) / 180;
    const wide = Math.hypot(size.x, size.z);              // the model turns under the orbit: its diagonal must fit
    const fitH = ((size.y / 2) / Math.tan(fov / 2)) * 1.24;
    const fitW = ((wide / 2) / Math.tan(fov / 2) / camera.aspect) * 1.1;
    const dist = Math.max(fitH, fitW) + wide / 4;
    const front = { '+z': 0, '-z': Math.PI, '+x': Math.PI / 2, '-x': -Math.PI / 2 }[st.front] || 0;
    const a = front + (yaw * Math.PI) / 180, p = (pitch * Math.PI) / 180;
    camera.position.set(center.x + Math.sin(a) * Math.cos(p) * dist, center.y + Math.sin(p) * dist,
      center.z + Math.cos(a) * Math.cos(p) * dist);
    camera.near = dist / 200; camera.far = dist * 40;
    camera.updateProjectionMatrix();
    camera.lookAt(center);
    scene.updateMatrixWorld();
    updateSkeleton();
    renderer.render(scene, camera);
    const g = capOut.getContext('2d');
    g.drawImage(renderer.domElement, 0, 0, w, h);
    if (label) {
      g.font = `600 ${Math.round(h * 0.019)}px system-ui, -apple-system, "Segoe UI", sans-serif`;
      g.textAlign = 'center';
      g.fillStyle = 'rgba(255,255,255,.5)';
      g.fillText(label, w / 2, h - Math.round(h * 0.03));
    }
    return capOut.toDataURL('image/jpeg', quality);
  }

  return {
    load,
    shot,
    get clips() { return st.clips.map((c) => ({ name: c.name, duration: c.duration })); },
    on(name, fn) { st.listeners[name] = fn; },
    playClip,
    togglePause() { setPaused(!st.paused); },
    setMode(mode) { if (['pbr', 'clay', 'weights'].includes(mode)) { st.mode = mode; applyMode(); } },
    setBones(on) { st.showBones = !!on; if (!st.fx || st.fx.kind !== 'build') setBonesVisible(st.showBones); },
    replay() { if (st.root && st.rigged) { stopFx(true); if (st.action) { st.action.stop(); st.action = null; st.mixer.stopAllAction(); } startBuild(false); } },
    skip() { if (st.fx && st.fx.kind === 'build') stopFx(false); },
    resetView() { st.userMoved = false; fit(true); st.dirty = true; },
    setFront(axis) { if (['+z', '-z', '+x', '-x'].includes(axis) && axis !== st.front) { st.front = axis; if (st.root && !st.userMoved) fit(true); st.dirty = true; } },
    get building() { return !!(st.fx && st.fx.kind === 'build'); },
    get debug() {
      const live = modelBox(st.meshes, new THREE.Box3());
      const r = (v) => [v.x, v.y, v.z].map((x) => Math.round(x * 1000) / 1000);
      return { rest: [r(st.box.min), r(st.box.max)], live: [r(live.min), r(live.max)], cam: r(camera.position), target: r(controls.target), front: st.front,
        roots: st.root ? st.root.children.map((c) => [c.name, r(c.position), c.quaternion.toArray().map((x) => Math.round(x * 1000) / 1000)]) : [] };
    },
    get hasModel() { return !!st.root; },
  };
}
