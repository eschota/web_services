import * as THREE from 'three';
import { GLTFLoader } from 'https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/loaders/GLTFLoader.js';
import { OrbitControls } from 'https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/controls/OrbitControls.js';
import { TransformControls } from 'https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/controls/TransformControls.js';

const ROLE_COLORS = {
    front_left_leg: 0x22d3ee,
    front_right_leg: 0x38bdf8,
    back_left_leg: 0xa78bfa,
    back_right_leg: 0xc084fc,
    spine: 0xfacc15,
    neck: 0x34d399,
    tail: 0xfb7185,
    trunk: 0xf97316,
    accessory: 0xf8fafc,
};

const VIEW_DEFS = {
    front: { dir: [0, -1, 0], up: [0, 0, 1] },
    back: { dir: [0, 1, 0], up: [0, 0, 1] },
    left: { dir: [-1, 0, 0], up: [0, 0, 1] },
    right: { dir: [1, 0, 0], up: [0, 0, 1] },
    top: { dir: [0, 0, 1], up: [0, 1, 0] },
    bottom: { dir: [0, 0, -1], up: [0, -1, 0] },
};

function getTaskId() {
    const raw = new URLSearchParams(window.location.search).get('id') || '';
    return raw.split('?')[0];
}

function nodePositionToArray(vec) {
    return [Number(vec.x), Number(vec.y), Number(vec.z)];
}

function arraysDiffer(a, b, eps = 1e-5) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== 3 || b.length !== 3) return true;
    return Math.abs(a[0] - b[0]) > eps || Math.abs(a[1] - b[1]) > eps || Math.abs(a[2] - b[2]) > eps;
}

function roleColor(role) {
    return ROLE_COLORS[role] || ROLE_COLORS[String(role || '').split('_').slice(0, -1).join('_')] || 0x7dd3fc;
}

function roleLabel(role) {
    const r = String(role || 'node').toLowerCase();
    if (r.includes('leg')) return 'LEG';
    if (r.includes('spine')) return 'SPINE';
    if (r.includes('neck')) return 'NECK';
    if (r.includes('tail')) return 'TAIL';
    if (r.includes('trunk')) return 'TRUNK';
    if (r.includes('accessory')) return 'ACC';
    if (r.includes('core')) return 'CORE';
    return 'NODE';
}

function createRoleSprite(THREERef, role, colorHex) {
    const canvas = document.createElement('canvas');
    canvas.width = 160;
    canvas.height = 64;
    const ctx = canvas.getContext('2d');
    const color = `#${colorHex.toString(16).padStart(6, '0')}`;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = 'rgba(2, 6, 23, 0.72)';
    ctx.strokeStyle = color;
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.roundRect(8, 10, 144, 44, 14);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = color;
    ctx.font = '700 22px system-ui, -apple-system, Segoe UI, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(roleLabel(role), 80, 33);

    const texture = new THREERef.CanvasTexture(canvas);
    texture.colorSpace = THREERef.SRGBColorSpace;
    const material = new THREERef.SpriteMaterial({ map: texture, transparent: true, depthTest: false });
    const sprite = new THREERef.Sprite(material);
    sprite.scale.set(0.18, 0.072, 1);
    sprite.renderOrder = 20;
    return sprite;
}

class AnimalBlueprintViewerController {
    constructor() {
        this.taskId = getTaskId();
        this.card = document.getElementById('blueprint-viewer-card');
        this.host = document.getElementById('blueprint-viewer-canvas');
        this.empty = document.getElementById('blueprint-viewer-empty');
        this.statusEl = document.getElementById('blueprint-viewer-status');
        this.selectedEl = document.getElementById('blueprint-selected-node');
        this.coordsEl = document.getElementById('blueprint-coordinates');
        this.retargetBtn = document.getElementById('blueprint-retarget-btn');
        this.task = null;
        this.loadedTaskId = null;
        this.loadedSkeletonUrl = null;
        this.skeleton = null;
        this.nodes = new Map();
        this.originalPositions = new Map();
        this.nodeMeshes = new Map();
        this.roleByNodeId = new Map();
        this.selectedNodeId = null;
        this.meshes = [];
        this.vertexCloud = [];
        this.activeView = 'right';
        this.draggingNodeId = null;
        this.dirty = false;
        this.initialized = false;
        this.loading = false;

        if (!this.card || !this.host) return;
        this.wireButtons();
        window.addEventListener('taskDataUpdated', (event) => this.syncTask(event.detail));
    }

    syncTask(task) {
        if (!this.card || !task) return;
        this.task = task;
        const isAnimal = String(task.input_type || '').toLowerCase() === 'animal';
        const hasSkeleton = Boolean(task.blueprint_skeleton_ready && task.blueprint_skeleton_url);
        const showPending = isAnimal && task.status === 'processing';
        if (!isAnimal || (!hasSkeleton && !showPending)) {
            this.card.classList.add('hidden');
            return;
        }

        this.card.classList.remove('hidden');
        if (!hasSkeleton) {
            this.setStatus('Waiting for skeleton.json...');
            this.showEmpty(true, 'Blueprint data will appear when the worker publishes skeleton.json.');
            return;
        }
        this.showEmpty(true, 'Loading Blueprint...');
        if (this.loadedTaskId === task.task_id && this.loadedSkeletonUrl === task.blueprint_skeleton_url) return;
        void this.load(task);
    }

    wireButtons() {
        this.card.querySelectorAll('[data-blueprint-view]').forEach((button) => {
            button.addEventListener('click', () => this.setView(button.dataset.blueprintView));
        });
        this.card.querySelector('[data-blueprint-action="fit"]')?.addEventListener('click', () => this.fitCamera());
        this.card.querySelector('[data-blueprint-action="reset"]')?.addEventListener('click', () => this.setView(this.activeView || 'right'));
        this.card.querySelector('[data-blueprint-action="reset-node"]')?.addEventListener('click', () => this.resetSelectedNode());
        this.card.querySelector('[data-blueprint-action="reset-all"]')?.addEventListener('click', () => this.resetAllNodes());
        this.retargetBtn?.addEventListener('click', () => this.retarget());
    }

    async load(task) {
        if (this.loading) return;
        this.loading = true;
        this.setStatus('Loading Blueprint...');
        try {
            this.ensureScene();
            const [skeleton] = await Promise.all([
                fetch(task.blueprint_skeleton_url, { cache: 'no-store' }).then((r) => {
                    if (!r.ok) throw new Error(`skeleton.json HTTP ${r.status}`);
                    return r.json();
                }),
                this.loadModel(),
            ]);
            this.skeleton = skeleton;
            this.buildSkeleton();
            this.loadedTaskId = task.task_id;
            this.loadedSkeletonUrl = task.blueprint_skeleton_url;
            this.setStatus('Semantic skeleton ready');
            this.showEmpty(false);
            this.setView('right');
        } catch (error) {
            console.error('[Blueprint] load failed:', error);
            this.setStatus(error?.message || 'Blueprint load failed');
            this.showEmpty(true, 'Blueprint data is not available yet.');
        } finally {
            this.loading = false;
        }
    }

    ensureScene() {
        if (this.initialized) return;
        this.scene = new THREE.Scene();
        this.scene.background = new THREE.Color(0x061a3a);
        this.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false });
        this.renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
        this.host.appendChild(this.renderer.domElement);

        this.orthoCamera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.01, 1000);
        this.perspectiveCamera = new THREE.PerspectiveCamera(42, 1, 0.01, 1000);
        this.camera = this.orthoCamera;
        this.controls = new OrbitControls(this.camera, this.renderer.domElement);
        this.controls.enableDamping = true;
        this.controls.dampingFactor = 0.08;

        this.transformControls = new TransformControls(this.camera, this.renderer.domElement);
        this.transformControls.setMode('translate');
        this.transformControls.addEventListener('dragging-changed', (event) => {
            this.controls.enabled = !event.value;
            if (!event.value && this.selectedNodeId) this.snapSelectedNode();
        });
        this.transformControls.addEventListener('objectChange', () => {
            if (!this.selectedNodeId) return;
            const mesh = this.nodeMeshes.get(this.selectedNodeId);
            if (!mesh) return;
            this.moveNodeTo(this.selectedNodeId, mesh.position, { skipMesh: true });
        });
        this.scene.add(this.transformControls);

        this.modelGroup = new THREE.Group();
        this.lineGroup = new THREE.Group();
        this.nodeGroup = new THREE.Group();
        this.scene.add(this.modelGroup, this.lineGroup, this.nodeGroup);

        const gridA = new THREE.GridHelper(20, 80, 0x38bdf8, 0x0e7490);
        gridA.material.transparent = true;
        gridA.material.opacity = 0.22;
        this.scene.add(gridA);
        const gridB = gridA.clone();
        gridB.rotation.x = Math.PI / 2;
        gridB.material = gridA.material.clone();
        gridB.material.opacity = 0.12;
        this.scene.add(gridB);

        this.scene.add(new THREE.HemisphereLight(0xbae6fd, 0x082f49, 2.4));
        const key = new THREE.DirectionalLight(0xe0f2fe, 2.2);
        key.position.set(3, -4, 6);
        this.scene.add(key);

        this.raycaster = new THREE.Raycaster();
        this.pointer = new THREE.Vector2();
        this.dragPlane = new THREE.Plane();
        this.resizeObserver = new ResizeObserver(() => this.resize());
        this.resizeObserver.observe(this.host);
        this.renderer.domElement.addEventListener('pointerdown', (event) => this.onPointerDown(event));
        window.addEventListener('pointermove', (event) => this.onPointerMove(event));
        window.addEventListener('pointerup', () => this.onPointerUp());
        this.resize();
        this.animate();
        this.initialized = true;
    }

    async loadModel() {
        if (this.model) return;
        const loader = new GLTFLoader();
        const gltf = await loader.loadAsync(`/api/task/${this.taskId}/prepared.glb`);
        this.model = gltf.scene;
        this.modelGroup.clear();
        this.modelGroup.add(this.model);
        this.meshes = [];
        this.vertexCloud = [];
        this.model.traverse((object) => {
            if (!object.isMesh || !object.geometry) return;
            this.meshes.push(object);
            object.material = new THREE.MeshStandardMaterial({
                color: 0x7dd3fc,
                transparent: true,
                opacity: 0.26,
                roughness: 0.95,
                metalness: 0.02,
                side: THREE.DoubleSide,
            });
            const pos = object.geometry.attributes.position;
            if (!pos) return;
            const stride = Math.max(1, Math.ceil(pos.count / 2500));
            const world = new THREE.Vector3();
            for (let i = 0; i < pos.count; i += stride) {
                world.fromBufferAttribute(pos, i).applyMatrix4(object.matrixWorld);
                this.vertexCloud.push(world.clone());
            }
        });
        this.bounds = new THREE.Box3().setFromObject(this.model);
        this.modelDiag = Math.max(this.bounds.getSize(new THREE.Vector3()).length(), 1);
    }

    buildSkeleton() {
        this.nodes.clear();
        this.originalPositions.clear();
        this.nodeMeshes.clear();
        this.roleByNodeId.clear();
        this.lineGroup.clear();
        this.nodeGroup.clear();
        this.selectedNodeId = null;
        this.dirty = false;
        this.setRetargetVisible(false);

        const lines = Array.isArray(this.skeleton?.semantic_lines) ? this.skeleton.semantic_lines : [];
        for (const line of lines) {
            const role = String(line.role || line.kind || 'node');
            for (const id of [line.tip_node_id, line.attachment_node_id, ...(line.node_ids || [])]) {
                if (id !== undefined && id !== null && !this.roleByNodeId.has(String(id))) {
                    this.roleByNodeId.set(String(id), role);
                }
            }
        }

        const graphNodes = Array.isArray(this.skeleton?.graph?.nodes) ? this.skeleton.graph.nodes : [];
        for (const node of graphNodes) {
            const id = String(node.id);
            const position = Array.isArray(node.position) ? node.position.map(Number) : [0, 0, 0];
            const role = this.roleByNodeId.get(id) || node.type || 'node';
            this.nodes.set(id, { id, type: node.type || 'node', role, position: [...position] });
            this.originalPositions.set(id, [...position]);
            this.createNodeMesh(id, role, position);
        }
        this.redrawLines();
        this.updateSelection(null);
    }

    createNodeMesh(id, role, position) {
        const radius = Math.max(0.008, this.modelDiag * 0.014);
        const color = roleColor(role);
        const mesh = new THREE.Mesh(
            new THREE.SphereGeometry(radius, 18, 12),
            new THREE.MeshStandardMaterial({
                color,
                emissive: color,
                emissiveIntensity: 0.28,
                roughness: 0.7,
            }),
        );
        mesh.position.set(position[0], position[1], position[2]);
        mesh.userData.blueprintNodeId = id;
        this.nodeGroup.add(mesh);
        this.nodeMeshes.set(id, mesh);

        const sprite = createRoleSprite(THREE, role, color);
        sprite.position.set(position[0], position[1], position[2] + Math.max(0.02, radius * 2.5));
        sprite.userData.blueprintNodeId = id;
        this.nodeGroup.add(sprite);
        mesh.userData.labelSprite = sprite;
    }

    redrawLines() {
        this.lineGroup.clear();
        const segments = Array.isArray(this.skeleton?.graph?.segments) ? this.skeleton.graph.segments : [];
        const roleBySegment = new Map();
        const colorBySegment = new Map();
        for (const line of this.skeleton?.semantic_lines || []) {
            const role = line.role || line.kind || 'node';
            const color = Array.isArray(line.color_rgba)
                ? new THREE.Color(line.color_rgba[0], line.color_rgba[1], line.color_rgba[2])
                : new THREE.Color(roleColor(role));
            for (const segmentId of line.segment_ids || []) {
                roleBySegment.set(String(segmentId), role);
                colorBySegment.set(String(segmentId), color);
            }
        }

        for (const segment of segments) {
            const u = this.nodes.get(String(segment.u));
            const v = this.nodes.get(String(segment.v));
            if (!u || !v) continue;
            const sid = String(segment.id);
            const color = colorBySegment.get(sid) || new THREE.Color(roleColor(roleBySegment.get(sid)));
            const geometry = new THREE.BufferGeometry().setFromPoints([
                new THREE.Vector3(u.position[0], u.position[1], u.position[2]),
                new THREE.Vector3(v.position[0], v.position[1], v.position[2]),
            ]);
            const material = new THREE.LineBasicMaterial({
                color,
                transparent: true,
                opacity: 0.92,
                depthTest: false,
            });
            const line = new THREE.Line(geometry, material);
            line.renderOrder = 10;
            this.lineGroup.add(line);
        }
    }

    resize() {
        if (!this.renderer || !this.host) return;
        const rect = this.host.getBoundingClientRect();
        const width = Math.max(1, Math.floor(rect.width));
        const height = Math.max(1, Math.floor(rect.height));
        this.renderer.setSize(width, height, false);
        this.perspectiveCamera.aspect = width / height;
        this.perspectiveCamera.updateProjectionMatrix();
        this.fitCamera(false);
    }

    setView(view) {
        if (!this.renderer || !this.bounds) return;
        this.activeView = view;
        this.card.querySelectorAll('[data-blueprint-view]').forEach((button) => {
            button.setAttribute('aria-pressed', button.dataset.blueprintView === view ? 'true' : 'false');
        });
        if (view === 'perspective') {
            this.camera = this.perspectiveCamera;
            this.transformControls.camera = this.camera;
            this.controls.object = this.camera;
            this.fitPerspective();
            if (this.selectedNodeId) this.transformControls.attach(this.nodeMeshes.get(this.selectedNodeId));
            this.setStatus('Perspective edit mode');
            return;
        }
        this.transformControls.detach();
        this.camera = this.orthoCamera;
        this.transformControls.camera = this.camera;
        this.controls.object = this.camera;
        this.fitOrthographic(view);
        this.setStatus(`${view[0].toUpperCase()}${view.slice(1)} orthographic edit mode`);
    }

    fitCamera(updateControls = true) {
        if (!this.bounds) return;
        if (this.activeView === 'perspective') this.fitPerspective(updateControls);
        else this.fitOrthographic(this.activeView || 'right', updateControls);
    }

    fitOrthographic(view = 'right', updateControls = true) {
        const def = VIEW_DEFS[view] || VIEW_DEFS.right;
        const center = this.bounds.getCenter(new THREE.Vector3());
        const size = this.bounds.getSize(new THREE.Vector3());
        const radius = Math.max(size.x, size.y, size.z, 0.4) * 0.78;
        const rect = this.host.getBoundingClientRect();
        const aspect = Math.max(0.1, rect.width / Math.max(1, rect.height));
        this.orthoCamera.left = -radius * aspect;
        this.orthoCamera.right = radius * aspect;
        this.orthoCamera.top = radius;
        this.orthoCamera.bottom = -radius;
        this.orthoCamera.near = -1000;
        this.orthoCamera.far = 1000;
        const dir = new THREE.Vector3(...def.dir).normalize();
        this.orthoCamera.position.copy(center).addScaledVector(dir, Math.max(this.modelDiag * 1.8, 2));
        this.orthoCamera.up.set(...def.up);
        this.orthoCamera.lookAt(center);
        this.orthoCamera.updateProjectionMatrix();
        if (updateControls) {
            this.controls.target.copy(center);
            this.controls.update();
        }
    }

    fitPerspective(updateControls = true) {
        const center = this.bounds.getCenter(new THREE.Vector3());
        const size = this.bounds.getSize(new THREE.Vector3());
        const distance = Math.max(size.length() * 1.25, 1.6);
        this.perspectiveCamera.position.copy(center).add(new THREE.Vector3(distance, -distance, distance * 0.7));
        this.perspectiveCamera.up.set(0, 0, 1);
        this.perspectiveCamera.lookAt(center);
        this.perspectiveCamera.updateProjectionMatrix();
        if (updateControls) {
            this.controls.target.copy(center);
            this.controls.update();
        }
    }

    onPointerDown(event) {
        if (!this.renderer || !this.skeleton) return;
        this.updatePointer(event);
        this.raycaster.setFromCamera(this.pointer, this.camera);
        const hits = this.raycaster.intersectObjects([...this.nodeMeshes.values()], false);
        if (!hits.length) return;
        const nodeId = hits[0].object.userData.blueprintNodeId;
        if (!nodeId) return;
        this.updateSelection(nodeId);
        if (this.activeView === 'perspective') {
            this.transformControls.attach(hits[0].object);
            return;
        }
        this.draggingNodeId = nodeId;
        this.controls.enabled = false;
        const normal = new THREE.Vector3();
        this.camera.getWorldDirection(normal);
        this.dragPlane.setFromNormalAndCoplanarPoint(normal, hits[0].object.position);
        event.preventDefault();
    }

    onPointerMove(event) {
        if (!this.draggingNodeId || this.activeView === 'perspective') return;
        const target = this.dragPositionFromPointer(event);
        if (target) this.moveNodeTo(this.draggingNodeId, target);
    }

    onPointerUp() {
        if (this.draggingNodeId) {
            this.snapSelectedNode();
            this.draggingNodeId = null;
        }
        if (this.controls) this.controls.enabled = true;
    }

    updatePointer(event) {
        const rect = this.renderer.domElement.getBoundingClientRect();
        this.pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
        this.pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    }

    dragPositionFromPointer(event) {
        this.updatePointer(event);
        this.raycaster.setFromCamera(this.pointer, this.camera);
        const meshHits = this.raycaster.intersectObjects(this.meshes, true);
        if (meshHits.length) {
            return this.nearestVertex(meshHits[0].point) || meshHits[0].point;
        }
        const planePoint = new THREE.Vector3();
        if (this.raycaster.ray.intersectPlane(this.dragPlane, planePoint)) {
            const nearest = this.nearestVertex(planePoint);
            if (nearest && nearest.distanceTo(planePoint) < this.modelDiag * 0.12) return nearest;
        }
        return null;
    }

    nearestVertex(point) {
        if (!this.vertexCloud.length) return null;
        let best = null;
        let bestDist = Infinity;
        for (const vertex of this.vertexCloud) {
            const dist = vertex.distanceToSquared(point);
            if (dist < bestDist) {
                bestDist = dist;
                best = vertex;
            }
        }
        return best ? best.clone() : null;
    }

    snapSelectedNode() {
        if (!this.selectedNodeId) return;
        const mesh = this.nodeMeshes.get(this.selectedNodeId);
        if (!mesh) return;
        const nearest = this.nearestVertex(mesh.position);
        if (nearest) this.moveNodeTo(this.selectedNodeId, nearest);
    }

    moveNodeTo(nodeId, position, options = {}) {
        const node = this.nodes.get(String(nodeId));
        if (!node) return;
        const vec = position.isVector3 ? position : new THREE.Vector3(position[0], position[1], position[2]);
        node.position = nodePositionToArray(vec);
        const mesh = this.nodeMeshes.get(String(nodeId));
        if (mesh && !options.skipMesh) mesh.position.copy(vec);
        if (mesh?.userData.labelSprite) {
            mesh.userData.labelSprite.position.set(vec.x, vec.y, vec.z + Math.max(0.02, this.modelDiag * 0.035));
        }
        this.redrawLines();
        this.updateSelection(String(nodeId));
        this.updateDirtyState();
    }

    updateSelection(nodeId) {
        this.selectedNodeId = nodeId;
        if (!nodeId) {
            this.selectedEl.textContent = 'No node selected';
            this.coordsEl.textContent = 'x - y - z -';
            this.transformControls?.detach();
            return;
        }
        const node = this.nodes.get(String(nodeId));
        if (!node) return;
        this.selectedEl.textContent = `${node.role} / ${node.id}`;
        this.coordsEl.textContent = node.position.map((v) => Number(v).toFixed(3)).join(' / ');
        if (this.activeView === 'perspective') {
            const mesh = this.nodeMeshes.get(String(nodeId));
            if (mesh) this.transformControls.attach(mesh);
        }
    }

    updateDirtyState() {
        let dirty = false;
        for (const [id, node] of this.nodes.entries()) {
            if (arraysDiffer(node.position, this.originalPositions.get(id))) {
                dirty = true;
                break;
            }
        }
        this.dirty = dirty;
        this.setRetargetVisible(dirty);
    }

    setRetargetVisible(visible) {
        this.retargetBtn?.classList.toggle('is-visible', Boolean(visible));
    }

    resetSelectedNode() {
        if (!this.selectedNodeId) return;
        const original = this.originalPositions.get(this.selectedNodeId);
        if (original) this.moveNodeTo(this.selectedNodeId, original);
    }

    resetAllNodes() {
        for (const [id, original] of this.originalPositions.entries()) {
            this.moveNodeTo(id, original);
        }
        this.updateDirtyState();
    }

    extractMarkers() {
        const markers = {};
        const counts = {};
        for (const line of this.skeleton?.semantic_lines || []) {
            const role = String(line.role || line.kind || 'node');
            const nodeId = [line.tip_node_id, line.attachment_node_id, ...(line.node_ids || [])]
                .map((id) => id === undefined || id === null ? '' : String(id))
                .find((id) => id && this.nodes.has(id));
            let position = nodeId ? this.nodes.get(nodeId).position : null;
            if (!position && Array.isArray(line.points) && line.points.length) {
                position = line.points[line.points.length - 1];
            }
            if (!position) continue;
            counts[role] = (counts[role] || 0) + 1;
            const key = counts[role] === 1 ? role : `${role}_${nodeId || counts[role]}`;
            markers[key] = position.map(Number);
        }
        return markers;
    }

    async retarget() {
        if (!this.task || !this.dirty || !this.retargetBtn) return;
        const detection = this.task.rig_v2_animal_detection || {};
        const animalType = detection.animal_type || detection.animal_type_string || detection.candidate_animal_type_string;
        if (!this.task.input_url || !animalType) {
            this.setStatus('Retarget requires input URL and animal type');
            return;
        }
        this.retargetBtn.disabled = true;
        this.setStatus('Creating retarget task...');
        const payload = {
            source: 'link',
            input_url: this.task.input_url,
            type: 'animal',
            mode: 'only_rig',
            animal_type: animalType,
            local_rotation: Array.isArray(detection.local_rotation) ? detection.local_rotation : [0, 0, 0],
            animal_semantic_markers: this.extractMarkers(),
        };
        try {
            const response = await fetch('/api/task/create', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const data = await response.json().catch(() => ({}));
            if (!response.ok || !data.task_id) {
                throw new Error(data.detail || data.message || `HTTP ${response.status}`);
            }
            window.location.href = `/task?id=${encodeURIComponent(data.task_id)}`;
        } catch (error) {
            console.error('[Blueprint] retarget failed:', error);
            this.setStatus(error?.message || 'Retarget failed');
            this.retargetBtn.disabled = false;
        }
    }

    showEmpty(visible, text) {
        if (!this.empty) return;
        this.empty.style.display = visible ? 'flex' : 'none';
        if (text) this.empty.textContent = text;
    }

    setStatus(text) {
        if (this.statusEl) this.statusEl.textContent = text || '';
    }

    animate() {
        if (!this.renderer) return;
        requestAnimationFrame(() => this.animate());
        this.controls?.update();
        this.renderer.render(this.scene, this.camera);
    }
}

window.AnimalBlueprintViewer = new AnimalBlueprintViewerController();
