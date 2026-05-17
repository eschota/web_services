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

const VIEW_ORDER = ['right', 'front', 'left', 'back', 'top', 'bottom', 'perspective'];

const VIEW_LABELS = {
    front: 'Front',
    back: 'Back',
    left: 'Left',
    right: 'Right',
    top: 'Top',
    bottom: 'Bottom',
    perspective: 'Perspective',
};

const FRESNEL_VERTEX_SHADER = `
    varying vec3 vNormalView;
    varying vec3 vViewDir;

    void main() {
        vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
        vNormalView = normalize(normalMatrix * normal);
        vViewDir = normalize(-mvPosition.xyz);
        gl_Position = projectionMatrix * mvPosition;
    }
`;

const FRESNEL_FRAGMENT_SHADER = `
    varying vec3 vNormalView;
    varying vec3 vViewDir;

    void main() {
        float rim = pow(1.0 - abs(dot(normalize(vNormalView), normalize(vViewDir))), 1.8);
        float alpha = clamp(rim, 0.08, 1.0);
        vec3 edge = vec3(1.0, 1.0, 1.0);
        vec3 blue = vec3(0.32, 0.82, 1.0);
        gl_FragColor = vec4(mix(blue, edge, alpha), alpha);
    }
`;

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

function bpT(key, fallback) {
    if (!key) return fallback || '';
    const translated = window.t ? window.t(key) : key;
    return translated && translated !== key ? translated : (fallback || key);
}

function createRoleTexture(THREERef, role, colorHex, selected = false) {
    const canvas = document.createElement('canvas');
    canvas.width = 160;
    canvas.height = 64;
    const ctx = canvas.getContext('2d');
    const color = `#${colorHex.toString(16).padStart(6, '0')}`;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = selected ? 'rgba(14, 116, 144, 0.9)' : 'rgba(2, 6, 23, 0.72)';
    ctx.strokeStyle = selected ? '#ffffff' : color;
    ctx.lineWidth = selected ? 5 : 3;
    ctx.beginPath();
    ctx.roundRect(8, 10, 144, 44, 14);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = selected ? '#ffffff' : color;
    ctx.font = '700 22px system-ui, -apple-system, Segoe UI, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(roleLabel(role), 80, 33);

    const texture = new THREERef.CanvasTexture(canvas);
    texture.colorSpace = THREERef.SRGBColorSpace;
    return texture;
}

function createRoleSprite(THREERef, role, colorHex) {
    const texture = createRoleTexture(THREERef, role, colorHex, false);
    const material = new THREERef.SpriteMaterial({ map: texture, transparent: true, depthTest: false });
    const sprite = new THREERef.Sprite(material);
    sprite.scale.set(0.18, 0.072, 1);
    sprite.renderOrder = 20;
    sprite.userData.baseScale = sprite.scale.clone();
    sprite.userData.role = role;
    sprite.userData.color = colorHex;
    return sprite;
}

function createBlueprintModelMaterial() {
    return new THREE.ShaderMaterial({
        vertexShader: FRESNEL_VERTEX_SHADER,
        fragmentShader: FRESNEL_FRAGMENT_SHADER,
        transparent: true,
        depthWrite: false,
        side: THREE.DoubleSide,
    });
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
        this.addNodeBtn = document.getElementById('blueprint-add-node-btn');
        this.viewSelect = document.getElementById('blueprint-view-select');
        this.viewCube = document.getElementById('blueprint-view-cube');
        this.viewCubeLabel = document.getElementById('blueprint-view-cube-label');
        this.heightTarget = document.getElementById('model-viewer-container');
        this.task = null;
        this.loadedTaskId = null;
        this.loadedSkeletonUrl = null;
        this.skeleton = null;
        this.nodes = new Map();
        this.originalPositions = new Map();
        this.nodeMeshes = new Map();
        this.labelSprites = new Map();
        this.additionalParents = new Map();
        this.roleByNodeId = new Map();
        this.selectedNodeId = null;
        this.meshes = [];
        this.vertexCloud = [];
        this.activeView = 'right';
        this.draggingNodeId = null;
        this.dragStartPointer = null;
        this.dragMoved = false;
        this.additionalNodeSeq = 0;
        this.dirty = false;
        this.initialized = false;
        this.loading = false;

        if (!this.card || !this.host) return;
        this.wireButtons();
        this.wireStageHeight();
        this.updateLocalizedUi();
        window.addEventListener('taskDataUpdated', (event) => this.syncTask(event.detail));
        window.addEventListener('languageChanged', () => this.updateLocalizedUi());
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
        this.viewSelect?.addEventListener('change', () => this.setView(this.viewSelect.value));
        this.card.querySelectorAll('[data-blueprint-cube-view]').forEach((button) => {
            button.addEventListener('click', (event) => {
                event.stopPropagation();
                this.setView(button.dataset.blueprintCubeView);
            });
        });
        this.card.querySelector('[data-blueprint-cycle]')?.addEventListener('click', () => this.cycleView());
        this.card.querySelector('[data-blueprint-action="fit"]')?.addEventListener('click', () => this.fitCamera());
        this.card.querySelector('[data-blueprint-action="reset"]')?.addEventListener('click', () => this.setView(this.activeView || 'right'));
        this.addNodeBtn?.addEventListener('click', () => this.addLinkedNode());
        this.retargetBtn?.addEventListener('click', () => this.retarget());
    }

    wireStageHeight() {
        this.syncStageHeight();
        if (!this.heightTarget || !this.card) return;
        this.heightObserver = new ResizeObserver(() => this.syncStageHeight());
        this.heightObserver.observe(this.heightTarget);
        window.addEventListener('resize', () => this.syncStageHeight());
    }

    syncStageHeight() {
        if (!this.card || !this.heightTarget) return;
        const height = this.heightTarget.getBoundingClientRect().height;
        if (height > 120) {
            this.card.style.setProperty('--blueprint-stage-height', `${Math.round(height)}px`);
            this.resize();
        }
    }

    updateLocalizedUi() {
        const retargetText = bpT('blueprint_retarget_label', 'Retarget Rig Animation');
        const retargetTitle = bpT(
            'blueprint_retarget_tooltip',
            'Creates a new rig and animation task from edited points. Place points at limb tips, not in the middle.',
        );
        if (this.retargetBtn) {
            this.retargetBtn.textContent = retargetText;
            this.retargetBtn.title = retargetTitle;
            this.retargetBtn.setAttribute('aria-label', retargetTitle);
        }
        if (this.addNodeBtn) {
            this.addNodeBtn.textContent = bpT('blueprint_add_node_label', 'Add node');
            const addTitle = bpT('blueprint_add_node_tooltip', 'Add a linked helper point near the selected node.');
            this.addNodeBtn.title = addTitle;
            this.addNodeBtn.setAttribute('aria-label', addTitle);
        }
        if (this.viewSelect) {
            const selectTitle = bpT('blueprint_view_select_title', 'Blueprint camera view');
            this.viewSelect.title = selectTitle;
            this.viewSelect.setAttribute('aria-label', selectTitle);
        }
        const fitButton = this.card?.querySelector('[data-blueprint-action="fit"]');
        if (fitButton) {
            const fitTitle = bpT('blueprint_fit_title', 'Fit camera');
            fitButton.title = fitTitle;
            fitButton.setAttribute('aria-label', fitTitle);
        }
        const resetButton = this.card?.querySelector('[data-blueprint-action="reset"]');
        if (resetButton) {
            const resetTitle = bpT('blueprint_reset_title', 'Reset camera');
            resetButton.title = resetTitle;
            resetButton.setAttribute('aria-label', resetTitle);
        }
        if (this.viewCube) {
            const cubeTitle = bpT('blueprint_cube_title', 'Blueprint view cube');
            this.viewCube.title = cubeTitle;
            this.viewCube.setAttribute('aria-label', cubeTitle);
        }
        const cubeNext = this.card?.querySelector('[data-blueprint-cycle]');
        if (cubeNext) {
            const nextTitle = bpT('blueprint_cube_next_title', 'Next blueprint camera view');
            cubeNext.title = nextTitle;
            cubeNext.setAttribute('aria-label', nextTitle);
        }
        const cubeTitles = {
            top: ['blueprint_cube_top_title', 'Top view'],
            right: ['blueprint_cube_right_title', 'Right view'],
            bottom: ['blueprint_cube_bottom_title', 'Bottom view'],
            left: ['blueprint_cube_left_title', 'Left view'],
        };
        Object.entries(cubeTitles).forEach(([view, [key, fallback]]) => {
            const button = this.card?.querySelector(`[data-blueprint-cube-view="${view}"]`);
            if (!button) return;
            const title = bpT(key, fallback);
            button.title = title;
            button.setAttribute('aria-label', title);
        });
        this.syncViewUi(this.activeView || 'right');
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
        this.controls.enableRotate = false;
        this.controls.enablePan = false;
        this.controls.enableZoom = true;

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
        let gltf = null;
        let lastError = null;
        for (const url of [
            `/api/task/${this.taskId}/blueprint/model.glb`,
            `/api/task/${this.taskId}/prepared.glb`,
            `/api/task/${this.taskId}/animations.glb`,
        ]) {
            try {
                gltf = await loader.loadAsync(url);
                break;
            } catch (error) {
                lastError = error;
            }
        }
        if (!gltf) throw lastError || new Error('Blueprint model is unavailable');
        this.model = gltf.scene;
        this.modelGroup.clear();
        this.modelGroup.add(this.model);
        this.meshes = [];
        this.vertexCloud = [];
        const blueprintMaterial = createBlueprintModelMaterial();
        this.model.traverse((object) => {
            if (!object.isMesh || !object.geometry) return;
            this.meshes.push(object);
            object.material = blueprintMaterial;
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
        this.labelSprites.clear();
        this.additionalParents.clear();
        this.roleByNodeId.clear();
        this.lineGroup.clear();
        this.nodeGroup.clear();
        this.selectedNodeId = null;
        this.additionalNodeSeq = 0;
        this.dirty = false;
        this.setRetargetVisible(false);
        this.setAddNodeVisible(false);

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
        mesh.userData.role = role;
        mesh.userData.color = color;
        mesh.userData.baseScale = mesh.scale.clone();
        this.labelSprites.set(id, sprite);
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
        for (const [nodeId, parentId] of this.additionalParents.entries()) {
            const parent = this.nodes.get(String(parentId));
            const child = this.nodes.get(String(nodeId));
            if (!parent || !child) continue;
            const geometry = new THREE.BufferGeometry().setFromPoints([
                new THREE.Vector3(parent.position[0], parent.position[1], parent.position[2]),
                new THREE.Vector3(child.position[0], child.position[1], child.position[2]),
            ]);
            const material = new THREE.LineBasicMaterial({
                color: roleColor('accessory'),
                transparent: true,
                opacity: 0.78,
                depthTest: false,
            });
            const line = new THREE.Line(geometry, material);
            line.renderOrder = 11;
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
        view = VIEW_ORDER.includes(view) ? view : 'right';
        this.activeView = view;
        this.syncViewUi(view);
        if (view === 'perspective') {
            this.camera = this.perspectiveCamera;
            this.transformControls.camera = this.camera;
            this.controls.object = this.camera;
            this.controls.enabled = true;
            this.controls.enableRotate = true;
            this.controls.enablePan = true;
            this.controls.enableZoom = true;
            this.fitPerspective();
            if (this.selectedNodeId) this.transformControls.attach(this.nodeMeshes.get(this.selectedNodeId));
            this.setStatus('Perspective edit mode');
            return;
        }
        this.transformControls.detach();
        this.camera = this.orthoCamera;
        this.transformControls.camera = this.camera;
        this.controls.object = this.camera;
        this.controls.enabled = true;
        this.controls.enableRotate = false;
        this.controls.enablePan = false;
        this.controls.enableZoom = true;
        this.fitOrthographic(view);
        this.setStatus(`${view[0].toUpperCase()}${view.slice(1)} orthographic edit mode`);
    }

    cycleView() {
        const index = VIEW_ORDER.indexOf(this.activeView);
        const next = VIEW_ORDER[(index + 1) % VIEW_ORDER.length] || 'right';
        this.setView(next);
    }

    syncViewUi(view) {
        if (this.viewSelect && this.viewSelect.value !== view) {
            this.viewSelect.value = view;
        }
        if (this.viewCube) {
            this.viewCube.dataset.activeView = view;
            const cubeTitle = bpT('blueprint_cube_title', 'Blueprint view cube');
            this.viewCube.title = cubeTitle;
            this.viewCube.setAttribute('aria-label', cubeTitle);
        }
        if (this.viewCubeLabel) {
            this.viewCubeLabel.textContent = bpT(`blueprint_view_${view}`, VIEW_LABELS[view] || 'Right');
        }
        const cubeNext = this.card.querySelector('[data-blueprint-cycle]');
        if (cubeNext) {
            const nextTitle = bpT('blueprint_cube_next_title', 'Next blueprint camera view');
            cubeNext.title = nextTitle;
            cubeNext.setAttribute('aria-label', nextTitle);
        }
        const cubeTitles = {
            top: ['blueprint_cube_top_title', 'Top view'],
            right: ['blueprint_cube_right_title', 'Right view'],
            bottom: ['blueprint_cube_bottom_title', 'Bottom view'],
            left: ['blueprint_cube_left_title', 'Left view'],
        };
        this.card.querySelectorAll('[data-blueprint-cube-view]').forEach((button) => {
            const buttonView = button.dataset.blueprintCubeView;
            const [key, fallback] = cubeTitles[buttonView] || [];
            if (key) {
                const title = bpT(key, fallback);
                button.title = title;
                button.setAttribute('aria-label', title);
            }
            button.setAttribute('aria-pressed', buttonView === view ? 'true' : 'false');
        });
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
        const hits = this.raycaster.intersectObjects(this.pickableObjects(), false);
        if (!hits.length) return;
        const nodeId = hits[0].object.userData.blueprintNodeId;
        if (!nodeId) return;
        this.updateSelection(nodeId);
        const mesh = this.nodeMeshes.get(String(nodeId));
        if (!mesh) return;
        if (this.activeView === 'perspective') {
            this.transformControls.attach(mesh);
            return;
        }
        this.draggingNodeId = nodeId;
        this.dragStartPointer = { x: event.clientX, y: event.clientY };
        this.dragMoved = false;
        this.controls.enabled = false;
        const normal = new THREE.Vector3();
        this.camera.getWorldDirection(normal);
        this.dragPlane.setFromNormalAndCoplanarPoint(normal, mesh.position);
        event.preventDefault();
    }

    pickableObjects() {
        return [...this.nodeMeshes.values(), ...this.labelSprites.values()];
    }

    onPointerMove(event) {
        if (!this.draggingNodeId || this.activeView === 'perspective') return;
        if (!this.dragMoved && this.dragStartPointer) {
            const dx = event.clientX - this.dragStartPointer.x;
            const dy = event.clientY - this.dragStartPointer.y;
            if ((dx * dx) + (dy * dy) < 16) return;
        }
        this.dragMoved = true;
        const target = this.dragPositionFromPointer(event);
        if (target) this.moveNodeTo(this.draggingNodeId, target);
    }

    onPointerUp() {
        if (this.draggingNodeId) {
            if (this.dragMoved) this.snapSelectedNode();
            this.draggingNodeId = null;
            this.dragStartPointer = null;
            this.dragMoved = false;
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
        const nextNodeId = nodeId ? String(nodeId) : null;
        const changed = this.selectedNodeId !== nextNodeId;
        this.selectedNodeId = nextNodeId;
        if (changed) this.refreshSelectionStyle();
        if (!nextNodeId) {
            this.selectedEl.textContent = '';
            this.coordsEl.textContent = '';
            this.transformControls?.detach();
            this.setAddNodeVisible(false);
            return;
        }
        const node = this.nodes.get(nextNodeId);
        if (!node) return;
        this.selectedEl.textContent = `${node.role} / ${node.id}`;
        this.coordsEl.textContent = node.position.map((v) => Number(v).toFixed(3)).join(' / ');
        this.setAddNodeVisible(true);
        if (this.activeView === 'perspective') {
            const mesh = this.nodeMeshes.get(nextNodeId);
            if (mesh) this.transformControls.attach(mesh);
        }
    }

    refreshSelectionStyle() {
        for (const [id, mesh] of this.nodeMeshes.entries()) {
            const selected = String(id) === String(this.selectedNodeId);
            const baseScale = mesh.userData.baseScale || new THREE.Vector3(1, 1, 1);
            mesh.scale.copy(baseScale).multiplyScalar(selected ? 1.6 : 1);
            if (mesh.material?.emissiveIntensity !== undefined) {
                mesh.material.emissiveIntensity = selected ? 0.95 : 0.28;
            }
            const sprite = this.labelSprites.get(String(id));
            if (!sprite?.material) continue;
            const role = sprite.userData.role || mesh.userData.role || 'node';
            const color = sprite.userData.color || mesh.userData.color || roleColor(role);
            const oldMap = sprite.material.map;
            sprite.material.map = createRoleTexture(THREE, role, color, selected);
            sprite.material.needsUpdate = true;
            oldMap?.dispose?.();
            const spriteBase = sprite.userData.baseScale || new THREE.Vector3(0.18, 0.072, 1);
            sprite.scale.copy(spriteBase).multiplyScalar(selected ? 1.22 : 1);
            sprite.renderOrder = selected ? 30 : 20;
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

    setAddNodeVisible(visible) {
        this.addNodeBtn?.classList.toggle('is-visible', Boolean(visible));
    }

    addLinkedNode() {
        if (!this.selectedNodeId) return;
        const parent = this.nodes.get(String(this.selectedNodeId));
        const parentMesh = this.nodeMeshes.get(String(this.selectedNodeId));
        if (!parent || !parentMesh) return;
        const cameraRight = new THREE.Vector3(1, 0, 0).applyQuaternion(this.camera.quaternion).normalize();
        const cameraUp = new THREE.Vector3(0, 1, 0).applyQuaternion(this.camera.quaternion).normalize();
        const offset = Math.max(this.modelDiag * 0.055, 0.035);
        const proposed = parentMesh.position.clone()
            .addScaledVector(cameraRight, offset)
            .addScaledVector(cameraUp, offset * 0.35);
        const snapped = this.nearestVertex(proposed) || proposed;
        const id = `additional_${String(this.selectedNodeId).replace(/[^A-Za-z0-9_.-]+/g, '_')}_${++this.additionalNodeSeq}`;
        const position = nodePositionToArray(snapped);
        this.nodes.set(id, {
            id,
            type: 'additional',
            role: 'additional',
            position,
            parentId: String(this.selectedNodeId),
            additional: true,
        });
        this.additionalParents.set(id, String(this.selectedNodeId));
        this.createNodeMesh(id, 'additional', position);
        this.redrawLines();
        this.updateSelection(id);
        this.updateDirtyState();
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
        for (const id of [...this.additionalParents.keys()]) {
            const mesh = this.nodeMeshes.get(String(id));
            const sprite = this.labelSprites.get(String(id));
            mesh?.parent?.remove(mesh);
            sprite?.parent?.remove(sprite);
            this.nodes.delete(String(id));
            this.nodeMeshes.delete(String(id));
            this.labelSprites.delete(String(id));
            this.additionalParents.delete(String(id));
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
        for (const [nodeId, parentId] of this.additionalParents.entries()) {
            const node = this.nodes.get(String(nodeId));
            if (!node) continue;
            markers[`additional:${parentId}:${nodeId}`] = node.position.map(Number);
        }
        return markers;
    }

    async retarget() {
        if (!this.task || !this.dirty || !this.retargetBtn) return;
        const detection = this.task.rig_v2_animal_detection || {};
        const animalType = this.task.animal_type
            || this.task.rig_type
            || detection.animal_type
            || detection.animal_type_string
            || detection.selected_type_string
            || detection.candidate_animal_type_string;
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
