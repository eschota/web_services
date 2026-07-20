import {
    TaskBoneCorrectionController,
    computeSkeletonSignature,
} from './task-bone-correction-controller.js';

const SVG_NS = 'http://www.w3.org/2000/svg';
const ROLE_LABELS = {
    all: 'All bones',
    head: 'Head',
    tail: 'Tail',
    spine: 'Spine',
    front_leg: 'Front legs',
    rear_leg: 'Rear legs',
    limb: 'Other limbs',
    other: 'Other',
};

function clamp(value, minimum, maximum) {
    return Math.min(maximum, Math.max(minimum, value));
}

function finite(value, fallback = 0) {
    const number = Number(value);
    return Number.isFinite(number) ? number : fallback;
}

function normalizedDegrees(value) {
    let degrees = finite(value, 0);
    while (degrees > 180) degrees -= 360;
    while (degrees < -180) degrees += 360;
    return clamp(degrees, -180, 180);
}

function createElement(tag, className = '', text = '') {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text) element.textContent = text;
    return element;
}

function formatStatus(value) {
    return String(value || '').replaceAll('_', ' ');
}

function injectStyles() {
    if (document.getElementById('task-bone-correction-panel-styles')) return;
    const style = document.createElement('style');
    style.id = 'task-bone-correction-panel-styles';
    style.textContent = `
        .bone-correction-root { position:absolute; inset:0; z-index:18; pointer-events:none; color:#e0f2fe; font:600 11px/1.25 system-ui,-apple-system,Segoe UI,sans-serif; }
        .bone-correction-tabs { position:absolute; top:.42rem; left:50%; transform:translateX(-50%); z-index:8; display:flex; gap:2px; padding:2px; border:1px solid rgba(125,211,252,.3); border-radius:9px; background:rgba(2,6,23,.78); backdrop-filter:blur(10px); pointer-events:auto; }
        .bone-correction-tabs button { min-height:25px; border:0; border-radius:6px; padding:0 .55rem; background:transparent; color:rgba(224,242,254,.72); font:700 10px system-ui; cursor:pointer; white-space:nowrap; }
        .bone-correction-tabs button.active { background:linear-gradient(135deg,rgba(6,182,212,.9),rgba(79,70,229,.92)); color:#fff; }
        .bone-correction-overlay { position:absolute; inset:0; z-index:4; width:100%; height:100%; overflow:visible; pointer-events:none; }
        .bone-correction-line { stroke:rgba(103,232,249,.58); stroke-width:1.2; vector-effect:non-scaling-stroke; }
        .bone-correction-handle { fill:rgba(8,47,73,.88); stroke:#67e8f9; stroke-width:1.5; vector-effect:non-scaling-stroke; pointer-events:auto; cursor:grab; }
        .bone-correction-handle:hover { fill:#06b6d4; stroke:#fff; }
        .bone-correction-handle.selected { fill:#f59e0b; stroke:#fff; r:6; }
        .bone-correction-inspector { position:absolute; z-index:7; top:2.65rem; right:.48rem; bottom:.48rem; width:min(280px,42%); display:flex; flex-direction:column; gap:.42rem; padding:.55rem; overflow:auto; border:1px solid rgba(125,211,252,.28); border-radius:10px; background:rgba(2,6,23,.86); backdrop-filter:blur(12px); box-shadow:0 14px 32px rgba(2,6,23,.32); pointer-events:auto; }
        .bone-correction-toolbar { display:grid; grid-template-columns:1fr 105px; gap:.35rem; }
        .bone-correction-inspector input,.bone-correction-inspector select,.bone-correction-inspector button { min-width:0; border:1px solid rgba(125,211,252,.24); border-radius:6px; background:rgba(8,47,73,.72); color:#e0f2fe; font:600 10px system-ui; }
        .bone-correction-inspector input,.bone-correction-inspector select { min-height:27px; padding:0 .38rem; }
        .bone-correction-inspector button { min-height:27px; padding:.25rem .45rem; cursor:pointer; }
        .bone-correction-inspector button:hover:not(:disabled) { border-color:#67e8f9; background:rgba(14,116,144,.78); }
        .bone-correction-inspector :disabled { opacity:.45; cursor:not-allowed; }
        .bone-correction-bones { width:100%; min-height:76px; flex:0 0 88px; padding:.2rem !important; }
        .bone-correction-selected { min-height:26px; display:flex; align-items:center; justify-content:space-between; gap:.35rem; color:#fff; overflow-wrap:anywhere; }
        .bone-correction-scope { display:grid; grid-template-columns:1fr 1fr; gap:.35rem; }
        .bone-correction-axis-grid { display:grid; grid-template-columns:45px repeat(3,minmax(0,1fr)); gap:.25rem; align-items:center; }
        .bone-correction-axis-grid input { width:100%; }
        .bone-correction-axis-head { text-align:center; color:rgba(224,242,254,.62); }
        .bone-correction-motion { display:grid; grid-template-columns:45px 1fr 56px; gap:.3rem; align-items:center; }
        .bone-correction-motion input[type=range] { padding:0; }
        .bone-correction-advanced { border:1px solid rgba(125,211,252,.16); border-radius:7px; padding:.35rem; }
        .bone-correction-advanced summary { cursor:pointer; color:rgba(224,242,254,.72); }
        .bone-correction-actions { display:grid; grid-template-columns:repeat(3,1fr); gap:.3rem; }
        .bone-correction-actions .wide { grid-column:span 2; }
        .bone-correction-enabled { display:flex; gap:.35rem; align-items:center; }
        .bone-correction-status { min-height:28px; color:rgba(224,242,254,.68); overflow-wrap:anywhere; }
        .bone-correction-downloads { display:flex; gap:.35rem; flex-wrap:wrap; }
        .bone-correction-downloads a { color:#67e8f9; pointer-events:auto; }
        .bone-correction-rail-hint { display:none; position:absolute; left:.4rem; right:.4rem; bottom:.35rem; z-index:7; padding:.25rem .4rem; border-radius:6px; background:rgba(2,6,23,.68); color:rgba(224,242,254,.78); text-align:center; pointer-events:none; }
        #blueprint-viewer-card[data-blueprint-mode='animation'] { pointer-events:none !important; }
        #blueprint-viewer-card[data-blueprint-mode='animation'] .blueprint-viewer-stage { background:transparent !important; border-color:rgba(103,232,249,.38); pointer-events:none; }
        #blueprint-viewer-card[data-blueprint-mode='animation'] #blueprint-viewer-canvas,
        #blueprint-viewer-card[data-blueprint-mode='animation'] #blueprint-viewer-empty,
        #blueprint-viewer-card[data-blueprint-mode='animation'] .blueprint-viewer-status,
        #blueprint-viewer-card[data-blueprint-mode='animation'] .blueprint-stage-controls,
        #blueprint-viewer-card[data-blueprint-mode='animation'] .blueprint-view-cube,
        #blueprint-viewer-card[data-blueprint-mode='animation'] .blueprint-stage-hud,
        #blueprint-viewer-card[data-blueprint-mode='animation'] > .blueprint-actions,
        #blueprint-viewer-card[data-blueprint-mode='animation'] > .blueprint-legend { display:none !important; }
        #blueprint-viewer-card[data-blueprint-mode='rig'] .bone-correction-overlay,
        #blueprint-viewer-card[data-blueprint-mode='rig'] .bone-correction-inspector,
        #blueprint-viewer-card[data-blueprint-mode='rig'] .bone-correction-rail-hint { display:none !important; }
        @container (max-width:430px) {
            .bone-correction-tabs { top:.28rem; }
            .bone-correction-tabs button { min-height:22px; padding:0 .35rem; font-size:8px; }
            .bone-correction-inspector { display:none; }
            #blueprint-viewer-card[data-blueprint-mode='animation'] .bone-correction-rail-hint { display:block; }
        }
    `;
    document.head.appendChild(style);
}

export class TaskBoneCorrectionPanel {
    constructor(options = {}) {
        this.THREE = options.THREE;
        this.taskId = String(options.taskId || '').trim();
        this.card = options.card || document.getElementById('blueprint-viewer-card');
        this.getCamera = typeof options.getCamera === 'function' ? options.getCamera : () => null;
        this.onModeChange = typeof options.onModeChange === 'function' ? options.onModeChange : () => {};
        this.onInteraction = typeof options.onInteraction === 'function' ? options.onInteraction : () => {};
        this.onCorrectionChange = typeof options.onCorrectionChange === 'function' ? options.onCorrectionChange : () => {};
        this.controller = new TaskBoneCorrectionController({
            THREE: this.THREE,
            onChange: (state, detail) => this._onControllerChange(state, detail),
        });
        this.mode = 'rig';
        this.currentModelType = 'unknown';
        this.canEdit = false;
        this.dirty = false;
        this.loaded = false;
        this.runtimeEnabled = false;
        this.runtimeAllowed = false;
        this.previewEnabled = true;
        this.selectedPath = '';
        this.activeClipId = '';
        this.signatureMismatch = false;
        this.exportState = { status: 'idle' };
        this.visibleBones = [];
        this.overlayNodes = new Map();
        this.skeletonHelper = null;
        this.skeletonLayer = 29;
        this.drag = null;
        this.exportPollTimer = null;
        this._suppressDirty = false;
        this.root = null;
        if (this.card && this.THREE) {
            injectStyles();
            this._mount();
            void this.load();
        }
    }

    _mount() {
        const stage = this.card.querySelector('.blueprint-viewer-stage');
        if (!stage || stage.querySelector('.bone-correction-root')) return;
        this.root = createElement('div', 'bone-correction-root');
        this.root.innerHTML = `
            <div class="bone-correction-tabs" role="tablist">
                <button type="button" data-correction-mode="rig" class="active">Rig Points</button>
                <button type="button" data-correction-mode="animation">Animation Correction</button>
            </div>
            <svg class="bone-correction-overlay" aria-label="Animation bone handles"><g data-lines></g><g data-handles></g></svg>
            <div class="bone-correction-rail-hint">Click this viewport to maximize and edit bones</div>
            <section class="bone-correction-inspector" aria-label="Animation correction controls">
                <div class="bone-correction-toolbar">
                    <input type="search" data-bone-search placeholder="Search bones" aria-label="Search bones">
                    <select data-role-filter aria-label="Bone role"></select>
                </div>
                <select class="bone-correction-bones" data-bone-list size="5" aria-label="Bones"></select>
                <div class="bone-correction-selected"><span data-selected-name>No bone selected</span><label class="bone-correction-enabled"><input type="checkbox" data-enabled checked> Enabled</label></div>
                <div class="bone-correction-scope">
                    <button type="button" data-scope="global" class="active">All clips</button>
                    <button type="button" data-scope="clip">This clip</button>
                </div>
                <div class="bone-correction-axis-grid">
                    <span>Rotate</span><span class="bone-correction-axis-head">X°</span><span class="bone-correction-axis-head">Y°</span><span class="bone-correction-axis-head">Z°</span>
                    <span></span><input type="number" min="-180" max="180" step="0.1" data-rotation="0"><input type="number" min="-180" max="180" step="0.1" data-rotation="1"><input type="number" min="-180" max="180" step="0.1" data-rotation="2">
                </div>
                <div class="bone-correction-motion"><span>Motion</span><input type="range" min="0" max="200" step="1" data-motion-range><input type="number" min="0" max="200" step="1" data-motion-number></div>
                <details class="bone-correction-advanced">
                    <summary>Advanced position offset (% model height)</summary>
                    <div class="bone-correction-axis-grid">
                        <span>Move</span><span class="bone-correction-axis-head">X%</span><span class="bone-correction-axis-head">Y%</span><span class="bone-correction-axis-head">Z%</span>
                        <span></span><input type="number" min="-100" max="100" step="0.05" data-position="0"><input type="number" min="-100" max="100" step="0.05" data-position="1"><input type="number" min="-100" max="100" step="0.05" data-position="2">
                    </div>
                </details>
                <div class="bone-correction-actions">
                    <button type="button" data-action="toggle-preview">A/B original</button><button type="button" data-action="mirror">Mirror L/R</button><button type="button" data-action="reset-bone">Reset bone</button>
                    <button type="button" data-action="apply-children">Apply to children</button><button type="button" data-action="reset-all">Reset all</button>
                    <button type="button" data-action="undo">Undo</button><button type="button" data-action="redo">Redo</button><button type="button" data-action="save">Save draft</button>
                    <button type="button" data-action="publish" class="wide">Publish + build files</button><button type="button" data-action="retry">Retry export</button>
                </div>
                <div class="bone-correction-status" data-status>Loading corrections…</div>
                <div class="bone-correction-downloads" data-downloads></div>
            </section>
        `;
        stage.appendChild(this.root);
        const roleSelect = this.root.querySelector('[data-role-filter]');
        Object.entries(ROLE_LABELS).forEach(([value, label]) => roleSelect.add(new Option(label, value)));
        this.root.querySelectorAll('[data-correction-mode]').forEach((button) => {
            button.addEventListener('click', (event) => this.setMode(event.currentTarget.dataset.correctionMode));
        });
        this.root.querySelector('[data-bone-search]').addEventListener('input', () => this._refreshBoneList());
        roleSelect.addEventListener('change', () => this._refreshBoneList());
        this.root.querySelector('[data-bone-list]').addEventListener('change', (event) => this.selectBone(event.target.value));
        this.root.querySelectorAll('[data-scope]').forEach((button) => {
            button.addEventListener('click', () => this._setScope(button.dataset.scope));
        });
        this.root.querySelectorAll('[data-rotation]').forEach((input) => input.addEventListener('change', () => this._commitVector('rotationDeg', 'rotation')));
        this.root.querySelectorAll('[data-position]').forEach((input) => input.addEventListener('change', () => this._commitVector('positionPct', 'position')));
        this.root.querySelector('[data-enabled]').addEventListener('change', (event) => this._setPatch({ enabled: event.target.checked }));
        const motionRange = this.root.querySelector('[data-motion-range]');
        const motionNumber = this.root.querySelector('[data-motion-number]');
        const commitMotion = (event) => {
            const percent = clamp(finite(event.target.value, 100), 0, 200);
            motionRange.value = String(percent);
            motionNumber.value = String(percent);
            this._setPatch({ motionScale: percent / 100 });
        };
        motionRange.addEventListener('input', commitMotion);
        motionNumber.addEventListener('change', commitMotion);
        this.root.querySelectorAll('[data-action]').forEach((button) => button.addEventListener('click', () => this._runAction(button.dataset.action)));
        const overlay = this.root.querySelector('.bone-correction-overlay');
        overlay.addEventListener('pointerdown', (event) => this._beginDrag(event));
        overlay.addEventListener('pointermove', (event) => this._moveDrag(event));
        overlay.addEventListener('pointerup', (event) => this._endDrag(event));
        overlay.addEventListener('pointercancel', (event) => this._endDrag(event, true));
        this.card.dataset.blueprintMode = 'rig';
    }

    async load() {
        if (!this.taskId) return null;
        try {
            const response = await fetch(`/api/task/${encodeURIComponent(this.taskId)}/animation-corrections`, {
                credentials: 'same-origin',
                headers: { Accept: 'application/json' },
                cache: 'no-store',
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const payload = await response.json();
            this.canEdit = payload.canEdit === true;
            this.exportState = payload.export || { status: 'idle' };
            this._suppressDirty = true;
            this.controller.loadState(payload.active || {});
            this._guardSkeletonSignature();
            this._suppressDirty = false;
            this.dirty = false;
            this.loaded = true;
            this._refreshEditableState();
            this._renderExportState();
            return payload;
        } catch (error) {
            this._setStatus(`Corrections unavailable: ${error?.message || error}`);
            return null;
        }
    }

    configure({ model = null, currentModelType = 'unknown' } = {}) {
        const previousType = this.currentModelType;
        const nextType = String(currentModelType || 'unknown');
        const modelChanged = this.controller.model !== model;
        this.currentModelType = nextType;
        if (modelChanged) {
            const records = this.controller.configure({ model });
            this._replaceSkeletonHelper(model);
            this._guardSkeletonSignature(records);
            this._refreshBoneList();
        }
        const hasBones = this.controller.bones.length > 0;
        if (hasBones && this.currentModelType === 'animations') {
            this.card.classList.remove('hidden');
            if (modelChanged || previousType !== 'animations') this.setMode('animation');
        } else if (modelChanged || previousType !== this.currentModelType) {
            this.setMode('rig');
        }
        return this.controller.bones;
    }

    _guardSkeletonSignature(records = this.controller.bones) {
        if (!records?.length) return false;
        const signature = computeSkeletonSignature(records);
        const state = this.controller.getState();
        this.signatureMismatch = Boolean(state.skeletonSignature && signature && state.skeletonSignature !== signature);
        if (!this.signatureMismatch) return false;
        this.controller.loadState({ schemaVersion: 1, skeletonSignature: signature, global: {}, clips: {} });
        this._setStatus('Saved corrections belong to another skeleton and were not applied.');
        return true;
    }

    setActiveClip(entry) {
        this.activeClipId = String(entry?.id || entry?.name || entry || '').trim();
        this.controller.setActiveClip(this.activeClipId);
        if (this._scope() === 'clip' && !this.activeClipId) this._setScope('global');
        this._refreshSelectedInputs();
    }

    setRuntimeEnabled(enabled) {
        this.runtimeAllowed = enabled === true && this.currentModelType === 'animations';
        this.runtimeEnabled = this.runtimeAllowed && this.previewEnabled;
        this.controller.setEnabled(this.runtimeEnabled);
        return this.runtimeEnabled;
    }

    beforeMixerUpdate() {
        if (this.runtimeEnabled) this.controller.prepareForMixerUpdate();
    }

    afterMixerUpdate() {
        return this.runtimeEnabled ? this.controller.applyAfterMixerUpdate() : false;
    }

    setMode(mode) {
        const next = mode === 'animation' && this.controller.bones.length ? 'animation' : 'rig';
        this.mode = next;
        if (this.skeletonHelper) this.skeletonHelper.visible = next === 'animation';
        this.card.dataset.blueprintMode = next;
        this.root?.querySelectorAll('[data-correction-mode]').forEach((button) => button.classList.toggle('active', button.dataset.correctionMode === next));
        if (next === 'animation') this._refreshBoneList();
        else this._clearOverlay();
        this.onModeChange(next);
        return next;
    }

    isAnimationMode() {
        return this.mode === 'animation';
    }

    selectBone(path) {
        this.selectedPath = String(path || '');
        const record = this.controller.boneByPath.get(this.selectedPath);
        this.root.querySelector('[data-selected-name]').textContent = record?.name || 'No bone selected';
        const list = this.root.querySelector('[data-bone-list]');
        if (list && list.value !== this.selectedPath) list.value = this.selectedPath;
        this._refreshSelectedInputs();
        this._updateOverlaySelection();
    }

    updateOverlay() {
        if (!this.root || this.mode !== 'animation') return;
        this._attachSkeletonHelper();
        if (this.controller.bones.length) this.card.classList.remove('hidden');
        if (!this.card.offsetParent) return;
        const camera = this.getCamera();
        if (!camera) return;
        const width = Math.max(1, this.card.clientWidth);
        const height = Math.max(1, this.card.clientHeight);
        camera.updateMatrixWorld?.(true);
        const positions = new Map();
        this.visibleBones.forEach((record) => {
            const vector = new this.THREE.Vector3();
            record.bone.getWorldPosition(vector);
            vector.project(camera);
            positions.set(record.path, {
                x: (vector.x + 1) * width * 0.5,
                y: (1 - vector.y) * height * 0.5,
                visible: vector.z >= -1.1 && vector.z <= 1.1,
            });
        });
        this.overlayNodes.forEach((nodes, path) => {
            const point = positions.get(path);
            nodes.handle.style.display = point?.visible ? '' : 'none';
            if (point?.visible) {
                nodes.handle.setAttribute('cx', point.x.toFixed(2));
                nodes.handle.setAttribute('cy', point.y.toFixed(2));
            }
            if (nodes.line) {
                const parentPoint = positions.get(nodes.parentPath);
                const showLine = point?.visible && parentPoint?.visible;
                nodes.line.style.display = showLine ? '' : 'none';
                if (showLine) {
                    nodes.line.setAttribute('x1', parentPoint.x.toFixed(2));
                    nodes.line.setAttribute('y1', parentPoint.y.toFixed(2));
                    nodes.line.setAttribute('x2', point.x.toFixed(2));
                    nodes.line.setAttribute('y2', point.y.toFixed(2));
                }
            }
        });
    }

    _replaceSkeletonHelper(model) {
        this._destroySkeletonHelper();
        if (!model || !this.THREE?.SkeletonHelper || !this.controller.bones.length) return;
        const helper = new this.THREE.SkeletonHelper(model);
        helper.name = 'AnimationCorrectionSkeletonHelper';
        helper.layers.set(this.skeletonLayer);
        helper.visible = this.mode === 'animation';
        helper.renderOrder = 50;
        if (helper.material) {
            helper.material.transparent = true;
            helper.material.opacity = 0.72;
            helper.material.depthTest = false;
            helper.material.depthWrite = false;
            helper.material.toneMapped = false;
            helper.material.needsUpdate = true;
        }
        this.skeletonHelper = helper;
        this._attachSkeletonHelper();
    }

    _attachSkeletonHelper() {
        const helper = this.skeletonHelper;
        const model = this.controller.model;
        const camera = this.getCamera();
        camera?.layers?.enable?.(this.skeletonLayer);
        if (helper && !helper.parent && model?.parent) model.parent.add(helper);
    }

    _destroySkeletonHelper() {
        const helper = this.skeletonHelper;
        if (!helper) return;
        helper.removeFromParent?.();
        helper.geometry?.dispose?.();
        if (Array.isArray(helper.material)) helper.material.forEach((material) => material?.dispose?.());
        else helper.material?.dispose?.();
        this.skeletonHelper = null;
    }

    _refreshBoneList() {
        if (!this.root) return;
        const query = this.root.querySelector('[data-bone-search]').value;
        const selectedRole = this.root.querySelector('[data-role-filter]').value;
        this.visibleBones = this.controller.listBones({ role: selectedRole === 'all' ? '' : selectedRole, query }).slice(0, 220);
        const list = this.root.querySelector('[data-bone-list]');
        const previous = this.selectedPath;
        list.replaceChildren(...this.visibleBones.map((record) => {
            const depth = Math.max(0, record.path.split('/').length - 1);
            const option = new Option(`${'  '.repeat(Math.min(depth, 5))}${record.name} · ${ROLE_LABELS[record.role] || record.role}`, record.path);
            option.title = record.path;
            return option;
        }));
        const next = this.visibleBones.some((record) => record.path === previous)
            ? previous
            : (this.visibleBones.find((record) => record.role === 'head')?.path || this.visibleBones[0]?.path || '');
        this._rebuildOverlay();
        this.selectBone(next);
        this._refreshEditableState();
    }

    _rebuildOverlay() {
        const lines = this.root.querySelector('[data-lines]');
        const handles = this.root.querySelector('[data-handles]');
        lines.replaceChildren();
        handles.replaceChildren();
        this.overlayNodes.clear();
        const visibleByBone = new Map(this.visibleBones.map((record) => [record.bone, record]));
        this.visibleBones.forEach((record) => {
            const parentRecord = visibleByBone.get(record.bone.parent);
            let line = null;
            if (parentRecord) {
                line = document.createElementNS(SVG_NS, 'line');
                line.setAttribute('class', 'bone-correction-line');
                lines.appendChild(line);
            }
            const handle = document.createElementNS(SVG_NS, 'circle');
            handle.setAttribute('class', 'bone-correction-handle');
            handle.setAttribute('r', '4');
            handle.dataset.bonePath = record.path;
            const title = document.createElementNS(SVG_NS, 'title');
            title.textContent = `${record.name} (${record.role})`;
            handle.appendChild(title);
            handles.appendChild(handle);
            this.overlayNodes.set(record.path, { handle, line, parentPath: parentRecord?.path || '' });
        });
        this._updateOverlaySelection();
    }

    _updateOverlaySelection() {
        this.overlayNodes.forEach((nodes, path) => nodes.handle.classList.toggle('selected', path === this.selectedPath));
    }

    _clearOverlay() {
        this.overlayNodes.forEach((nodes) => {
            nodes.handle.style.display = 'none';
            if (nodes.line) nodes.line.style.display = 'none';
        });
    }

    _scope() {
        return this.root?.querySelector('[data-scope].active')?.dataset.scope || 'global';
    }

    _scopeOptions() {
        const scope = this._scope();
        return { scope, clipId: scope === 'clip' ? this.activeClipId : '' };
    }

    _setScope(scope) {
        const next = scope === 'clip' && this.activeClipId ? 'clip' : 'global';
        this.root.querySelectorAll('[data-scope]').forEach((button) => button.classList.toggle('active', button.dataset.scope === next));
        this._refreshSelectedInputs();
    }

    _refreshSelectedInputs() {
        if (!this.root) return;
        const correction = this.selectedPath
            ? this.controller.getResolvedCorrection(this.selectedPath, this.activeClipId)
            : { rotationDeg: [0, 0, 0], positionPct: [0, 0, 0], motionScale: 1, enabled: true };
        this.root.querySelectorAll('[data-rotation]').forEach((input) => { input.value = finite(correction.rotationDeg?.[Number(input.dataset.rotation)], 0).toFixed(2); });
        this.root.querySelectorAll('[data-position]').forEach((input) => { input.value = finite(correction.positionPct?.[Number(input.dataset.position)], 0).toFixed(3); });
        const motion = clamp(finite(correction.motionScale, 1) * 100, 0, 200);
        this.root.querySelector('[data-motion-range]').value = String(motion);
        this.root.querySelector('[data-motion-number]').value = String(Math.round(motion));
        this.root.querySelector('[data-enabled]').checked = correction.enabled !== false;
        this._refreshEditableState();
    }

    _refreshEditableState() {
        if (!this.root) return;
        const hasBone = Boolean(this.selectedPath);
        const editable = this.canEdit && hasBone;
        this.root.querySelectorAll('[data-rotation],[data-position],[data-motion-range],[data-motion-number],[data-enabled]').forEach((input) => { input.disabled = !editable; });
        this.root.querySelector('[data-scope="clip"]').disabled = !this.activeClipId;
        ['mirror', 'reset-bone', 'apply-children'].forEach((action) => { this.root.querySelector(`[data-action="${action}"]`).disabled = !editable; });
        ['reset-all', 'undo', 'redo', 'save', 'publish'].forEach((action) => { this.root.querySelector(`[data-action="${action}"]`).disabled = !this.canEdit; });
        this.root.querySelector('[data-action="retry"]').disabled = !this.canEdit || !['failed', 'awaiting_worker'].includes(this.exportState.status);
    }

    _commitVector(field, datasetKey) {
        const values = [...this.root.querySelectorAll(`[data-${datasetKey}]`)].map((input) => finite(input.value, 0));
        this._setPatch({ [field]: values });
    }

    _setPatch(patch) {
        if (!this.canEdit || !this.selectedPath) return false;
        this.onInteraction('bone-correction-input');
        return this.controller.setCorrection(this.selectedPath, patch, this._scopeOptions());
    }

    _onControllerChange(_state, detail = {}) {
        if (!this._suppressDirty && detail.reason !== 'load') {
            this.dirty = true;
            this._setStatus('Unsaved realtime corrections');
            this.onCorrectionChange(detail);
        }
        this._refreshSelectedInputs();
        this._updateOverlaySelection();
    }

    async _runAction(action) {
        try {
            if (action === 'mirror') this.controller.mirrorBone(this.selectedPath, this._scopeOptions());
            else if (action === 'toggle-preview') {
                this.previewEnabled = !this.previewEnabled;
                const button = this.root.querySelector('[data-action="toggle-preview"]');
                button.textContent = this.previewEnabled ? 'A/B original' : 'A/B corrected';
                button.classList.toggle('active', !this.previewEnabled);
                this.runtimeEnabled = this.runtimeAllowed && this.previewEnabled;
                this.controller.setEnabled(this.runtimeEnabled);
                this.onCorrectionChange({ reason: 'preview-toggle' });
            }
            else if (action === 'apply-children') this._applyToChildren();
            else if (action === 'reset-bone') this.controller.resetBone(this.selectedPath, this._scopeOptions());
            else if (action === 'reset-all') this.controller.resetAll({ scope: 'all' });
            else if (action === 'undo') this.controller.undo();
            else if (action === 'redo') this.controller.redo();
            else if (action === 'save') await this.saveDraft();
            else if (action === 'publish') await this.publish();
            else if (action === 'retry') await this.retryExport();
        } catch (error) {
            this._setStatus(error?.message || String(error));
        }
    }

    _applyToChildren() {
        const selected = this.controller.boneByPath.get(this.selectedPath);
        if (!selected || !this.canEdit) return false;
        const options = this._scopeOptions();
        const state = this.controller.getState();
        const source = options.scope === 'clip'
            ? state.clips?.[options.clipId]?.[this.selectedPath]
            : state.global?.[this.selectedPath];
        if (!source) return false;
        const isDescendant = (bone) => {
            let current = bone.parent;
            while (current) {
                if (current === selected.bone) return true;
                current = current.parent;
            }
            return false;
        };
        this.controller.beginBatch();
        this.controller.bones.filter((record) => isDescendant(record.bone)).forEach((record) => {
            this.controller.setCorrection(record.path, source, options);
        });
        return this.controller.endBatch();
    }

    async saveDraft() {
        if (!this.canEdit) return null;
        this._setStatus('Saving draft…');
        const response = await fetch(`/api/task/${encodeURIComponent(this.taskId)}/animation-corrections`, {
            method: 'PUT',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
            body: this.controller.serialize(),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.detail || `Save failed (${response.status})`);
        this.dirty = false;
        this.exportState = payload.export || this.exportState;
        this._setStatus('Draft saved. Publish when the preview is ready.');
        this._renderExportState();
        return payload;
    }

    async publish() {
        if (!this.canEdit) return null;
        try {
            if (this.dirty) await this.saveDraft();
            this._setStatus('Publishing correction revision…');
            const response = await fetch(`/api/task/${encodeURIComponent(this.taskId)}/animation-corrections/publish`, {
                method: 'POST', credentials: 'same-origin', headers: { Accept: 'application/json' },
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) throw new Error(payload.detail || `Publish failed (${response.status})`);
            this.exportState = payload.export || { status: 'queued' };
            this._renderExportState();
            this._setStatus(`Published revision ${payload.publishedRevision || ''}.`);
            return payload;
        } catch (error) {
            this._setStatus(error?.message || String(error));
            return null;
        }
    }

    async retryExport() {
        if (!this.canEdit) return null;
        const response = await fetch(`/api/task/${encodeURIComponent(this.taskId)}/animation-corrections/export/retry`, {
            method: 'POST', credentials: 'same-origin', headers: { Accept: 'application/json' },
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
            this._setStatus(payload.detail || `Retry failed (${response.status})`);
            return null;
        }
        this.exportState = payload.export || { status: 'queued' };
        this._renderExportState();
        return payload;
    }

    _renderExportState() {
        if (!this.root) return;
        const downloads = this.root.querySelector('[data-downloads]');
        downloads.replaceChildren();
        const glb = this.exportState.correctedGlbUrl;
        const zip = this.exportState.correctedFbxZipUrl;
        if (glb) {
            const link = createElement('a', '', 'Corrected GLB');
            link.href = glb;
            downloads.appendChild(link);
        }
        if (zip) {
            const link = createElement('a', '', 'Corrected FBX ZIP');
            link.href = zip;
            downloads.appendChild(link);
        }
        const error = this.exportState.error ? ` · ${this.exportState.error}` : '';
        if (!this.dirty) this._setStatus(`Export: ${formatStatus(this.exportState.status || 'idle')}${error}`);
        this._refreshEditableState();
        if (['queued', 'submitting', 'processing'].includes(this.exportState.status)) this._scheduleExportPoll();
    }

    _scheduleExportPoll() {
        clearTimeout(this.exportPollTimer);
        this.exportPollTimer = setTimeout(async () => {
            try {
                const response = await fetch(`/api/task/${encodeURIComponent(this.taskId)}/animation-corrections`, {
                    credentials: 'same-origin', headers: { Accept: 'application/json' }, cache: 'no-store',
                });
                if (response.ok) {
                    const payload = await response.json();
                    this.exportState = payload.export || this.exportState;
                    this._renderExportState();
                }
            } catch (_) {
                this._scheduleExportPoll();
            }
        }, 4000);
    }

    _setStatus(message) {
        const target = this.root?.querySelector('[data-status]');
        if (target) target.textContent = String(message || '');
    }

    _beginDrag(event) {
        const path = event.target?.dataset?.bonePath;
        if (!path) return;
        this.selectBone(path);
        if (!this.canEdit) return;
        const camera = this.getCamera();
        const record = this.controller.boneByPath.get(path);
        if (!camera || !record) return;
        event.preventDefault();
        event.stopPropagation();
        this.onInteraction('bone-correction-ik');
        const point = new this.THREE.Vector3();
        record.bone.getWorldPosition(point);
        const normal = new this.THREE.Vector3();
        camera.getWorldDirection(normal);
        this.drag = {
            pointerId: event.pointerId,
            record,
            plane: new this.THREE.Plane().setFromNormalAndCoplanarPoint(normal, point),
        };
        this.controller.beginBatch('ik-drag');
        event.currentTarget.setPointerCapture?.(event.pointerId);
    }

    _moveDrag(event) {
        if (!this.drag || event.pointerId !== this.drag.pointerId) return;
        event.preventDefault();
        const camera = this.getCamera();
        if (!camera) return;
        const rect = this.card.getBoundingClientRect();
        const ndc = new this.THREE.Vector2(
            ((event.clientX - rect.left) / Math.max(1, rect.width)) * 2 - 1,
            -(((event.clientY - rect.top) / Math.max(1, rect.height)) * 2 - 1),
        );
        const raycaster = new this.THREE.Raycaster();
        raycaster.setFromCamera(ndc, camera);
        const target = new this.THREE.Vector3();
        if (!raycaster.ray.intersectPlane(this.drag.plane, target)) return;
        this._solveCcd(this.drag.record, target);
        this.updateOverlay();
    }

    _endDrag(event, cancelled = false) {
        if (!this.drag || event.pointerId !== this.drag.pointerId) return;
        if (cancelled) this.controller.cancelBatch();
        else this.controller.endBatch();
        event.currentTarget.releasePointerCapture?.(event.pointerId);
        this.drag = null;
    }

    _solveCcd(effectorRecord, target) {
        const lengths = { head: 2, tail: 4, front_leg: 3, rear_leg: 3, limb: 3, spine: 4, other: 4 };
        const maximum = lengths[effectorRecord.role] || 4;
        const chain = [];
        let joint = effectorRecord.bone.parent;
        while (joint && chain.length < maximum) {
            const metadata = this.controller.bones.find((candidate) => candidate.bone === joint);
            if (!metadata) break;
            chain.push(metadata);
            joint = joint.parent;
        }
        if (!chain.length) return;
        const scope = this._scopeOptions();
        const jointPoint = new this.THREE.Vector3();
        const effectorPoint = new this.THREE.Vector3();
        const toEffector = new this.THREE.Vector3();
        const toTarget = new this.THREE.Vector3();
        for (let iteration = 0; iteration < 2; iteration += 1) {
            chain.forEach((metadata) => {
                metadata.bone.updateWorldMatrix?.(true, true);
                metadata.bone.getWorldPosition(jointPoint);
                effectorRecord.bone.getWorldPosition(effectorPoint);
                toEffector.copy(effectorPoint).sub(jointPoint);
                toTarget.copy(target).sub(jointPoint);
                if (toEffector.lengthSq() < 1e-9 || toTarget.lengthSq() < 1e-9) return;
                toEffector.normalize();
                toTarget.normalize();
                const worldDelta = new this.THREE.Quaternion().setFromUnitVectors(toEffector, toTarget);
                const parentWorld = new this.THREE.Quaternion();
                metadata.bone.parent?.getWorldQuaternion?.(parentWorld);
                const localDelta = parentWorld.clone().invert().multiply(worldDelta).multiply(parentWorld);
                const correction = this.controller.getResolvedCorrection(metadata.path, this.activeClipId);
                const euler = new this.THREE.Euler(
                    this.THREE.MathUtils.degToRad(correction.rotationDeg[0]),
                    this.THREE.MathUtils.degToRad(correction.rotationDeg[1]),
                    this.THREE.MathUtils.degToRad(correction.rotationDeg[2]),
                    'XYZ',
                );
                const offset = new this.THREE.Quaternion().setFromEuler(euler);
                offset.premultiply(localDelta);
                euler.setFromQuaternion(offset, 'XYZ');
                const rotationDeg = [euler.x, euler.y, euler.z].map((radians) => normalizedDegrees(this.THREE.MathUtils.radToDeg(radians)));
                this.controller.setCorrection(metadata.path, { rotationDeg }, scope);
                this.controller.applyAfterMixerUpdate();
                this.controller.model?.updateMatrixWorld?.(true);
            });
        }
    }

    destroy() {
        clearTimeout(this.exportPollTimer);
        this._destroySkeletonHelper();
        this.controller.destroy();
        this.root?.remove();
        this.root = null;
    }
}
