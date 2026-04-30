import RAPIER from 'https://cdn.jsdelivr.net/npm/@dimforge/rapier3d-compat@0.12.0/rapier.es.js';

const DEFAULTS = {
    walk_speed: 2.35,
    run_speed: 5.15,
    jump_speed: 5.8,
    gravity: 14.0,
    ground_accel: 16.0,
    ground_decel: 12.0,
    air_accel: 5.5,
    air_decel: 1.8,
    turn_lerp: 12.0,
    camera_lerp: 7.5,
    camera_offset: [0, 1.65, -3.7],
    camera_look_offset: [0, 1.1, 0],
    ground_snap: 3.0,
    jump_buffer_seconds: 0.24,
    coyote_seconds: 0.18,
    jump_start_seconds: 0.26,
    land_window_seconds: 0.12,
    recover_min_seconds: 0.9,
    character_gap: 0.03,
    ground_extent: 160,
    ground_thickness: 0.2,
    backpedal_factor: 0.72,
    ragdoll_impulse: [0.25, 0.18, 1.0],
    ragdoll_part_impulse: 0.28,
    ragdoll_spring_stiffness: 6.5,
    ragdoll_spring_damping: 1.8,
};

const STATE_TO_CLIP_TYPE = {
    idle: 'idle',
    walk: 'walk',
    walkBack: 'walkBack',
    run: 'run',
    jump: 'jump',
    fall: 'jump',
    land: 'land',
    recover: 'recover',
    ragdoll: null,
};

const CLIP_HINTS = {
    idle: ['idle', 'idle2', 'idle_glance_around', 'idle_happy', 'idle_sad', 'idle_dwarf'],
    walk: ['walking', 'walking2', 'happy_walk', 'strut_walking'],
    walkBack: ['walking_backwards', 'walking_backwards_happy', 'walk_backwards', 'backward', 'backwards'],
    run: ['running', 'running2', 'running3'],
    jump: ['jump_forward', 'jump_forward_run', 'jump', 'jump_hunched'],
    land: ['land', 'landing'],
    die: ['dying', 'defeat2'],
    recover: ['getting_up', 'get_up', 'stand_up'],
};

const CLIP_ROLE_RULES = {
    idle: {
        expectedTypes: ['idle'],
        preferred: ['idle', 'idle2', 'idle_glance_around', 'idle_happy', 'idle_sad', 'idle_dwarf'],
        include: ['idle'],
        avoid: ['fight', 'combat', 'dance', 'gesture', 'emote', 'magic', 'attack', 'die'],
    },
    walk: {
        expectedTypes: ['walk'],
        preferred: ['walking', 'walking2', 'walk', 'happy_walk', 'strut_walking'],
        include: ['walk', 'walking'],
        avoid: ['backward', 'backwards', 'zombie', 'drunk', 'limp', 'lumber', 'tiptoe', 'crouch', 'scared', 'stumble'],
    },
    run: {
        expectedTypes: ['run'],
        preferred: ['running', 'running2', 'running3', 'run'],
        include: ['run', 'running'],
        avoid: ['injured', 'drunk', 'scared', 'zombie', 'stumble', 'backward', 'backwards'],
    },
    jump: {
        expectedTypes: ['jump'],
        preferred: ['jump_forward', 'jump_forward_run', 'jump', 'jump_hunched'],
        include: ['jump'],
        avoid: ['joyful', 'dance', 'fight', 'hunched', 'scared'],
    },
    land: {
        expectedTypes: ['jump', 'die'],
        preferred: ['landing', 'land'],
        include: ['land', 'landing'],
        avoid: ['jump', 'dying', 'defeat'],
    },
    die: {
        expectedTypes: ['die'],
        preferred: ['dying', 'defeat2'],
        include: ['dying', 'defeat', 'die'],
        avoid: ['getting_up', 'get_up', 'stand_up'],
    },
    recover: {
        expectedTypes: ['die'],
        preferred: ['getting_up', 'get_up', 'stand_up'],
        include: ['getting_up', 'get_up', 'stand_up'],
        avoid: ['dying', 'defeat', 'die'],
    },
};

const ONE_SHOT_STATES = new Set(['jump', 'land', 'recover']);

const TRACKED_RAGDOLL_PARTS = [
    { key: 'hips', radius: 0.14, parentKey: null },
    { key: 'spine', radius: 0.12, parentKey: 'hips' },
    { key: 'head', radius: 0.11, parentKey: 'spine' },
    { key: 'leftUpperArm', radius: 0.075, parentKey: 'spine' },
    { key: 'leftLowerArm', radius: 0.065, parentKey: 'leftUpperArm' },
    { key: 'leftHand', radius: 0.055, parentKey: 'leftLowerArm' },
    { key: 'rightUpperArm', radius: 0.075, parentKey: 'spine' },
    { key: 'rightLowerArm', radius: 0.065, parentKey: 'rightUpperArm' },
    { key: 'rightHand', radius: 0.055, parentKey: 'rightLowerArm' },
    { key: 'leftUpperLeg', radius: 0.085, parentKey: 'hips' },
    { key: 'leftLowerLeg', radius: 0.075, parentKey: 'leftUpperLeg' },
    { key: 'leftFoot', radius: 0.065, parentKey: 'leftLowerLeg' },
    { key: 'rightUpperLeg', radius: 0.085, parentKey: 'hips' },
    { key: 'rightLowerLeg', radius: 0.075, parentKey: 'rightUpperLeg' },
    { key: 'rightFoot', radius: 0.065, parentKey: 'rightLowerLeg' },
];

const RAGDOLL_BODY_SEGMENTS = [
    { key: 'torso', fromKey: 'hips', toKey: 'spine', driveKey: 'hips', radius: 0.13, parentKey: null },
    { key: 'head', fromKey: 'spine', toKey: 'head', driveKey: 'head', radius: 0.09, parentKey: 'torso' },
    { key: 'leftUpperArm', fromKey: 'leftUpperArm', toKey: 'leftLowerArm', driveKey: 'leftUpperArm', radius: 0.055, parentKey: 'torso' },
    { key: 'leftLowerArm', fromKey: 'leftLowerArm', toKey: 'leftHand', driveKey: 'leftLowerArm', radius: 0.045, parentKey: 'leftUpperArm' },
    { key: 'rightUpperArm', fromKey: 'rightUpperArm', toKey: 'rightLowerArm', driveKey: 'rightUpperArm', radius: 0.055, parentKey: 'torso' },
    { key: 'rightLowerArm', fromKey: 'rightLowerArm', toKey: 'rightHand', driveKey: 'rightLowerArm', radius: 0.045, parentKey: 'rightUpperArm' },
    { key: 'leftUpperLeg', fromKey: 'leftUpperLeg', toKey: 'leftLowerLeg', driveKey: 'leftUpperLeg', radius: 0.07, parentKey: 'torso' },
    { key: 'leftLowerLeg', fromKey: 'leftLowerLeg', toKey: 'leftFoot', driveKey: 'leftLowerLeg', radius: 0.055, parentKey: 'leftUpperLeg' },
    { key: 'rightUpperLeg', fromKey: 'rightUpperLeg', toKey: 'rightLowerLeg', driveKey: 'rightUpperLeg', radius: 0.07, parentKey: 'torso' },
    { key: 'rightLowerLeg', fromKey: 'rightLowerLeg', toKey: 'rightFoot', driveKey: 'rightLowerLeg', radius: 0.055, parentKey: 'rightUpperLeg' },
];

let rapierReadyPromise = null;

async function ensureRapierReady() {
    if (!rapierReadyPromise) {
        rapierReadyPromise = RAPIER.init().then(() => RAPIER);
    }
    return rapierReadyPromise;
}

function normalizeKey(value) {
    return String(value || '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function damp(current, target, lambda, dt) {
    return current + (target - current) * (1.0 - Math.exp(-lambda * dt));
}

function dampAngle(current, target, lambda, dt) {
    const delta = Math.atan2(Math.sin(target - current), Math.cos(target - current));
    return current + delta * (1.0 - Math.exp(-lambda * dt));
}

function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
}

function pickFirst(arr) {
    return Array.isArray(arr) && arr.length ? arr[0] : null;
}

function safeClipDuration(clip) {
    const duration = Number(clip?.duration || 0);
    return Number.isFinite(duration) && duration > 0 ? duration : 0;
}

function makeRapierVector(R, x = 0, y = 0, z = 0) {
    return new R.Vector3(Number(x) || 0, Number(y) || 0, Number(z) || 0);
}

function uniqueStrings(values) {
    return [...new Set((values || []).map((value) => String(value || '')).filter(Boolean))];
}

function scoreClipForRole(clip, role, manifestType) {
    const rule = CLIP_ROLE_RULES[role];
    if (!clip || !rule) return Number.NEGATIVE_INFINITY;

    const key = normalizeKey(clip.name);
    let score = 0;

    if (rule.expectedTypes.includes(String(manifestType || '').toLowerCase())) {
        score += 350;
    }

    let bestPreferredScore = 0;
    for (let i = 0; i < rule.preferred.length; i += 1) {
        const probe = normalizeKey(rule.preferred[i]);
        if (!probe) continue;
        if (key === probe) {
            bestPreferredScore = Math.max(bestPreferredScore, 1200 - (i * 40));
        } else if (key.includes(probe)) {
            bestPreferredScore = Math.max(bestPreferredScore, 600 - (i * 20));
        }
    }
    score += bestPreferredScore;

    for (const token of rule.include) {
        const probe = normalizeKey(token);
        if (probe && key.includes(probe)) score += 120;
    }

    for (const token of rule.avoid) {
        const probe = normalizeKey(token);
        if (probe && key.includes(probe)) score -= 260;
    }

    if (/^[a-z]+[0-9]*$/.test(key)) {
        score += 24;
    }

    return score;
}

export class PlayModeController {
    constructor(options = {}) {
        this.THREE = options.THREE || window.THREE;
        this.scene = options.scene;
        this.renderer = options.renderer || null;
        this.camera = options.camera;
        this.controls = options.controls;
        this.viewerControls = options.viewerControls;
        this.getCurrentModel = options.getCurrentModel || (() => null);
        this.getAnimations = options.getAnimations || (() => []);
        this.playClipByName = options.playClipByName || (() => {});
        this.getMixer = options.getMixer || (() => null);
        this.setStatus = options.setStatus || (() => {});
        this._configOverrideKeys = new Set(Object.keys(options.config || {}));
        this.config = { ...DEFAULTS, ...(options.config || {}) };

        this.active = false;
        this.mode = 'idle';
        this.ragdollActive = false;
        this.state = 'idle';
        this.animationManifest = null;
        this.model = null;
        this.clockNowProvider = () => performance.now() / 1000;

        this.input = {
            forward: false,
            back: false,
            left: false,
            right: false,
            sprint: false,
            jumpQueuedUntil: 0,
            ragdollTogglePressed: false,
        };

        this.motion = {
            grounded: true,
            lastGroundedAt: 0,
            moveSpeed: 0,
            yaw: 0,
            velocityY: 0,
            lastJumpTime: -100,
            lastLandTime: -100,
            recoverUntil: 0,
        };

        this.animationMap = {
            idle: null,
            walk: null,
            walkBack: null,
            run: null,
            jump: null,
            jumpRun: null,
            land: null,
            die: null,
            recover: null,
        };

        this._playingClipName = null;
        this._previousCameraMode = null;
        this._previousControlsEnabled = null;
        this._previousControlsState = null;
        this._baseTransform = null;
        this._baseYaw = 0;
        this._baseGroundY = 0;
        this._rootGroundOffset = 0;
        this._capsuleRadius = 0.2;
        this._capsuleHalfHeight = 0.6;
        this._capsuleBottomToCenter = 0.8;
        this._tmpForward = this.THREE ? new this.THREE.Vector3() : null;
        this._tmpRight = this.THREE ? new this.THREE.Vector3() : null;
        this._tmpDesired = this.THREE ? new this.THREE.Vector3() : null;
        this._tmpVelocity = this.THREE ? new this.THREE.Vector3() : null;
        this._tmpTargetVelocity = this.THREE ? new this.THREE.Vector3() : null;
        this._tmpCamTarget = this.THREE ? new this.THREE.Vector3() : null;
        this._tmpLookTarget = this.THREE ? new this.THREE.Vector3() : null;
        this._tmpQuat = this.THREE ? new this.THREE.Quaternion() : null;
        this._tmpQuatB = this.THREE ? new this.THREE.Quaternion() : null;
        this._tmpEuler = this.THREE ? new this.THREE.Euler() : null;
        this._movementBasisYaw = 0;
        this._wasMoving = false;
        this._lastMoveX = 0;
        this._lastMoveZ = 0;
        this._cameraStableTargetY = null;

        this.rapier = null;
        this.world = null;
        this.characterBody = null;
        this.characterCollider = null;
        this.characterController = null;
        this.ragdollNodes = [];
        this.ragdollJoints = [];
        this.ragdollSnapshots = new Map();
        this.ragdollParentSnapshots = new Map();
        this.ragdollGrab = {
            active: false,
            node: null,
            pointerId: null,
            startClientY: 0,
            startTranslation: null,
            target: null,
            lastGrabKey: null,
        };
        this._ragdollGrabHandlers = null;
        this.boneMap = null;
        this._savedCharacterTranslation = null;
        this._playingState = null;
    }

    async enter() {
        if (this.active && this.mode !== 'play') {
            await this.exit();
        }
        if (this.active) return;
        this.model = this.getCurrentModel();
        if (!this.model || !this.THREE) return;

        this.captureBaseTransform();
        await this.ensureManifestLoaded();
        await this.setupPhysics();
        this.buildAnimationMap();
        this.resetInputState();
        this.syncInitialYaw();

        if (this.viewerControls?.cameraMode === 'fly' && typeof this.viewerControls.setCameraMode === 'function') {
            this._previousCameraMode = this.viewerControls.cameraMode;
            this.viewerControls.setCameraMode('orbit');
        } else {
            this._previousCameraMode = null;
        }

        if (this.controls) {
            this._previousControlsState = {
                enabled: this.controls.enabled,
                enablePan: this.controls.enablePan,
                enableRotate: this.controls.enableRotate,
                enableZoom: this.controls.enableZoom,
            };
            this.controls.enabled = true;
            if (typeof this.controls.enablePan === 'boolean') this.controls.enablePan = false;
            if (typeof this.controls.enableRotate === 'boolean') this.controls.enableRotate = true;
            if (typeof this.controls.enableZoom === 'boolean') this.controls.enableZoom = true;
        }

        this.active = true;
        this.mode = 'play';
        this.ragdollActive = false;
        this.state = 'idle';
        this.motion.moveSpeed = 0;
        this.motion.velocityY = 0;
        this.motion.grounded = true;
        this.motion.lastGroundedAt = this.clockNowProvider();
        this.motion.lastJumpTime = -100;
        this.motion.lastLandTime = -100;
        this.motion.recoverUntil = 0;
        this._cameraStableTargetY = null;
        this._tmpVelocity?.set(0, 0, 0);
        this._playingState = null;
        this.syncVisualFromCharacterBody(1 / 60);
        this.snapCameraBehind();
        this.applyStateAnimation(true);
        this.setStatus(this.buildPlayReadyStatus());
    }

    async exit() {
        if (!this.active) return;

        if (this.ragdollActive) {
            this.disableRagdoll({ keepRecoverState: false });
        }

        this.destroyPhysics();
        this.restoreBaseTransform();

        const mixer = this.getMixer();
        if (mixer) {
            mixer.timeScale = 1;
        }

        if (this.controls && this._previousControlsState) {
            this.controls.enabled = this._previousControlsState.enabled;
            if (typeof this.controls.enablePan === 'boolean') this.controls.enablePan = !!this._previousControlsState.enablePan;
            if (typeof this.controls.enableRotate === 'boolean') this.controls.enableRotate = !!this._previousControlsState.enableRotate;
            if (typeof this.controls.enableZoom === 'boolean') this.controls.enableZoom = !!this._previousControlsState.enableZoom;
        } else if (this.controls && this._previousControlsEnabled !== null) {
            this.controls.enabled = this._previousControlsEnabled;
        }
        this._previousControlsState = null;
        this._previousControlsEnabled = null;
        if (
            this.viewerControls &&
            this._previousCameraMode &&
            typeof this.viewerControls.setCameraMode === 'function'
        ) {
            this.viewerControls.setCameraMode(this._previousCameraMode);
        }

        this.active = false;
        this.mode = 'idle';
        this.state = 'idle';
        this._playingClipName = null;
        this._playingState = null;
    }

    prepareInteractiveControls() {
        if (this.viewerControls?.cameraMode === 'fly' && typeof this.viewerControls.setCameraMode === 'function') {
            this._previousCameraMode = this.viewerControls.cameraMode;
            this.viewerControls.setCameraMode('orbit');
        } else {
            this._previousCameraMode = null;
        }

        if (this.controls) {
            this._previousControlsState = {
                enabled: this.controls.enabled,
                enablePan: this.controls.enablePan,
                enableRotate: this.controls.enableRotate,
                enableZoom: this.controls.enableZoom,
            };
            this.controls.enabled = true;
            if (typeof this.controls.enablePan === 'boolean') this.controls.enablePan = false;
            if (typeof this.controls.enableRotate === 'boolean') this.controls.enableRotate = true;
            if (typeof this.controls.enableZoom === 'boolean') this.controls.enableZoom = true;
        }
    }

    async enterRagdollMode() {
        if (this.active && this.mode !== 'ragdoll') {
            await this.exit();
        }
        if (this.active && this.mode === 'ragdoll') {
            this.resetRagdollMode();
            return;
        }

        this.model = this.getCurrentModel();
        if (!this.model || !this.THREE) return;

        this.captureBaseTransform();
        await this.ensureManifestLoaded();
        await this.setupPhysics({ includeCharacterController: false });
        this.buildAnimationMap();
        this.resetInputState();
        this.syncInitialYaw();
        this.prepareInteractiveControls();

        this.active = true;
        this.mode = 'ragdoll';
        this.state = 'ragdoll';
        this.motion.moveSpeed = 0;
        this.motion.velocityY = 0;
        this.motion.grounded = true;
        this.motion.lastGroundedAt = this.clockNowProvider();
        this.motion.recoverUntil = 0;
        this._cameraStableTargetY = null;
        this._tmpVelocity?.set(0, 0, 0);
        this.syncVisualFromCharacterBody(1 / 60);
        this.snapCameraBehind();
        this.enableRagdoll({ initialImpulse: true, statusText: 'Ragdoll mode active · R reset · Space impulse' });
    }

    refreshModel() {
        this.model = this.getCurrentModel();
        this.captureBaseTransform();
        this.buildAnimationMap();
    }

    consumesKey(code) {
        return [
            'KeyW', 'KeyA', 'KeyS', 'KeyD',
            'ShiftLeft', 'ShiftRight',
            'Space',
            'AltLeft', 'AltRight',
            'KeyF', 'KeyR',
        ].includes(code);
    }

    handleKeyDown(event, viewerFocused = true) {
        if (!this.active || !viewerFocused) return false;
        const code = String(event?.code || '');
        if (!this.consumesKey(code)) return false;

        if (code === 'KeyW') this.input.forward = true;
        if (code === 'KeyS') this.input.back = true;
        if (code === 'KeyA') this.input.left = true;
        if (code === 'KeyD') this.input.right = true;
        if (code === 'ShiftLeft' || code === 'ShiftRight') this.input.sprint = true;
        if (code === 'Space') {
            if (this.mode === 'ragdoll' && this.ragdollActive) {
                this.applyRagdollImpulse();
            } else {
                this.input.jumpQueuedUntil = this.clockNowProvider() + this.config.jump_buffer_seconds;
            }
        }
        if (code === 'KeyF') {
            this.snapCameraBehind();
        }
        if (code === 'KeyR' && this.mode === 'ragdoll') {
            this.resetRagdollMode();
        }
        if (code === 'AltLeft' || code === 'AltRight') {
            if (!this.input.ragdollTogglePressed) {
                this.input.ragdollTogglePressed = true;
                this.toggleRagdoll();
            }
        }
        return true;
    }

    handleKeyUp(event, viewerFocused = true) {
        if (!this.active || !viewerFocused) return false;
        const code = String(event?.code || '');
        if (!this.consumesKey(code)) return false;

        if (code === 'KeyW') this.input.forward = false;
        if (code === 'KeyS') this.input.back = false;
        if (code === 'KeyA') this.input.left = false;
        if (code === 'KeyD') this.input.right = false;
        if (code === 'ShiftLeft' || code === 'ShiftRight') this.input.sprint = false;
        if (code === 'AltLeft' || code === 'AltRight') this.input.ragdollTogglePressed = false;
        return true;
    }

    update(dt) {
        if (!this.active || !this.model || !this.THREE || !this.world) return;
        const clampedDt = clamp(Number(dt) || 0, 1 / 240, 1 / 20);
        this.world.timestep = clampedDt;

        if (this.ragdollActive) {
            try {
                this.applyRagdollGrabTarget();
                this.world.step();
                this.applyRagdollGrabTarget();
                this.updateRagdollFromPhysics(clampedDt);
            } catch (error) {
                if (!this._ragdollStepWarningShown) {
                    this._ragdollStepWarningShown = true;
                    console.warn('[PlayMode] Ragdoll physics step failed:', error);
                }
            }
            this.updateCamera(clampedDt);
            return;
        }

        this.updateMovement(clampedDt);
        this.world.step();
        this.syncVisualFromCharacterBody(clampedDt);
        this.updateStateMachine();
        this.applyStateAnimation();
        this.updateCamera(clampedDt);
    }

    async ensureManifestLoaded() {
        if (this.animationManifest) return;
        try {
            const resp = await fetch('/static/all_animations/manifest.json', { cache: 'no-store' });
            if (resp.ok) {
                this.animationManifest = await resp.json();
            }
        } catch (error) {
            console.warn('[PlayMode] Manifest load failed:', error);
        }
    }

    buildAnimationMap() {
        const clips = Array.isArray(this.getAnimations()) ? this.getAnimations() : [];
        const manifestAnimations = Array.isArray(this.animationManifest?.animations)
            ? this.animationManifest.animations
            : [];
        const manifestTypeByKey = new Map();
        for (const item of manifestAnimations) {
            const type = String(item?.type || '').toLowerCase();
            for (const probe of uniqueStrings([item?.id, item?.name, ...(Array.isArray(item?.aliases) ? item.aliases : [])])) {
                manifestTypeByKey.set(normalizeKey(probe), type);
            }
        }

        const pickRole = (role, extraHints = []) => {
            let bestClip = null;
            let bestScore = Number.NEGATIVE_INFINITY;
            const roleHints = uniqueStrings([...(CLIP_HINTS[role] || []), ...extraHints]);

            for (const clip of clips) {
                const key = normalizeKey(clip?.name);
                const manifestType =
                    manifestTypeByKey.get(key) ||
                    (Array.from(manifestTypeByKey.entries()).find(([probe]) => key === probe || key.includes(probe) || probe.includes(key))?.[1] || null);

                let score = scoreClipForRole(clip, role, manifestType);
                let bestHintScore = 0;
                for (let i = 0; i < roleHints.length; i += 1) {
                    const hint = normalizeKey(roleHints[i]);
                    if (!hint) continue;
                    if (key === hint) bestHintScore = Math.max(bestHintScore, 900 - (i * 35));
                    else if (key.includes(hint)) bestHintScore = Math.max(bestHintScore, 320 - (i * 15));
                }
                score += bestHintScore;

                if (score > bestScore) {
                    bestScore = score;
                    bestClip = clip;
                }
            }

            return bestClip;
        };

        const pickExactOrIncludes = (hints = []) => {
            const normalizedHints = uniqueStrings(hints.map(normalizeKey)).filter(Boolean);
            for (const hint of normalizedHints) {
                const exact = clips.find((clip) => normalizeKey(clip?.name) === hint);
                if (exact) return exact;
            }
            for (const hint of normalizedHints) {
                const fuzzy = clips.find((clip) => normalizeKey(clip?.name).includes(hint));
                if (fuzzy) return fuzzy;
            }
            return null;
        };

        this.animationMap.idle = pickRole('idle') || pickFirst(clips);
        this.animationMap.walk = pickRole('walk') || this.animationMap.idle;
        this.animationMap.walkBack =
            pickExactOrIncludes(['walking_backwards', 'walking_backwards_happy', 'walk_backwards', 'backward_walk', 'backwards_walk']) ||
            null;
        this.animationMap.run = pickRole('run') || this.animationMap.walk || this.animationMap.idle;
        this.animationMap.jump = pickRole('jump', ['jump_forward', 'jump_forward_run', 'jump']) || this.animationMap.run || this.animationMap.walk || this.animationMap.idle;
        this.animationMap.jumpRun =
            pickExactOrIncludes(['jump_forward_run', 'jump_run', 'jump_forward']) ||
            this.animationMap.jump;
        this.animationMap.land = pickRole('land', ['land', 'landing']) || null;
        this.animationMap.die = pickRole('die', ['dying', 'defeat2']) || null;
        this.animationMap.recover = pickRole('recover', ['getting_up', 'get_up', 'stand_up']) || null;
    }

    captureBaseTransform() {
        this.model = this.getCurrentModel();
        if (!this.model || !this.THREE) return;

        const box = new this.THREE.Box3().setFromObject(this.model);
        const size = box.getSize(new this.THREE.Vector3());
        this._baseTransform = {
            position: this.model.position.clone(),
            quaternion: this.model.quaternion.clone(),
            scale: this.model.scale.clone(),
            size,
        };

        const snapDelta = -box.min.y;
        if (Number.isFinite(snapDelta) && Math.abs(snapDelta) > 1e-4) {
            this.model.position.y += snapDelta;
        }

        const snappedBox = new this.THREE.Box3().setFromObject(this.model);
        this._baseGroundY = snappedBox.min.y;
        this._rootGroundOffset = this.model.position.y - this._baseGroundY;
        this._tmpEuler.setFromQuaternion(this.model.quaternion, 'YXZ');
        this._baseYaw = this._tmpEuler.y;
    }

    restoreBaseTransform() {
        if (!this.model || !this._baseTransform) return;
        this.model.position.copy(this._baseTransform.position);
        this.model.quaternion.copy(this._baseTransform.quaternion);
        this.model.scale.copy(this._baseTransform.scale);
        this.restoreRagdollBoneSnapshots();
    }

    resetInputState() {
        this.input.forward = false;
        this.input.back = false;
        this.input.left = false;
        this.input.right = false;
        this.input.sprint = false;
        this.input.jumpQueuedUntil = 0;
        this.input.ragdollTogglePressed = false;
        this._wasMoving = false;
        this._movementBasisYaw = this.motion.yaw;
        this._lastMoveX = 0;
        this._lastMoveZ = 0;
    }

    syncInitialYaw() {
        if (!this.model || !this.THREE) return;
        this._tmpEuler.setFromQuaternion(this.model.quaternion, 'YXZ');
        this.motion.yaw = this._tmpEuler.y;
        this._movementBasisYaw = this.motion.yaw;
        this._wasMoving = false;
        this._lastMoveX = 0;
        this._lastMoveZ = 0;
    }

    buildPlayReadyStatus() {
        const names = {
            idle: this.animationMap.idle?.name || '-',
            walk: this.animationMap.walk?.name || '-',
            walkBack: this.animationMap.walkBack?.name || '-',
            run: this.animationMap.run?.name || '-',
            jump: this.animationMap.jump?.name || '-',
            recover: this.animationMap.recover?.name || '-',
        };
        return `Play mode ready: idle ${names.idle} | walk ${names.walk} | back ${names.walkBack} | run ${names.run} | jump ${names.jump} | recover ${names.recover}`;
    }

    async setupPhysics({ includeCharacterController = true } = {}) {
        const R = await ensureRapierReady();
        this.rapier = R;
        this.destroyPhysics();

        if (!this.model || !this._baseTransform) return;

        const size = this._baseTransform.size;
        const standingHeight = clamp(size.y * 0.92, 0.45, 2.6);
        this._capsuleRadius = clamp(Math.max(size.x, size.z) * 0.18, 0.06, 0.5);
        this._capsuleHalfHeight = Math.max(0.08, standingHeight * 0.5 - this._capsuleRadius);
        this._capsuleBottomToCenter = this._capsuleHalfHeight + this._capsuleRadius;

        const height = clamp(size.y, 0.35, 2.8);
        const heightScale = clamp(height / 1.7, 0.25, 3.0);
        const hasOverride = (key) => this._configOverrideKeys?.has(key);

        if (!hasOverride('walk_speed')) this.config.walk_speed = DEFAULTS.walk_speed * heightScale;
        if (!hasOverride('run_speed')) this.config.run_speed = DEFAULTS.run_speed * heightScale;
        if (!hasOverride('camera_offset') && Array.isArray(DEFAULTS.camera_offset)) {
            this.config.camera_offset = DEFAULTS.camera_offset.map((v) => v * heightScale);
        }
        if (!hasOverride('camera_look_offset') && Array.isArray(DEFAULTS.camera_look_offset)) {
            this.config.camera_look_offset = DEFAULTS.camera_look_offset.map((v) => v * heightScale);
        }
        if (!hasOverride('backpedal_factor')) this.config.backpedal_factor = DEFAULTS.backpedal_factor;
        if (!hasOverride('ground_snap')) this.config.ground_snap = DEFAULTS.ground_snap * heightScale;

        const timeToApex = 0.34;
        const jumpHeight = clamp(height * 0.30, 0.28, 0.72);
        const computedGravity = (2 * jumpHeight) / (timeToApex * timeToApex);
        if (!hasOverride('gravity')) this.config.gravity = computedGravity;
        if (!hasOverride('jump_speed')) this.config.jump_speed = computedGravity * timeToApex;

        this.world = new R.World(makeRapierVector(R, 0, -this.config.gravity, 0));
        const groundBody = this.world.createRigidBody(
            R.RigidBodyDesc.fixed().setTranslation(
                0,
                this._baseGroundY - this.config.ground_thickness,
                0
            )
        );
        this.world.createCollider(
            R.ColliderDesc.cuboid(
                this.config.ground_extent,
                this.config.ground_thickness,
                this.config.ground_extent
            )
                .setFriction(1.0)
                .setRestitution(0.0),
            groundBody
        );

        const initialCharacterPos = this.computeCharacterTranslationFromVisual();
        this.characterBody = this.world.createRigidBody(
            R.RigidBodyDesc.kinematicPositionBased().setTranslation(
                initialCharacterPos.x,
                initialCharacterPos.y,
                initialCharacterPos.z
            )
        );
        this.characterCollider = this.world.createCollider(
            R.ColliderDesc.capsule(this._capsuleHalfHeight, this._capsuleRadius)
                .setFriction(0.0)
                .setRestitution(0.0),
            this.characterBody
        );
        if (includeCharacterController) {
            this.characterController = this.world.createCharacterController(this.config.character_gap);
            this.characterController.enableAutostep(0.55, 0.24, true);
            this.characterController.enableSnapToGround(0.05);
            this.characterController.setMaxSlopeClimbAngle((55 * Math.PI) / 180);
            this.characterController.setMinSlopeSlideAngle((30 * Math.PI) / 180);
            this.characterController.setSlideEnabled(true);
            this.characterController.setApplyImpulsesToDynamicBodies(true);
            this.characterController.setCharacterMass(80.0);
        } else {
            this.characterController = null;
        }
    }

    destroyPhysics() {
        if (this.world) {
            for (const joint of this.ragdollJoints) {
                try {
                    this.world.removeImpulseJoint(joint, true);
                } catch (_) {
                    // ignore
                }
            }
            this.ragdollJoints = [];
            for (const node of this.ragdollNodes) {
                try {
                    this.world.removeRigidBody(node.body);
                } catch (_) {
                    // ignore
                }
            }
            this.ragdollNodes = [];
            try {
                this.world.free();
            } catch (_) {
                // ignore
            }
        }
        this.world = null;
        this.characterBody = null;
        this.characterCollider = null;
        this.characterController = null;
        this.ragdollNodes = [];
        this.ragdollJoints = [];
        this.ragdollSnapshots.clear();
        this.ragdollParentSnapshots.clear();
        this.removeRagdollGrabControls();
        this.boneMap = null;
        this.ragdollActive = false;
        this._savedCharacterTranslation = null;
    }

    computeCharacterTranslationFromVisual() {
        return {
            x: this.model?.position?.x || 0,
            y: (this.model?.position?.y || 0) - this._rootGroundOffset + this._capsuleBottomToCenter,
            z: this.model?.position?.z || 0,
        };
    }

    updateMovement(dt) {
        if (!this.characterBody || !this.characterCollider || !this.characterController || !this.camera) return;

        const now = this.clockNowProvider();
        const moveX = (this.input.right ? 1 : 0) - (this.input.left ? 1 : 0);
        const moveZ = (this.input.forward ? 1 : 0) - (this.input.back ? 1 : 0);
        this._lastMoveX = moveX;
        this._lastMoveZ = moveZ;
        const hasMoveInput = Math.abs(moveX) + Math.abs(moveZ) > 0;
        const sprintAllowed = this.input.sprint && hasMoveInput && moveZ >= 0;
        const baseSpeed = sprintAllowed ? this.config.run_speed : this.config.walk_speed;
        const isBackpedal = moveZ < 0;
        const targetSpeed = hasMoveInput
            ? (isBackpedal ? baseSpeed * this.config.backpedal_factor : baseSpeed)
            : 0;
        this.motion.moveSpeed = damp(this.motion.moveSpeed, targetSpeed, 10.0, dt);

        this.camera.getWorldDirection(this._tmpForward);
        this._tmpForward.y = 0;
        if (this._tmpForward.lengthSq() < 1e-6) {
            this._tmpForward.set(0, 0, 1);
        } else {
            this._tmpForward.normalize();
        }
        this._tmpRight.set(-this._tmpForward.z, 0, this._tmpForward.x).normalize();
        this._tmpDesired.set(0, 0, 0);
        this._tmpDesired.addScaledVector(this._tmpForward, moveZ);
        this._tmpDesired.addScaledVector(this._tmpRight, moveX);

        const shouldRotate = moveZ >= 0;
        if (this._tmpDesired.lengthSq() > 1e-6) {
            this._tmpDesired.normalize();
            if (shouldRotate) {
                const targetYaw = Math.atan2(this._tmpDesired.x, this._tmpDesired.z);
                this.motion.yaw = dampAngle(this.motion.yaw, targetYaw, this.config.turn_lerp, dt);
            }
        }

        this._tmpTargetVelocity.copy(this._tmpDesired).multiplyScalar(targetSpeed);
        const velocityLerp = hasMoveInput
            ? (this.motion.grounded ? this.config.ground_accel : this.config.air_accel)
            : (this.motion.grounded ? this.config.ground_decel : this.config.air_decel);
        this._tmpVelocity.lerp(this._tmpTargetVelocity, 1.0 - Math.exp(-velocityLerp * dt));
        if (!hasMoveInput && this._tmpVelocity.lengthSq() < 1e-4) {
            this._tmpVelocity.set(0, 0, 0);
        }
        this.motion.moveSpeed = this._tmpVelocity.length();

        const currentPosBeforeJump = this.characterBody.translation();
        const groundCenterY = this._baseGroundY + this._capsuleBottomToCenter;
        const nearGround = currentPosBeforeJump.y <= (groundCenterY + 0.12);
        const canJump = this.motion.grounded || nearGround || (now - this.motion.lastGroundedAt) <= this.config.coyote_seconds;
        const wantsJump = this.input.jumpQueuedUntil >= now;
        if (wantsJump && canJump) {
            this.motion.velocityY = this.config.jump_speed;
            this.motion.grounded = false;
            this.motion.lastJumpTime = now;
            this.input.jumpQueuedUntil = 0;
        }

        if (!this.motion.grounded) {
            this.motion.velocityY -= this.config.gravity * dt;
        } else if (this.motion.velocityY < 0) {
            this.motion.velocityY = 0;
        }

        const desiredHorizontal = this._tmpVelocity.clone().multiplyScalar(dt);
        const desiredVertical = this.motion.grounded
            ? (-this.config.ground_snap * dt)
            : (this.motion.velocityY * dt);

        this.characterController.computeColliderMovement(
            this.characterCollider,
            makeRapierVector(this.rapier, desiredHorizontal.x, desiredVertical, desiredHorizontal.z)
        );

        const corrected = this.characterController.computedMovement();
        const currentPos = this.characterBody.translation();
        const nextPos = {
            x: currentPos.x + corrected.x,
            y: currentPos.y + corrected.y,
            z: currentPos.z + corrected.z,
        };
        this.characterBody.setNextKinematicTranslation(nextPos);

        const justStartedJump = (now - this.motion.lastJumpTime) < 0.16 && this.motion.velocityY > 0;
        const groundedNow = !justStartedJump && (
            !!this.characterController.computedGrounded() ||
            nextPos.y <= (groundCenterY + 0.12)
        );

        if (groundedNow) {
            if (!this.motion.grounded && this.motion.velocityY < -1.4) {
                this.motion.lastLandTime = now;
            }
            this.motion.grounded = true;
            this.motion.lastGroundedAt = now;
            this.motion.velocityY = 0;
            if (nextPos.y <= (groundCenterY + 0.12)) {
                nextPos.y = groundCenterY;
                this.characterBody.setNextKinematicTranslation(nextPos);
            }
        } else {
            this.motion.grounded = false;
        }
    }

    syncVisualFromCharacterBody(dt = 1 / 60) {
        if (!this.model || !this.characterBody || !this.THREE || !this._baseTransform) return;
        const pos = this.characterBody.translation();
        const bottomY = pos.y - this._capsuleBottomToCenter;

        this.model.position.set(pos.x, bottomY + this._rootGroundOffset, pos.z);

        const deltaYaw = this.motion.yaw - this._baseYaw;
        this._tmpQuat.setFromAxisAngle(new this.THREE.Vector3(0, 1, 0), deltaYaw);
        this._tmpQuat.premultiply(this._baseTransform.quaternion);
        this.model.quaternion.slerp(this._tmpQuat, 1.0 - Math.exp(-this.config.turn_lerp * dt));
    }

    updateStateMachine() {
        const now = this.clockNowProvider();
        if (this.motion.recoverUntil > now) {
            this.state = 'recover';
            return;
        }
        if (!this.motion.grounded) {
            this.state = (now - this.motion.lastJumpTime) <= this.config.jump_start_seconds || this.motion.velocityY > 0.35
                ? 'jump'
                : 'fall';
            return;
        }
        if (this.animationMap.land && (now - this.motion.lastLandTime) < this.config.land_window_seconds) {
            this.state = 'land';
            return;
        }
        if (this.motion.moveSpeed > 0.28 && this._lastMoveZ < -0.1) {
            this.state = this.animationMap.walkBack ? 'walkBack' : 'walk';
            return;
        }
        if (this.motion.moveSpeed > this.config.walk_speed + 0.45) {
            this.state = 'run';
            return;
        }
        if (this.motion.moveSpeed > 0.28) {
            this.state = 'walk';
            return;
        }
        this.state = 'idle';
    }

    applyStateAnimation(force = false) {
        const clipType = STATE_TO_CLIP_TYPE[this.state];
        if (!clipType) return;

        let clip = null;
        if (this.state === 'jump' || this.state === 'fall') {
            clip = this.motion.moveSpeed > this.config.walk_speed * 0.8
                ? (this.animationMap.jumpRun || this.animationMap.jump)
                : this.animationMap.jump;
        } else {
            clip = this.animationMap[clipType];
        }
        clip = clip || this.animationMap.idle || pickFirst(this.getAnimations());
        if (!clip) return;

        const shouldRestart = force || (ONE_SHOT_STATES.has(this.state) && this._playingState !== this.state);
        if (!shouldRestart && this._playingClipName === clip.name) {
            this._playingState = this.state;
            return;
        }

        this.playClipByName(clip.name, {
            fade: ONE_SHOT_STATES.has(this.state) ? 0.12 : 0.18,
            loopOnce: ONE_SHOT_STATES.has(this.state),
            clampWhenFinished: ONE_SHOT_STATES.has(this.state),
            restart: shouldRestart,
        });
        this._playingClipName = clip.name;
        this._playingState = this.state;
    }

    updateCamera(dt) {
        if (!this.model || !this.controls || !this.THREE) return;

        const lookOffset = new this.THREE.Vector3(...this.config.camera_look_offset);
        const desiredY = this.motion.grounded
            ? this._baseGroundY + lookOffset.y
            : this.model.position.y + lookOffset.y;
        if (!Number.isFinite(this._cameraStableTargetY)) {
            this._cameraStableTargetY = desiredY;
        }
        const yLerp = this.motion.grounded
            ? Math.max(0.01, this.config.camera_lerp * 0.25)
            : this.config.camera_lerp;
        this._cameraStableTargetY = damp(this._cameraStableTargetY, desiredY, yLerp, dt);

        this._tmpLookTarget.set(
            this.model.position.x + lookOffset.x,
            this._cameraStableTargetY,
            this.model.position.z + lookOffset.z
        );
        this.controls.target.x = damp(this.controls.target.x, this._tmpLookTarget.x, this.config.camera_lerp, dt);
        this.controls.target.y = damp(this.controls.target.y, this._tmpLookTarget.y, this.config.camera_lerp, dt);
        this.controls.target.z = damp(this.controls.target.z, this._tmpLookTarget.z, this.config.camera_lerp, dt);
    }

    snapCameraBehind() {
        if (!this.camera || !this.controls || !this.model || !this.THREE) return;
        const offset = new this.THREE.Vector3(...this.config.camera_offset);
        offset.applyAxisAngle(new this.THREE.Vector3(0, 1, 0), this.motion.yaw);
        this.camera.position.copy(this.model.position).add(offset);
        this.controls.target.copy(
            this.model.position.clone().add(new this.THREE.Vector3(...this.config.camera_look_offset))
        );
        this.camera.lookAt(this.controls.target);
    }

    toggleRagdoll() {
        if (!this.active || !this.model || !this.world) return;
        if (this.ragdollActive) {
            this.disableRagdoll({ keepRecoverState: true });
        } else {
            this.enableRagdoll();
        }
    }

    getRapierQuaternion(body) {
        const q = body?.rotation?.();
        return new this.THREE.Quaternion(q?.x || 0, q?.y || 0, q?.z || 0, q?.w ?? 1).normalize();
    }

    worldToBodyLocal(point, center, quaternion) {
        return point.clone().sub(center).applyQuaternion(quaternion.clone().invert());
    }

    createCapsuleSegment(segment) {
        const fromBone = this.boneMap[segment.fromKey];
        const toBone = this.boneMap[segment.toKey];
        const driveBone = this.boneMap[segment.driveKey] || fromBone;
        if (!fromBone || !toBone || !driveBone) return null;

        const start = new this.THREE.Vector3();
        const end = new this.THREE.Vector3();
        const center = new this.THREE.Vector3();
        const dir = new this.THREE.Vector3();
        const restDriveWorldQuat = new this.THREE.Quaternion();
        const restBodyQuat = new this.THREE.Quaternion();

        fromBone.getWorldPosition(start);
        toBone.getWorldPosition(end);
        driveBone.getWorldQuaternion(restDriveWorldQuat);
        dir.copy(end).sub(start);
        const length = Math.max(0.001, dir.length());
        dir.normalize();
        center.copy(start).add(end).multiplyScalar(0.5);

        restBodyQuat.setFromUnitVectors(new this.THREE.Vector3(0, 1, 0), dir).normalize();
        const radius = clamp(Math.min(segment.radius, length * 0.32), 0.025, 0.18);
        const halfHeight = Math.max(length * 0.5 - radius, radius * 0.2);

        const body = this.world.createRigidBody(
            this.rapier.RigidBodyDesc.dynamic()
                .setTranslation(center.x, center.y, center.z)
                .setRotation({ x: restBodyQuat.x, y: restBodyQuat.y, z: restBodyQuat.z, w: restBodyQuat.w })
                .setLinearDamping(0.92)
                .setAngularDamping(1.65)
                .setCcdEnabled(true)
        );
        const collider = this.world.createCollider(
            this.rapier.ColliderDesc.capsule(halfHeight, radius)
                .setDensity(1.1)
                .setRestitution(0.0)
                .setFriction(0.95),
            body
        );

        return {
            ...segment,
            bone: driveBone,
            fromBone,
            toBone,
            body,
            collider,
            worldPos: center.clone(),
            worldQuat: restBodyQuat.clone(),
            restStart: start.clone(),
            restEnd: end.clone(),
            restDir: dir.clone(),
            restLength: length,
            restBodyQuat,
            restPhysicsQuat: restBodyQuat.clone(),
            restDriveWorldQuat,
            radius,
            halfHeight,
        };
    }

    applyRagdollAngularSprings(dt) {
        if (!this.ragdollNodes.length || !this.THREE || !this.rapier) return;
        return;
        const stiffness = Number(this.config.ragdoll_spring_stiffness) || DEFAULTS.ragdoll_spring_stiffness;
        const damping = Number(this.config.ragdoll_spring_damping) || DEFAULTS.ragdoll_spring_damping;
        const currentQuat = new this.THREE.Quaternion();
        const correction = new this.THREE.Quaternion();
        const axis = new this.THREE.Vector3();

        for (const node of this.ragdollNodes) {
            currentQuat.copy(this.getRapierQuaternion(node.body));
            correction.copy(node.restPhysicsQuat || new this.THREE.Quaternion()).multiply(currentQuat.clone().invert()).normalize();
            if (correction.w < 0) {
                correction.x *= -1;
                correction.y *= -1;
                correction.z *= -1;
                correction.w *= -1;
            }
            const sinHalf = Math.sqrt(Math.max(0, 1 - correction.w * correction.w));
            if (sinHalf < 1e-4) continue;
            const angle = 2 * Math.atan2(sinHalf, correction.w);
            axis.set(correction.x / sinHalf, correction.y / sinHalf, correction.z / sinHalf);
            const angvel = node.body.angvel?.() || { x: 0, y: 0, z: 0 };
            try {
                node.body.applyTorqueImpulse(makeRapierVector(
                    this.rapier,
                    (axis.x * angle * stiffness - angvel.x * damping) * dt,
                    (axis.y * angle * stiffness - angvel.y * damping) * dt,
                    (axis.z * angle * stiffness - angvel.z * damping) * dt
                ), true);
            } catch (_) {
                // ignore
            }
        }
    }

    setupRagdollGrabControls() {
        const domElement = this.renderer?.domElement;
        if (!domElement || this._ragdollGrabHandlers || !this.THREE) return;

        const onPointerDown = (event) => {
            if (this.ragdollGrab.active) return;
            if (!this.ragdollActive || event.button !== 0) return;
            const node = this.findRagdollNodeFromPointer(event);
            if (!node) return;
            event.preventDefault();
            event.stopPropagation();
            const t = node.body.translation();
            this.ragdollGrab = {
                active: true,
                node,
                pointerId: event.pointerId ?? 'mouse',
                startClientY: event.clientY,
                startTranslation: { x: t.x, y: t.y, z: t.z },
                target: { x: t.x, y: t.y, z: t.z },
                lastGrabKey: node.key,
            };
            if (event.pointerId !== undefined) domElement.setPointerCapture?.(event.pointerId);
            domElement.style.cursor = 'grabbing';
            if (this.controls) this.controls.enabled = false;
        };

        const onPointerMove = (event) => {
            const pointerId = event.pointerId ?? 'mouse';
            if (!this.ragdollGrab.active || pointerId !== this.ragdollGrab.pointerId) return;
            event.preventDefault();
            const lift = (this.ragdollGrab.startClientY - event.clientY) * 0.006;
            this.ragdollGrab.target = {
                x: this.ragdollGrab.startTranslation.x,
                y: this.ragdollGrab.startTranslation.y + lift,
                z: this.ragdollGrab.startTranslation.z,
            };
            this.applyRagdollGrabTarget();
        };

        const endGrab = (event) => {
            if (!this.ragdollGrab.active) return;
            const pointerId = event?.pointerId ?? 'mouse';
            if (pointerId !== this.ragdollGrab.pointerId) return;
            try {
                if (event?.pointerId !== undefined) domElement.releasePointerCapture?.(this.ragdollGrab.pointerId);
            } catch (_) {
                // ignore
            }
            this.ragdollGrab.active = false;
            this.ragdollGrab.node = null;
            this.ragdollGrab.pointerId = null;
            this.ragdollGrab.target = null;
            domElement.style.cursor = '';
            if (this.controls) this.controls.enabled = true;
        };

        domElement.addEventListener('pointerdown', onPointerDown, { capture: true });
        domElement.addEventListener('pointermove', onPointerMove, { capture: true });
        domElement.addEventListener('pointerup', endGrab, { capture: true });
        domElement.addEventListener('pointercancel', endGrab, { capture: true });
        domElement.addEventListener('pointerleave', endGrab, { capture: true });
        domElement.addEventListener('mousedown', onPointerDown, { capture: true });
        window.addEventListener('mousemove', onPointerMove, { capture: true });
        window.addEventListener('mouseup', endGrab, { capture: true });
        this._ragdollGrabHandlers = { onPointerDown, onPointerMove, endGrab };
    }

    removeRagdollGrabControls() {
        const domElement = this.renderer?.domElement;
        const handlers = this._ragdollGrabHandlers;
        if (!domElement || !handlers) return;
        domElement.removeEventListener('pointerdown', handlers.onPointerDown, { capture: true });
        domElement.removeEventListener('pointermove', handlers.onPointerMove, { capture: true });
        domElement.removeEventListener('pointerup', handlers.endGrab, { capture: true });
        domElement.removeEventListener('pointercancel', handlers.endGrab, { capture: true });
        domElement.removeEventListener('pointerleave', handlers.endGrab, { capture: true });
        domElement.removeEventListener('mousedown', handlers.onPointerDown, { capture: true });
        window.removeEventListener('mousemove', handlers.onPointerMove, { capture: true });
        window.removeEventListener('mouseup', handlers.endGrab, { capture: true });
        domElement.style.cursor = '';
        this._ragdollGrabHandlers = null;
        this.ragdollGrab.active = false;
    }

    pointerRay(event) {
        const domElement = this.renderer?.domElement;
        if (!domElement || !this.camera || !this.THREE) return null;
        const rect = domElement.getBoundingClientRect();
        const ndc = new this.THREE.Vector2(
            ((event.clientX - rect.left) / rect.width) * 2 - 1,
            -(((event.clientY - rect.top) / rect.height) * 2 - 1)
        );
        const raycaster = new this.THREE.Raycaster();
        raycaster.setFromCamera(ndc, this.camera);
        return raycaster.ray;
    }

    findRagdollNodeFromPointer(event) {
        const ray = this.pointerRay(event);
        const domElement = this.renderer?.domElement;
        if (!ray || !domElement || !this.ragdollNodes.length || !this.THREE) return null;
        const rect = domElement.getBoundingClientRect();
        const segmentStart = new this.THREE.Vector3();
        const segmentEnd = new this.THREE.Vector3();
        const segmentDir = new this.THREE.Vector3();
        const pointOnRay = new this.THREE.Vector3();
        const pointOnSegment = new this.THREE.Vector3();
        const projected = new this.THREE.Vector3();
        let best = null;
        let bestDistanceSq = Infinity;
        let bestScreen = null;
        let bestScreenDistanceSq = Infinity;

        for (const node of this.ragdollNodes) {
            const t = node.body.translation();
            const q = this.getRapierQuaternion(node.body);
            const center = new this.THREE.Vector3(t.x, t.y, t.z);
            segmentDir.set(0, 1, 0).applyQuaternion(q).normalize();
            segmentStart.copy(center).addScaledVector(segmentDir, -node.restLength * 0.5);
            segmentEnd.copy(center).addScaledVector(segmentDir, node.restLength * 0.5);
            const distanceSq = ray.distanceSqToSegment(segmentStart, segmentEnd, pointOnRay, pointOnSegment);
            const threshold = Math.max(node.radius * 4.0, 0.16);
            if (distanceSq <= threshold * threshold && distanceSq < bestDistanceSq) {
                bestDistanceSq = distanceSq;
                best = node;
            }

            projected.copy(center).project(this.camera);
            const sx = rect.left + ((projected.x + 1) * 0.5) * rect.width;
            const sy = rect.top + ((1 - projected.y) * 0.5) * rect.height;
            const dx = sx - event.clientX;
            const dy = sy - event.clientY;
            const screenDistanceSq = dx * dx + dy * dy;
            if (screenDistanceSq < bestScreenDistanceSq) {
                bestScreenDistanceSq = screenDistanceSq;
                bestScreen = node;
            }
        }
        return best || (bestScreenDistanceSq < 96 * 96 ? bestScreen : null);
    }

    applyRagdollGrabTarget() {
        const grab = this.ragdollGrab;
        if (!grab.active || !grab.node?.body || !grab.target || !this.rapier) return;
        grab.node.body.setTranslation(makeRapierVector(this.rapier, grab.target.x, grab.target.y, grab.target.z), true);
        try {
            grab.node.body.setLinvel(makeRapierVector(this.rapier, 0, 0, 0), true);
            grab.node.body.setAngvel(makeRapierVector(this.rapier, 0, 0, 0), true);
        } catch (_) {
            // ignore
        }
    }

    enableRagdoll({ initialImpulse = true, statusText = 'Ragdoll active · Alt to recover' } = {}) {
        if (!this.world || !this.THREE || !this.model) return;
        this.boneMap = this.buildHumanoidBoneMap() || this.boneMap;
        if (!this.boneMap) {
            this.setStatus('Ragdoll unavailable: humanoid skeleton not detected');
            return;
        }

        const mixer = this.getMixer();
        if (mixer) {
            mixer.timeScale = 0;
        }

        this.clearRagdollBodies();
        this.ragdollSnapshots.clear();
        this.ragdollParentSnapshots.clear();

        const snapshotKeys = new Set();
        for (const segment of RAGDOLL_BODY_SEGMENTS) {
            snapshotKeys.add(segment.fromKey);
            snapshotKeys.add(segment.toKey);
            snapshotKeys.add(segment.driveKey);
        }
        for (const key of snapshotKeys) {
            const bone = this.boneMap[key];
            if (!bone || this.ragdollSnapshots.has(key)) continue;

            this.ragdollSnapshots.set(key, {
                position: bone.position.clone(),
                quaternion: bone.quaternion.clone(),
                rotation: bone.rotation.clone(),
                parent: bone.parent || null,
            });
            if (bone.parent?.isBone && !this.ragdollParentSnapshots.has(bone.parent)) {
                this.ragdollParentSnapshots.set(bone.parent, {
                    position: bone.parent.position.clone(),
                    quaternion: bone.parent.quaternion.clone(),
                    rotation: bone.parent.rotation.clone(),
                });
            }
        }

        this.model.updateMatrixWorld(true);
        for (const segment of RAGDOLL_BODY_SEGMENTS) {
            const node = this.createCapsuleSegment(segment);
            if (node) this.ragdollNodes.push(node);
        }

        const nodesByKey = new Map(this.ragdollNodes.map((node) => [node.key, node]));
        for (const node of this.ragdollNodes) {
            if (!node.parentKey) continue;
            const parent = nodesByKey.get(node.parentKey);
            if (!parent) continue;

            const jointWorld = node.restStart.clone();
            const parentAnchor = this.worldToBodyLocal(jointWorld, parent.worldPos, parent.restPhysicsQuat);
            const childAnchor = this.worldToBodyLocal(jointWorld, node.worldPos, node.restPhysicsQuat);
            const joint = this.world.createImpulseJoint(
                this.rapier.JointData.spherical(
                    makeRapierVector(this.rapier, parentAnchor.x, parentAnchor.y, parentAnchor.z),
                    makeRapierVector(this.rapier, childAnchor.x, childAnchor.y, childAnchor.z)
                ),
                parent.body,
                node.body,
                true
            );
            this.ragdollJoints.push(joint);
        }

        if (initialImpulse) {
            this.applyRagdollImpulse(nodesByKey, { includeParts: true });
        }

        if (this.characterBody) {
            const currentTranslation = this.characterBody.translation();
            this._savedCharacterTranslation = {
                x: currentTranslation.x,
                y: currentTranslation.y,
                z: currentTranslation.z,
            };
            this.characterBody.setTranslation(
                makeRapierVector(this.rapier, 0, -999, 0),
                true
            );
            this.characterBody.setNextKinematicTranslation(
                makeRapierVector(this.rapier, 0, -999, 0)
            );
        }

        this.ragdollActive = true;
        this.setupRagdollGrabControls();
        this.state = 'ragdoll';
        this.motion.recoverUntil = 0;
        this._playingClipName = null;
        this._playingState = 'ragdoll';
        this.setStatus(statusText);
    }

    applyRagdollImpulse(nodesByKey = null, { includeParts = false } = {}) {
        if (!this.rapier || (!this.ragdollActive && !nodesByKey)) return;
        const lookup = nodesByKey || new Map(this.ragdollNodes.map((node) => [node.key, node]));
        const torsoNode = lookup.get('torso') || this.ragdollNodes[0];
        const headNode = lookup.get('head') || torsoNode;
        const targetNode = torsoNode || headNode;
        if (!targetNode) return;

        const impulse = this.config.ragdoll_impulse;
        let x = impulse[0];
        let y = impulse[1];
        let z = impulse[2];
        if (this.camera && this.THREE) {
            this.camera.getWorldDirection(this._tmpForward);
            this._tmpForward.y = 0;
            if (this._tmpForward.lengthSq() > 1e-6) {
                this._tmpForward.normalize();
                x = this._tmpForward.x * impulse[2];
                z = this._tmpForward.z * impulse[2];
            }
        }

        const addVelocity = (node, vx, vy, vz) => {
            if (!node?.body) return;
            try {
                const current = node.body.linvel?.() || { x: 0, y: 0, z: 0 };
                node.body.setLinvel(makeRapierVector(
                    this.rapier,
                    current.x + vx,
                    current.y + vy,
                    current.z + vz
                ), true);
            } catch (_) {
                node.body.applyImpulse(makeRapierVector(this.rapier, vx * 0.025, vy * 0.025, vz * 0.025), true);
            }
        };

        addVelocity(targetNode, x, y, z);
        if (headNode && headNode !== targetNode) {
            addVelocity(headNode, x * 0.35, y * 0.25, z * 0.35);
        }

        if (includeParts) {
            const side = Number(this.config.ragdoll_part_impulse) || DEFAULTS.ragdoll_part_impulse;
            const partImpulses = {
                leftUpperArm: [-side, 0.25, z * 0.18],
                leftLowerArm: [-side * 0.8, 0.12, z * 0.22],
                leftHand: [-side * 1.1, 0.18, z * 0.28],
                rightUpperArm: [side, 0.25, z * 0.18],
                rightLowerArm: [side * 0.8, 0.12, z * 0.22],
                rightHand: [side * 1.1, 0.18, z * 0.28],
                leftUpperLeg: [-side * 0.35, -0.1, z * 0.12],
                rightUpperLeg: [side * 0.35, -0.1, z * 0.12],
                leftFoot: [-side * 0.2, -0.05, z * 0.08],
                rightFoot: [side * 0.2, -0.05, z * 0.08],
            };
            for (const [key, values] of Object.entries(partImpulses)) {
                const node = lookup.get(key);
                if (!node?.body) continue;
                addVelocity(node, values[0], values[1], values[2]);
                try {
                    node.body.applyTorqueImpulse(makeRapierVector(this.rapier, values[2] * 0.01, values[0] * 0.015, values[1] * 0.01), true);
                } catch (_) {
                    // Older Rapier builds may not expose torque impulse on all body handles.
                }
            }
        }
    }

    resetRagdollMode() {
        if (!this.active || this.mode !== 'ragdoll' || !this.model || !this.characterBody) return;
        if (this.ragdollActive) {
            this.disableRagdoll({ keepRecoverState: false });
        }

        this.restoreBaseTransform();
        this.captureBaseTransform();
        this.motion.yaw = this._baseYaw;
        this.motion.moveSpeed = 0;
        this.motion.velocityY = 0;
        this.motion.grounded = true;
        this.motion.lastGroundedAt = this.clockNowProvider();
        this._tmpVelocity?.set(0, 0, 0);

        const translation = this.computeCharacterTranslationFromVisual();
        this.characterBody.setTranslation(makeRapierVector(this.rapier, translation.x, translation.y, translation.z), true);
        this.characterBody.setNextKinematicTranslation(makeRapierVector(this.rapier, translation.x, translation.y, translation.z));
        this.syncVisualFromCharacterBody(1 / 60);
        this.enableRagdoll({ initialImpulse: true, statusText: 'Ragdoll mode reset · R reset · Space impulse' });
    }

    disableRagdoll({ keepRecoverState = true } = {}) {
        if (!this.ragdollActive) return;

        this.removeRagdollGrabControls();
        this.restoreRagdollBoneSnapshots();
        this.clearRagdollBodies();

        if (this.characterBody) {
            const restore = this._savedCharacterTranslation || this.computeCharacterTranslationFromVisual();
            this.characterBody.setTranslation(
                makeRapierVector(this.rapier, restore.x, restore.y, restore.z),
                true
            );
            this.characterBody.setNextKinematicTranslation(
                makeRapierVector(this.rapier, restore.x, restore.y, restore.z)
            );
        }
        this._savedCharacterTranslation = null;

        const mixer = this.getMixer();
        if (mixer) {
            mixer.timeScale = 1;
        }

        this.ragdollActive = false;
        const recoverClip = this.animationMap.recover;
        if (this.mode !== 'ragdoll' && keepRecoverState && recoverClip) {
            this.playClipByName(recoverClip.name, {
                fade: 0.12,
                loopOnce: true,
                clampWhenFinished: true,
                restart: true,
            });
            this._playingClipName = recoverClip.name;
            this._playingState = 'recover';
            this.motion.recoverUntil =
                this.clockNowProvider() + Math.max(this.config.recover_min_seconds, safeClipDuration(recoverClip) * 0.8);
            this.state = 'recover';
            this.setStatus(`Recovered from ragdoll · ${recoverClip.name}`);
        } else {
            this.motion.recoverUntil = 0;
            this.state = 'idle';
            if (this.mode !== 'ragdoll') {
                this.applyStateAnimation(true);
            }
            this.setStatus('Ragdoll recovered');
        }
    }

    clearRagdollBodies() {
        if (!this.world) return;
        for (const joint of this.ragdollJoints) {
            try {
                this.world.removeImpulseJoint(joint, true);
            } catch (_) {
                // ignore
            }
        }
        this.ragdollJoints = [];
        for (const node of this.ragdollNodes) {
            try {
                this.world.removeRigidBody(node.body);
            } catch (_) {
                // ignore
            }
        }
        this.ragdollNodes = [];
    }

    restoreRagdollBoneSnapshots() {
        for (const [bone, snapshot] of this.ragdollParentSnapshots.entries()) {
            if (!bone || !snapshot) continue;
            bone.position.copy(snapshot.position);
            bone.quaternion.copy(snapshot.quaternion);
            bone.rotation.copy(snapshot.rotation);
        }
        for (const [key, snapshot] of this.ragdollSnapshots.entries()) {
            const bone = this.boneMap?.[key];
            if (!bone || !snapshot) continue;
            bone.position.copy(snapshot.position);
            bone.quaternion.copy(snapshot.quaternion);
            bone.rotation.copy(snapshot.rotation);
        }
    }

    updateRagdollFromPhysics(dt) {
        if (!this.ragdollNodes.length || !this.THREE) return;

        this.applyRagdollAngularSprings(dt);

        const center = new this.THREE.Vector3();
        const localRoot = new this.THREE.Vector3();
        const currentDir = new this.THREE.Vector3(0, 1, 0);
        const segmentStart = new this.THREE.Vector3();
        const alignQuat = new this.THREE.Quaternion();
        const targetWorldQuat = new this.THREE.Quaternion();
        const parentWorldQuat = new this.THREE.Quaternion();
        const localTargetQuat = new this.THREE.Quaternion();
        const rotationAlpha = 1.0 - Math.exp(-16 * dt);
        const positionAlpha = 1.0 - Math.exp(-14 * dt);

        for (const node of this.ragdollNodes) {
            let t = null;
            try {
                t = node.body.translation();
            } catch (error) {
                if (!this._ragdollTranslationWarningShown) {
                    this._ragdollTranslationWarningShown = true;
                    console.warn('[PlayMode] Ragdoll translation read failed:', error);
                }
                continue;
            }
            center.set(t.x, t.y, t.z);
            node.worldPos.copy(center);
            node.worldQuat.copy(this.getRapierQuaternion(node.body));

            currentDir.set(0, 1, 0).applyQuaternion(node.worldQuat).normalize();
            if (node.key === 'torso' && node.bone?.parent) {
                segmentStart.copy(center).addScaledVector(currentDir, -node.restLength * 0.5);
                localRoot.copy(segmentStart);
                node.bone.parent.worldToLocal(localRoot);
                node.bone.position.lerp(localRoot, positionAlpha);
                this.model.updateMatrixWorld(true);
            }

            alignQuat.setFromUnitVectors(node.restDir, currentDir);
            targetWorldQuat.copy(alignQuat).multiply(node.restDriveWorldQuat).normalize();
            if (node.bone?.parent) {
                node.bone.parent.getWorldQuaternion(parentWorldQuat);
                localTargetQuat.copy(parentWorldQuat).invert().multiply(targetWorldQuat).normalize();
                node.bone.quaternion.slerp(localTargetQuat, rotationAlpha);
            }
        }

        this.model?.updateMatrixWorld?.(true);
    }

    buildHumanoidBoneMap() {
        if (!this.model) return null;
        const bones = [];
        this.model.traverse((obj) => {
            if (obj?.isBone) bones.push(obj);
            if (obj?.isSkinnedMesh && obj.skeleton?.bones?.length) {
                for (const bone of obj.skeleton.bones) {
                    if (bone && !bones.includes(bone)) bones.push(bone);
                }
            }
        });
        if (!bones.length) return null;

        const byName = new Map(bones.map((bone) => [normalizeKey(bone.name), bone]));
        const findByHints = (hints) => {
            const normalized = hints.map(normalizeKey).filter(Boolean);
            for (const hint of normalized) {
                if (byName.has(hint)) return byName.get(hint);
            }
            for (const hint of normalized) {
                for (const [name, bone] of byName.entries()) {
                    if (name.includes(hint)) return bone;
                }
            }
            return null;
        };

        const map = {
            hips: findByHints(['hips', 'pelvis', 'rootx', 'root', 'mixamorig_hips']),
            spine: findByHints(['spine_03x', 'spine_02x', 'spine2', 'spine1', 'spine', 'chest', 'upperchest', 'mixamorig_spine']),
            head: findByHints(['headx', 'head', 'mixamorig_head']),
            leftUpperArm: findByHints(['arm_stretchl', 'leftupperarm', 'leftarm', 'upperarm_l', 'l_upperarm', 'mixamorig_leftarm']),
            leftLowerArm: findByHints(['forearm_stretchl', 'leftforearm', 'leftlowerarm', 'lowerarm_l', 'l_forearm', 'mixamorig_leftforearm']),
            leftHand: findByHints(['handl', 'lefthand', 'hand_l', 'l_hand', 'mixamorig_lefthand']),
            rightUpperArm: findByHints(['arm_stretchr', 'rightupperarm', 'rightarm', 'upperarm_r', 'r_upperarm', 'mixamorig_rightarm']),
            rightLowerArm: findByHints(['forearm_stretchr', 'rightforearm', 'rightlowerarm', 'lowerarm_r', 'r_forearm', 'mixamorig_rightforearm']),
            rightHand: findByHints(['handr', 'righthand', 'hand_r', 'r_hand', 'mixamorig_righthand']),
            leftUpperLeg: findByHints(['thigh_stretchl', 'leftupleg', 'leftupperleg', 'leftthigh', 'thigh_l', 'l_thigh', 'mixamorig_leftupleg']),
            leftLowerLeg: findByHints(['leg_stretchl', 'leftleg', 'leftlowerleg', 'leftshin', 'calf_l', 'l_calf', 'mixamorig_leftleg']),
            leftFoot: findByHints(['footl', 'leftfoot', 'foot_l', 'l_foot', 'mixamorig_leftfoot']),
            rightUpperLeg: findByHints(['thigh_stretchr', 'rightupleg', 'rightupperleg', 'rightthigh', 'thigh_r', 'r_thigh', 'mixamorig_rightupleg']),
            rightLowerLeg: findByHints(['leg_stretchr', 'rightleg', 'rightlowerleg', 'rightshin', 'calf_r', 'r_calf', 'mixamorig_rightleg']),
            rightFoot: findByHints(['footr', 'rightfoot', 'foot_r', 'r_foot', 'mixamorig_rightfoot']),
        };

        if (!map.hips || !map.spine || !map.head || !map.leftUpperLeg || !map.rightUpperLeg) {
            return null;
        }
        return map;
    }
}
