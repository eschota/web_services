/**
 * Rig Editor Module
 * Modal dialog for changing rig type and restarting tasks
 */

// Rig types enum
export const RigType = {
    CHAR: 'char',
    HAND: 'hand',           // Not supported yet
    RIGIDBODIES: 'rigidbodies',
    ANIMALS: 'animals',     // Not supported
    CARS: 'cars',           // Not supported yet
    SOLID: 'solid',
    RAGDOLL: 'ragdoll',     // Not supported yet
    ANIMATIONS: 'animations'
};

// Camera modes enum
export const CameraMode = {
    STATIC: 'static',
    ORBIT: 'orbit',
    FLY: 'fly'
};

// Material channels enum
export const MaterialChannel = {
    PBR: 1,
    AO: 2,
    NORMAL: 3,
    ALBEDO: 4,
    METALNESS: 5,
    ROUGHNESS: 6,
    EMISSIVE: 7
};

// Supported rig types (others are disabled)
const SUPPORTED_RIG_TYPES = [RigType.CHAR, RigType.RIGIDBODIES, RigType.SOLID, RigType.ANIMATIONS];

/**
 * RigEditor class - manages the rig editor modal and state
 */
export class RigEditor {
    constructor(options = {}) {
        this.taskId = options.taskId || null;
        this.taskStatus = options.taskStatus || 'created';
        this.currentRigType = options.rigType || RigType.CHAR;
        this.onRestart = options.onRestart || (() => {});
        this.onRigTypeChange = options.onRigTypeChange || (() => {});
        this.t = options.t || ((key) => key); // Translation function
        
        this.modal = null;
        this.isOpen = false;
    }

    /**
     * Create and inject modal HTML into the DOM
     */
    createModal() {
        if (this.modal) return;

        const modalHtml = `
            <div id="rig-editor-modal" class="rig-editor-modal hidden" style="
                position: fixed;
                inset: 0;
                background: rgba(0,0,0,0.85);
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 1200;
                padding: 1.5rem;
                opacity: 0;
                transition: opacity 0.2s ease;
            ">
                <div class="rig-editor-content card" style="
                    min-width: 400px;
                    max-width: 500px;
                    padding: 1.5rem;
                    position: relative;
                    transform: scale(0.95);
                    transition: transform 0.2s ease;
                ">
                    <!-- Close button -->
                    <button id="rig-editor-close" class="btn btn-ghost" style="
                        position: absolute;
                        top: 0.75rem;
                        right: 0.75rem;
                        padding: 0.5rem;
                        font-size: 1.2rem;
                        line-height: 1;
                    ">✕</button>

                    <!-- Header with rig type selector -->
                    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 1.25rem; padding-right: 2rem;">
                        <h3 id="rig-editor-title" style="margin: 0; font-size: 1.25rem;">
                            ${this.t('rig_editor_title')}
                        </h3>
                        <div style="display: flex; flex-direction: column; align-items: flex-end; gap: 0.25rem;">
                            <label style="font-size: 0.75rem; color: var(--text-muted);">
                                ${this.t('rig_editor_rig_type')}
                            </label>
                            <select id="rig-editor-type-select" class="form-select" style="
                                padding: 0.4rem 0.6rem;
                                font-size: 0.85rem;
                                min-width: 160px;
                            ">
                                ${this.renderRigTypeOptions()}
                            </select>
                        </div>
                    </div>

                    <!-- Message area -->
                    <div id="rig-editor-message" style="
                        margin-bottom: 1.25rem;
                        padding: 1rem;
                        background: rgba(0,0,0,0.2);
                        border-radius: 12px;
                        border: 1px solid rgba(255,255,255,0.08);
                    ">
                        <p id="rig-editor-desc" style="margin: 0; color: var(--text-secondary); line-height: 1.5;">
                            ${this.getMessageText()}
                        </p>
                    </div>

                    <!-- Action buttons -->
                    <div id="rig-editor-actions" style="display: flex; gap: 0.75rem; justify-content: flex-end;">
                        ${this.renderActionButtons()}
                    </div>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHtml);
        this.modal = document.getElementById('rig-editor-modal');
        this.bindEvents();
    }

    /**
     * Render rig type options for the dropdown
     */
    renderRigTypeOptions() {
        const types = [
            { value: RigType.CHAR, label: 'rig_type_char', supported: true },
            { value: RigType.HAND, label: 'rig_type_hand', supported: false },
            { value: RigType.RIGIDBODIES, label: 'rig_type_rigidbodies', supported: true },
            { value: RigType.ANIMALS, label: 'rig_type_animals', supported: false },
            { value: RigType.CARS, label: 'rig_type_cars', supported: false },
            { value: RigType.SOLID, label: 'rig_type_solid', supported: true },
            { value: RigType.RAGDOLL, label: 'rig_type_ragdoll', supported: false },
            { value: RigType.ANIMATIONS, label: 'rig_type_animations', supported: true },
        ];

        return types.map(t => {
            const selected = t.value === this.currentRigType ? 'selected' : '';
            const disabled = t.supported ? '' : 'disabled';
            return `<option value="${t.value}" ${selected} ${disabled}>${this.t(t.label)}</option>`;
        }).join('');
    }

    /**
     * Get message text based on task status
     */
    getMessageText() {
        const isRunning = this.taskStatus === 'processing' || this.taskStatus === 'created';
        if (isRunning) {
            return this.t('rig_editor_interrupt_desc');
        }
        return this.t('rig_editor_restart_desc');
    }

    /**
     * Render action buttons based on task status
     */
    renderActionButtons() {
        const isRunning = this.taskStatus === 'processing' || this.taskStatus === 'created';
        
        if (isRunning) {
            return `
                <button id="rig-editor-btn-no" class="btn btn-secondary">
                    ${this.t('rig_editor_btn_no')}
                </button>
                <button id="rig-editor-btn-yes" class="btn btn-primary" style="
                    background: var(--error);
                    border-color: var(--error);
                ">
                    ${this.t('rig_editor_btn_yes')}
                </button>
            `;
        }
        
        return `
            <button id="rig-editor-btn-cancel" class="btn btn-secondary">
                ${this.t('rig_editor_btn_cancel')}
            </button>
            <button id="rig-editor-btn-yes" class="btn btn-primary">
                ${this.t('rig_editor_btn_yes')}
            </button>
        `;
    }

    /**
     * Bind event listeners
     */
    bindEvents() {
        // Close button
        document.getElementById('rig-editor-close')?.addEventListener('click', () => this.close());
        
        // Background click to close
        this.modal?.addEventListener('click', (e) => {
            if (e.target === this.modal) this.close();
        });

        // ESC key to close
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) this.close();
        });

        // Rig type change
        document.getElementById('rig-editor-type-select')?.addEventListener('change', (e) => {
            this.currentRigType = e.target.value;
            this.onRigTypeChange(this.currentRigType);
        });

        // Yes/Restart button
        document.getElementById('rig-editor-btn-yes')?.addEventListener('click', () => {
            this.handleRestart();
        });

        // No/Cancel button
        document.getElementById('rig-editor-btn-no')?.addEventListener('click', () => this.close());
        document.getElementById('rig-editor-btn-cancel')?.addEventListener('click', () => this.close());
    }

    /**
     * Handle restart action
     */
    async handleRestart() {
        const yesBtn = document.getElementById('rig-editor-btn-yes');
        if (yesBtn) {
            yesBtn.disabled = true;
            yesBtn.textContent = '...';
        }

        try {
            await this.onRestart(this.currentRigType);
            this.close();
        } catch (e) {
            console.error('[RigEditor] Restart error:', e);
            if (yesBtn) {
                yesBtn.disabled = false;
                yesBtn.textContent = this.t('rig_editor_btn_yes');
            }
        }
    }

    /**
     * Open the modal
     */
    open(taskStatus = null) {
        if (taskStatus !== null) {
            this.taskStatus = taskStatus;
        }
        
        if (!this.modal) {
            this.createModal();
        }

        // Update content based on current status
        this.updateContent();

        this.modal.classList.remove('hidden');
        this.modal.style.display = 'flex';
        
        // Trigger animation
        requestAnimationFrame(() => {
            this.modal.style.opacity = '1';
            this.modal.querySelector('.rig-editor-content').style.transform = 'scale(1)';
        });
        
        this.isOpen = true;
    }

    /**
     * Close the modal
     */
    close() {
        if (!this.modal) return;
        
        this.modal.style.opacity = '0';
        this.modal.querySelector('.rig-editor-content').style.transform = 'scale(0.95)';
        
        setTimeout(() => {
            this.modal.classList.add('hidden');
            this.modal.style.display = 'none';
            this.isOpen = false;
        }, 200);
    }

    /**
     * Update modal content (when task status changes)
     */
    updateContent() {
        const title = document.getElementById('rig-editor-title');
        const desc = document.getElementById('rig-editor-desc');
        const actions = document.getElementById('rig-editor-actions');

        const isRunning = this.taskStatus === 'processing' || this.taskStatus === 'created';

        if (title) {
            title.textContent = isRunning 
                ? this.t('rig_editor_interrupt_title')
                : this.t('rig_editor_restart_title');
        }

        if (desc) {
            desc.textContent = this.getMessageText();
        }

        if (actions) {
            actions.innerHTML = this.renderActionButtons();
            // Re-bind action buttons
            document.getElementById('rig-editor-btn-yes')?.addEventListener('click', () => this.handleRestart());
            document.getElementById('rig-editor-btn-no')?.addEventListener('click', () => this.close());
            document.getElementById('rig-editor-btn-cancel')?.addEventListener('click', () => this.close());
        }
    }

    /**
     * Update task status externally
     */
    setTaskStatus(status) {
        this.taskStatus = status;
        if (this.isOpen) {
            this.updateContent();
        }
    }

    /**
     * Get current rig type
     */
    getRigType() {
        return this.currentRigType;
    }

    /**
     * Set rig type
     */
    setRigType(type) {
        if (SUPPORTED_RIG_TYPES.includes(type)) {
            this.currentRigType = type;
            const select = document.getElementById('rig-editor-type-select');
            if (select) select.value = type;
        }
    }

    /**
     * Destroy the modal
     */
    destroy() {
        if (this.modal) {
            this.modal.remove();
            this.modal = null;
        }
        this.isOpen = false;
    }
}

/**
 * AOBaker class - bakes ambient occlusion into a texture using Cavity AO algorithm
 * Cavity AO calculates occlusion based on surface curvature (normal variation)
 * Fast and produces visible results that respond well to parameter changes
 */
export class AOBaker {
    constructor(renderer, resolution = 512) {
        this.renderer = renderer;
        this.resolution = resolution;
        this.numSamples = 16;     // Number of samples around each point
        this.aoRadius = 0.02;     // Radius for curvature detection (UV space)
        this.aoIntensity = 2.0;   // Intensity multiplier
    }
    
    /**
     * Main bake function - creates AO texture for the model using Cavity AO
     * @param {THREE.Object3D} model - The 3D model to bake AO for
     * @returns {THREE.Texture} - The baked AO texture
     */
    bake(model) {
        console.log('[AOBaker] Starting Cavity AO bake...', {
            samples: this.numSamples,
            radius: this.aoRadius,
            intensity: this.aoIntensity
        });
        const startTime = performance.now();
        
        // Bake AO directly into UV space using curvature-based algorithm
        const aoTexture = this.bakeToUV(model);
        
        const elapsed = performance.now() - startTime;
        console.log(`[AOBaker] AO bake complete in ${elapsed.toFixed(0)}ms`);
        
        return aoTexture;
    }
    
    /**
     * Bake Cavity AO into UV space texture
     * Uses dFdx/dFdy derivatives to detect surface curvature
     */
    bakeToUV(model) {
        const resolution = this.resolution;
        
        // Create UV bake render target
        const aoTarget = new THREE.WebGLRenderTarget(resolution, resolution, {
            minFilter: THREE.LinearFilter,
            magFilter: THREE.LinearFilter,
            format: THREE.RGBAFormat,
            type: THREE.UnsignedByteType
        });
        
        // Create Cavity AO material
        // Uses screen-space derivatives to detect curvature
        const cavityMaterial = new THREE.ShaderMaterial({
            vertexShader: `
                varying vec3 vWorldPos;
                varying vec3 vWorldNormal;
                varying vec2 vUv;
                
                void main() {
                    // Transform to world space
                    vec4 worldPos = modelMatrix * vec4(position, 1.0);
                    vWorldPos = worldPos.xyz;
                    vWorldNormal = normalize((modelMatrix * vec4(normal, 0.0)).xyz);
                    vUv = uv;
                    
                    // Use UV as screen position (bake to UV space)
                    vec2 uvPos = uv * 2.0 - 1.0;
                    gl_Position = vec4(uvPos.x, uvPos.y, 0.0, 1.0);
                }
            `,
            fragmentShader: `
                #extension GL_OES_standard_derivatives : enable
                
                uniform float aoRadius;
                uniform float aoIntensity;
                uniform int numSamples;
                
                varying vec3 vWorldPos;
                varying vec3 vWorldNormal;
                varying vec2 vUv;
                
                void main() {
                    vec3 normal = normalize(vWorldNormal);
                    
                    // Method 1: Derivative-based curvature detection
                    // How fast does the normal change across the surface?
                    vec3 ddxNormal = dFdx(vWorldNormal);
                    vec3 ddyNormal = dFdy(vWorldNormal);
                    
                    // Normal variation = curvature indicator
                    // Higher values = more curved surface = more concave areas
                    float normalVariation = length(ddxNormal) + length(ddyNormal);
                    
                    // Position derivatives for edge detection
                    vec3 ddxPos = dFdx(vWorldPos);
                    vec3 ddyPos = dFdy(vWorldPos);
                    
                    // Cross product magnitude indicates surface detail
                    float surfaceDetail = length(cross(ddxPos, ddyPos));
                    
                    // Combine curvature metrics
                    // aoRadius controls sensitivity to different detail sizes
                    float cavity = normalVariation * aoRadius * 50.0;
                    
                    // Add edge darkening based on normal facing
                    // Surfaces facing away from "up" get slightly darker
                    float edgeFactor = 1.0 - abs(dot(normal, vec3(0.0, 1.0, 0.0)));
                    cavity += edgeFactor * aoRadius * 5.0;
                    
                    // Apply intensity and clamp
                    float ao = 1.0 - clamp(cavity * aoIntensity, 0.0, 0.95);
                    
                    // Gamma correction for better visual distribution
                    ao = pow(ao, 1.2);
                    
                    gl_FragColor = vec4(vec3(ao), 1.0);
                }
            `,
            uniforms: {
                aoRadius: { value: this.aoRadius },
                aoIntensity: { value: this.aoIntensity },
                numSamples: { value: this.numSamples }
            },
            side: THREE.DoubleSide,
            extensions: {
                derivatives: true
            }
        });
        
        // Create scene for UV baking
        const bakeScene = new THREE.Scene();
        const bakeCamera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0, 1);
        
        // Add meshes with cavity material
        model.traverse((child) => {
            if (child.isMesh && child.geometry) {
                const bakeMesh = new THREE.Mesh(child.geometry, cavityMaterial);
                bakeMesh.matrixWorld.copy(child.matrixWorld);
                bakeScene.add(bakeMesh);
            }
        });
        
        // Render to AO target
        this.renderer.setRenderTarget(aoTarget);
        this.renderer.setClearColor(0xffffff, 1); // White = no occlusion
        this.renderer.clear();
        this.renderer.render(bakeScene, bakeCamera);
        
        // Read pixels
        const pixels = new Uint8Array(resolution * resolution * 4);
        this.renderer.readRenderTargetPixels(aoTarget, 0, 0, resolution, resolution, pixels);
        
        // Apply blur to smooth AO
        this.blurAO(pixels, resolution);
        
        // Create final texture
        const aoTexture = new THREE.DataTexture(
            pixels,
            resolution,
            resolution,
            THREE.RGBAFormat,
            THREE.UnsignedByteType
        );
        aoTexture.needsUpdate = true;
        aoTexture.flipY = false;
        aoTexture.wrapS = THREE.ClampToEdgeWrapping;
        aoTexture.wrapT = THREE.ClampToEdgeWrapping;
        
        // Cleanup
        this.renderer.setRenderTarget(null);
        aoTarget.dispose();
        cavityMaterial.dispose();
        
        console.log('[AOBaker] Cavity AO texture created:', resolution + 'x' + resolution);
        return aoTexture;
    }
    
    /**
     * Box blur for AO smoothing
     */
    blurAO(pixels, resolution) {
        const blurRadius = 3;
        const temp = new Uint8Array(pixels.length);
        
        // Horizontal pass
        for (let y = 0; y < resolution; y++) {
            for (let x = 0; x < resolution; x++) {
                let sum = 0;
                let count = 0;
                
                for (let dx = -blurRadius; dx <= blurRadius; dx++) {
                    const nx = x + dx;
                    if (nx >= 0 && nx < resolution) {
                        const idx = (y * resolution + nx) * 4;
                        // Only count non-white pixels (actual geometry)
                        if (pixels[idx] < 255) {
                            sum += pixels[idx];
                            count++;
                        }
                    }
                }
                
                const idx = (y * resolution + x) * 4;
                if (count > 0) {
                    const val = Math.round(sum / count);
                    temp[idx] = temp[idx + 1] = temp[idx + 2] = val;
                } else {
                    temp[idx] = temp[idx + 1] = temp[idx + 2] = 255;
                }
                temp[idx + 3] = 255;
            }
        }
        
        // Vertical pass
        for (let y = 0; y < resolution; y++) {
            for (let x = 0; x < resolution; x++) {
                let sum = 0;
                let count = 0;
                
                for (let dy = -blurRadius; dy <= blurRadius; dy++) {
                    const ny = y + dy;
                    if (ny >= 0 && ny < resolution) {
                        const idx = (ny * resolution + x) * 4;
                        if (temp[idx] < 255) {
                            sum += temp[idx];
                            count++;
                        }
                    }
                }
                
                const idx = (y * resolution + x) * 4;
                if (count > 0) {
                    const val = Math.round(sum / count);
                    pixels[idx] = pixels[idx + 1] = pixels[idx + 2] = val;
                } else {
                    pixels[idx] = pixels[idx + 1] = pixels[idx + 2] = 255;
                }
                pixels[idx + 3] = 255;
            }
        }
    }
    
    /**
     * Dispose resources
     */
    dispose() {
        // Nothing to dispose in simplified version
    }
}

/**
 * ViewerControls class - manages camera modes, channels, and controllers
 * Supports post-processing effects like Bloom.
 */
export class ViewerControls {
    constructor(options = {}) {
        this.scene = options.scene;
        this.camera = options.camera;
        this.renderer = options.renderer;
        this.controls = options.controls;
        this.transformControls = options.transform; // TransformControls instance
        this.groundPlane = options.groundPlane; // Ground plane mesh
        this.model = options.model;
        this.t = options.t || ((key) => key);
        
        this.bloomPass = options.bloomPass || null;
        
        this.cameraMode = CameraMode.ORBIT;
        this.materialChannel = MaterialChannel.PBR;
        this.rigType = RigType.CHAR;
        this.gizmosVisible = true;
        this.viewMode = 'tpose'; // 'tpose', 'rig', 'animation'
        
        // Original materials backup for channel switching
        this.originalMaterials = new Map();
        
        // Physics settings for RigidBodies mode
        this.physicsAttractionForce = 0.5;
        this.physicsResetForce = 0.2;
        
        // ABCDE Controllers
        this.controllerSpheres = [];
        this.controllerLabels = [];
        
        // Fly camera state
        this.flyState = {
            moveForward: false,
            moveBackward: false,
            moveLeft: false,
            moveRight: false,
            moveUp: false,
            moveDown: false,
            velocity: { x: 0, y: 0, z: 0 },
            euler: { x: 0, y: 0 }
        };
        
        // Skip focusOnModel once after camera restore
        this.skipNextFocus = false;
        
        // AO Baking
        this.aoBaker = null;
        this.bakedAOTexture = null;
        
        this.onCameraModeChange = options.onCameraModeChange || (() => {});
        this.onChannelChange = options.onChannelChange || (() => {});
        this.onRigTypeChange = options.onRigTypeChange || (() => {});
        this.onViewModeChange = options.onViewModeChange || (() => {});
        this.onSaveDefaultSettings = options.onSaveDefaultSettings || null;

        this.currentRotationPreset = 'none';
        this.modelFlipped = false;

        // Per-channel uniforms for all materials
        this.channelUniforms = {
            debugMode: { value: 0 }, // 0 = PBR, 1 = AO, 2 = Normal, 3 = Albedo, 4 = Metalness, 5 = Roughness, 6 = Emissive
            albedo: { 
                brightness: { value: 1.0 }, contrast: { value: 1.0 }, saturation: { value: 1.0 },
                mode: { value: 0.0 }, // 0 = Default, 1 = Emissive Mask
                maskColor: { value: new THREE.Color(1, 1, 1) },
                softness: { value: 0.5 },
                emissiveMult: { value: 2.0 },
                blendColor: { value: new THREE.Color(1, 1, 1) },
                invert: { value: 0.0 }
            },
            ao: { 
                brightness: { value: 1.0 }, contrast: { value: 1.0 }, saturation: { value: 1.0 },
                mode: { value: 0.0 }, maskColor: { value: new THREE.Color(1, 1, 1) },
                softness: { value: 0.5 }, emissiveMult: { value: 2.0 },
                blendColor: { value: new THREE.Color(1, 1, 1) },
                invert: { value: 0.0 }
            },
            normal: { 
                brightness: { value: 1.0 }, contrast: { value: 1.0 }, saturation: { value: 1.0 },
                mode: { value: 0.0 }, maskColor: { value: new THREE.Color(1, 1, 1) },
                softness: { value: 0.5 }, emissiveMult: { value: 2.0 },
                blendColor: { value: new THREE.Color(1, 1, 1) },
                invert: { value: 0.0 }
            },
            roughness: { 
                brightness: { value: 1.0 }, contrast: { value: 1.0 }, saturation: { value: 1.0 },
                mode: { value: 0.0 }, maskColor: { value: new THREE.Color(1, 1, 1) },
                softness: { value: 0.5 }, emissiveMult: { value: 2.0 },
                blendColor: { value: new THREE.Color(1, 1, 1) },
                invert: { value: 0.0 }
            },
            metalness: { 
                brightness: { value: 1.0 }, contrast: { value: 1.0 }, saturation: { value: 1.0 },
                mode: { value: 0.0 }, maskColor: { value: new THREE.Color(1, 1, 1) },
                softness: { value: 0.5 }, emissiveMult: { value: 2.0 },
                blendColor: { value: new THREE.Color(1, 1, 1) },
                invert: { value: 0.0 }
            },
            emissive: { 
                brightness: { value: 1.0 }, contrast: { value: 1.0 }, saturation: { value: 1.0 },
                mode: { value: 0.0 }, maskColor: { value: new THREE.Color(1, 1, 1) },
                softness: { value: 0.5 }, emissiveMult: { value: 2.0 },
                blendColor: { value: new THREE.Color(1, 1, 1) },
                invert: { value: 0.0 }
            },
        };
    }

    /**
     * Set adjustments for a specific channel
     */
    setChannelAdjustments(channel, params) {
        if (!this.channelUniforms[channel]) return;
        
        const u = this.channelUniforms[channel];
        const THREE = window.THREE;

        if (params.brightness !== undefined) u.brightness.value = params.brightness;
        if (params.contrast !== undefined) u.contrast.value = params.contrast;
        if (params.saturation !== undefined) u.saturation.value = params.saturation;
        
        if (params.mode !== undefined) u.mode.value = parseFloat(params.mode);
        if (params.maskColor !== undefined && THREE) u.maskColor.value.set(params.maskColor);
        if (params.softness !== undefined) u.softness.value = params.softness;
        if (params.emissiveMult !== undefined) u.emissiveMult.value = params.emissiveMult;
        if (params.blendColor !== undefined && THREE) u.blendColor.value.set(params.blendColor);
        if (params.invert !== undefined) u.invert.value = params.invert ? 1.0 : 0.0;
    }

    /**
     * Set ground plane parameters
     */
    setGroundParams(params) {
        if (!this.groundPlane) return;
        const THREE = window.THREE;
        if (!THREE) return;

        if (params.color !== undefined) {
            this.groundPlane.material.color.set(params.color);
        }
        if (params.size !== undefined) {
            // Plane is 2x2 by default (1m radius). Scale it.
            const s = params.size;
            this.groundPlane.scale.set(s, s, 1);
        }
    }

    /**
     * Set Bloom effect parameters
     */
    setBloomSettings(params) {
        if (!this.bloomPass) return;
        if (params.strength !== undefined) this.bloomPass.strength = params.strength;
        if (params.threshold !== undefined) this.bloomPass.threshold = params.threshold;
        if (params.radius !== undefined) this.bloomPass.radius = params.radius;
    }

    /**
     * Set model flip (invert Y scale)
     */
    setModelFlip(flip) {
        if (!this.model) return;
        this.modelFlipped = flip;
        this.model.scale.y = flip ? -1 : 1;
        console.log('[ViewerControls] Model flip:', flip);
    }

    /**
     * Set model rotation based on preset
     */
    setModelRotation(preset) {
        if (!this.model) return;

        const THREE = window.THREE;
        if (!THREE) return;

        this.currentRotationPreset = preset;

        // Reset rotation first
        this.model.rotation.set(0, 0, 0);

        const degToRad = Math.PI / 180;

        switch (preset) {
            case 'x90_neg': this.model.rotation.x = -90 * degToRad; break;
            case 'x90_pos': this.model.rotation.x = 90 * degToRad; break;
            case 'x180': this.model.rotation.x = 180 * degToRad; break;
            case 'y90': this.model.rotation.y = 90 * degToRad; break;
            case 'y180': this.model.rotation.y = 180 * degToRad; break;
            case 'y270': this.model.rotation.y = 270 * degToRad; break;
            case 'z90': this.model.rotation.z = 90 * degToRad; break;
            case 'z90_neg': this.model.rotation.z = -90 * degToRad; break;
            case 'z180': this.model.rotation.z = 180 * degToRad; break;
        }
        
        this.model.updateMatrixWorld(true);
        
        // Re-align to ground and center
        this.alignModelToGround();
    }

    /**
     * Align model to ground (Y=0) and center (X=0, Z=0)
     */
    alignModelToGround() {
        if (!this.model) return;
        
        const THREE = window.THREE;
        if (!THREE) return;

        const box = new THREE.Box3().setFromObject(this.model);
        const center = box.getCenter(new THREE.Vector3());
        
        this.model.position.x -= center.x;
        this.model.position.z -= center.z;
        this.model.position.y -= box.min.y;
        
        this.model.updateMatrixWorld(true);
    }

    /**
     * Toggle visibility of gizmos (spheres, transform controls)
     */
    setGizmosVisibility(visible) {
        this.gizmosVisible = visible;
        
        // Toggle controller spheres and labels
        this.controllerSpheres.forEach(sphere => {
            sphere.visible = visible;
        });
        
        this.controllerLabels.forEach(label => {
            label.visible = visible;
        });

        // Toggle transform controls
        if (this.transformControls) {
            this.transformControls.enabled = visible;
            this.transformControls.visible = visible;
        }
    }

    /**
     * Hide ABCDE rig spheres
     */
    hideRigSpheres() {
        this.controllerSpheres.forEach(sphere => {
            sphere.visible = false;
        });
        this.controllerLabels.forEach(label => {
            label.visible = false;
        });
        console.log('[ViewerControls] Rig spheres hidden');
    }

    /**
     * Show ABCDE rig spheres
     */
    showRigSpheres() {
        if (!this.gizmosVisible) return; // Respect global gizmo visibility
        this.controllerSpheres.forEach(sphere => {
            sphere.visible = true;
        });
        this.controllerLabels.forEach(label => {
            label.visible = true;
        });
        console.log('[ViewerControls] Rig spheres shown');
    }

    /**
     * Set view mode: 'tpose', 'rig', 'animation'
     * Controls which elements are visible and active
     */
    setViewMode(mode) {
        this.viewMode = mode;
        
        switch (mode) {
            case 'tpose':
                // T-Pose mode: show T-pose reference, hide rig spheres, enable gizmos
                this.hideRigSpheres();
                this.setGizmosVisibility(true);
                break;
            case 'rig':
                // RIG mode: show rig spheres, interact with skeleton, enable gizmos
                this.showRigSpheres();
                this.setGizmosVisibility(true);
                break;
            case 'animation':
                // Animation mode: hide rig spheres, enable animation playback
                this.hideRigSpheres();
                break;
            default:
                console.warn('[ViewerControls] Unknown view mode:', mode);
                return;
        }
        
        console.log('[ViewerControls] View mode set to:', mode);
        
        // Trigger callback if defined
        if (this.onViewModeChange) {
            this.onViewModeChange(mode);
        }
    }

    /**
     * Get current view mode
     */
    getViewMode() {
        return this.viewMode || 'tpose';
    }

    /**
     * Set camera mode
     */
    setCameraMode(mode) {
        if (!Object.values(CameraMode).includes(mode)) return;
        
        const prevMode = this.cameraMode;
        this.cameraMode = mode;

        // Disable all control modes first
        if (this.controls) {
            this.controls.enabled = false;
            this.controls.autoRotate = false;
        }

        switch (mode) {
            case CameraMode.STATIC:
                this.setupStaticCamera();
                break;
            case CameraMode.ORBIT:
                this.setupOrbitCamera();
                break;
            case CameraMode.FLY:
                this.setupFlyCamera();
                break;
        }

        this.onCameraModeChange(mode, prevMode);
    }

    /**
     * Setup static orthographic camera (for Char mode)
     */
    setupStaticCamera() {
        // Cleanup fly camera handlers if switching from fly mode
        this.cleanupFlyCamera();
        
        if (this.controls) {
            this.controls.enabled = false;
            this.controls.autoRotate = false;
        }

        // Position camera for front view, centered on model
        if (this.camera && this.model) {
            const box = new THREE.Box3().setFromObject(this.model);
            const center = box.getCenter(new THREE.Vector3());
            const size = box.getSize(new THREE.Vector3());
            const maxDim = Math.max(size.x, size.y, size.z);
            
            // Fixed front view for static mode
            this.camera.position.set(0, center.y, maxDim * 3);
            this.camera.lookAt(0, center.y, 0);
            
            if (this.controls) {
                this.controls.target.set(0, center.y, 0);
                this.controls.update();
            }
        }
    }

    /**
     * Setup orbit camera (default)
     */
    setupOrbitCamera() {
        // Cleanup fly camera handlers if switching from fly mode
        this.cleanupFlyCamera();
        
        if (this.controls) {
            this.controls.enabled = true;
            this.controls.enableDamping = true;
            this.controls.autoRotate = false;
        }

        // Exit pointer lock if active
        if (document.pointerLockElement) {
            document.exitPointerLock();
        }
        
        // Focus on model ONLY if not skipping (camera was restored from saved state)
        if (this.skipNextFocus) {
            console.log('[ViewerControls] Skipping focusOnModel - camera was restored');
            this.skipNextFocus = false;
        } else {
            this.focusOnModel(true);
        }
    }

    /**
     * Setup fly camera (WASD + mouse look)
     */
    setupFlyCamera() {
        if (this.controls) {
            this.controls.enabled = false;
            this.controls.autoRotate = false;
        }

        // Initialize euler from current camera rotation
        if (this.camera) {
            this.flyState.euler.x = this.camera.rotation.x;
            this.flyState.euler.y = this.camera.rotation.y;
        }

        // Start mouse look on right-click drag (more intuitive than pointer lock)
        if (this.renderer && this.renderer.domElement && !this._flyMouseHandler) {
            const domElement = this.renderer.domElement;
            let isRightDragging = false;
            
            const onMouseDown = (e) => {
                if (e.button === 2) { // Right mouse button
                    isRightDragging = true;
                    e.preventDefault();
                }
            };
            
            const onMouseUp = (e) => {
                if (e.button === 2) {
                    isRightDragging = false;
                }
            };
            
            const onMouseMove = (e) => {
                if (isRightDragging && this.cameraMode === CameraMode.FLY) {
                    this.handleFlyMouseMove(e.movementX, e.movementY);
                }
            };
            
            const onContextMenu = (e) => {
                if (this.cameraMode === CameraMode.FLY) {
                    e.preventDefault(); // Prevent context menu in fly mode
                }
            };
            
            domElement.addEventListener('mousedown', onMouseDown);
            domElement.addEventListener('mouseup', onMouseUp);
            domElement.addEventListener('mousemove', onMouseMove);
            domElement.addEventListener('contextmenu', onContextMenu);
            
            this._flyMouseHandler = { onMouseDown, onMouseUp, onMouseMove, onContextMenu };
        }
        
        console.log('[ViewerControls] Fly camera mode active. WASD to move, E/C for up/down, F to focus, right-drag to look.');
    }
    
    /**
     * Cleanup fly camera mouse handlers
     */
    cleanupFlyCamera() {
        if (this._flyMouseHandler && this.renderer) {
            const domElement = this.renderer.domElement;
            domElement.removeEventListener('mousedown', this._flyMouseHandler.onMouseDown);
            domElement.removeEventListener('mouseup', this._flyMouseHandler.onMouseUp);
            domElement.removeEventListener('mousemove', this._flyMouseHandler.onMouseMove);
            domElement.removeEventListener('contextmenu', this._flyMouseHandler.onContextMenu);
            this._flyMouseHandler = null;
        }
    }

    /**
     * Handle fly camera input
     */
    updateFlyCamera(delta) {
        if (this.cameraMode !== CameraMode.FLY || !this.camera) return;

        const speed = 5.0 * delta;
        const fs = this.flyState;

        // Update velocity based on input
        const direction = new THREE.Vector3();
        
        if (fs.moveForward) direction.z -= 1;
        if (fs.moveBackward) direction.z += 1;
        if (fs.moveLeft) direction.x -= 1;
        if (fs.moveRight) direction.x += 1;
        if (fs.moveUp) direction.y += 1;
        if (fs.moveDown) direction.y -= 1;

        direction.normalize();
        direction.applyQuaternion(this.camera.quaternion);
        
        this.camera.position.addScaledVector(direction, speed);
    }

    /**
     * Handle fly camera mouse movement
     */
    handleFlyMouseMove(movementX, movementY) {
        if (this.cameraMode !== CameraMode.FLY || !this.camera) return;

        const sensitivity = 0.002;
        this.flyState.euler.y -= movementX * sensitivity;
        this.flyState.euler.x -= movementY * sensitivity;
        
        // Clamp vertical rotation
        this.flyState.euler.x = Math.max(-Math.PI / 2, Math.min(Math.PI / 2, this.flyState.euler.x));
        
        this.camera.rotation.order = 'YXZ';
        this.camera.rotation.x = this.flyState.euler.x;
        this.camera.rotation.y = this.flyState.euler.y;
    }

    /**
     * Set material channel
     */
    setMaterialChannel(channel) {
        if (!Object.values(MaterialChannel).includes(channel)) {
            console.warn('[ViewerControls] Invalid channel:', channel);
            return;
        }
        
        const prevChannel = this.materialChannel;
        this.materialChannel = channel;
        
        console.log('[ViewerControls] Switching material channel:', {
            from: prevChannel,
            to: channel,
            channelName: Object.keys(MaterialChannel).find(k => MaterialChannel[k] === channel),
            hasModel: !!this.model,
            currentDebugMode: this.channelUniforms.debugMode.value
        });

        if (this.model) {
            this.applyMaterialChannel(channel);
        } else {
            console.warn('[ViewerControls] No model loaded, channel will be applied after load');
        }

        this.onChannelChange(channel, prevChannel);
    }

    /**
     * Create a shader material that extracts a specific channel from ORM texture
     * or displays full color with post-processing adjustments for that channel.
     * @param {THREE.Texture} texture - The texture to display
     * @param {THREE.Vector3|null} channelMask - If set, extract single channel (e.g. vec3(0,1,0) for green)
     * @param {number} fallbackValue - Value to use if no texture
     * @param {string} channelKey - Key for adjustment uniforms ('albedo', 'normal', etc.)
     * @param {boolean} isNormalMap - If true, use linear color space (no sRGB decoding)
     */
    createAdjustableMaterial(texture, channelMask = null, fallbackValue = 0.5, channelKey = 'albedo', isNormalMap = false) {
        const uniforms = this.channelUniforms[channelKey] || this.channelUniforms.albedo;
        
        if (!texture) {
            const mat = new THREE.MeshBasicMaterial();
            const v = Math.max(0, Math.min(1, fallbackValue * uniforms.brightness.value));
            mat.color.setRGB(v, v, v);
            return mat;
        }
        
        // For normal maps - ensure linear color space (no sRGB decoding)
        // This prevents the texture from appearing washed out/too bright
        if (isNormalMap) {
            texture.colorSpace = THREE.LinearSRGBColorSpace;
        }

        const vertexShader = `
            varying vec2 vUv;
            void main() {
                vUv = uv;
                gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
            }
        `;

        const fragmentShader = `
            uniform sampler2D tMap;
            uniform vec3 channelMask;
            uniform bool isSingleChannel;
            uniform bool applyGamma;
            uniform float brightness;
            uniform float contrast;
            uniform float saturation;
            varying vec2 vUv;

            void main() {
                // SAMPLE FROM LOD 1 (effectively 2x smaller in each dimension)
                // for performance during adjustments
                vec4 texel = texture2D(tMap, vUv, 1.0);
                vec3 color;
                
                if (isSingleChannel) {
                    float val = dot(texel.rgb, channelMask);
                    color = vec3(val);
                } else {
                    color = texel.rgb;
                }
                
                // Apply gamma 2.2 for normal maps (makes them darker/more contrasty)
                if (applyGamma) {
                    color = pow(color, vec3(2.2));
                }
                
                // Post-processing math
                vec3 res = color * brightness;
                float grey = dot(res, vec3(0.299, 0.587, 0.114));
                res = mix(vec3(grey), res, saturation);
                res = (res - 0.5) * contrast + 0.5;
                
                gl_FragColor = vec4(clamp(res, 0.0, 1.0), 1.0);
            }
        `;

        return new THREE.ShaderMaterial({
            uniforms: {
                tMap: { value: texture },
                channelMask: { value: channelMask || new THREE.Vector3(1, 1, 1) },
                isSingleChannel: { value: channelMask !== null },
                applyGamma: { value: isNormalMap },
                brightness: uniforms.brightness,
                contrast: uniforms.contrast,
                saturation: uniforms.saturation
            },
            vertexShader,
            fragmentShader
        });
    }

    /**
     * Inject per-channel post-processing into MeshStandardMaterial
     * Note: PBR mode uses full-resolution textures (LOD 0).
     */
    injectPostProcessing(material) {
        if (material._postProcInjected) {
            console.log('[ViewerControls] Material already has post-processing injected, skipping:', material.name || material.type);
            return;
        }
        
        console.log('[ViewerControls] Injecting post-processing into material:', {
            type: material.type,
            name: material.name || 'unnamed',
            hasMaps: {
                map: !!material.map,
                aoMap: !!material.aoMap,
                normalMap: !!material.normalMap,
                roughnessMap: !!material.roughnessMap,
                metalnessMap: !!material.metalnessMap,
                emissiveMap: !!material.emissiveMap
            }
        });

        material.onBeforeCompile = (shader) => {
            console.log('[ViewerControls] onBeforeCompile called for material:', material.name || material.type);
            
            // Add all channel uniforms to the shader
            const channels = ['albedo', 'ao', 'normal', 'roughness', 'metalness', 'emissive'];
            channels.forEach(chan => {
                const u = this.channelUniforms[chan];
                shader.uniforms[`u_${chan}_b`] = u.brightness;
                shader.uniforms[`u_${chan}_c`] = u.contrast;
                shader.uniforms[`u_${chan}_s`] = u.saturation;
                
                shader.uniforms[`u_${chan}_mode`] = u.mode;
                shader.uniforms[`u_${chan}_mcol`] = u.maskColor;
                shader.uniforms[`u_${chan}_soft`] = u.softness;
                shader.uniforms[`u_${chan}_mult`] = u.emissiveMult;
                shader.uniforms[`u_${chan}_bcol`] = u.blendColor;
                shader.uniforms[`u_${chan}_inv`] = u.invert;
            });
            
            shader.uniforms.u_debug_mode = this.channelUniforms.debugMode;
            
            // 1. Declarations (Global scope)
            const declarations = `
                #include <common>
                
                uniform float u_debug_mode;
                
                // Shared variable for extra emission from any channel
                vec3 extraEmissive;

                uniform float u_albedo_b; uniform float u_albedo_c; uniform float u_albedo_s;
                uniform float u_albedo_mode; uniform vec3 u_albedo_mcol; uniform float u_albedo_soft; uniform float u_albedo_mult; uniform vec3 u_albedo_bcol; uniform float u_albedo_inv;

                uniform float u_ao_b;     uniform float u_ao_c;     uniform float u_ao_s;
                uniform float u_ao_mode; uniform vec3 u_ao_mcol; uniform float u_ao_soft; uniform float u_ao_mult; uniform vec3 u_ao_bcol; uniform float u_ao_inv;

                uniform float u_normal_b; uniform float u_normal_c; uniform float u_normal_s;
                uniform float u_normal_mode; uniform vec3 u_normal_mcol; uniform float u_normal_soft; uniform float u_normal_mult; uniform vec3 u_normal_bcol; uniform float u_normal_inv;

                uniform float u_roughness_b; uniform float u_roughness_c; uniform float u_roughness_s;
                uniform float u_roughness_mode; uniform vec3 u_roughness_mcol; uniform float u_roughness_soft; uniform float u_roughness_mult; uniform vec3 u_roughness_bcol; uniform float u_roughness_inv;

                uniform float u_metalness_b; uniform float u_metalness_c; uniform float u_metalness_s;
                uniform float u_metalness_mode; uniform vec3 u_metalness_mcol; uniform float u_metalness_soft; uniform float u_metalness_mult; uniform vec3 u_metalness_bcol; uniform float u_metalness_inv;

                uniform float u_emissive_b;  uniform float u_emissive_c;  uniform float u_emissive_s;
                uniform float u_emissive_mode; uniform vec3 u_emissive_mcol; uniform float u_emissive_soft; uniform float u_emissive_mult; uniform vec3 u_emissive_bcol; uniform float u_emissive_inv;

                #ifndef APPLY_ADJ_FUNC
                #define APPLY_ADJ_FUNC
                // Helper to get correct LOD based on debug mode
                float getLOD() {
                    return u_debug_mode > 0.5 ? 1.0 : 0.0;
                }

                vec3 applyAdj(vec3 color, float b, float c, float s) {
                    vec3 res = color * b;
                    float grey = dot(res, vec3(0.299, 0.587, 0.114));
                    res = mix(vec3(grey), res, s);
                    res = (res - 0.5) * c + 0.5;
                    return clamp(res, 0.0, 1.0);
                }
                
                vec3 getEmissiveMask(vec3 color, float mode, vec3 maskCol, float soft, float mult, vec3 blendCol, float inv) {
                    if (mode < 0.5) return vec3(0.0);
                    float d = distance(color, maskCol);
                    // Avoid division by zero in smoothstep by adding epsilon
                    float mask = smoothstep(soft + 0.00001, 0.0, d);
                    if (inv > 0.5) mask = 1.0 - mask;
                    // Return pure HDR emissive color without mixing with original color
                    return blendCol * mult * mask;
                }
                #endif
            `;

            shader.fragmentShader = shader.fragmentShader.replace('#include <common>', declarations);

            // 2. Initialization at start of main()
            shader.fragmentShader = shader.fragmentShader.replace(
                'void main() {',
                'void main() {\n    extraEmissive = vec3(0.0);'
            );

            // 3. Albedo Adjustment (Before lighting)
            shader.fragmentShader = shader.fragmentShader.replace(
                '#include <lights_physical_fragment>',
                `
                if (u_debug_mode < 0.5) {
                    diffuseColor.rgb = applyAdj(diffuseColor.rgb, u_albedo_b, u_albedo_c, u_albedo_s);
                    extraEmissive += getEmissiveMask(diffuseColor.rgb, u_albedo_mode, u_albedo_mcol, u_albedo_soft, u_albedo_mult, u_albedo_bcol, u_albedo_inv);
                }
                #include <lights_physical_fragment>
                `
            );

            // 4. Roughness Adjustment
            shader.fragmentShader = shader.fragmentShader.replace(
                '#include <roughnessmap_fragment>',
                `
                #include <roughnessmap_fragment>
                #ifdef USE_ROUGHNESSMAP
                    if (u_debug_mode < 0.5) {
                        roughnessFactor = applyAdj(vec3(roughnessFactor), u_roughness_b, u_roughness_c, u_roughness_s).r;
                        extraEmissive += getEmissiveMask(vec3(roughnessFactor), u_roughness_mode, u_roughness_mcol, u_roughness_soft, u_roughness_mult, u_roughness_bcol, u_roughness_inv);
                    }
                #endif
                `
            );

            // 5. Metalness Adjustment
            shader.fragmentShader = shader.fragmentShader.replace(
                '#include <metalnessmap_fragment>',
                `
                #include <metalnessmap_fragment>
                #ifdef USE_METALNESSMAP
                    if (u_debug_mode < 0.5) {
                        metalnessFactor = applyAdj(vec3(metalnessFactor), u_metalness_b, u_metalness_c, u_metalness_s).r;
                        extraEmissive += getEmissiveMask(vec3(metalnessFactor), u_metalness_mode, u_metalness_mcol, u_metalness_soft, u_metalness_mult, u_metalness_bcol, u_metalness_inv);
                    }
                #endif
                `
            );

            // 6. AO Adjustment
            shader.fragmentShader = shader.fragmentShader.replace(
                '#include <aomap_fragment>',
                `
                #include <aomap_fragment>
                #ifdef USE_AOMAP
                    if (u_debug_mode < 0.5) {
                        ambientOcclusion = applyAdj(vec3(ambientOcclusion), u_ao_b, u_ao_c, u_ao_s).r;
                        extraEmissive += getEmissiveMask(vec3(ambientOcclusion), u_ao_mode, u_ao_mcol, u_ao_soft, u_ao_mult, u_ao_bcol, u_ao_inv);
                    }
                #endif
                `
            );

            // 7. Emissive Adjustment & Application
            shader.fragmentShader = shader.fragmentShader.replace(
                '#include <emissivemap_fragment>',
                `
                #include <emissivemap_fragment>
                #ifdef USE_EMISSIVEMAP
                    if (u_debug_mode < 0.5) {
                        totalEmissiveRadiance = applyAdj(totalEmissiveRadiance, u_emissive_b, u_emissive_c, u_emissive_s);
                        extraEmissive += getEmissiveMask(totalEmissiveRadiance, u_emissive_mode, u_emissive_mcol, u_emissive_soft, u_emissive_mult, u_emissive_bcol, u_emissive_inv);
                    }
                #endif
                if (u_debug_mode < 0.5) {
                    totalEmissiveRadiance += extraEmissive;
                }
                `
            );

            // 8. Normal Adjustment - Robust regex replacement
            const normalRegex = /vec3\s+mapN\s*=\s*texture2D\s*\(\s*normalMap\s*,\s*vNormalMapUv\s*\)\.xyz\s*\*\s*2\.0\s*-\s*1\.0\s*;/g;
            shader.fragmentShader = shader.fragmentShader.replace(
                normalRegex,
                `
                vec3 myNormalTex = texture2D( normalMap, vNormalMapUv, getLOD() ).xyz;
                if (u_debug_mode < 0.5) {
                    myNormalTex = applyAdj(myNormalTex, u_normal_b, u_normal_c, u_normal_s);
                    extraEmissive += getEmissiveMask(myNormalTex, u_normal_mode, u_normal_mcol, u_normal_soft, u_normal_mult, u_normal_bcol, u_normal_inv);
                }
                vec3 mapN = myNormalTex * 2.0 - 1.0;
                `
            );
            
            // 8.5 Robust replacements for other texture2D calls to include LOD support
            const textureMaps = [
                { id: 'map', uv: 'vMapUv' },
                { id: 'roughnessMap', uv: 'vRoughnessMapUv' },
                { id: 'metalnessMap', uv: 'vMetalnessMapUv' },
                { id: 'emissiveMap', uv: 'vEmissiveMapUv' },
                { id: 'aoMap', uv: 'vAoMapUv' }
            ];

            textureMaps.forEach(m => {
                const regex = new RegExp(`texture2D\\s*\\(\\s*${m.id}\\s*,\\s*${m.uv}\\s*\\)`, 'g');
                shader.fragmentShader = shader.fragmentShader.replace(regex, `texture2D(${m.id}, ${m.uv}, getLOD())`);
            });

            // 9. Output Debug Overrides
            const originalShader = shader.fragmentShader;
            const hasOutputFragment = originalShader.includes('#include <output_fragment>');

            if (hasOutputFragment) {
                shader.fragmentShader = shader.fragmentShader.replace(
                    '#include <output_fragment>',
                    `
                    if (u_debug_mode > 0.5) {
                    vec3 debugCol = vec3(1.0, 0.0, 1.0); // Magenta fallback
                    
                    if (u_debug_mode < 1.5) {
                        // AO - read directly from aoMap
                        #ifdef USE_AOMAP
                            vec4 aoTexel = texture2D(aoMap, vAoMapUv, getLOD());
                            debugCol = vec3(aoTexel.r);
                        #else
                            debugCol = vec3(1.0); // No AO = white
                        #endif
                    }
                    else if (u_debug_mode < 2.5) {
                        // Normal - read directly from normalMap
                        #ifdef USE_NORMALMAP
                            vec3 normalTexel = texture2D(normalMap, vNormalMapUv, getLOD()).xyz;
                            debugCol = normalTexel; // Show as-is (0-1 range)
                        #else
                            debugCol = vec3(0.5, 0.5, 1.0); // Flat normal
                        #endif
                    }
                    else if (u_debug_mode < 3.5) {
                        // Albedo - read directly from map (base color)
                        #ifdef USE_MAP
                            vec4 albedoTexel = texture2D(map, vMapUv, getLOD());
                            debugCol = albedoTexel.rgb;
                        #else
                            debugCol = diffuseColor.rgb; // Use material color
                        #endif
                    }
                    else if (u_debug_mode < 4.5) {
                        // Metalness - read from metalnessMap (blue channel for GLB)
                        #ifdef USE_METALNESSMAP
                            vec4 metalnessTexel = texture2D(metalnessMap, vMetalnessMapUv, getLOD());
                            debugCol = vec3(metalnessTexel.b); // Blue channel = metalness
                        #else
                            debugCol = vec3(metalness); // Use material value
                        #endif
                    }
                    else if (u_debug_mode < 5.5) {
                        // Roughness - read from roughnessMap (green channel for GLB)
                        #ifdef USE_ROUGHNESSMAP
                            vec4 roughnessTexel = texture2D(roughnessMap, vRoughnessMapUv, getLOD());
                            debugCol = vec3(roughnessTexel.g); // Green channel = roughness
                        #else
                            debugCol = vec3(roughness); // Use material value
                        #endif
                    }
                    else if (u_debug_mode < 6.5) {
                        // Emissive - read directly from emissiveMap
                        #ifdef USE_EMISSIVEMAP
                            vec4 emissiveTexel = texture2D(emissiveMap, vEmissiveMapUv, getLOD());
                            debugCol = emissiveTexel.rgb * emissive;
                        #else
                            debugCol = emissive; // Use material emissive color
                        #endif
                    }
                    
                    gl_FragColor = vec4(debugCol, 1.0);
                } else {
                    #include <output_fragment>
                }
                `
                );
            } else {
                console.warn('[ViewerControls] #include <output_fragment> not found, adding debug block at end of shader');
                const lastBrace = shader.fragmentShader.lastIndexOf('}');
                if (lastBrace > 0) {
                    const beforeLastBrace = shader.fragmentShader.substring(0, lastBrace);
                    const afterLastBrace = shader.fragmentShader.substring(lastBrace);
                    shader.fragmentShader = beforeLastBrace + `
    // DEBUG MODE FALLBACK
    if (u_debug_mode > 0.5) {
        vec3 debugCol = vec3(1.0, 0.0, 1.0);
        gl_FragColor = vec4(debugCol, 1.0);
        return;
    }
` + afterLastBrace;
                }
            }
            
            // Store shader reference for later uniform updates
            material.userData.shader = shader;
            material._postProcInjected = true;
        };
        
        material.needsUpdate = true;
    }

    /**
     * Update debugMode uniform in already compiled shaders
     */
    updateCompiledShadersDebugMode(debugVal) {
        if (!this.model) return;

        let updatedCount = 0;
        this.model.traverse((child) => {
            if (!child.isMesh || !child.material) return;

            const materials = Array.isArray(child.material) ? child.material : [child.material];
            materials.forEach((mat) => {
                if (mat.userData && mat.userData.shader && mat.userData.shader.uniforms) {
                    if (mat.userData.shader.uniforms.u_debug_mode) {
                        mat.userData.shader.uniforms.u_debug_mode.value = debugVal;
                        updatedCount++;
                    }
                }
            });
        });

        console.log('[ViewerControls] Updated debugMode in', updatedCount, 'compiled shaders to value:', debugVal);
    }

    /**
     * Apply material channel to model
     * Simple approach: replace with MeshBasicMaterial showing the texture
     */
    applyMaterialChannel(channel) {
        if (!this.model) {
            console.warn('[ViewerControls] applyMaterialChannel: No model loaded');
            return;
        }

        // Set debug mode (0 for PBR with adjustments, >0 for channel views)
        let debugVal;
        if (channel === MaterialChannel.PBR) {
            debugVal = 0;
        } else {
            debugVal = 1;
            switch (channel) {
                case MaterialChannel.AO: debugVal = 1; break;
                case MaterialChannel.NORMAL: debugVal = 2; break;
                case MaterialChannel.ALBEDO: debugVal = 3; break;
                case MaterialChannel.METALNESS: debugVal = 4; break;
                case MaterialChannel.ROUGHNESS: debugVal = 5; break;
                case MaterialChannel.EMISSIVE: debugVal = 6; break;
            }
        }

        console.log('[ViewerControls] Setting debug mode in applyMaterialChannel:', {
            channel,
            channelName: this.getChannelLabel(channel),
            debugVal,
            previousValue: this.channelUniforms.debugMode.value
        });

        this.channelUniforms.debugMode.value = debugVal;

        // Update debugMode in already compiled shaders
        this.updateCompiledShadersDebugMode(debugVal);

        console.log('[ViewerControls] Applying material channel (simple mode):', {
            channel,
            channelName: this.getChannelLabel(channel),
            debugMode: this.channelUniforms.debugMode.value
        });

        this.model.traverse((child) => {
            if (!child.isMesh || !child.material) return;

            // Handle both single material and array of materials
            const materials = Array.isArray(child.material) ? child.material : [child.material];
            
            materials.forEach((mat, index) => {
                const matKey = `${child.uuid}_${index}`;
                
                // Backup original material (now with injected post-processing)
                if (!this.originalMaterials.has(matKey)) {
                    this.originalMaterials.set(matKey, mat);
                }

                const original = this.originalMaterials.get(matKey);

                if (channel === MaterialChannel.PBR) {
                    // Restore original PBR material (already has injected adjustments)
                    if (Array.isArray(child.material)) {
                        child.material[index] = original;
                    } else {
                        child.material = original;
                    }
                    return;
                }

                // Create channel visualization material WITH adjustments support
                let channelMat;

                switch (channel) {
                    case MaterialChannel.AO:
                        // Use original aoMap or baked AO texture
                        const aoTexture = original.aoMap || this.bakedAOTexture;
                        if (aoTexture) {
                            // Use adjustable material for AO with proper channel extraction
                            channelMat = this.createAdjustableMaterial(
                                aoTexture,
                                null, // No channel mask, display as-is
                                1.0,
                                'ao'
                            );
                        } else {
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.setHex(0xffffff); // White = no AO
                        }
                        break;

                    case MaterialChannel.NORMAL:
                        if (original.normalMap) {
                            channelMat = this.createAdjustableMaterial(
                                original.normalMap,
                                null,
                                0.5,
                                'normal',
                                true  // isNormalMap = true (use linear color space)
                            );
                        } else {
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.setHex(0x8080ff); // Flat normal color
                        }
                        break;

                    case MaterialChannel.ALBEDO:
                        if (original.map) {
                            channelMat = this.createAdjustableMaterial(
                                original.map,
                                null,
                                0.5,
                                'albedo'
                            );
                        } else if (original.color) {
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.copy(original.color);
                        } else {
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.setHex(0xcccccc);
                        }
                        break;

                    case MaterialChannel.METALNESS:
                        if (original.metalnessMap) {
                            // Extract blue channel for GLB metalness
                            channelMat = this.createAdjustableMaterial(
                                original.metalnessMap,
                                new THREE.Vector3(0, 0, 1), // Blue channel
                                original.metalness !== undefined ? original.metalness : 0,
                                'metalness'
                            );
                        } else {
                            const m = original.metalness !== undefined ? original.metalness : 0;
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.setRGB(m, m, m);
                        }
                        break;

                    case MaterialChannel.ROUGHNESS:
                        if (original.roughnessMap) {
                            // Extract green channel for GLB roughness
                            channelMat = this.createAdjustableMaterial(
                                original.roughnessMap,
                                new THREE.Vector3(0, 1, 0), // Green channel
                                original.roughness !== undefined ? original.roughness : 0.5,
                                'roughness'
                            );
                        } else {
                            const r = original.roughness !== undefined ? original.roughness : 0.5;
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.setRGB(r, r, r);
                        }
                        break;

                    case MaterialChannel.EMISSIVE:
                        if (original.emissiveMap) {
                            channelMat = this.createAdjustableMaterial(
                                original.emissiveMap,
                                null,
                                0.0,
                                'emissive'
                            );
                        } else if (original.emissive) {
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.copy(original.emissive);
                        } else {
                            channelMat = new THREE.MeshBasicMaterial();
                            channelMat.color.setHex(0x000000);
                        }
                        break;
                }

                // Replace material
                if (Array.isArray(child.material)) {
                    child.material[index] = channelMat;
                } else {
                    child.material = channelMat;
                }
            });
        });

        if (channel === MaterialChannel.PBR) {
            console.log('[ViewerControls] Material channel applied: PBR mode (original materials restored)');
        } else {
            console.log('[ViewerControls] Material channel applied: channel', channel, '(materials replaced with ShaderMaterial)');
        }
    }


    /**
     * Set rig type (changes viewer behavior)
     */
    setRigType(type) {
        const prevType = this.rigType;
        this.rigType = type;
        this.clearControllers();
        this.hidePhysicsUI();

        switch (type) {
            case RigType.CHAR:
            case 'char':
                this.rigType = RigType.CHAR;
                this.setCameraMode(CameraMode.STATIC);
                this.createCharControllers();
                break;
            case RigType.ANIMATIONS:
            case 'animations':
                this.rigType = RigType.ANIMATIONS;
                this.setCameraMode(CameraMode.ORBIT);
                this.createCharControllers();
                break;
            case RigType.RIGIDBODIES:
            case 'rigidbodies':
                this.rigType = RigType.RIGIDBODIES;
                this.setCameraMode(CameraMode.ORBIT);
                this.showPhysicsUI();
                break;
            case RigType.SOLID:
            case 'solid':
                this.rigType = RigType.SOLID;
                this.setCameraMode(CameraMode.ORBIT);
                this.createSolidController();
                break;
            default:
                // Unknown type, default to orbit
                this.setCameraMode(CameraMode.ORBIT);
        }

        // Notify callback
        if (this.onRigTypeChange && this.rigType !== prevType) {
            this.onRigTypeChange(this.rigType);
        }
    }

    /**
     * Show physics sliders UI for RigidBodies mode
     */
    showPhysicsUI() {
        // Check if UI already exists
        let physicsUI = document.getElementById('viewer-physics-ui');
        if (physicsUI) {
            physicsUI.classList.remove('hidden');
            return;
        }

        // Find viewer overlay element to insert UI
        const viewerOverlay = document.getElementById('viewer-overlay');
        if (!viewerOverlay) return;

        const html = `
            <div id="viewer-physics-ui" style="
                position: absolute;
                left: 0.75rem;
                top: 50%;
                transform: translateY(-50%);
                display: flex;
                flex-direction: column;
                gap: 1rem;
                background: rgba(0,0,0,0.7);
                padding: 1rem;
                border-radius: 12px;
                border: 1px solid rgba(255,255,255,0.15);
                backdrop-filter: blur(8px);
                z-index: 15;
                pointer-events: auto;
            ">
                <div>
                    <label style="font-size: 0.7rem; color: var(--text-muted); display: block; margin-bottom: 0.25rem;">
                        Attraction Force
                    </label>
                    <input type="range" id="physics-attraction" min="0" max="100" value="50" style="
                        width: 120px;
                        accent-color: var(--accent);
                    ">
                </div>
                <div>
                    <label style="font-size: 0.7rem; color: var(--text-muted); display: block; margin-bottom: 0.25rem;">
                        Reset Force
                    </label>
                    <input type="range" id="physics-reset" min="0" max="100" value="20" style="
                        width: 120px;
                        accent-color: var(--accent);
                    ">
                </div>
            </div>
        `;

        // Insert inside viewer overlay
        viewerOverlay.insertAdjacentHTML('beforeend', html);

        // Wire up slider events
        const attractionSlider = document.getElementById('physics-attraction');
        const resetSlider = document.getElementById('physics-reset');

        if (attractionSlider) {
            attractionSlider.addEventListener('input', (e) => {
                this.physicsAttractionForce = parseFloat(e.target.value) / 100;
            });
        }

        if (resetSlider) {
            resetSlider.addEventListener('input', (e) => {
                this.physicsResetForce = parseFloat(e.target.value) / 100;
            });
        }
    }

    /**
     * Hide physics sliders UI
     */
    hidePhysicsUI() {
        const physicsUI = document.getElementById('viewer-physics-ui');
        if (physicsUI) {
            physicsUI.classList.add('hidden');
        }
    }

    /**
     * Create single pivot controller for Solid mode
     */
    createSolidController() {
        if (!this.scene || !this.model) return;

        const THREE = window.THREE;
        if (!THREE) return;

        // Get model center
        const box = new THREE.Box3().setFromObject(this.model);
        const center = box.getCenter(new THREE.Vector3());
        const size = box.getSize(new THREE.Vector3());
        const sphereRadius = Math.max(size.x, size.y, size.z) * 0.05;

        // Create pivot sphere
        const geometry = new THREE.SphereGeometry(sphereRadius, 16, 16);
        const material = new THREE.MeshStandardMaterial({
            color: 0xffa500,
            transparent: true,
            opacity: 0.8,
            emissive: 0xffa500,
            emissiveIntensity: 0.1,
        });
        const sphere = new THREE.Mesh(geometry, material);
        sphere.position.copy(center);
        sphere.userData.controllerId = 'PIVOT';
        sphere.userData.controllerLabel = 'Pivot';
        sphere.userData.isDraggable = true;

        this.scene.add(sphere);
        this.controllerSpheres.push(sphere);

        // Add axes helper
        const axes = new THREE.AxesHelper(sphereRadius * 3);
        sphere.add(axes);

        // Setup drag interaction
        this.setupControllerDrag();
    }

    /**
     * Create ABCDE controllers for Char/Animations mode
     */
    createCharControllers() {
        if (!this.scene || !this.model) return;

        const THREE = window.THREE;
        if (!THREE) {
            console.warn('[ViewerControls] THREE not available globally');
            return;
        }

        // Controller definitions
        const controllers = [
            { id: 'A', color: 0xff0000, label: 'A (Head)', offset: [0, 1.7, 0] },
            { id: 'B', color: 0x00ff00, label: 'B (L.Hand)', offset: [-0.6, 1.0, 0] },
            { id: 'C', color: 0x0000ff, label: 'C (R.Hand)', offset: [0.6, 1.0, 0] },
            { id: 'D', color: 0xffff00, label: 'D (L.Foot)', offset: [-0.2, 0.1, 0] },
            { id: 'E', color: 0xff00ff, label: 'E (R.Foot)', offset: [0.2, 0.1, 0] },
        ];

        // Get model center and bounds
        const box = new THREE.Box3().setFromObject(this.model);
        const center = box.getCenter(new THREE.Vector3());
        const size = box.getSize(new THREE.Vector3());
        const scale = size.y || 1;
        const sphereRadius = 0.09 * scale;

        controllers.forEach((ctrl) => {
            // Create sphere
            const geometry = new THREE.SphereGeometry(sphereRadius, 16, 16);
            const material = new THREE.MeshStandardMaterial({
                color: ctrl.color,
                transparent: true,
                opacity: 0.8,
                emissive: ctrl.color,
                emissiveIntensity: 0.1,
            });
            const sphere = new THREE.Mesh(geometry, material);
            
            // Position based on offset scaled to model
            sphere.position.set(
                center.x + ctrl.offset[0] * (scale / 2),
                box.min.y + ctrl.offset[1] * scale,
                center.z + ctrl.offset[2] * (scale / 2)
            );
            
            sphere.userData.controllerId = ctrl.id;
            sphere.userData.controllerLabel = ctrl.label;
            sphere.userData.isDraggable = true;
            
            this.scene.add(sphere);
            this.controllerSpheres.push(sphere);

            // Create 3D arrows (axes helper)
            const axesSize = sphereRadius * 2.5;
            const axes = new THREE.AxesHelper(axesSize);
            sphere.add(axes);

            // Create label sprite
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            canvas.width = 64;
            canvas.height = 64;
            ctx.fillStyle = '#' + ctrl.color.toString(16).padStart(6, '0');
            ctx.beginPath();
            ctx.arc(32, 32, 28, 0, Math.PI * 2);
            ctx.fill();
            ctx.fillStyle = '#fff';
            ctx.font = 'bold 36px Arial';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(ctrl.id, 32, 34);

            const texture = new THREE.CanvasTexture(canvas);
            const spriteMat = new THREE.SpriteMaterial({ map: texture, transparent: true });
            const sprite = new THREE.Sprite(spriteMat);
            sprite.scale.set(sphereRadius * 2, sphereRadius * 2, 1);
            sprite.position.set(0, sphereRadius * 2, 0);
            sphere.add(sprite);
            this.controllerLabels.push(sprite);
        });

        // Setup drag interaction
        this.setupControllerDrag();
    }

    /**
     * Setup drag interaction for controllers
     */
    setupControllerDrag() {
        if (!this.renderer || !this.camera) return;
        
        const THREE = window.THREE;
        if (!THREE) return;

        const raycaster = new THREE.Raycaster();
        const mouse = new THREE.Vector2();
        const plane = new THREE.Plane();
        const intersection = new THREE.Vector3();
        
        let dragging = null;
        let dragOffset = new THREE.Vector3();

        const domElement = this.renderer.domElement;

        const getMousePos = (e) => {
            const rect = domElement.getBoundingClientRect();
            mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
            mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;
        };

        const onPointerDown = (e) => {
            if (this.rigType !== RigType.CHAR && this.rigType !== RigType.ANIMATIONS) return;
            if (this.cameraMode === CameraMode.FLY) return;

            getMousePos(e);
            raycaster.setFromCamera(mouse, this.camera);

            const intersects = raycaster.intersectObjects(this.controllerSpheres, false);
            if (intersects.length > 0) {
                dragging = intersects[0].object;
                
                // Disable orbit controls while dragging
                if (this.controls) this.controls.enabled = false;

                // Setup drag plane (perpendicular to camera, through the sphere)
                plane.setFromNormalAndCoplanarPoint(
                    this.camera.getWorldDirection(new THREE.Vector3()).negate(),
                    dragging.position
                );

                // Calculate offset from intersection to sphere center
                if (raycaster.ray.intersectPlane(plane, intersection)) {
                    dragOffset.copy(dragging.position).sub(intersection);
                }

                domElement.style.cursor = 'grabbing';
                e.preventDefault();
            }
        };

        const onPointerMove = (e) => {
            if (!dragging) return;

            getMousePos(e);
            raycaster.setFromCamera(mouse, this.camera);

            if (raycaster.ray.intersectPlane(plane, intersection)) {
                // Move sphere to new position
                const newPos = intersection.clone().add(dragOffset);
                
                // Raycast to model surface for magnetization
                if (this.model) {
                    const modelRay = new THREE.Raycaster(
                        newPos.clone().add(new THREE.Vector3(0, 0, 2)),
                        new THREE.Vector3(0, 0, -1)
                    );
                    const modelIntersects = modelRay.intersectObject(this.model, true);
                    
                    if (modelIntersects.length > 0) {
                        // Snap to surface
                        dragging.position.copy(modelIntersects[0].point);
                    } else {
                        // No surface hit, just move freely
                        dragging.position.copy(newPos);
                    }
                } else {
                    dragging.position.copy(newPos);
                }
            }
        };

        const onPointerUp = () => {
            if (dragging) {
                dragging = null;
                domElement.style.cursor = '';
                
                // Re-enable orbit controls
                if (this.controls && this.cameraMode === CameraMode.ORBIT) {
                    this.controls.enabled = true;
                }
            }
        };

        // Store handlers for cleanup
        this._dragHandlers = { onPointerDown, onPointerMove, onPointerUp };
        
        domElement.addEventListener('pointerdown', onPointerDown);
        domElement.addEventListener('pointermove', onPointerMove);
        domElement.addEventListener('pointerup', onPointerUp);
        domElement.addEventListener('pointerleave', onPointerUp);
    }

    /**
     * Cleanup drag handlers
     */
    cleanupDragHandlers() {
        if (this._dragHandlers && this.renderer) {
            const domElement = this.renderer.domElement;
            domElement.removeEventListener('pointerdown', this._dragHandlers.onPointerDown);
            domElement.removeEventListener('pointermove', this._dragHandlers.onPointerMove);
            domElement.removeEventListener('pointerup', this._dragHandlers.onPointerUp);
            domElement.removeEventListener('pointerleave', this._dragHandlers.onPointerUp);
            this._dragHandlers = null;
        }
    }

    /**
     * Clear all controllers
     */
    clearControllers() {
        // Cleanup drag handlers first
        this.cleanupDragHandlers();

        this.controllerSpheres.forEach(sphere => {
            if (sphere.parent) sphere.parent.remove(sphere);
            sphere.geometry?.dispose();
            sphere.material?.dispose();
        });
        this.controllerSpheres = [];

        this.controllerLabels.forEach(label => {
            if (label.parent) label.parent.remove(label);
            if (label.material?.map) label.material.map.dispose();
            label.material?.dispose();
        });
        this.controllerLabels = [];
    }

    /**
     * Update model reference
     */
    setModel(model) {
        this.model = model;
        this.originalMaterials.clear();
        
        // Bake AO texture for the model
        this.bakeAOForModel(model);
        
        // Enable real-time shadows and inject adjustments into all PBR materials
        model.traverse((child) => {
            if (child.isMesh) {
                child.castShadow = true;
                child.receiveShadow = true;
                
                // Inject post-processing for adjustments support
                const materials = Array.isArray(child.material) ? child.material : [child.material];
                materials.forEach((mat) => {
                    if (mat.isMeshStandardMaterial || mat.isMeshPhysicalMaterial) {
                        this.injectPostProcessing(mat);
                        
                        // Apply baked AO if material doesn't have its own aoMap
                        if (!mat.aoMap && this.bakedAOTexture) {
                            mat.aoMap = this.bakedAOTexture;
                            mat.aoMapIntensity = 1.0;
                            mat.needsUpdate = true;
                            console.log('[ViewerControls] Applied baked AO to material');
                        }
                    }
                });
            }
        });
        
        // Set debug mode to 0 (PBR with adjustments enabled)
        this.channelUniforms.debugMode.value = 0;

        // Re-apply rotation and flip
        if (this.currentRotationPreset !== 'none') {
            this.setModelRotation(this.currentRotationPreset);
        } else {
            this.alignModelToGround();
        }
        if (this.modelFlipped) {
            this.setModelFlip(true);
        }

        // ALWAYS apply material channel logic, even for PBR mode,
        // to ensure shader injection happens for all materials.
        this.applyMaterialChannel(this.materialChannel);
    }
    
    /**
     * Bake AO texture for the model
     */
    bakeAOForModel(model) {
        if (!this.renderer) {
            console.warn('[ViewerControls] Cannot bake AO - no renderer');
            return;
        }
        
        try {
            // Create AOBaker if not exists
            if (!this.aoBaker) {
                this.aoBaker = new AOBaker(this.renderer, 512);
            }
            
            // Dispose previous baked texture
            if (this.bakedAOTexture) {
                this.bakedAOTexture.dispose();
            }
            
            // Bake new AO texture
            this.bakedAOTexture = this.aoBaker.bake(model);
            console.log('[ViewerControls] AO texture baked successfully');
        } catch (err) {
            console.error('[ViewerControls] AO baking failed:', err);
            this.bakedAOTexture = null;
        }
    }
    
    /**
     * Set AO Baker settings
     * @param {Object} settings - { samples, radius, intensity }
     */
    setAOSettings(settings) {
        if (!this.aoBaker) {
            this.aoBaker = new AOBaker(this.renderer, 512);
        }
        
        if (settings.samples !== undefined) {
            this.aoBaker.numSamples = settings.samples;
        }
        if (settings.radius !== undefined) {
            this.aoBaker.aoRadius = settings.radius;
        }
        if (settings.intensity !== undefined) {
            this.aoBaker.aoIntensity = settings.intensity;
        }
        
        console.log('[ViewerControls] AO settings updated:', settings);
    }
    
    /**
     * Rebake AO with current settings and re-apply to materials
     * @returns {THREE.Texture|null} - The new baked AO texture
     */
    rebakeAO() {
        if (!this.model) {
            console.warn('[ViewerControls] Cannot rebake AO - no model');
            return null;
        }
        
        console.log('[ViewerControls] Rebaking AO...');
        
        // Bake new AO texture
        this.bakeAOForModel(this.model);
        
        if (!this.bakedAOTexture) {
            console.error('[ViewerControls] Rebake failed - no texture created');
            return null;
        }
        
        // Re-apply to all materials that don't have their own aoMap
        this.model.traverse((child) => {
            if (child.isMesh && child.material) {
                const mats = Array.isArray(child.material) ? child.material : [child.material];
                mats.forEach((mat, idx) => {
                    // Get original material to check if it had aoMap
                    const originalKey = `${child.uuid}_${idx}`;
                    const originalMat = this.originalMaterials.get(originalKey);
                    
                    // If original didn't have aoMap, update with new baked one
                    if (!originalMat?.aoMap && mat.aoMap !== this.bakedAOTexture) {
                        mat.aoMap = this.bakedAOTexture;
                        mat.aoMapIntensity = 1.0;
                        mat.needsUpdate = true;
                    }
                });
            }
        });
        
        // If currently viewing AO channel, re-apply to show updated texture
        if (this.materialChannel === MaterialChannel.AO) {
            this.applyMaterialChannel(MaterialChannel.AO);
        }
        
        console.log('[ViewerControls] AO rebaked and applied');
        return this.bakedAOTexture;
    }

    /**
     * Skip next focusOnModel call (used when restoring camera from saved state)
     */
    skipNextFocusOnModel() {
        this.skipNextFocus = true;
        console.log('[ViewerControls] Will skip next focusOnModel call');
    }

    /**
     * Focus camera on model (center view)
     * Camera positioned at 6 meters from center
     */
    focusOnModel(resetAngle = false) {
        if (!this.model || !this.camera) return;
        
        const box = new THREE.Box3().setFromObject(this.model);
        const center = box.getCenter(new THREE.Vector3());
        
        // Fixed distance of 6 meters from model center
        const distance = 6;
        
        if (resetAngle) {
            // Nice diagonal starting view at 6m distance
            // Position: slightly to the side and above, looking at center
            const angle = Math.PI / 6; // 30 degrees
            this.camera.position.set(
                Math.sin(angle) * distance,
                center.y + distance * 0.3,
                Math.cos(angle) * distance
            );
        } else {
            const direction = this.camera.getWorldDirection(new THREE.Vector3());
            // Keep current viewing angle, set to 6m distance
            this.camera.position.copy(center).sub(direction.multiplyScalar(distance));
        }
        
        // Update controls target
        if (this.controls) {
            this.controls.target.set(0, center.y, 0);
            this.controls.update();
        }
        
        // Reset fly euler to match current camera orientation
        if (this.cameraMode === CameraMode.FLY) {
            this.flyState.euler.x = this.camera.rotation.x;
            this.flyState.euler.y = this.camera.rotation.y;
        }
        
        console.log('[ViewerControls] Focused on model at 6m distance (center.y=' + center.y + ')');
    }

    /**
     * Handle keyboard input
     */
    handleKeyDown(key) {
        // Admin: save current viewer defaults (Z)
        if (key && key.toLowerCase && key.toLowerCase() === 'z') {
            if (typeof this.onSaveDefaultSettings === 'function') {
                try {
                    this.onSaveDefaultSettings();
                } catch (e) {
                    console.warn('[ViewerControls] onSaveDefaultSettings failed:', e);
                }
                return true;
            }
        }

        // Material channels (1-7)
        const channelKeys = {
            '1': MaterialChannel.PBR,
            '2': MaterialChannel.AO,
            '3': MaterialChannel.NORMAL,
            '4': MaterialChannel.ALBEDO,
            '5': MaterialChannel.METALNESS,
            '6': MaterialChannel.ROUGHNESS,
            '7': MaterialChannel.EMISSIVE,
        };

        if (channelKeys[key]) {
            this.setMaterialChannel(channelKeys[key]);
            return true;
        }

        // Camera mode cycling (9)
        if (key === '9') {
            const modes = [CameraMode.STATIC, CameraMode.ORBIT, CameraMode.FLY];
            const currentIdx = modes.indexOf(this.cameraMode);
            const nextIdx = (currentIdx + 1) % modes.length;
            this.setCameraMode(modes[nextIdx]);
            return true;
        }
        
        // F key - focus/center on model (works in any camera mode)
        if (key.toLowerCase() === 'f') {
            this.focusOnModel();
            return true;
        }

        // Fly camera movement (WASD + E/C for up/down)
        if (this.cameraMode === CameraMode.FLY) {
            switch (key.toLowerCase()) {
                case 'w': this.flyState.moveForward = true; return true;
                case 's': this.flyState.moveBackward = true; return true;
                case 'a': this.flyState.moveLeft = true; return true;
                case 'd': this.flyState.moveRight = true; return true;
                case 'e': this.flyState.moveUp = true; return true;      // E = ascend
                case 'c': this.flyState.moveDown = true; return true;    // C = descend
                case ' ': this.flyState.moveUp = true; return true;      // Space = ascend
                case 'shift': this.flyState.moveDown = true; return true; // Shift = descend
            }
        }

        return false;
    }

    /**
     * Handle keyboard release
     */
    handleKeyUp(key) {
        if (this.cameraMode === CameraMode.FLY) {
            switch (key.toLowerCase()) {
                case 'w': this.flyState.moveForward = false; return true;
                case 's': this.flyState.moveBackward = false; return true;
                case 'a': this.flyState.moveLeft = false; return true;
                case 'd': this.flyState.moveRight = false; return true;
                case 'e': this.flyState.moveUp = false; return true;
                case 'c': this.flyState.moveDown = false; return true;
                case ' ': this.flyState.moveUp = false; return true;
                case 'shift': this.flyState.moveDown = false; return true;
            }
        }
        return false;
    }

    /**
     * Get camera mode label
     */
    getCameraModeLabel(mode = null) {
        mode = mode || this.cameraMode;
        const labels = {
            [CameraMode.STATIC]: this.t('camera_mode_static'),
            [CameraMode.ORBIT]: this.t('camera_mode_orbit'),
            [CameraMode.FLY]: this.t('camera_mode_fly'),
        };
        return labels[mode] || mode;
    }

    /**
     * Get channel label
     */
    getChannelLabel(channel = null) {
        channel = channel || this.materialChannel;
        const labels = {
            [MaterialChannel.PBR]: this.t('channel_pbr'),
            [MaterialChannel.AO]: this.t('channel_ao'),
            [MaterialChannel.NORMAL]: this.t('channel_normal'),
            [MaterialChannel.ALBEDO]: this.t('channel_albedo'),
            [MaterialChannel.METALNESS]: this.t('channel_metalness'),
            [MaterialChannel.ROUGHNESS]: this.t('channel_roughness'),
            [MaterialChannel.EMISSIVE]: this.t('channel_emissive'),
        };
        return labels[channel] || `Channel ${channel}`;
    }

    /**
     * Cleanup
     */
    destroy() {
        this.clearControllers();
        this.cleanupDragHandlers();
        this.originalMaterials.clear();
        
        // Exit pointer lock
        if (document.pointerLockElement) {
            document.exitPointerLock();
        }
    }

    /**
     * Get controller positions (for saving/sending to server)
     */
    getControllerPositions() {
        const positions = {};
        this.controllerSpheres.forEach(sphere => {
            const id = sphere.userData.controllerId;
            if (id) {
                positions[id] = {
                    x: sphere.position.x,
                    y: sphere.position.y,
                    z: sphere.position.z
                };
            }
        });
        return positions;
    }

    /**
     * Set controller positions (from saved data)
     */
    setControllerPositions(positions) {
        if (!positions) return;
        this.controllerSpheres.forEach(sphere => {
            const id = sphere.userData.controllerId;
            if (id && positions[id]) {
                sphere.position.set(
                    positions[id].x,
                    positions[id].y,
                    positions[id].z
                );
            }
        });
    }
}

// =============================================================================
// Transform System (QWER modes)
// =============================================================================

/**
 * Transform modes enum - keyboard shortcuts QWER
 */
export const TransformMode = {
    SELECT: 'select',    // Q - selection mode (show bounding boxes)
    MOVE: 'move',        // W - translation
    ROTATE: 'rotate',    // E - rotation
    SCALE: 'scale'       // R - scale
};

/**
 * Snap settings for transform operations
 */
export const SnapSettings = {
    rotation: 15,        // degrees
    move: 0.1,           // units
    scale: 0.1           // multiplier
};

/**
 * SelectionSystem class - handles object selection with raycasting and bounding boxes
 */
export class SelectionSystem {
    constructor(scene, camera, renderer) {
        this.scene = scene;
        this.camera = camera;
        this.renderer = renderer;
        
        this.selected = [];                // Array of selected objects
        this.boundingBoxHelpers = [];      // THREE.BoxHelper instances
        this.highlightMeshes = [];         // Semi-transparent highlight meshes
        this.raycaster = null;             // Initialized when THREE is available
        this.selectionEnabled = false;     // Only show highlights in SELECT mode
        
        // Callbacks
        this.onSelectionChange = () => {};
        
        console.log('[SelectionSystem] Initialized');
    }
    
    /**
     * Initialize raycaster (call after THREE is available)
     */
    init() {
        const THREE = window.THREE;
        if (!THREE) {
            console.warn('[SelectionSystem] THREE not available');
            return;
        }
        this.raycaster = new THREE.Raycaster();
        console.log('[SelectionSystem] Raycaster initialized');
    }
    
    /**
     * Enable selection mode - show bounding boxes on all selectable objects
     */
    enableSelectionMode(model) {
        if (!model) return;
        
        const THREE = window.THREE;
        if (!THREE) return;
        
        this.selectionEnabled = true;
        this.clearHighlights();
        
        // Create semi-transparent bounding boxes for all mesh children
        model.traverse((child) => {
            if (child.isMesh && child.geometry) {
                // Create box helper
                const box = new THREE.BoxHelper(child, 0x4f46e5);
                box.material.transparent = true;
                box.material.opacity = 0.3;
                box.userData.targetObject = child;
                this.scene.add(box);
                this.boundingBoxHelpers.push(box);
            }
        });
        
        console.log('[SelectionSystem] Selection mode enabled,', this.boundingBoxHelpers.length, 'boxes created');
    }
    
    /**
     * Disable selection mode - remove all bounding box highlights
     */
    disableSelectionMode() {
        this.selectionEnabled = false;
        this.clearHighlights();
        console.log('[SelectionSystem] Selection mode disabled');
    }
    
    /**
     * Clear all highlights
     */
    clearHighlights() {
        this.boundingBoxHelpers.forEach(box => {
            this.scene.remove(box);
            box.geometry?.dispose();
            box.material?.dispose();
        });
        this.boundingBoxHelpers = [];
        
        this.highlightMeshes.forEach(mesh => {
            this.scene.remove(mesh);
            mesh.geometry?.dispose();
            mesh.material?.dispose();
        });
        this.highlightMeshes = [];
    }
    
    /**
     * Select object at mouse position
     * @param {Object} mouse - {x, y} normalized device coordinates
     * @param {THREE.Object3D} model - Root model to search in
     * @param {boolean} addToSelection - If true, add to existing selection (Shift+click)
     * @returns {THREE.Object3D|null} - Selected object or null
     */
    selectAtPoint(mouse, model, addToSelection = false) {
        if (!this.raycaster || !model || !this.camera) return null;
        
        const THREE = window.THREE;
        if (!THREE) return null;
        
        const mouseVec = new THREE.Vector2(mouse.x, mouse.y);
        this.raycaster.setFromCamera(mouseVec, this.camera);
        
        // Get all meshes from model
        const meshes = [];
        model.traverse((child) => {
            if (child.isMesh) meshes.push(child);
        });
        
        const intersects = this.raycaster.intersectObjects(meshes, false);
        
        if (intersects.length === 0) {
            if (!addToSelection) {
                this.deselectAll();
            }
            return null;
        }
        
        // Get the first hit object
        let hitObject = intersects[0].object;
        
        // Walk up to find a meaningful parent (with name or userData)
        while (hitObject.parent && hitObject.parent !== model && hitObject.parent.type !== 'Scene') {
            if (hitObject.parent.name || hitObject.parent.userData?.isSelectable) {
                hitObject = hitObject.parent;
            } else {
                break;
            }
        }
        
        if (!addToSelection) {
            this.deselectAll();
        }
        
        this.select(hitObject);
        return hitObject;
    }
    
    /**
     * Select an object
     */
    select(object) {
        if (!object || this.selected.includes(object)) return;
        
        const THREE = window.THREE;
        if (!THREE) return;
        
        this.selected.push(object);
        
        // Create bright bounding box for selected object
        const box = new THREE.BoxHelper(object, 0x00ff00);
        box.material.transparent = false;
        box.userData.isSelectionBox = true;
        box.userData.targetObject = object;
        this.scene.add(box);
        this.boundingBoxHelpers.push(box);
        
        console.log('[SelectionSystem] Selected:', object.name || object.type, '| Total selected:', this.selected.length);
        this.onSelectionChange(this.selected);
    }
    
    /**
     * Deselect an object
     */
    deselect(object) {
        const idx = this.selected.indexOf(object);
        if (idx === -1) return;
        
        this.selected.splice(idx, 1);
        
        // Remove its bounding box
        const boxIdx = this.boundingBoxHelpers.findIndex(b => b.userData.targetObject === object && b.userData.isSelectionBox);
        if (boxIdx !== -1) {
            const box = this.boundingBoxHelpers[boxIdx];
            this.scene.remove(box);
            box.geometry?.dispose();
            box.material?.dispose();
            this.boundingBoxHelpers.splice(boxIdx, 1);
        }
        
        console.log('[SelectionSystem] Deselected:', object.name || object.type);
        this.onSelectionChange(this.selected);
    }
    
    /**
     * Deselect all objects
     */
    deselectAll() {
        // Remove selection boxes only (keep mode boxes if in select mode)
        const toRemove = this.boundingBoxHelpers.filter(b => b.userData.isSelectionBox);
        toRemove.forEach(box => {
            this.scene.remove(box);
            box.geometry?.dispose();
            box.material?.dispose();
        });
        this.boundingBoxHelpers = this.boundingBoxHelpers.filter(b => !b.userData.isSelectionBox);
        
        this.selected = [];
        console.log('[SelectionSystem] Deselected all');
        this.onSelectionChange(this.selected);
    }
    
    /**
     * Get first selected object
     */
    getSelected() {
        return this.selected[0] || null;
    }
    
    /**
     * Get all selected objects
     */
    getAllSelected() {
        return [...this.selected];
    }
    
    /**
     * Update bounding boxes (call in animation loop)
     */
    update() {
        this.boundingBoxHelpers.forEach(box => {
            if (box.userData.targetObject) {
                box.update();
            }
        });
    }
    
    /**
     * Cleanup
     */
    dispose() {
        this.clearHighlights();
        this.selected = [];
        this.raycaster = null;
    }
}

/**
 * GizmoLoader class - loads and manages transform gizmos from GLB files
 */
export class GizmoLoader {
    constructor(scene) {
        this.scene = scene;
        this.gizmos = {
            move: null,
            rotate: null,
            scale: null
        };
        this.activeGizmo = null;
        this.gizmoScale = 1.0;
        
        // Axis colors
        this.axisColors = {
            x: 0xff0000,
            y: 0x00ff00,
            z: 0x0000ff
        };
        
        // Currently hovered axis
        this.hoveredAxis = null;
        
        console.log('[GizmoLoader] Initialized');
    }
    
    /**
     * Load all gizmo models
     */
    async loadGizmos() {
        const THREE = window.THREE;
        if (!THREE) {
            console.warn('[GizmoLoader] THREE not available');
            return;
        }
        
        // Dynamic import of GLTFLoader
        const { GLTFLoader } = await import('https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/loaders/GLTFLoader.js');
        const loader = new GLTFLoader();
        
        const gizmoFiles = {
            move: '/static/glb/gizmo_move.glb',
            rotate: '/static/glb/gizmo_rotate.glb',
            scale: '/static/glb/gizmo_scale.glb'
        };
        
        const loadPromises = Object.entries(gizmoFiles).map(async ([type, path]) => {
            try {
                const gltf = await loader.loadAsync(path);
                this.gizmos[type] = gltf.scene;
                this.gizmos[type].visible = false;
                this.gizmos[type].userData.gizmoType = type;
                
                // Setup materials for axis highlighting
                this.setupGizmoMaterials(this.gizmos[type], type);
                
                this.scene.add(this.gizmos[type]);
                console.log(`[GizmoLoader] Loaded ${type} gizmo`);
            } catch (err) {
                console.error(`[GizmoLoader] Failed to load ${type} gizmo:`, err);
            }
        });
        
        await Promise.all(loadPromises);
        console.log('[GizmoLoader] All gizmos loaded');
    }
    
    /**
     * Setup materials for gizmo axes
     */
    setupGizmoMaterials(gizmo, type) {
        const THREE = window.THREE;
        if (!THREE) return;
        
        gizmo.traverse((child) => {
            if (child.isMesh) {
                const name = child.name.toLowerCase();
                
                // Determine axis from name
                let axis = null;
                if (name.includes('_x') || name.includes('x_')) axis = 'x';
                else if (name.includes('_y') || name.includes('y_')) axis = 'y';
                else if (name.includes('_z') || name.includes('z_')) axis = 'z';
                
                if (axis) {
                    child.userData.axis = axis;
                    child.userData.gizmoType = type;
                    
                    // Create emissive material
                    const color = this.axisColors[axis];
                    child.material = new THREE.MeshBasicMaterial({
                        color: color,
                        transparent: true,
                        opacity: 0.8,
                        depthTest: false,
                        depthWrite: false
                    });
                    child.renderOrder = 999;
                }
            }
        });
    }
    
    /**
     * Show gizmo of specified type at position
     */
    showGizmo(type, position, rotation = null) {
        // Hide all gizmos first
        this.hideAllGizmos();
        
        const gizmo = this.gizmos[type];
        if (!gizmo) {
            console.warn('[GizmoLoader] Gizmo not loaded:', type);
            return;
        }
        
        gizmo.position.copy(position);
        if (rotation) {
            gizmo.rotation.copy(rotation);
        }
        gizmo.scale.setScalar(this.gizmoScale);
        gizmo.visible = true;
        this.activeGizmo = gizmo;
        
        console.log('[GizmoLoader] Showing', type, 'gizmo at', position.toArray());
    }
    
    /**
     * Hide all gizmos
     */
    hideAllGizmos() {
        Object.values(this.gizmos).forEach(g => {
            if (g) g.visible = false;
        });
        this.activeGizmo = null;
    }
    
    /**
     * Update gizmo position/rotation to match object
     */
    attachToObject(object) {
        if (!this.activeGizmo || !object) return;
        
        const THREE = window.THREE;
        if (!THREE) return;
        
        // Get world position
        const worldPos = new THREE.Vector3();
        object.getWorldPosition(worldPos);
        this.activeGizmo.position.copy(worldPos);
        
        // Scale gizmo based on object size
        const box = new THREE.Box3().setFromObject(object);
        const size = box.getSize(new THREE.Vector3());
        const maxDim = Math.max(size.x, size.y, size.z);
        this.gizmoScale = Math.max(0.5, maxDim * 0.3);
        this.activeGizmo.scale.setScalar(this.gizmoScale);
    }
    
    /**
     * Check if point intersects gizmo axis
     * @returns {{ axis: 'x'|'y'|'z', type: string } | null}
     */
    checkIntersection(mouse, camera) {
        if (!this.activeGizmo) return null;
        
        const THREE = window.THREE;
        if (!THREE) return null;
        
        const raycaster = new THREE.Raycaster();
        raycaster.setFromCamera(new THREE.Vector2(mouse.x, mouse.y), camera);
        
        // Get all axis meshes
        const axisMeshes = [];
        this.activeGizmo.traverse((child) => {
            if (child.isMesh && child.userData.axis) {
                axisMeshes.push(child);
            }
        });
        
        const intersects = raycaster.intersectObjects(axisMeshes, false);
        if (intersects.length > 0) {
            const hit = intersects[0].object;
            return {
                axis: hit.userData.axis,
                type: hit.userData.gizmoType
            };
        }
        
        return null;
    }
    
    /**
     * Highlight axis on hover
     */
    highlightAxis(axis) {
        if (this.hoveredAxis === axis) return;
        this.hoveredAxis = axis;
        
        if (!this.activeGizmo) return;
        
        this.activeGizmo.traverse((child) => {
            if (child.isMesh && child.userData.axis) {
                const isHovered = child.userData.axis === axis;
                child.material.opacity = isHovered ? 1.0 : 0.6;
                if (isHovered) {
                    child.scale.setScalar(1.2);
                } else {
                    child.scale.setScalar(1.0);
                }
            }
        });
    }
    
    /**
     * Reset axis highlights
     */
    resetHighlights() {
        this.hoveredAxis = null;
        if (!this.activeGizmo) return;
        
        this.activeGizmo.traverse((child) => {
            if (child.isMesh && child.userData.axis) {
                child.material.opacity = 0.8;
                child.scale.setScalar(1.0);
            }
        });
    }
    
    /**
     * Get active gizmo type
     */
    getActiveType() {
        return this.activeGizmo?.userData?.gizmoType || null;
    }
    
    /**
     * Cleanup
     */
    dispose() {
        Object.values(this.gizmos).forEach(g => {
            if (g) {
                this.scene.remove(g);
                g.traverse((child) => {
                    if (child.geometry) child.geometry.dispose();
                    if (child.material) child.material.dispose();
                });
            }
        });
        this.gizmos = { move: null, rotate: null, scale: null };
        this.activeGizmo = null;
    }
}

/**
 * TransformManager class - manages QWER transform modes
 */
export class TransformManager {
    constructor(options = {}) {
        this.scene = options.scene;
        this.camera = options.camera;
        this.renderer = options.renderer;
        this.controls = options.controls;      // OrbitControls
        this.model = options.model;
        
        this.mode = TransformMode.SELECT;
        this.snapEnabled = true;
        this.snapSettings = { ...SnapSettings };
        
        // Sub-systems
        this.selectionSystem = new SelectionSystem(this.scene, this.camera, this.renderer);
        this.gizmoLoader = new GizmoLoader(this.scene);
        
        // Drag state
        this.isDragging = false;
        this.dragAxis = null;
        this.dragStartPoint = null;
        this.dragStartValue = null;
        
        // Callbacks
        this.onModeChange = options.onModeChange || (() => {});
        this.onTransform = options.onTransform || (() => {});
        
        // Bind event handlers
        this._onPointerDown = this._onPointerDown.bind(this);
        this._onPointerMove = this._onPointerMove.bind(this);
        this._onPointerUp = this._onPointerUp.bind(this);
        
        console.log('[TransformManager] Initialized');
    }
    
    /**
     * Initialize systems (call after THREE is available)
     */
    async init() {
        this.selectionSystem.init();
        await this.gizmoLoader.loadGizmos();
        this._bindEvents();
        console.log('[TransformManager] Systems initialized');
    }
    
    /**
     * Bind pointer events
     */
    _bindEvents() {
        if (!this.renderer) return;
        
        const canvas = this.renderer.domElement;
        canvas.addEventListener('pointerdown', this._onPointerDown);
        canvas.addEventListener('pointermove', this._onPointerMove);
        canvas.addEventListener('pointerup', this._onPointerUp);
        canvas.addEventListener('pointerleave', this._onPointerUp);
    }
    
    /**
     * Unbind pointer events
     */
    _unbindEvents() {
        if (!this.renderer) return;
        
        const canvas = this.renderer.domElement;
        canvas.removeEventListener('pointerdown', this._onPointerDown);
        canvas.removeEventListener('pointermove', this._onPointerMove);
        canvas.removeEventListener('pointerup', this._onPointerUp);
        canvas.removeEventListener('pointerleave', this._onPointerUp);
    }
    
    /**
     * Get normalized mouse coordinates
     */
    _getMouse(event) {
        const rect = this.renderer.domElement.getBoundingClientRect();
        return {
            x: ((event.clientX - rect.left) / rect.width) * 2 - 1,
            y: -((event.clientY - rect.top) / rect.height) * 2 + 1
        };
    }
    
    /**
     * Pointer down handler
     */
    _onPointerDown(event) {
        if (event.button !== 0) return; // Left click only

        const mouse = this._getMouse(event);

        // Check gizmo interaction first
        if (this.mode !== TransformMode.SELECT) {
            const hit = this.gizmoLoader.checkIntersection(mouse, this.camera);
            if (hit) {
                this.isDragging = true;
                this.dragAxis = hit.axis;
                this.dragStartPoint = { ...mouse };

                // Use root model (or selected) for transform
                const target = this.getTransformTarget();
                if (target) {
                    this.dragStartValue = {
                        position: target.position.clone(),
                        rotation: target.rotation.clone(),
                        scale: target.scale.clone()
                    };
                }

                // Disable orbit controls during drag
                if (this.controls) this.controls.enabled = false;

                console.log('[TransformManager] Started drag on axis:', hit.axis, 'target:', target?.name);
                event.preventDefault();
                return;
            }
        }
        
        // Selection
        if (this.model) {
            const addToSelection = event.shiftKey;
            this.selectionSystem.selectAtPoint(mouse, this.model, addToSelection);
            this._updateGizmoForSelection();
        }
    }
    
    /**
     * Pointer move handler
     */
    _onPointerMove(event) {
        const mouse = this._getMouse(event);
        
        if (this.isDragging && this.dragAxis) {
            this._handleDrag(mouse);
            return;
        }
        
        // Hover highlight for gizmo
        if (this.mode !== TransformMode.SELECT) {
            const hit = this.gizmoLoader.checkIntersection(mouse, this.camera);
            if (hit) {
                this.gizmoLoader.highlightAxis(hit.axis);
                this.renderer.domElement.style.cursor = 'pointer';
            } else {
                this.gizmoLoader.resetHighlights();
                this.renderer.domElement.style.cursor = '';
            }
        }
    }
    
    /**
     * Pointer up handler
     */
    _onPointerUp(event) {
        if (this.isDragging) {
            this.isDragging = false;
            this.dragAxis = null;
            this.dragStartPoint = null;
            this.dragStartValue = null;

            // Re-enable orbit controls
            if (this.controls) this.controls.enabled = true;

            // Notify transform complete - pass root model (the actual transformed object)
            const target = this.getTransformTarget();
            if (target) {
                this.onTransform(target, this.mode);
            }

            console.log('[TransformManager] Drag ended, target:', target?.name);
        }
    }
    
    /**
     * Set root model - all transforms will apply to this model
     */
    setRootModel(model) {
        this.rootModel = model;
        console.log('[TransformManager] Root model set:', model?.name || 'none');
    }
    
    /**
     * Get transform target - root model if set, otherwise selected object
     */
    getTransformTarget() {
        // Always use rootModel if set (for moving entire model)
        if (this.rootModel) {
            return this.rootModel;
        }
        return this.selectionSystem.getSelected();
    }
    
    /**
     * Handle drag operation - transforms apply to root model
     */
    _handleDrag(mouse) {
        // Get target: root model if set, otherwise selected
        const target = this.getTransformTarget();
        if (!target || !this.dragStartValue) return;
        
        const THREE = window.THREE;
        if (!THREE) return;
        
        // Calculate delta in screen space
        const deltaX = mouse.x - this.dragStartPoint.x;
        const deltaY = mouse.y - this.dragStartPoint.y;
        
        // Map axis to component
        const axisMap = { x: 'x', y: 'y', z: 'z' };
        const axis = axisMap[this.dragAxis];
        
        // Sensitivity (increased for more responsive feel)
        const moveSens = 8.0;      // Increased from 5.0
        const rotateSens = Math.PI * 1.5;  // Increased from PI
        const scaleSens = 3.0;     // Increased from 2.0
        
        // Use deltaX for X/Z axes, deltaY for Y axis
        // Fix axis inversion to match expected direction
        let delta;
        if (this.dragAxis === 'y') {
            delta = deltaY; // Inverted: down in screen = positive Y
        } else if (this.dragAxis === 'x') {
            delta = -deltaX; // Inverted: right in screen = negative X
        } else {
            delta = deltaX; // Z axis unchanged
        }
        
        switch (this.mode) {
            case TransformMode.MOVE: {
                let moveAmount = delta * moveSens;
                if (this.snapEnabled) {
                    moveAmount = Math.round(moveAmount / this.snapSettings.move) * this.snapSettings.move;
                }
                target.position[axis] = this.dragStartValue.position[axis] + moveAmount;
                break;
            }
            
            case TransformMode.ROTATE: {
                let rotateAmount = delta * rotateSens;
                if (this.snapEnabled) {
                    const snapRad = this.snapSettings.rotation * (Math.PI / 180);
                    rotateAmount = Math.round(rotateAmount / snapRad) * snapRad;
                }
                target.rotation[axis] = this.dragStartValue.rotation[axis] + rotateAmount;
                break;
            }
            
            case TransformMode.SCALE: {
                let scaleAmount = 1 + delta * scaleSens;
                if (this.snapEnabled) {
                    scaleAmount = Math.round(scaleAmount / this.snapSettings.scale) * this.snapSettings.scale;
                }
                scaleAmount = Math.max(0.1, scaleAmount); // Minimum scale
                target.scale[axis] = this.dragStartValue.scale[axis] * scaleAmount;
                break;
            }
        }
        
        // Update gizmo position
        this.gizmoLoader.attachToObject(target);
    }
    
    /**
     * Update gizmo based on current selection
     */
    _updateGizmoForSelection() {
        const selected = this.selectionSystem.getSelected();
        
        if (!selected) {
            this.gizmoLoader.hideAllGizmos();
            return;
        }
        
        const THREE = window.THREE;
        if (!THREE) return;
        
        // Show appropriate gizmo based on mode
        const gizmoType = {
            [TransformMode.MOVE]: 'move',
            [TransformMode.ROTATE]: 'rotate',
            [TransformMode.SCALE]: 'scale'
        }[this.mode];
        
        if (gizmoType) {
            const worldPos = new THREE.Vector3();
            selected.getWorldPosition(worldPos);
            this.gizmoLoader.showGizmo(gizmoType, worldPos);
            this.gizmoLoader.attachToObject(selected);
        } else {
            this.gizmoLoader.hideAllGizmos();
        }
    }
    
    /**
     * Set transform mode
     */
    setMode(mode) {
        if (!Object.values(TransformMode).includes(mode)) {
            console.warn('[TransformManager] Invalid mode:', mode);
            return;
        }
        
        const prevMode = this.mode;
        this.mode = mode;
        
        // Update selection system
        if (mode === TransformMode.SELECT) {
            this.selectionSystem.enableSelectionMode(this.model);
            this.gizmoLoader.hideAllGizmos();
        } else {
            this.selectionSystem.disableSelectionMode();
            this._updateGizmoForSelection();
        }
        
        console.log('[TransformManager] Mode changed:', prevMode, '->', mode);
        this.onModeChange(mode, prevMode);
    }
    
    /**
     * Set model reference
     */
    setModel(model) {
        this.model = model;
        this.selectionSystem.deselectAll();
        
        if (this.mode === TransformMode.SELECT) {
            this.selectionSystem.enableSelectionMode(model);
        }
    }
    
    /**
     * Set snap enabled/disabled
     */
    setSnapEnabled(enabled) {
        this.snapEnabled = enabled;
        console.log('[TransformManager] Snap:', enabled);
    }
    
    /**
     * Update snap settings
     */
    setSnapSettings(settings) {
        this.snapSettings = { ...this.snapSettings, ...settings };
        console.log('[TransformManager] Snap settings updated:', this.snapSettings);
    }
    
    /**
     * Get current mode
     */
    getMode() {
        return this.mode;
    }
    
    /**
     * Get mode label
     */
    getModeLabel() {
        const labels = {
            [TransformMode.SELECT]: 'Select (Q)',
            [TransformMode.MOVE]: 'Move (W)',
            [TransformMode.ROTATE]: 'Rotate (E)',
            [TransformMode.SCALE]: 'Scale (R)'
        };
        return labels[this.mode] || this.mode;
    }
    
    /**
     * Handle keyboard shortcuts
     */
    handleKeyDown(key) {
        const keyLower = key.toLowerCase();
        
        switch (keyLower) {
            case 'q':
                this.setMode(TransformMode.SELECT);
                return true;
            case 'w':
                this.setMode(TransformMode.MOVE);
                return true;
            case 'e':
                this.setMode(TransformMode.ROTATE);
                return true;
            case 'r':
                this.setMode(TransformMode.SCALE);
                return true;
            case 'escape':
                this.selectionSystem.deselectAll();
                this.gizmoLoader.hideAllGizmos();
                return true;
        }
        
        return false;
    }
    
    /**
     * Update (call in animation loop)
     */
    update() {
        this.selectionSystem.update();
        
        // Keep gizmo attached to selected object
        const selected = this.selectionSystem.getSelected();
        if (selected && this.gizmoLoader.activeGizmo) {
            this.gizmoLoader.attachToObject(selected);
        }
    }
    
    /**
     * Cleanup
     */
    dispose() {
        this._unbindEvents();
        this.selectionSystem.dispose();
        this.gizmoLoader.dispose();
    }
}

/**
 * HierarchyNavigator class - navigate object hierarchy
 */
export class HierarchyNavigator {
    constructor(options = {}) {
        this.model = options.model;
        this.selectionSystem = options.selectionSystem;
        this.currentLevel = null;           // Current hierarchy level object
        this.navigationStack = [];          // Stack of parent objects for going back
        
        // Callbacks
        this.onNavigate = options.onNavigate || (() => {});
        
        console.log('[HierarchyNavigator] Initialized');
    }
    
    /**
     * Set model reference
     */
    setModel(model) {
        this.model = model;
        this.currentLevel = model;
        this.navigationStack = [];
        console.log('[HierarchyNavigator] Model set, root level');
    }
    
    /**
     * Enter into selected object's children (Space key)
     */
    enterChildren() {
        if (!this.selectionSystem) return false;
        
        const selected = this.selectionSystem.getSelected();
        if (!selected) {
            console.log('[HierarchyNavigator] No selection to enter');
            return false;
        }
        
        if (!selected.children || selected.children.length === 0) {
            console.log('[HierarchyNavigator] Selected object has no children');
            return false;
        }
        
        // Push current level to stack
        this.navigationStack.push(this.currentLevel);
        this.currentLevel = selected;
        
        // Select first child
        this.selectionSystem.deselectAll();
        const firstChild = selected.children.find(c => c.isMesh || c.isGroup || c.isObject3D);
        if (firstChild) {
            this.selectionSystem.select(firstChild);
        }
        
        console.log('[HierarchyNavigator] Entered:', selected.name || selected.type, '| Children:', selected.children.length);
        this.onNavigate(this.currentLevel, 'enter');
        return true;
    }
    
    /**
     * Go to parent level (↑ button or Backspace)
     */
    goToParent() {
        if (this.navigationStack.length === 0) {
            console.log('[HierarchyNavigator] Already at root level');
            return false;
        }
        
        const parent = this.navigationStack.pop();
        
        // Select the object we're leaving
        const leavingObject = this.currentLevel;
        this.currentLevel = parent;
        
        this.selectionSystem.deselectAll();
        this.selectionSystem.select(leavingObject);
        
        console.log('[HierarchyNavigator] Went to parent:', parent.name || parent.type);
        this.onNavigate(this.currentLevel, 'parent');
        return true;
    }
    
    /**
     * Get current level name
     */
    getCurrentLevelName() {
        if (!this.currentLevel) return 'None';
        return this.currentLevel.name || this.currentLevel.type || 'Object';
    }
    
    /**
     * Get navigation path (breadcrumb)
     */
    getNavigationPath() {
        const path = this.navigationStack.map(obj => obj.name || obj.type || '?');
        if (this.currentLevel) {
            path.push(this.currentLevel.name || this.currentLevel.type || 'Current');
        }
        return path;
    }
    
    /**
     * Reset to root
     */
    resetToRoot() {
        this.currentLevel = this.model;
        this.navigationStack = [];
        this.selectionSystem?.deselectAll();
        console.log('[HierarchyNavigator] Reset to root');
        this.onNavigate(this.currentLevel, 'reset');
    }
    
    /**
     * Handle keyboard input
     */
    handleKeyDown(key) {
        switch (key) {
            case ' ':
            case 'Space':
                return this.enterChildren();
            case 'Backspace':
                return this.goToParent();
        }
        return false;
    }
}

/**
 * TPoseReference class - manages T-pose reference model overlay
 * Self-illuminating with configurable color and transparency
 */
export class TPoseReference {
    constructor(scene, options = {}) {
        this.scene = scene;
        this.model = null;
        this.visible = false;
        
        // Configurable parameters
        this.color = options.color || 0x00ffff;  // Cyan default (self-illuminating look)
        this.opacity = options.opacity || 0.4;
        this.emissiveIntensity = options.emissiveIntensity || 1.0;
        
        console.log('[TPoseReference] Initialized with color:', this.color.toString(16), 'opacity:', this.opacity);
    }
    
    /**
     * Load T-pose reference model
     */
    async load() {
        const THREE = window.THREE;
        if (!THREE) {
            console.warn('[TPoseReference] THREE not available');
            return;
        }
        
        try {
            const { GLTFLoader } = await import('https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/loaders/GLTFLoader.js');
            const loader = new GLTFLoader();
            
            const gltf = await loader.loadAsync('/static/glb/default_t_pose.glb');
            this.model = gltf.scene;
            
            // Fixed position at origin
            this.model.position.set(0, 0, 0);
            this.model.rotation.set(0, 0, 0);
            this.model.scale.set(1, 1, 1);
            
            // Apply self-illuminating material (emissive)
            this._applyMaterial();
            
            this.model.visible = this.visible;
            this.scene.add(this.model);
            
            console.log('[TPoseReference] Model loaded with emissive material');
        } catch (err) {
            console.error('[TPoseReference] Failed to load:', err);
        }
    }
    
    /**
     * Apply falloff shader material to model (Fresnel edge glow effect)
     */
    _applyMaterial() {
        const THREE = window.THREE;
        if (!this.model || !THREE) return;
        
        // Create falloff shader material
        const falloffMaterial = new THREE.ShaderMaterial({
            uniforms: {
                color: { value: new THREE.Color(this.color) },
                opacity: { value: this.opacity },
                falloffPower: { value: 2.0 }
            },
            vertexShader: `
                varying vec3 vNormal;
                varying vec3 vViewDir;
                void main() {
                    vNormal = normalize(normalMatrix * normal);
                    vec4 worldPos = modelViewMatrix * vec4(position, 1.0);
                    vViewDir = normalize(-worldPos.xyz);
                    gl_Position = projectionMatrix * worldPos;
                }
            `,
            fragmentShader: `
                uniform vec3 color;
                uniform float opacity;
                uniform float falloffPower;
                varying vec3 vNormal;
                varying vec3 vViewDir;
                void main() {
                    // Fresnel falloff - brighter at edges
                    float fresnel = pow(1.0 - abs(dot(vNormal, vViewDir)), falloffPower);
                    float intensity = 0.3 + fresnel * 0.7;
                    gl_FragColor = vec4(color * intensity, opacity * intensity);
                }
            `,
            transparent: true,
            side: THREE.DoubleSide,
            depthWrite: false,
            depthTest: true,
            blending: THREE.AdditiveBlending
        });
        
        this.model.traverse((child) => {
            if (child.isMesh) {
                child.material = falloffMaterial.clone();
                child.renderOrder = 10; // Render on top for visibility
            }
        });
        
        console.log('[TPoseReference] Applied falloff shader material');
    }
    
    /**
     * Set color (hex number or string)
     */
    setColor(color) {
        const THREE = window.THREE;
        if (typeof color === 'string') {
            this.color = parseInt(color.replace('#', ''), 16);
        } else {
            this.color = color;
        }
        
        if (this.model && THREE) {
            this.model.traverse((child) => {
                if (child.isMesh && child.material) {
                    // Support both ShaderMaterial uniforms and regular material.color
                    if (child.material.uniforms && child.material.uniforms.color) {
                        child.material.uniforms.color.value.setHex(this.color);
                    } else if (child.material.color) {
                        child.material.color.setHex(this.color);
                    }
                }
            });
        }
        console.log('[TPoseReference] Color set to:', this.color.toString(16));
    }
    
    /**
     * Set opacity (0-1)
     */
    setOpacity(opacity) {
        this.opacity = Math.max(0, Math.min(1, opacity));
        
        if (this.model) {
            this.model.traverse((child) => {
                if (child.isMesh && child.material) {
                    // Support both ShaderMaterial uniforms and regular material.opacity
                    if (child.material.uniforms && child.material.uniforms.opacity) {
                        child.material.uniforms.opacity.value = this.opacity;
                    } else {
                        child.material.opacity = this.opacity;
                    }
                }
            });
        }
        console.log('[TPoseReference] Opacity set to:', this.opacity);
    }
    
    /**
     * Toggle visibility (T key)
     */
    toggle() {
        this.visible = !this.visible;
        if (this.model) {
            this.model.visible = this.visible;
        }
        console.log('[TPoseReference] Visibility:', this.visible);
        return this.visible;
    }
    
    /**
     * Set visibility
     */
    setVisible(visible) {
        this.visible = visible;
        if (this.model) {
            this.model.visible = visible;
        }
        console.log('[TPoseReference] setVisible:', visible);
    }

    /**
     * Set wireframe mode
     */
    setWireframe(wireframe) {
        this.wireframe = wireframe;
        if (this.model) {
            this.model.traverse((child) => {
                if (child.isMesh && child.material) {
                    child.material.wireframe = wireframe;
                }
            });
        }
    }
    
    /**
     * Check if loaded
     */
    isLoaded() {
        return this.model !== null;
    }
    
    /**
     * Cleanup
     */
    dispose() {
        if (this.model) {
            this.scene.remove(this.model);
            this.model.traverse((child) => {
                if (child.geometry) child.geometry.dispose();
                if (child.material) child.material.dispose();
            });
            this.model = null;
        }
    }
}

// =============================================================================
// Browser Auto-Test System
// =============================================================================

/**
 * TransformSystemTests - Auto-tests with console logging
 */
export class TransformSystemTests {
    constructor() {
        this.testResults = [];
        this.testCount = 0;
        this.passCount = 0;
        this.failCount = 0;
    }
    
    /**
     * Run all tests
     */
    async runAll(transformManager, viewerControls, tPoseRef) {
        console.log('='.repeat(60));
        console.log('[TEST] 🧪 Starting Transform System Auto-Tests');
        console.log('='.repeat(60));
        
        this.testResults = [];
        this.testCount = 0;
        this.passCount = 0;
        this.failCount = 0;
        
        // Test groups
        await this.testTransformModes(transformManager);
        await this.testSelectionSystem(transformManager);
        await this.testGizmoLoader(transformManager);
        await this.testTPoseReference(tPoseRef);
        await this.testSnapSettings(transformManager);
        await this.testKeyboardShortcuts(transformManager, viewerControls, tPoseRef);
        
        // Summary
        console.log('='.repeat(60));
        console.log(`[TEST] 📊 Results: ${this.passCount}/${this.testCount} passed, ${this.failCount} failed`);
        console.log('='.repeat(60));
        
        return {
            total: this.testCount,
            passed: this.passCount,
            failed: this.failCount,
            results: this.testResults
        };
    }
    
    /**
     * Assert helper
     */
    assert(condition, testName, details = '') {
        this.testCount++;
        if (condition) {
            this.passCount++;
            console.log(`[TEST] ✅ ${testName}`);
            this.testResults.push({ name: testName, passed: true, details });
        } else {
            this.failCount++;
            console.error(`[TEST] ❌ ${testName}`, details);
            this.testResults.push({ name: testName, passed: false, details });
        }
    }
    
    /**
     * Test transform modes
     */
    async testTransformModes(tm) {
        console.log('\n[TEST] 📁 Transform Modes');
        
        if (!tm) {
            this.assert(false, 'TransformManager exists', 'TransformManager is null');
            return;
        }
        
        this.assert(tm.mode === TransformMode.SELECT, 'Initial mode is SELECT');
        
        tm.setMode(TransformMode.MOVE);
        this.assert(tm.mode === TransformMode.MOVE, 'Can set MOVE mode');
        
        tm.setMode(TransformMode.ROTATE);
        this.assert(tm.mode === TransformMode.ROTATE, 'Can set ROTATE mode');
        
        tm.setMode(TransformMode.SCALE);
        this.assert(tm.mode === TransformMode.SCALE, 'Can set SCALE mode');
        
        tm.setMode(TransformMode.SELECT);
        this.assert(tm.mode === TransformMode.SELECT, 'Can return to SELECT mode');
        
        this.assert(tm.getModeLabel().includes('Select'), 'getModeLabel() returns correct label');
    }
    
    /**
     * Test selection system
     */
    async testSelectionSystem(tm) {
        console.log('\n[TEST] 📁 Selection System');
        
        if (!tm?.selectionSystem) {
            this.assert(false, 'SelectionSystem exists', 'SelectionSystem is null');
            return;
        }
        
        const ss = tm.selectionSystem;
        
        this.assert(ss.raycaster !== null, 'Raycaster is initialized');
        this.assert(Array.isArray(ss.selected), 'Selected array exists');
        this.assert(ss.selected.length === 0, 'Initial selection is empty');
        
        // Test deselect all
        ss.deselectAll();
        this.assert(ss.selected.length === 0, 'deselectAll() clears selection');
        
        this.assert(typeof ss.onSelectionChange === 'function', 'onSelectionChange callback exists');
    }
    
    /**
     * Test gizmo loader
     */
    async testGizmoLoader(tm) {
        console.log('\n[TEST] 📁 Gizmo Loader');
        
        if (!tm?.gizmoLoader) {
            this.assert(false, 'GizmoLoader exists', 'GizmoLoader is null');
            return;
        }
        
        const gl = tm.gizmoLoader;
        
        this.assert(gl.gizmos.move !== null, 'Move gizmo loaded');
        this.assert(gl.gizmos.rotate !== null, 'Rotate gizmo loaded');
        this.assert(gl.gizmos.scale !== null, 'Scale gizmo loaded');
        
        // Test hide all
        gl.hideAllGizmos();
        this.assert(gl.activeGizmo === null, 'hideAllGizmos() clears active gizmo');
        
        // Test axis colors
        this.assert(gl.axisColors.x === 0xff0000, 'X axis color is red');
        this.assert(gl.axisColors.y === 0x00ff00, 'Y axis color is green');
        this.assert(gl.axisColors.z === 0x0000ff, 'Z axis color is blue');
    }
    
    /**
     * Test T-pose reference
     */
    async testTPoseReference(tPose) {
        console.log('\n[TEST] 📁 T-Pose Reference');
        
        if (!tPose) {
            this.assert(false, 'TPoseReference exists', 'TPoseReference is null');
            return;
        }
        
        this.assert(tPose.isLoaded(), 'T-pose model loaded');
        
        const initialVisible = tPose.visible;
        tPose.toggle();
        this.assert(tPose.visible !== initialVisible, 'toggle() changes visibility');
        tPose.toggle(); // Restore
        
        tPose.setOpacity(0.5);
        this.assert(tPose.opacity === 0.5, 'setOpacity() updates opacity');
        tPose.setOpacity(0.3); // Restore
        
        tPose.setWireframe(false);
        this.assert(tPose.wireframe === false, 'setWireframe() updates wireframe');
        tPose.setWireframe(true); // Restore
    }
    
    /**
     * Test snap settings
     */
    async testSnapSettings(tm) {
        console.log('\n[TEST] 📁 Snap Settings');
        
        if (!tm) return;
        
        this.assert(tm.snapEnabled === true, 'Snap is enabled by default');
        
        tm.setSnapEnabled(false);
        this.assert(tm.snapEnabled === false, 'setSnapEnabled(false) works');
        
        tm.setSnapEnabled(true);
        this.assert(tm.snapEnabled === true, 'setSnapEnabled(true) works');
        
        const originalRotation = tm.snapSettings.rotation;
        tm.setSnapSettings({ rotation: 30 });
        this.assert(tm.snapSettings.rotation === 30, 'setSnapSettings() updates rotation');
        tm.setSnapSettings({ rotation: originalRotation }); // Restore
    }
    
    /**
     * Test keyboard shortcuts (simulated)
     */
    async testKeyboardShortcuts(tm, vc, tPose) {
        console.log('\n[TEST] 📁 Keyboard Shortcuts');
        
        if (!tm) return;
        
        // QWER modes
        tm.handleKeyDown('q');
        this.assert(tm.mode === TransformMode.SELECT, 'Q key sets SELECT mode');
        
        tm.handleKeyDown('w');
        this.assert(tm.mode === TransformMode.MOVE, 'W key sets MOVE mode');
        
        tm.handleKeyDown('e');
        this.assert(tm.mode === TransformMode.ROTATE, 'E key sets ROTATE mode');
        
        tm.handleKeyDown('r');
        this.assert(tm.mode === TransformMode.SCALE, 'R key sets SCALE mode');
        
        // ESC
        tm.handleKeyDown('escape');
        this.assert(tm.selectionSystem.selected.length === 0, 'ESC clears selection');
        
        // T for T-pose
        if (tPose) {
            const vis = tPose.visible;
            // T key should be handled by ViewerControls, not TransformManager
            this.assert(typeof tPose.toggle === 'function', 'T-pose has toggle method');
        }
        
        // Reset to SELECT
        tm.setMode(TransformMode.SELECT);
    }
}

// Export default for convenience
export default { 
    RigEditor, 
    ViewerControls, 
    RigType, 
    CameraMode, 
    MaterialChannel,
    TransformMode,
    TransformManager,
    SelectionSystem,
    GizmoLoader,
    HierarchyNavigator,
    TPoseReference,
    SnapSettings,
    TransformSystemTests
};
