/**
 * AutoRig Online - Main Application
 */

const App = {
    state: {
        user: null,
        anon: null,
        creditsRemaining: 0,
        loginRequired: false,
        selectedFile: null,
        activeTab: 'upload',
        free3dCreateInFlight: false,
        taskSubmitInProgress: false,
        rigV2VisionDeps: null
    },

    scheduleNonCriticalWork(callback, timeout = 1200) {
        if (typeof window.requestIdleCallback === 'function') {
            window.requestIdleCallback(() => callback(), { timeout });
            return;
        }
        window.setTimeout(callback, 0);
    },
    
    /**
     * Initialize application
     */
    async init() {
        // Initialize i18n (global)
        await I18n.init();

        // Setup critical UI first so first paint is not blocked by network.
        this.setupThemeToggle();

        // Conversion form (home page only)
        const hasConvertForm = !!document.getElementById('convert-form');
        if (hasConvertForm) {
            this.setupTabs();
            this.setupUploadZone();
            this.setupForm();
        }

        // Defer non-critical network work until after the page is interactive.
        this.scheduleNonCriticalWork(() => {
            this.checkAuth();
        });

        this.scheduleNonCriticalWork(() => {
            this.loadHistory();
            this.loadGalleryPreview();
        });

        this.scheduleNonCriticalWork(() => {
            this.initFree3DSearch();
        }, 2000);

        const hasQueue = !!document.getElementById('queue-active');
        if (hasQueue) {
            this.scheduleNonCriticalWork(() => {
                this.loadQueueStatus();
                setInterval(() => {
                    if (!document.hidden) {
                        this.loadQueueStatus();
                    }
                }, 10000);
            });
        }
        
        // Listen for language changes (re-apply translations + refresh auth-derived labels)
        window.addEventListener('languageChanged', () => {
            this.updateUI();
        });
    },
    
    /**
     * Check authentication status
     */
    async checkAuth() {
        try {
            const response = await fetch('/auth/me');
            const data = await response.json();
            
            this.state.user = data.user;
            this.state.anon = data.anon;
            this.state.creditsRemaining = data.credits_remaining;
            this.state.loginRequired = data.login_required;
            
            this.updateAuthUI();
        } catch (error) {
            console.error('Auth check failed:', error);
        }
    },
    
    /**
     * Update authentication UI
     */
    updateAuthUI() {
        const loginBtn = document.getElementById('login-btn');
        const userInfo = document.getElementById('user-info');
        const creditsEl = document.getElementById('credits-count');
        const creditsLabel = document.getElementById('credits-label');
        const startBtn = document.getElementById('start-btn');
        const loginHint = document.getElementById('login-hint');
        
        if (this.state.user) {
            // Logged in
            if (loginBtn) loginBtn.classList.add('hidden');
            if (userInfo) {
                userInfo.classList.remove('hidden');
                const avatar = userInfo.querySelector('.user-avatar');
                const name = userInfo.querySelector('.user-name');
                if (avatar && this.state.user.picture) {
                    avatar.src = this.state.user.picture;
                }
                if (name) {
                    name.textContent = this.state.user.name || this.state.user.email;
                }
            }
            if (creditsLabel) creditsLabel.textContent = t('credits_remaining');
            if (loginHint) loginHint.classList.add('hidden');
        } else {
            // Anonymous
            if (loginBtn) loginBtn.classList.remove('hidden');
            if (userInfo) userInfo.classList.add('hidden');
            if (creditsLabel) creditsLabel.textContent = t('credits_free');
            if (loginHint && this.state.creditsRemaining < 3) {
                loginHint.classList.remove('hidden');
            }
        }
        
        if (creditsEl) {
            creditsEl.textContent = this.state.creditsRemaining;
        }
        
        // Update start button
        if (startBtn) {
            // Only show if activeTab is 'link'
            if (this.state.activeTab === 'link') {
                startBtn.classList.remove('hidden');
            } else {
                startBtn.classList.add('hidden');
            }

            if (this.state.loginRequired) {
                startBtn.textContent = t('btn_login_continue');
                startBtn.onclick = () => window.location.href = '/auth/login';
            } else {
                startBtn.onclick = () => this.submitTask();
                if (!this.state.taskSubmitInProgress) {
                    startBtn.textContent = t('btn_start');
                    startBtn.disabled = false;
                }
            }
        }
    },
    
    /**
     * Update all UI text
     */
    updateUI() {
        this.updateAuthUI();
        I18n.applyTranslations();
    },

    /**
     * Home convert form: full-card overlay while POST /api/task/create is in flight.
     */
    setConvertFormBusyMessage(i18nKey) {
        const textEl = document.getElementById('convert-form-busy-text');
        if (!textEl || !i18nKey) return;
        textEl.setAttribute('data-i18n', i18nKey);
        textEl.textContent = typeof t === 'function' ? t(i18nKey) : '';
    },

    setConvertFormBusyText(text) {
        const textEl = document.getElementById('convert-form-busy-text');
        if (!textEl) return;
        textEl.removeAttribute('data-i18n');
        textEl.textContent = text || '';
    },

    setConvertFormUploadProgress(pct) {
        const wrap = document.getElementById('convert-form-busy-progress-wrap');
        const track = document.getElementById('convert-form-busy-progress-track');
        const fill = document.getElementById('convert-form-busy-progress-fill');
        const pctEl = document.getElementById('convert-form-busy-progress-pct');
        if (!track || !fill || !pctEl || !wrap || wrap.classList.contains('link-mode')) {
            return;
        }
        if (pct === null || pct === undefined) {
            track.classList.add('indeterminate');
            pctEl.textContent = '…';
            return;
        }
        track.classList.remove('indeterminate');
        const v = Math.max(0, Math.min(100, pct));
        fill.style.width = `${v}%`;
        pctEl.textContent = `${v}%`;
    },

    resetConvertFormProgressUI(mode) {
        const wrap = document.getElementById('convert-form-busy-progress-wrap');
        const track = document.getElementById('convert-form-busy-progress-track');
        const fill = document.getElementById('convert-form-busy-progress-fill');
        const pctEl = document.getElementById('convert-form-busy-progress-pct');
        if (!wrap || !track || !fill || !pctEl) return;
        track.classList.remove('indeterminate');
        fill.style.width = '0%';
        pctEl.textContent = '0%';
        if (mode === 'link') {
            wrap.classList.add('link-mode');
            track.classList.add('indeterminate');
        } else {
            wrap.classList.remove('link-mode');
        }
    },

    setConvertFormBusy(isBusy, mode) {
        const overlay = document.getElementById('convert-form-busy');
        const card = document.querySelector('.convert-form-card');
        if (!overlay) return;

        this.state.taskSubmitInProgress = !!isBusy;

        if (isBusy) {
            const key =
                mode === 'upload' ? 'upload_progress_uploading' : 'upload_progress_creating_task';
            this.setConvertFormBusyMessage(key);
            this.resetConvertFormProgressUI(mode);
            overlay.classList.remove('hidden');
            overlay.setAttribute('aria-busy', 'true');
            card?.classList.add('form-is-busy');
        } else {
            overlay.classList.add('hidden');
            overlay.setAttribute('aria-busy', 'false');
            card?.classList.remove('form-is-busy');
            document.body.classList.remove('rig-detect-modal-open');
            const rigScrim = document.getElementById('rig-detect-scrim');
            rigScrim?.classList.add('hidden');
            rigScrim?.setAttribute('aria-hidden', 'true');
            this.resetConvertFormProgressUI('upload');
            const track = document.getElementById('convert-form-busy-progress-track');
            track?.classList.remove('indeterminate');
        }
    },

    /**
     * Multipart POST with upload progress (fetch cannot report upload bytes).
     */
    postTaskCreateMultipart(formData, onProgress) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/api/task/create');
            xhr.upload.addEventListener('progress', (e) => {
                if (e.lengthComputable && e.total > 0) {
                    onProgress(Math.min(100, Math.round((100 * e.loaded) / e.total)));
                } else {
                    onProgress(null);
                }
            });
            xhr.upload.addEventListener('load', () => {
                this.setConvertFormUploadProgress(100);
                this.setConvertFormBusyMessage('upload_progress_creating_task');
            });
            xhr.onload = () => {
                let data = {};
                try {
                    data = JSON.parse(xhr.responseText || '{}');
                } catch (err) {
                    data = {};
                }
                resolve({
                    ok: xhr.status >= 200 && xhr.status < 300,
                    status: xhr.status,
                    data,
                });
            };
            xhr.onerror = () => reject(new Error('network'));
            xhr.send(formData);
        });
    },

    async loadRigV2VisionDeps() {
        if (this.state.rigV2VisionDeps) return this.state.rigV2VisionDeps;
        const [
            THREE,
            { GLTFLoader },
            { FBXLoader },
            { OBJLoader },
            { OrbitControls },
            { RoomEnvironment },
        ] = await Promise.all([
            import('three'),
            import('https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/loaders/GLTFLoader.js'),
            import('https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/loaders/FBXLoader.js'),
            import('https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/loaders/OBJLoader.js'),
            import('https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/controls/OrbitControls.js'),
            import('https://cdn.jsdelivr.net/npm/three@0.160.0/examples/jsm/environments/RoomEnvironment.js'),
        ]);
        this.state.rigV2VisionDeps = { THREE, GLTFLoader, FBXLoader, OBJLoader, OrbitControls, RoomEnvironment };
        return this.state.rigV2VisionDeps;
    },

    /** All rig types shown on home detection modal (humanoid + 12 animals). */
    RIG_DETECT_RIG_TYPES: ['humanoid', 'dog', 'bear', 'cat', 'cow', 'deer', 'elephant', 'giraffe', 'horse', 'mouse', 'pig', 'rabbit', 'turtle'],
    RIG_DETECT_REVIEW_SECONDS: 30,

    _rigDetectLabelKey(rigKey) {
        const k = String(rigKey || '').toLowerCase();
        return k === 'humanoid' ? 'upload_rig_type_humanoid' : `upload_rig_type_${k}`;
    },

    rigDetectTypeLabel(rigKey) {
        const key = this._rigDetectLabelKey(rigKey);
        const label = typeof t === 'function' ? t(key) : key;
        return label === key ? String(rigKey) : label;
    },

    rigDetectAutoKey(detection) {
        if (!detection) return 'humanoid';
        if (detection.type === 'animal' && detection.animal_type) return String(detection.animal_type).toLowerCase();
        return 'humanoid';
    },

    _rigDetectJitter01(str) {
        let h = 0;
        const s = String(str || '');
        for (let i = 0; i < s.length; i += 1) h = (h * 31 + s.charCodeAt(i)) >>> 0;
        return (h % 1000) / 1000;
    },

    /**
     * Client-side detection + optional live viewer in viewerHost.
     * @returns {{ detection: object|null, dispose: () => void }}
     */
    async runHiddenAnimalDetection(source, options = {}) {
        const viewerHost = options.viewerHost || null;
        const deferDisposal = !!viewerHost;
        const animalTypes = ['dog', 'bear', 'cat', 'cow', 'deer', 'elephant', 'giraffe', 'horse', 'mouse', 'pig', 'rabbit', 'turtle'];
        const animalDecisionThreshold = 0.62;
        const animalDecisionMinMargin = 0.14;
        const animalDecisionMinVotes = 3;
        const model = 'gpt-5.4-nano';

        let objectUrl = '';
        let rafId = 0;
        let disposed = false;
        let renderer = null;
        let controls = null;
        let pmremGenerator = null;
        let offHost = null;
        let resizeObserver = null;

        const dispose = () => {
            if (disposed) return;
            disposed = true;
            try {
                resizeObserver?.disconnect();
            } catch (e) {
                /* ignore */
            }
            resizeObserver = null;
            if (rafId) {
                cancelAnimationFrame(rafId);
                rafId = 0;
            }
            if (objectUrl) {
                try {
                    URL.revokeObjectURL(objectUrl);
                } catch (e) {
                    /* ignore */
                }
                objectUrl = '';
            }
            try {
                controls?.dispose();
            } catch (e) {
                /* ignore */
            }
            controls = null;
            try {
                pmremGenerator?.dispose();
            } catch (e) {
                /* ignore */
            }
            pmremGenerator = null;
            try {
                renderer?.dispose();
            } catch (e) {
                /* ignore */
            }
            renderer = null;
            if (viewerHost) viewerHost.replaceChildren();
            if (offHost && offHost.parentNode) offHost.parentNode.removeChild(offHost);
            offHost = null;
        };

        try {
            const deps = await this.loadRigV2VisionDeps();
            const { THREE, GLTFLoader, FBXLoader, OBJLoader, OrbitControls, RoomEnvironment } = deps;
            const url = source.file ? URL.createObjectURL(source.file) : source.url;
            objectUrl = source.file ? url : '';
            const ext = (source.ext || '').replace(/^\./, '').toLowerCase();

            if (viewerHost) {
                viewerHost.replaceChildren();
            } else {
                offHost = document.createElement('div');
                offHost.style.cssText = 'position:fixed;left:-9999px;top:-9999px;width:512px;height:512px;pointer-events:none;';
                document.body.appendChild(offHost);
            }
            const mountEl = viewerHost || offHost;

            const scene = new THREE.Scene();
            scene.background = new THREE.Color(0x808080);
            const camera = new THREE.PerspectiveCamera(45, 1, 0.01, 10000);
            renderer = new THREE.WebGLRenderer({ antialias: true, preserveDrawingBuffer: true });
            renderer.outputColorSpace = THREE.SRGBColorSpace;
            renderer.toneMapping = THREE.ACESFilmicToneMapping;
            renderer.toneMappingExposure = 1.35;
            renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));

            controls = new OrbitControls(camera, renderer.domElement);

            scene.add(new THREE.AmbientLight(0xffffff, 1.8));
            scene.add(new THREE.HemisphereLight(0xffffff, 0xdbeafe, 2.2));
            const key = new THREE.DirectionalLight(0xffffff, 3.0);
            key.position.set(-3, 5, -4);
            scene.add(key);
            const fill = new THREE.DirectionalLight(0xffffff, 1.8);
            fill.position.set(4, 2, 5);
            scene.add(fill);
            const rim = new THREE.DirectionalLight(0xffffff, 1.4);
            rim.position.set(0, 4, -6);
            scene.add(rim);
            pmremGenerator = new THREE.PMREMGenerator(renderer);
            scene.environment = pmremGenerator.fromScene(new RoomEnvironment(), 0.04).texture;

            const syncViewerSize = () => {
                if (!renderer || !camera) return;
                let w = 512;
                let h = 512;
                if (viewerHost) {
                    w = Math.max(160, viewerHost.clientWidth || 512);
                    h = Math.max(160, viewerHost.clientHeight || w);
                }
                renderer.setSize(w, h, false);
                camera.aspect = w / Math.max(h, 1);
                camera.updateProjectionMatrix();
            };

            if (viewerHost) {
                mountEl.appendChild(renderer.domElement);
                renderer.domElement.style.touchAction = 'none';
                syncViewerSize();
                controls.enableDamping = true;
                controls.dampingFactor = 0.06;
                const tick = () => {
                    if (disposed) return;
                    rafId = requestAnimationFrame(tick);
                    controls.update();
                    renderer.render(scene, camera);
                };
                rafId = requestAnimationFrame(tick);
                if (typeof ResizeObserver !== 'undefined') {
                    resizeObserver = new ResizeObserver(() => {
                        if (!disposed) syncViewerSize();
                    });
                    resizeObserver.observe(viewerHost);
                }
            } else {
                renderer.setSize(512, 512, false);
                mountEl.appendChild(renderer.domElement);
                syncViewerSize();
            }

            const loadWithLoader = (loader, loadUrl) => new Promise((resolve, reject) => loader.load(loadUrl, resolve, undefined, reject));
            let object;
            if (ext === 'glb' || ext === 'gltf') {
                object = (await loadWithLoader(new GLTFLoader(), url)).scene;
            } else if (ext === 'fbx') {
                object = await loadWithLoader(new FBXLoader(), url);
            } else if (ext === 'obj') {
                object = await loadWithLoader(new OBJLoader(), url);
            } else {
                dispose();
                return { detection: null, dispose: () => {} };
            }
            const detectorFallbackMaterial = new THREE.MeshStandardMaterial({
                name: 'vision_detector_neutral_gray',
                color: 0xaab4c3,
                roughness: 0.82,
                metalness: 0.02,
                side: THREE.DoubleSide,
            });
            const materialUsesTexture = (mat) => !!mat && [
                'map',
                'normalMap',
                'roughnessMap',
                'metalnessMap',
                'aoMap',
                'emissiveMap',
                'alphaMap',
                'bumpMap',
                'displacementMap',
            ].some((k) => !!mat[k]);
            const materialLuminance = (mat) => {
                const color = mat?.color;
                if (!color) return 1;
                return (color.r * 0.2126) + (color.g * 0.7152) + (color.b * 0.0722);
            };
            const prepareDetectorMaterial = (mat, node) => {
                const hasTexture = materialUsesTexture(mat);
                const tooDark = materialLuminance(mat) < 0.22;
                if (!mat || !hasTexture || tooDark) {
                    const replacement = detectorFallbackMaterial.clone();
                    replacement.skinning = !!node?.isSkinnedMesh;
                    replacement.vertexColors = false;
                    replacement.needsUpdate = true;
                    return replacement;
                }
                mat.side = THREE.DoubleSide;
                if (mat.color && materialLuminance(mat) < 0.55) {
                    mat.color.set(0xffffff);
                }
                if ('roughness' in mat) mat.roughness = Math.min(Number(mat.roughness ?? 0.8), 0.88);
                if ('metalness' in mat) mat.metalness = Math.min(Number(mat.metalness ?? 0), 0.12);
                if ('envMapIntensity' in mat) mat.envMapIntensity = Math.max(Number(mat.envMapIntensity || 0), 1.3);
                mat.needsUpdate = true;
                return mat;
            };
            object.traverse?.((node) => {
                if (!node?.isMesh) return;
                if (Array.isArray(node.material)) {
                    node.material = node.material.map((mat) => prepareDetectorMaterial(mat, node));
                } else {
                    node.material = prepareDetectorMaterial(node.material, node);
                }
            });
            scene.add(object);
            const box = new THREE.Box3().setFromObject(object);
            object.position.sub(box.getCenter(new THREE.Vector3()));
            const size = new THREE.Box3().setFromObject(object).getSize(new THREE.Vector3());
            const maxDim = Math.max(size.x, size.y, size.z, 1);
            const dist = (maxDim / (2 * Math.tan(THREE.MathUtils.degToRad(camera.fov) / 2))) * 1.45;
            camera.near = Math.max(0.001, dist / 1000);
            camera.far = Math.max(1000, dist * 10);
            syncViewerSize();
            camera.updateProjectionMatrix();

            const views = [
                { id: 'top_side_45', label: 'top-side 45', forward: new THREE.Vector3(-1, -1, -1), up: new THREE.Vector3(0, 1, 0) },
                { id: 'front', label: 'front', forward: new THREE.Vector3(0, 0, -1), up: new THREE.Vector3(0, 1, 0) },
                { id: 'back', label: 'back', forward: new THREE.Vector3(0, 0, 1), up: new THREE.Vector3(0, 1, 0) },
                { id: 'left', label: 'left', forward: new THREE.Vector3(1, 0, 0), up: new THREE.Vector3(0, 1, 0) },
                { id: 'right', label: 'right', forward: new THREE.Vector3(-1, 0, 0), up: new THREE.Vector3(0, 1, 0) },
                { id: 'top', label: 'top', forward: new THREE.Vector3(0, -1, 0), up: new THREE.Vector3(0, 0, -1) },
                { id: 'bottom', label: 'bottom', forward: new THREE.Vector3(0, 1, 0), up: new THREE.Vector3(0, 0, 1) },
            ];
            const capture = (view) => {
                const target = new THREE.Vector3(0, 0, 0);
                camera.up.copy(view.up);
                camera.position.copy(target).sub(view.forward.clone().normalize().multiplyScalar(dist));
                controls.target.copy(target);
                camera.lookAt(target);
                controls.update();
                renderer.render(scene, camera);
                return renderer.domElement.toDataURL('image/jpeg', 0.9);
            };
            const analyze = async (view, capturedImage = null) => {
                const image = capturedImage || capture(view);
                const resp = await fetch('/api/rig-v2/vision/animal-type', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        image_jpg_base64_string: image,
                        view_id_string: view.id,
                        force_openai_bool: true,
                        open_ai_model_override_string: model,
                    }),
                });
                const data = await resp.json();
                return { view_id_string: view.id, ...data };
            };
            const preflightRender = capture(views[0]);
            const first = await analyze(views[0], preflightRender);
            const rest = await Promise.all(views.slice(1).map(analyze));
            const results = [first].concat(rest);
            const scores = {};
            const votes = {};
            for (const result of results) {
                const type = String(result.animal_type_string || '').toLowerCase();
                if (!type) continue;
                scores[type] = (scores[type] || 0) + Math.max(0.05, Math.min(1, Number(result.confidence_float || 0.5)));
                votes[type] = (votes[type] || 0) + 1;
            }
            const sortedScores = Object.entries(scores).sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0));
            const best = String(sortedScores[0]?.[0] || '').toLowerCase();
            const bestScore = Math.max(0, Number(sortedScores[0]?.[1] || 0));
            const runnerUp = String(sortedScores[1]?.[0] || '').toLowerCase();
            const runnerUpScore = Math.max(0, Number(sortedScores[1]?.[1] || 0));
            const viewCount = Math.max(1, results.length || 0);
            const bestVotes = Number(votes[best] || 0);
            const decisionWeight = Math.max(0, Math.min(1, bestScore / viewCount));
            const decisionMargin = Math.max(0, Math.min(1, (bestScore - runnerUpScore) / viewCount));
            const selectedAvgConfidence = bestVotes > 0 ? Math.max(0, Math.min(1, bestScore / bestVotes)) : 0;
            const bestIsAllowedAnimal = animalTypes.includes(best);
            const acceptedAnimal = (
                bestIsAllowedAnimal
                && decisionWeight >= animalDecisionThreshold
                && decisionMargin >= animalDecisionMinMargin
                && bestVotes >= animalDecisionMinVotes
            );
            let rejectedReason = '';
            if (!acceptedAnimal) {
                if (!bestIsAllowedAnimal) {
                    rejectedReason = best === 'humanoid' ? 'best_is_humanoid' : 'best_is_not_allowed_animal';
                } else if (decisionWeight < animalDecisionThreshold) {
                    rejectedReason = 'decision_weight_below_threshold';
                } else if (decisionMargin < animalDecisionMinMargin) {
                    rejectedReason = 'decision_margin_below_threshold';
                } else if (bestVotes < animalDecisionMinVotes) {
                    rejectedReason = 'not_enough_consistent_views';
                } else {
                    rejectedReason = 'animal_decision_rejected';
                }
            }
            const detection = {
                type: acceptedAnimal ? 'animal' : 'humanoid',
                animal_type: acceptedAnimal ? best : '',
                candidate_animal_type_string: bestIsAllowedAnimal ? best : '',
                mode: 'only_rig',
                model_used: model,
                first_result: first,
                results,
                scores,
                selected_score: bestScore,
                selected_type_string: best,
                runner_up_type_string: runnerUp,
                runner_up_score: runnerUpScore,
                selected_votes_int: bestVotes,
                view_count_int: viewCount,
                selected_avg_confidence_float: selectedAvgConfidence,
                animal_decision_weight_float: decisionWeight,
                animal_decision_threshold_float: animalDecisionThreshold,
                animal_decision_margin_float: decisionMargin,
                animal_decision_min_margin_float: animalDecisionMinMargin,
                animal_decision_min_votes_int: animalDecisionMinVotes,
                animal_decision_accepted_bool: acceptedAnimal,
                animal_decision_rejected_reason_string: rejectedReason,
                preflight_render_jpg_base64_string: preflightRender,
            };

            if (!deferDisposal) {
                dispose();
                return { detection, dispose: () => {} };
            }
            return { detection, dispose };
        } catch (err) {
            console.warn('[RigV2Preflight] animal detection skipped:', err);
            dispose();
            return { detection: null, dispose: () => {} };
        }
    },

    buildRigDetectionSubmitPayload(detection, selectedRigKey) {
        const d = JSON.parse(JSON.stringify(detection));
        delete d.preflight_render_jpg_base64_string;
        const autoKey = this.rigDetectAutoKey(detection);
        const sel = String(selectedRigKey || 'humanoid').toLowerCase();
        if (sel === 'humanoid') {
            d.type = 'humanoid';
            d.animal_type = '';
            d.user_selected_bool = autoKey !== 'humanoid';
        } else {
            d.type = 'animal';
            d.animal_type = sel;
            d.mode = 'only_rig';
            d.user_selected_bool = sel !== autoKey;
        }
        return d;
    },

    applyRigSelectionToFormData(formData, detection, selectedRigKey) {
        formData.set('type', 't_pose');
        formData.delete('animal_type');
        formData.delete('mode');
        const payload = this.buildRigDetectionSubmitPayload(detection, selectedRigKey);
        formData.set('rig_v2_animal_detection_json', JSON.stringify(payload));
        const preflight = detection.preflight_render_jpg_base64_string;
        if (preflight) {
            formData.set('preflight_render_jpg_base64_string', preflight);
        }
        const sel = String(selectedRigKey || 'humanoid').toLowerCase();
        if (sel !== 'humanoid') {
            formData.set('type', 'animal');
            formData.set('animal_type', sel);
            formData.set('mode', 'only_rig');
        }
    },

    updateRigDetectSelection(selectedKey) {
        document.querySelectorAll('.rig-detect-card').forEach((btn) => {
            btn.classList.toggle('rig-detect-card--selected', btn.dataset.rigType === selectedKey);
        });
    },

    renderRigDetectCloud(detection, selectedKey, onSelect) {
        const cloud = document.getElementById('rig-detect-cloud');
        if (!cloud) return;
        const scores = detection.scores || {};
        const vc = Math.max(1, Number(detection.view_count_int) || 7);
        const typesOrdered = [...this.RIG_DETECT_RIG_TYPES]
            .map((rigType) => ({
                rigType,
                raw: Number(scores[rigType] || 0),
            }))
            .sort((a, b) => {
                if (b.raw !== a.raw) return b.raw - a.raw;
                return a.rigType.localeCompare(b.rigType);
            })
            .map((x) => x.rigType);

        cloud.replaceChildren();
        cloud.classList.remove('hidden');
        cloud.classList.add('rig-detect-cloud--grid');

        for (const rigType of typesOrdered) {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'rig-detect-card';
            if (rigType === selectedKey) btn.classList.add('rig-detect-card--selected');
            btn.dataset.rigType = rigType;

            const raw = Number(scores[rigType] || 0);
            const weightPct = Math.max(0, Math.min(100, Math.round((raw / vc) * 100)));
            const iconSrc = (typeof resolveRigIconUrl === 'function')
                ? resolveRigIconUrl(rigType)
                : `/static/Icons_png/${rigType === 'humanoid' ? 'Human' : (rigType.charAt(0).toUpperCase() + rigType.slice(1))}.png?v=rigicons1`;
            const label = this.rigDetectTypeLabel(rigType);
            btn.title = label;
            btn.setAttribute('aria-label', label);
            btn.innerHTML = `
                <span class="rig-detect-card-visual">
                    <img src="${iconSrc}" alt="" loading="lazy" decoding="async" />
                    <span class="rig-detect-card-weight">${weightPct}</span>
                </span>
            `;
            btn.addEventListener('click', () => onSelect(rigType));
            cloud.appendChild(btn);
        }
    },

    refreshRigDetectCloudLabels() {
        document.querySelectorAll('.rig-detect-card').forEach((btn) => {
            const rt = btn.dataset.rigType;
            if (!rt) return;
            const label = this.rigDetectTypeLabel(rt);
            btn.title = label;
            btn.setAttribute('aria-label', label);
        });
    },

    /**
     * @returns {Promise<string>} selected rig key (humanoid | animal)
     */
    waitRigDetectReview(detection, initialSelected) {
        return new Promise((resolve) => {
            const overlay = document.getElementById('convert-form-busy');
            const review = document.getElementById('rig-detect-review');
            const hint = document.getElementById('rig-detect-hint');
            const startBtn = document.getElementById('rig-detect-start-now');
            const footer = document.getElementById('rig-detect-footer');

            let selected = initialSelected;
            let secondsLeft = this.RIG_DETECT_REVIEW_SECONDS;
            let interval = 0;

            const renderHint = () => {
                if (hint && typeof t === 'function') {
                    hint.textContent = t('upload_rig_review_hint', {
                        animation_type: this.rigDetectTypeLabel(selected),
                        timer: String(secondsLeft),
                    });
                }
            };
            const refreshFooter = () => {
                if (footer && typeof t === 'function') {
                    footer.textContent = t('upload_rig_review_footer');
                }
            };
            const refreshStartBtn = () => {
                if (startBtn && typeof t === 'function') {
                    startBtn.textContent = t('upload_rig_start_now_with_timer', { timer: String(secondsLeft) });
                }
            };

            overlay?.classList.add('rig-detect--review-phase');
            review?.classList.remove('hidden');

            this.renderRigDetectCloud(detection, selected, (rig) => {
                selected = rig;
                this.updateRigDetectSelection(selected);
                renderHint();
            });

            renderHint();
            refreshFooter();
            refreshStartBtn();

            const onLang = () => {
                renderHint();
                refreshFooter();
                refreshStartBtn();
                this.refreshRigDetectCloudLabels();
            };
            window.addEventListener('languageChanged', onLang);

            const finish = (value) => {
                if (interval) clearInterval(interval);
                window.removeEventListener('languageChanged', onLang);
                if (startBtn) startBtn.onclick = null;
                overlay?.classList.remove('rig-detect--review-phase');
                resolve(value);
            };

            interval = window.setInterval(() => {
                secondsLeft -= 1;
                renderHint();
                refreshStartBtn();
                if (secondsLeft <= 0) {
                    finish(selected);
                }
            }, 1000);

            if (startBtn) {
                startBtn.onclick = () => finish(selected);
            }
        });
    },

    showFree3DCreateOverlay(title) {
        let overlay = document.getElementById('free3d-create-overlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'free3d-create-overlay';
            overlay.className = 'free3d-create-overlay hidden';
            overlay.innerHTML = `
                <div class="free3d-create-overlay-card">
                    <div class="free3d-create-spinner" aria-hidden="true"></div>
                    <div class="free3d-create-title" id="free3d-create-title"></div>
                    <div class="free3d-create-subtitle" id="free3d-create-subtitle"></div>
                </div>
            `;
            document.body.appendChild(overlay);
        }

        const safeTitle = (title || '3D model').trim();
        const titleEl = overlay.querySelector('#free3d-create-title');
        const subtitleEl = overlay.querySelector('#free3d-create-subtitle');
        if (titleEl) {
            titleEl.textContent = t('free3d_creating_task_title').replace('{title}', safeTitle);
        }
        if (subtitleEl) {
            subtitleEl.textContent = t('free3d_creating_task_subtitle');
        }
        overlay.classList.remove('hidden');
        document.body.classList.add('free3d-create-busy');
    },

    hideFree3DCreateOverlay() {
        const overlay = document.getElementById('free3d-create-overlay');
        if (overlay) {
            overlay.classList.add('hidden');
        }
        document.body.classList.remove('free3d-create-busy');
    },
    
    /**
     * Setup theme toggle
     */
    setupThemeToggle() {
        const toggle = document.getElementById('theme-toggle');
        if (!toggle) return;
        
        // Load saved theme
        const savedTheme = localStorage.getItem('autorig_theme') || 'dark';
        document.documentElement.setAttribute('data-theme', savedTheme);
        this.updateThemeIcon(savedTheme);
        
        toggle.addEventListener('click', () => {
            const current = document.documentElement.getAttribute('data-theme');
            const newTheme = current === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('autorig_theme', newTheme);
            this.updateThemeIcon(newTheme);
        });
    },
    
    updateThemeIcon(theme) {
        const toggle = document.getElementById('theme-toggle');
        if (toggle) {
            toggle.textContent = theme === 'dark' ? '☀️' : '🌙';
        }
    },
    
    /**
     * Setup tabs
     */
    setupTabs() {
        const tabs = document.querySelectorAll('.tab');
        const uploadPanel = document.getElementById('upload-panel');
        const linkPanel = document.getElementById('link-panel');
        
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                if (this.state.taskSubmitInProgress) {
                    return;
                }
                const target = tab.getAttribute('data-tab');
                this.state.activeTab = target;
                
                tabs.forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                
                if (target === 'upload') {
                    uploadPanel?.classList.remove('hidden');
                    linkPanel?.classList.add('hidden');
                    document.getElementById('start-btn')?.classList.add('hidden');
                } else {
                    uploadPanel?.classList.add('hidden');
                    linkPanel?.classList.remove('hidden');
                    document.getElementById('start-btn')?.classList.remove('hidden');
                }
            });
        });
    },
    
    /**
     * Setup upload zone
     */
    setupUploadZone() {
        const zone = document.getElementById('upload-zone');
        const input = document.getElementById('file-input');
        const fileInfo = document.getElementById('file-info');
        const fileName = document.getElementById('file-name');
        const removeBtn = document.getElementById('remove-file');
        
        if (!zone || !input) return;
        
        // Click to upload
        zone.addEventListener('click', () => {
            if (this.state.taskSubmitInProgress) {
                return;
            }
            input.click();
        });
        
        // Drag events
        zone.addEventListener('dragover', (e) => {
            e.preventDefault();
            if (this.state.taskSubmitInProgress) {
                return;
            }
            zone.classList.add('dragover');
        });
        
        zone.addEventListener('dragleave', () => {
            zone.classList.remove('dragover');
        });
        
        zone.addEventListener('drop', (e) => {
            e.preventDefault();
            zone.classList.remove('dragover');
            if (this.state.taskSubmitInProgress) {
                return;
            }

            const files = e.dataTransfer.files;
            if (files.length > 0) {
                this.handleFileSelect(files[0]);
            }
        });
        
        // File input change
        input.addEventListener('change', () => {
            if (this.state.taskSubmitInProgress) {
                input.value = '';
                return;
            }
            if (input.files.length > 0) {
                this.handleFileSelect(input.files[0]);
            }
        });
        
        // Remove file
        removeBtn?.addEventListener('click', (e) => {
            e.stopPropagation();
            if (this.state.taskSubmitInProgress) {
                return;
            }
            this.state.selectedFile = null;
            input.value = '';
            fileInfo?.classList.add('hidden');
        });
    },
    
    handleFileSelect(file) {
        if (this.state.taskSubmitInProgress) {
            return;
        }
        const allowedExtensions = ['.glb', '.fbx', '.obj'];
        const ext = '.' + file.name.split('.').pop().toLowerCase();
        
        if (!allowedExtensions.includes(ext)) {
            alert('Please select a GLB, FBX, or OBJ file');
            return;
        }
        
        this.state.selectedFile = file;
        
        const fileInfo = document.getElementById('file-info');
        const fileName = document.getElementById('file-name');
        
        if (fileInfo && fileName) {
            fileName.textContent = file.name;
            fileInfo.classList.remove('hidden');
        }
        
        // Auto-submit immediately after file selection
        this.submitTask();
    },
    
    /**
     * Setup form submission
     */
    setupForm() {
        const form = document.getElementById('convert-form');
        if (form) {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                this.submitTask();
            });
        }
    },
    
    /**
     * Submit conversion task
     */
    async submitTask() {
        if (this.state.loginRequired) {
            window.location.href = '/auth/login';
            return;
        }

        if (this.state.taskSubmitInProgress) {
            return;
        }

        // Keep JS state in sync with the visible tab (fixes stale activeTab vs DOM).
        const activeTabBtn = document.querySelector('.tab.active');
        const domTab = activeTabBtn && activeTabBtn.getAttribute('data-tab');
        if (domTab === 'upload' || domTab === 'link') {
            this.state.activeTab = domTab;
        }
        
        const linkInput = document.getElementById('link-input');
        const startBtn = document.getElementById('start-btn');
        
        let formData = new FormData();
        const linkVal = (linkInput && typeof linkInput.value === 'string')
            ? linkInput.value.trim()
            : '';

        if (this.state.activeTab === 'upload' && this.state.selectedFile) {
            formData.append('source', 'upload');
            formData.append('file', this.state.selectedFile);
        } else if (this.state.activeTab === 'link' && linkVal) {
            formData.append('source', 'link');
            formData.append('input_url', linkVal);
        } else if (this.state.selectedFile) {
            // Tab state was stale vs DOM; user picked a file but activeTab still said "link".
            formData.append('source', 'upload');
            formData.append('file', this.state.selectedFile);
        } else {
            alert(t('error_no_file'));
            return;
        }
        
        formData.append('type', 't_pose');
        
        // Add GA client ID if available
        try {
            if (typeof gtag === 'function') {
                // Try to get client_id from gtag
                const gaMeasurementId = 'G-T4E781EHE4';
                // Since gtag('get', ...) is async, we might want to use a more reliable way or just the cookie
                const gaCookie = document.cookie.split('; ').find(row => row.startsWith('_ga='));
                if (gaCookie) {
                    const clientId = gaCookie.split('=')[1].split('.').slice(-2).join('.');
                    formData.append('ga_client_id', clientId);
                }
            }
        } catch (e) {
            console.warn('[GA4] Failed to get client_id:', e);
        }

        const busyMode = formData.get('source') === 'upload' ? 'upload' : 'link';
        const isUpload = busyMode === 'upload';
        this.setConvertFormBusy(true, busyMode);

        const overlay = document.getElementById('convert-form-busy');
        const rigLayout = document.getElementById('rig-detect-layout');
        const viewerHost = document.getElementById('rig-detect-viewer-host');
        const rigCloud = document.getElementById('rig-detect-cloud');
        const rigReview = document.getElementById('rig-detect-review');

        overlay?.classList.add('form-busy--rig-detect');
        document.body.classList.add('rig-detect-modal-open');
        const rigScrimEl = document.getElementById('rig-detect-scrim');
        rigScrimEl?.classList.remove('hidden');
        rigScrimEl?.setAttribute('aria-hidden', 'false');
        rigLayout?.classList.remove('hidden');
        rigCloud?.classList.add('hidden');
        rigReview?.classList.add('hidden');
        overlay?.classList.remove('rig-detect--review-phase');

        // Disable button (visible on link tab)
        if (startBtn) {
            startBtn.disabled = true;
            startBtn.textContent = typeof t === 'function' ? t('upload_progress_btn') : 'Please wait…';
        }

        let detectionDispose = () => {};
        try {
            this.setConvertFormBusyMessage('upload_rig_detect_analyzing');
            this.setConvertFormUploadProgress(null);
            const sourceInfo = isUpload
                ? {
                    file: this.state.selectedFile,
                    ext: '.' + String(this.state.selectedFile?.name || '').split('.').pop().toLowerCase()
                }
                : {
                    url: linkVal,
                    ext: '.' + String(linkVal || '').split('?')[0].split('#')[0].split('.').pop().toLowerCase()
                };
            const { detection, dispose: dDispose } = await this.runHiddenAnimalDetection(sourceInfo, { viewerHost: viewerHost || null });
            detectionDispose = dDispose;

            let selectedRigKey = this.rigDetectAutoKey(detection);
            if (detection) {
                selectedRigKey = await this.waitRigDetectReview(detection, selectedRigKey);
            }
            detectionDispose();
            detectionDispose = () => {};

            rigLayout?.classList.add('hidden');
            overlay?.classList.remove('form-busy--rig-detect', 'rig-detect--review-phase');
            document.body.classList.remove('rig-detect-modal-open');
            const rigScrimDone = document.getElementById('rig-detect-scrim');
            rigScrimDone?.classList.add('hidden');
            rigScrimDone?.setAttribute('aria-hidden', 'true');
            rigReview?.classList.add('hidden');
            rigCloud?.classList.add('hidden');

            if (detection) {
                this.applyRigSelectionToFormData(formData, detection, selectedRigKey);
            }

            this.setConvertFormBusyMessage(isUpload ? 'upload_progress_uploading' : 'upload_progress_creating_task');
            this.resetConvertFormProgressUI(busyMode);

            let ok;
            let status;
            let data;

            if (isUpload) {
                const result = await this.postTaskCreateMultipart(formData, (pct) =>
                    this.setConvertFormUploadProgress(pct)
                );
                ok = result.ok;
                status = result.status;
                data = result.data;
            } else {
                const response = await fetch('/api/task/create', {
                    method: 'POST',
                    body: formData,
                });
                status = response.status;
                data = await response.json();
                ok = response.ok;
            }

            if (ok) {
                window.location.href = `/task?id=${data.task_id}`;
            } else {
                if (status === 401) {
                    alert(t('error_login_required'));
                    window.location.href = '/auth/login';
                } else if (status === 402) {
                    window.location.href = '/buy';
                } else {
                    alert(data.detail || t('error_generic'));
                }
            }
        } catch (error) {
            console.error('Submit error:', error);
            alert(t('error_generic'));
        } finally {
            try {
                detectionDispose();
            } catch (e) {
                /* ignore */
            }
            this.setConvertFormBusy(false);
            overlay?.classList.remove('form-busy--rig-detect', 'rig-detect--review-phase');
            document.body.classList.remove('rig-detect-modal-open');
            const rigScrimFinally = document.getElementById('rig-detect-scrim');
            rigScrimFinally?.classList.add('hidden');
            rigScrimFinally?.setAttribute('aria-hidden', 'true');
            rigLayout?.classList.add('hidden');
            rigReview?.classList.add('hidden');
            rigCloud?.classList.add('hidden');
            if (startBtn) {
                startBtn.disabled = false;
                startBtn.textContent = t('btn_start');
            }
        }
    },
    
    /**
     * Load task history
     */


    /**
     * Load public gallery preview (recent completed tasks with videos)
     */
    async loadGalleryPreview() {
        const grid = document.getElementById('gallery-preview-grid');
        if (!grid) return;

        console.log('[Gallery] Loading preview...');

        try {
            // Homepage preview should show top liked by default
            const resp = await fetch('/api/gallery?per_page=12&sort=likes&t=' + Date.now());
            const data = await resp.json();
            const items = (data && data.items) ? data.items : [];
            
            console.log('[Gallery] Received items:', items.length);
            const total = (data && typeof data.total === 'number') ? data.total : null;

            const viewAllLink = document.getElementById('gallery-view-all-link');
            if (viewAllLink && total !== null) {
                // Localized: "View all (N)"
                if (typeof window.t === 'function') {
                    viewAllLink.textContent = t('gallery_view_all', { count: total });
                } else {
                    viewAllLink.textContent = `View all (${total})`;
                }
                viewAllLink.href = '/gallery';
            }

            if (!items.length) {
                grid.innerHTML = `<div class="card" style="padding: 1rem; color: var(--text-muted)">—</div>`;
                return;
            }

            // Use TaskCard component if available
            if (typeof TaskCard !== 'undefined') {
                grid.innerHTML = items.map(it => TaskCard.render(it, { currentSort: 'likes' })).join('');
                TaskCard.attachHandlers(grid, { currentSort: 'likes' });
            } else {
                // Fallback if TaskCard not loaded
                grid.innerHTML = items.map(it => {
                    const taskUrl = `/task?id=${it.task_id}`;
                    const thumbUrl = it.thumbnail_url || `/api/thumb/${it.task_id}`;
                    const rigKey = (typeof it.rig_icon_key === 'string' && it.rig_icon_key) ? it.rig_icon_key : 'humanoid';
                    const rigSrc = (typeof resolveRigIconUrl === 'function')
                        ? resolveRigIconUrl(rigKey)
                        : `/static/Icons_png/${rigKey === 'humanoid' ? 'Human' : (rigKey.charAt(0).toUpperCase() + rigKey.slice(1))}.png?v=rigicons1`;
                    const rigBadge = `<span class="gallery-rig-icon" title="Rig type"><img src="${rigSrc}" alt="" width="64" height="64" loading="lazy" decoding="async" aria-hidden="true"></span>`;
                    return `<a href="${taskUrl}" style="display:block; border-radius:12px; overflow:hidden;">
                        <div style="position:relative; width:100%; aspect-ratio: 9 / 16; background:#111;">
                            <img src="${thumbUrl}" style="width:100%; height:100%; object-fit: cover;" alt="" />
                            ${rigBadge}
                        </div>
                    </a>`;
                }).join('');
            }

        } catch (e) {
            console.error('Failed to load gallery:', e);
            grid.innerHTML = `<div class="card" style="padding: 1rem; color: var(--text-muted)">-</div>`;
        }
    },
    async loadHistory() {
        const container = document.getElementById('history-list');
        if (!container) return;

        try {
            const response = await fetch('/api/history?per_page=5');
            const data = await response.json();

            if (data.tasks.length === 0) {
                container.innerHTML = `<p class="text-center" style="color: var(--text-muted)">${t('history_empty')}</p>`;
                return;
            }

            container.innerHTML = data.tasks.map(task => {
                const hasThumbnail = task.status === 'done' && task.thumbnail_url;
                const thumbHtml = hasThumbnail 
                    ? `<div class="history-item-thumb"><img src="${task.thumbnail_url}" alt="" loading="lazy" onload="this.classList.add('loaded')"></div>` 
                    : '';
                
                return `
                <a href="/task?id=${task.task_id}" class="history-item ${hasThumbnail ? 'has-thumb' : ''}">
                    ${thumbHtml}
                    <div class="history-item-content">
                        <div class="history-item-info">
                            <span class="history-item-status ${task.status}"></span>
                            <span>${task.status === 'done' ? t('task_status_done') :
                                   task.status === 'processing' ? `${task.progress}%` :
                                   t('task_status_' + task.status)}</span>
                        </div>
                        <span class="history-item-date">${this.formatDate(task.created_at)}</span>
                    </div>
                </a>
            `}).join('');
        } catch (error) {
            console.error('Failed to load history:', error);
        }
    },
    
    /**
     * Format date for display
     */
    formatDate(dateStr) {
        const date = new Date(dateStr);
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    },
    
    /**
     * Load queue status from all workers
     */
    async loadQueueStatus() {
        const activeEl = document.getElementById('queue-active');
        const pendingEl = document.getElementById('queue-pending');
        const waitEl = document.getElementById('queue-wait');
        const serversEl = document.getElementById('queue-servers');
        
        if (!activeEl) return;
        
        try {
            const response = await fetch('/api/queue/status');
            const data = await response.json();
            
            const formatWait = (seconds) => {
                const s = Number(seconds || 0);
                if (s < 60) return t('queue_wait_lt1min');
                if (s < 3600) {
                    const minutes = Math.ceil(s / 60);
                    return t('queue_wait_minutes', { minutes: String(minutes) });
                }
                const hours = Math.floor(s / 3600);
                const minutes = Math.floor((s % 3600) / 60);
                return t('queue_wait_hours', { hours: String(hours), minutes: String(minutes) });
            };

            // Update values
            activeEl.textContent = data.total_active;
            pendingEl.textContent = data.total_pending;
            waitEl.textContent = formatWait(data.estimated_wait_seconds);
            serversEl.textContent = `${data.available_workers}/${data.total_workers}`;
            
            // Add warning class if queue is long
            if (data.total_pending > 5) {
                pendingEl.classList.add('warning');
            } else {
                pendingEl.classList.remove('warning');
            }
            
            // Add success class if no wait
            if (data.estimated_wait_seconds < 60) {
                waitEl.classList.add('success');
                waitEl.classList.remove('warning');
            } else if (data.estimated_wait_seconds > 1800) {
                waitEl.classList.add('warning');
                waitEl.classList.remove('success');
            } else {
                waitEl.classList.remove('success', 'warning');
            }
            
        } catch (error) {
            console.error('Failed to load queue status:', error);
            activeEl.textContent = '-';
            pendingEl.textContent = '-';
            waitEl.textContent = '-';
            serversEl.textContent = '-';
        }
    },

    // =========================================================================
    // Free3D Model Search
    // =========================================================================
    
    free3dState: {
        lastQuery: '',
        debounceTimer: null,
        isSearching: false,
        hasFocusedOnce: false,
        keywords: [], // Loaded from external file
        keywordsLoaded: false,
        ribbonEnabled: false,
        sectionHiddenByHealth: false,
    },

    hideFree3DRibbon(reason = 'unknown') {
        const section = document.querySelector('.free3d-search');
        if (section) {
            section.classList.add('hidden');
        }
        this.free3dState.ribbonEnabled = false;
        this.free3dState.sectionHiddenByHealth = true;
        console.warn(`[Free3D] Ribbon hidden: ${reason}`);
    },

    showFree3DRibbon() {
        const section = document.querySelector('.free3d-search');
        if (section) {
            section.classList.remove('hidden');
        }
        this.free3dState.ribbonEnabled = true;
        this.free3dState.sectionHiddenByHealth = false;
    },

    async checkFree3DRibbonHealth() {
        try {
            const resp = await fetch('/api/free3d/search?mode=browse&topK=1&type=1');
            if (!resp.ok) {
                this.hideFree3DRibbon(`http_${resp.status}`);
                return false;
            }
            const data = await resp.json();
            if (!data || data.ok !== true || data.degraded) {
                this.hideFree3DRibbon('degraded_upstream');
                return false;
            }
            this.showFree3DRibbon();
            return true;
        } catch (error) {
            this.hideFree3DRibbon(`probe_failed_${error?.message || 'unknown'}`);
            return false;
        }
    },

    /**
     * Load keywords from external JSON file
     */
    async loadFree3DKeywords() {
        if (this.free3dState.keywordsLoaded) return;
        try {
            const resp = await fetch('/static/data/search-keywords.json');
            const data = await resp.json();
            if (data.keywords && data.keywords.length > 0) {
                this.free3dState.keywords = data.keywords;
            }
            this.free3dState.keywordsLoaded = true;
        } catch (e) {
            console.warn('Failed to load search keywords:', e);
            // Fallback keywords
            this.free3dState.keywords = ['girl', 'robot', 'warrior', 'alien', 'monster'];
            this.free3dState.keywordsLoaded = true;
        }
    },

    /**
     * Get random character keyword
     */
    getRandomCharacterKeyword() {
        const keywords = this.free3dState.keywords;
        if (!keywords.length) return 'character';
        return keywords[Math.floor(Math.random() * keywords.length)];
    },

    /**
     * Trigger random search
     */
    triggerRandomSearch() {
        const input = document.getElementById('free3d-search-input');
        if (!input) return;
        
        const randomKeyword = this.getRandomCharacterKeyword();
        input.value = randomKeyword;
        this.free3dState.lastQuery = randomKeyword;
        this.searchFree3D(randomKeyword);
    },

    /**
     * Initialize Free3D search functionality
     */
    async initFree3DSearch() {
        const input = document.getElementById('free3d-search-input');
        const categorySelect = document.getElementById('free3d-category-select');
        const results = document.getElementById('free3d-results');
        const status = document.getElementById('free3d-search-status');
        const randomizeBtn = document.getElementById('free3d-randomize-btn');
        
        if (!input || !results) return;

        const isHealthy = await this.checkFree3DRibbonHealth();
        if (!isHealthy) return;

        // Load keywords from file
        await this.loadFree3DKeywords();

        // Randomize button click
        if (randomizeBtn) {
            randomizeBtn.addEventListener('click', () => {
                this.triggerRandomSearch();
            });
        }

        // Auto-search on first focus with random keyword
        input.addEventListener('focus', () => {
            if (!this.free3dState.hasFocusedOnce && !input.value.trim()) {
                this.free3dState.hasFocusedOnce = true;
                this.triggerRandomSearch();
            }
        });

        // Category change triggers new search
        if (categorySelect) {
            categorySelect.addEventListener('change', () => {
                const query = input.value.trim();
                if (query) {
                    this.free3dState.lastQuery = ''; // Force re-search
                    this.searchFree3D(query);
                }
            });
        }

        input.addEventListener('input', () => {
            const query = input.value.trim();
            
            // Clear previous timer
            if (this.free3dState.debounceTimer) {
                clearTimeout(this.free3dState.debounceTimer);
            }
            
            // Hide results if query is empty
            if (!query) {
                results.classList.add('hidden');
                status?.classList.add('hidden');
                this.free3dState.lastQuery = '';
                return;
            }
            
            // Debounce: wait 500ms before searching
            this.free3dState.debounceTimer = setTimeout(() => {
                if (query !== this.free3dState.lastQuery) {
                    this.free3dState.lastQuery = query;
                    this.searchFree3D(query);
                }
            }, 500);
        });
    },

    /**
     * Search Free3D API for models (via our proxy to bypass CORS)
     */
    async searchFree3D(query) {
        const results = document.getElementById('free3d-results');
        const status = document.getElementById('free3d-search-status');
        const categorySelect = document.getElementById('free3d-category-select');
        
        if (!results || !this.free3dState.ribbonEnabled) return;

        // Show searching status
        status?.classList.remove('hidden');
        this.free3dState.isSearching = true;

        // Build search query with category
        const category = categorySelect?.value || 'characters';
        let searchQuery = query;
        
        // Append category modifier to query for better results
        if (category !== 'all') {
            const categoryModifiers = {
                'characters': 'character humanoid',
                'animals': 'animal creature',
                'vehicles': 'vehicle car',
                'weapons': 'weapon sword',
                'props': 'prop object'
            };
            if (categoryModifiers[category]) {
                searchQuery = `${query} ${categoryModifiers[category]}`;
            }
        }

        try {
            // Use our backend proxy to avoid CORS issues
            const url = `/api/free3d/search?q=${encodeURIComponent(searchQuery)}&topK=20&mode=semantic&_=${Date.now()}`;
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`search_http_${response.status}`);
            }
            const data = await response.json();

            status?.classList.add('hidden');
            this.free3dState.isSearching = false;

            if (!data || data.ok !== true || data.degraded) {
                this.hideFree3DRibbon('search_degraded');
                return;
            }

            if (data.results && data.results.length > 0) {
                this.renderFree3DResults(data.results);
                results.classList.remove('hidden');
            } else {
                results.innerHTML = `<div class="free3d-no-results" data-i18n="free3d_no_results">${t('free3d_no_results')}</div>`;
                results.classList.remove('hidden');
            }
        } catch (error) {
            console.error('Free3D search failed:', error);
            status?.classList.add('hidden');
            this.free3dState.isSearching = false;
            this.hideFree3DRibbon('search_failed');
        }
    },

    /**
     * Render Free3D search results
     */
    renderFree3DResults(models) {
        const results = document.getElementById('free3d-results');
        if (!results) return;

        const normalized = (models || []).map((model) => {
            if (!model || typeof model !== 'object') return null;
            const title = model.title || model.name || model.model_name || model.display_name || '';
            const preview =
                model.previewSmallAbsUrl ||
                model.previewMediumAbsUrl ||
                model.preview_small_abs_url ||
                model.preview_medium_abs_url ||
                model.previewSmallUrl ||
                model.previewMediumUrl ||
                model.preview_url ||
                model.thumbnail_url ||
                '';
            const glbUrl =
                model.glb_url ||
                model.glb100k_url ||
                model.glb_base_url ||
                model.model_url ||
                model.url ||
                '';
            return {
                title: (title && String(title).trim()) || 'Untitled',
                preview: preview || '/static/images/placeholder-thumb.svg',
                glbUrl: (glbUrl && String(glbUrl).trim()) || '',
            };
        }).filter(Boolean);

        if (!normalized.length) {
            results.innerHTML = `<div class="free3d-no-results" data-i18n="free3d_no_results">${t('free3d_no_results')}</div>`;
            return;
        }

        results.innerHTML = normalized.map(model => {
            const previewUrl = model.preview;
            const glbUrl = model.glbUrl;
            const title = model.title;

            return `
                <div class="free3d-item" 
                     data-glb-url="${glbUrl}" 
                     data-title="${title.replace(/"/g, '&quot;')}"
                     title="${title}">
                    <div class="free3d-item-inner">
                        <img src="${previewUrl}" 
                             alt="${title}" 
                             loading="lazy"
                             onerror="this.src='/static/images/placeholder-thumb.svg'">
                    </div>
                    <div class="free3d-item-title">${title}</div>
                </div>
            `;
        }).join('');

        // Add click handlers
        results.querySelectorAll('.free3d-item').forEach(item => {
            item.addEventListener('click', () => {
                const glbUrl = item.dataset.glbUrl;
                const title = item.dataset.title;
                this.createTaskFromFree3D(glbUrl, title);
            });
        });
    },

    /**
     * Create a new AutoRig task from a Free3D model
     */
    async createTaskFromFree3D(glbUrl, title) {
        if (!glbUrl) {
            alert(t('error_generic'));
            return;
        }

        if (this.state.free3dCreateInFlight) {
            return;
        }

        if (this.state.loginRequired) {
            window.location.href = '/auth/login';
            return;
        }

        // Confirm action
        const confirmed = confirm(t('free3d_confirm_create').replace('{title}', title));
        if (!confirmed) return;

        const formData = new FormData();
        formData.append('source', 'link');
        formData.append('input_url', glbUrl);
        formData.append('type', 't_pose');

        this.state.free3dCreateInFlight = true;
        this.showFree3DCreateOverlay(title);
        let navigatingAway = false;

        try {
            const response = await fetch('/api/task/create', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (response.ok) {
                navigatingAway = true;
                window.location.href = `/task?id=${data.task_id}`;
            } else {
                if (response.status === 401) {
                    navigatingAway = true;
                    alert(t('error_login_required'));
                    window.location.href = '/auth/login';
                } else if (response.status === 402) {
                    navigatingAway = true;
                    window.location.href = '/buy';
                } else {
                    alert(data.detail || t('error_generic'));
                }
            }
        } catch (error) {
            console.error('Failed to create task:', error);
            alert(t('error_generic'));
        } finally {
            if (!navigatingAway) {
                this.hideFree3DCreateOverlay();
            }
            this.state.free3dCreateInFlight = false;
        }
    }
};

window.App = App;

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => App.init());

