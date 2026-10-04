/**
 * OneClick Online - Main Application
 */

const App = {
    state: {
        user: null,
        anon: null,
        creditsRemaining: 0,
        loginRequired: false,
        selectedFile: null,
        activeTab: 'upload'
    },
    
    /**
     * Initialize application
     */
    async init() {
        // Initialize i18n (global)
        await I18n.init();
        
        // Check auth status (global header)
        await this.checkAuth();
        
        // Setup UI (safe on pages that don't have the conversion form)
        this.setupThemeToggle();

        // Conversion form (home page only)
        const hasConvertForm = !!document.getElementById('convert-form');
        if (hasConvertForm) {
        this.setupTabs();
        this.setupUploadZone();
        this.setupForm();
        }
        
        // Optional widgets (only when the container exists)
        this.loadHistory();
        this.loadGalleryPreview();
        this.loadVisitorCount();
        
        // Free3D model search (home page)
        this.initFree3DSearch();

        // Queue status (only if present)
        const hasQueue = !!document.getElementById('queue-active');
        if (hasQueue) {
        this.loadQueueStatus();
        // Refresh queue status every 10 seconds
        setInterval(() => this.loadQueueStatus(), 10000);
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
        } else {
            // Anonymous
            if (loginBtn) loginBtn.classList.remove('hidden');
            if (userInfo) userInfo.classList.add('hidden');
            if (creditsLabel) creditsLabel.textContent = t('credits_free');
        }
        
        if (creditsEl) {
            creditsEl.textContent = this.state.creditsRemaining;
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
     * Setup theme toggle
     */
    setupThemeToggle() {
        const toggle = document.getElementById('theme-toggle');
        if (!toggle) return;
        
        // Load saved theme
        const savedTheme = localStorage.getItem('oneclick_theme') || 'dark';
        document.documentElement.setAttribute('data-theme', savedTheme);
        this.updateThemeIcon(savedTheme);
        
        toggle.addEventListener('click', () => {
            const current = document.documentElement.getAttribute('data-theme');
            const newTheme = current === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('oneclick_theme', newTheme);
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
                const target = tab.getAttribute('data-tab');
                console.log('[App] Tab clicked:', target);
                this.state.activeTab = target;
                
                tabs.forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                
                if (target === 'upload') {
                    uploadPanel?.classList.remove('hidden');
                    linkPanel?.classList.add('hidden');
                } else {
                    uploadPanel?.classList.add('hidden');
                    linkPanel?.classList.remove('hidden');
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
        
        console.log('[App] setupUploadZone, zone:', !!zone, 'input:', !!input);
        
        if (!zone || !input) return;
        
        // Click to upload (but do not hijack clicks on inner interactive elements)
        zone.addEventListener('click', (e) => {
            const target = e.target;
            if (target && target.closest('.upload-scene-guide-link')) {
                // Let guide link work normally without opening file picker.
                return;
            }
            if (target && target.closest('#remove-file')) {
                // Remove button has its own handler.
                return;
            }
            input.click();
        });
        
        // Drag events
        zone.addEventListener('dragover', (e) => {
            e.preventDefault();
            zone.classList.add('dragover');
        });
        
        zone.addEventListener('dragleave', () => {
            zone.classList.remove('dragover');
        });
        
        zone.addEventListener('drop', (e) => {
            e.preventDefault();
            zone.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                this.handleFileSelect(files[0]);
            }
        });
        
        // File input change
        input.addEventListener('change', () => {
            if (input.files.length > 0) {
                this.handleFileSelect(input.files[0]);
            }
        });
        
        // Remove file
        removeBtn?.addEventListener('click', (e) => {
            e.stopPropagation();
            this.state.selectedFile = null;
            input.value = '';
            fileInfo?.classList.add('hidden');
        });
    },
    
    handleFileSelect(file) {
        console.log('[App] handleFileSelect:', file.name, file.size);
        const allowedExtensions = ['.zip'];
        const ext = '.' + file.name.split('.').pop().toLowerCase();
        
        if (!allowedExtensions.includes(ext)) {
            alert('Please select a .Zip file (3ds MAX Archive)');
            return;
        }
        
        this.state.selectedFile = file;
        
        const fileInfo = document.getElementById('file-info');
        const fileName = document.getElementById('file-name');
        
        if (fileInfo && fileName) {
            fileName.textContent = file.name;
            fileInfo.classList.remove('hidden');
        }
        
        console.log('[App] Auto-submitting task...');
        // Auto-submit immediately after file selection
        this.submitTask();
    },
    
    /**
     * Setup form submission
     */
    setupForm() {
        const form = document.getElementById('convert-form');
        console.log('[App] setupForm, form exists:', !!form);
        if (form) {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                console.log('[App] Form submit event triggered');
                this.submitTask();
            });
        }
    },
    
    /**
     * Submit conversion task
     */
    async submitTask() {
        console.log('[App] submitTask called');
        if (this.state.loginRequired) {
            console.log('[App] Login required, redirecting');
            window.location.href = '/auth/login';
            return;
        }
        
        const linkInput = document.getElementById('link-input');
        const startBtn = document.getElementById('start-btn');
        
        console.log('[App] activeTab:', this.state.activeTab);
        console.log('[App] selectedFile:', this.state.selectedFile ? this.state.selectedFile.name : 'none');

        // Check if we are in upload tab and have a file
        if (this.state.activeTab === 'upload') {
            if (this.state.selectedFile) {
                console.log('[App] Starting chunked upload');
                await this.uploadFileChunked(this.state.selectedFile);
            } else {
                console.log('[App] No file selected for upload');
                alert(typeof t === 'function' ? t('error_no_file') : 'Please select a file');
            }
            return;
        } else if (this.state.activeTab === 'link' && linkInput?.value) {
            // Standard link submission
            let formData = new FormData();
            formData.append('source', 'link');
            formData.append('input_url', linkInput.value);
            formData.append('type', 't_pose');
            
            if (startBtn) {
                startBtn.disabled = true;
                startBtn.textContent = 'Processing...';
            }
            
            try {
                const response = await fetch('/api/task/create', {
                    method: 'POST',
                    body: formData
                });
                const data = await response.json();
                if (response.ok) {
                    window.location.href = `/task?id=${data.task_id}`;
                } else {
                    alert(data.detail || (typeof t === 'function' ? t('error_generic') : 'Something went wrong'));
                }
            } catch (error) {
                console.error('Submit error:', error);
                alert(typeof t === 'function' ? t('error_generic') : 'Something went wrong');
            } finally {
                if (startBtn) {
                    startBtn.disabled = false;
                    startBtn.textContent = typeof t === 'function' ? t('btn_start') : 'Start';
                }
            }
        } else {
            alert(typeof t === 'function' ? t('error_no_file') : 'Please select a file');
            return;
        }
    },

    /**
     * Chunked upload implementation with real-time byte tracking
     */
    async uploadFileChunked(file) {
        const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
        const downlinkMbps = Number(connection && connection.downlink) || 0;
        const downlinkBps = downlinkMbps > 0 ? ((downlinkMbps * 1024 * 1024) / 8) : 0;
        const fileSizeMb = file.size / (1024 * 1024);
        const hasDownlink = downlinkMbps > 0;

        const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
        const nowMs = () => Date.now();

        let CHUNK_SIZE = 10 * 1024 * 1024;
        let initialConcurrency = 2;
        let maxConcurrency = 6;
        if (fileSizeMb >= 8192) {
            CHUNK_SIZE = 64 * 1024 * 1024;
            initialConcurrency = 4;
            maxConcurrency = 7;
        } else if (fileSizeMb >= 4096) {
            CHUNK_SIZE = 48 * 1024 * 1024;
            initialConcurrency = 4;
            maxConcurrency = 6;
        } else if (fileSizeMb >= 2048) {
            CHUNK_SIZE = 32 * 1024 * 1024;
            initialConcurrency = 4;
            maxConcurrency = 6;
        } else if (fileSizeMb >= 700) {
            CHUNK_SIZE = 24 * 1024 * 1024;
            initialConcurrency = 3;
            maxConcurrency = 5;
        } else {
            if (downlinkMbps >= 40) {
                CHUNK_SIZE = 16 * 1024 * 1024;
                initialConcurrency = 5;
                maxConcurrency = 7;
            } else if (downlinkMbps >= 15) {
                CHUNK_SIZE = 12 * 1024 * 1024;
                initialConcurrency = 4;
                maxConcurrency = 6;
            } else if (downlinkMbps >= 6) {
                CHUNK_SIZE = 10 * 1024 * 1024;
                initialConcurrency = 3;
                maxConcurrency = 5;
            } else if (hasDownlink) {
                CHUNK_SIZE = 8 * 1024 * 1024;
                initialConcurrency = 2;
                maxConcurrency = 4;
            } else {
                CHUNK_SIZE = 12 * 1024 * 1024;
                initialConcurrency = 3;
                maxConcurrency = 5;
            }
        }

        if (file.size < 250 * 1024 * 1024) {
            CHUNK_SIZE = Math.min(CHUNK_SIZE, 6 * 1024 * 1024);
            initialConcurrency = Math.min(initialConcurrency, 3);
            maxConcurrency = Math.min(maxConcurrency, 4);
        }

        const serverProfileCacheKey = 'upload_server_profile_v1';
        try {
            const rawProfile = localStorage.getItem(serverProfileCacheKey);
            if (rawProfile) {
                const cachedProfile = JSON.parse(rawProfile);
                const profileAgeMs = nowMs() - Number(cachedProfile.timestamp || 0);
                const profileFresh = Number.isFinite(profileAgeMs) && profileAgeMs >= 0 && profileAgeMs <= 24 * 60 * 60 * 1000;
                if (profileFresh) {
                    const recommendedChunk = Number(cachedProfile.recommended_chunk_size || 0);
                    const recommendedParallel = Number(cachedProfile.max_parallel_chunks || 0);
                    if (!hasDownlink && recommendedChunk > 0) {
                        CHUNK_SIZE = clamp(recommendedChunk, 6 * 1024 * 1024, 64 * 1024 * 1024);
                    }
                    if (recommendedParallel > 0) {
                        maxConcurrency = clamp(Math.round(recommendedParallel), 2, 8);
                    }
                }
            }
        } catch (_) {}

        initialConcurrency = clamp(initialConcurrency, 1, maxConcurrency);
        const minConcurrency = (fileSizeMb >= 700) ? 2 : 1;

        const MAX_RETRIES = 6;
        const RETRY_BASE_DELAY_MS = 700;
        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
        const uploadProfileVersion = 'chunk-profile-v4';
        const uploadSessionKey = `upload_session_${uploadProfileVersion}_${file.name}_${file.size}_${file.lastModified}_${CHUNK_SIZE}_${totalChunks}`;
        const RETRYABLE_HTTP_STATUSES = new Set([429, 500, 502, 503, 504]);

        const adaptiveState = {
            currentConcurrency: initialConcurrency,
            minConcurrency,
            maxConcurrency,
            emaThroughputBps: downlinkBps,
            successStreak: 0,
            recentErrorScore: 0,
            growthFrozenUntil: 0,
            inRecovery: false,
            totalRetries: 0,
            totalTimeouts: 0
        };

        const formatBytes = (bytes) => {
            const value = Number(bytes) || 0;
            if (value >= 1024 * 1024 * 1024) return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
            if (value >= 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(2)} MB`;
            if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`;
            return `${value} B`;
        };

        const persistServerProfile = (profile) => {
            try {
                const recommendedChunk = Number(profile && profile.recommended_chunk_size);
                const recommendedParallel = Number(profile && profile.max_parallel_chunks);
                if (recommendedChunk > 0 || recommendedParallel > 0) {
                    localStorage.setItem(serverProfileCacheKey, JSON.stringify({
                        recommended_chunk_size: recommendedChunk,
                        max_parallel_chunks: recommendedParallel,
                        timestamp: nowMs()
                    }));
                }
            } catch (_) {}
        };

        const updateThroughputEstimate = (bytes, durationMs) => {
            if (!bytes || !durationMs || durationMs <= 0) return;
            const instantBps = bytes / (durationMs / 1000);
            if (!Number.isFinite(instantBps) || instantBps <= 0) return;
            if (!adaptiveState.emaThroughputBps || adaptiveState.emaThroughputBps <= 0) {
                adaptiveState.emaThroughputBps = instantBps;
                return;
            }
            const alpha = 0.24;
            adaptiveState.emaThroughputBps = (adaptiveState.emaThroughputBps * (1 - alpha)) + (instantBps * alpha);
        };

        const estimateChunkTimeoutMs = (chunkBytes) => {
            const minBps = 512 * 1024;
            const estimatedBps = Math.max(minBps, adaptiveState.emaThroughputBps || downlinkBps || 0);
            const estimatedTransferMs = (chunkBytes / estimatedBps) * 1000;
            const safetyMultiplier = adaptiveState.inRecovery ? 4.6 : 3.4;
            const baseExtraMs = adaptiveState.inRecovery ? 30000 : 18000;
            const rawTimeout = Math.round((estimatedTransferMs * safetyMultiplier) + baseExtraMs);
            return clamp(rawTimeout, 120000, 600000);
        };

        const classifyUploadError = (err) => {
            if (err && err.errorType) return err.errorType;
            return 'unknown';
        };

        const retryDelayMs = (attempt, errorType) => {
            const base = RETRY_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1));
            let multiplier = 1.0;
            let cap = 30000;
            if (errorType === 'http_429') {
                multiplier = 2.2;
                cap = 45000;
            } else if (errorType === 'timeout') {
                multiplier = 1.8;
                cap = 35000;
            } else if (errorType === 'network') {
                multiplier = 1.4;
                cap = 30000;
            } else if (errorType === 'http_5xx') {
                multiplier = 1.6;
                cap = 35000;
            }
            const jitter = Math.floor(Math.random() * 900);
            return Math.min(Math.round(base * multiplier) + jitter, cap);
        };

        const applyFailureBackpressure = (errorType) => {
            adaptiveState.successStreak = 0;
            adaptiveState.recentErrorScore = clamp(adaptiveState.recentErrorScore + 1, 0, 8);
            adaptiveState.inRecovery = true;
            if (adaptiveState.currentConcurrency > adaptiveState.minConcurrency) {
                adaptiveState.currentConcurrency = Math.max(
                    adaptiveState.minConcurrency,
                    Math.floor(adaptiveState.currentConcurrency * 0.7)
                );
            }

            const freezeBaseMs = errorType === 'http_429' ? 16000 : 9000;
            const freezePenaltyMs = adaptiveState.recentErrorScore * 1200;
            adaptiveState.growthFrozenUntil = nowMs() + freezeBaseMs + freezePenaltyMs;
        };

        const maybeScaleConcurrencyUp = () => {
            const now = nowMs();
            if (now < adaptiveState.growthFrozenUntil) return;
            if (adaptiveState.currentConcurrency >= adaptiveState.maxConcurrency) return;
            if (adaptiveState.successStreak < Math.max(4, adaptiveState.currentConcurrency)) return;

            adaptiveState.currentConcurrency += 1;
            adaptiveState.successStreak = 0;
            adaptiveState.recentErrorScore = Math.max(0, adaptiveState.recentErrorScore - 1);
            adaptiveState.inRecovery = adaptiveState.recentErrorScore > 0;
        };

        const onChunkSuccess = (bytes, durationMs) => {
            updateThroughputEstimate(bytes, durationMs);
            adaptiveState.successStreak += 1;
            if (adaptiveState.recentErrorScore > 0) {
                adaptiveState.recentErrorScore = Math.max(0, adaptiveState.recentErrorScore - 1);
            }
            if (adaptiveState.recentErrorScore === 0 && nowMs() >= adaptiveState.growthFrozenUntil) {
                adaptiveState.inRecovery = false;
            }
            maybeScaleConcurrencyUp();
        };

        console.log('[Upload] profile', {
            chunkSizeMb: (CHUNK_SIZE / (1024 * 1024)).toFixed(1),
            initialConcurrency,
            maxConcurrency: adaptiveState.maxConcurrency,
            downlinkMbps,
            totalChunks
        });
        
        const overlay = document.getElementById('upload-overlay');
        const filenameEl = document.getElementById('upload-filename');
        const percentEl = document.getElementById('upload-percent');
        const speedEl = document.getElementById('upload-speed');
        const barInner = document.getElementById('upload-progress-bar-inner');
        const etaEl = document.getElementById('upload-eta');
        const chunksEl = document.getElementById('upload-chunks');
        
        if (overlay) overlay.classList.remove('hidden');
        if (filenameEl) filenameEl.textContent = `${file.name} (${formatBytes(file.size)})`;
        
        const startTime = Date.now();
        let completedBytes = 0;
        let activeChunks = {};
        let uploadedChunkSet = new Set();
        const fetchJsonWithRetry = async (
            url,
            options,
            label,
            maxAttempts = MAX_RETRIES
        ) => {
            let lastError = null;
            for (let attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    const resp = await fetch(url, options);
                    if (resp.ok) return await resp.json();

                    const retryable = RETRYABLE_HTTP_STATUSES.has(resp.status);
                    if (!retryable || attempt >= maxAttempts) {
                        throw new Error(`${label} failed with status ${resp.status}`);
                    }
                    lastError = new Error(`${label} failed with status ${resp.status}`);
                } catch (err) {
                    lastError = err;
                    if (attempt >= maxAttempts) {
                        throw lastError;
                    }
                }

                if (etaEl) {
                    etaEl.textContent = `Retrying ${label.toLowerCase()} (${attempt}/${maxAttempts})...`;
                }
                await sleep(retryDelayMs(attempt, 'http_5xx'));
            }

            throw lastError || new Error(`${label} failed`);
        };
        const getChunkSize = (index) => {
            const start = index * CHUNK_SIZE;
            const end = Math.min(start + CHUNK_SIZE, file.size);
            return Math.max(0, end - start);
        };

        const updateUI = () => {
            const activeBytes = Object.values(activeChunks).reduce((a, b) => a + b, 0);
            const totalUploadedBytes = Math.min(completedBytes + activeBytes, file.size);
            
            const percent = Math.round((totalUploadedBytes / file.size) * 100);
            const elapsedMs = Date.now() - startTime;
            const speedBps = totalUploadedBytes / (elapsedMs / 1000 || 1);
            const speedMbps = (speedBps / (1024 * 1024)).toFixed(2);
            
            const remainingBytes = file.size - totalUploadedBytes;
            const etaSeconds = Math.round(remainingBytes / (speedBps || 1));
            
            let etaText = 'Calculating...';
            if (totalUploadedBytes > 0) {
                if (etaSeconds < 60) etaText = `${etaSeconds}s remaining`;
                else etaText = `${Math.floor(etaSeconds / 60)}m ${etaSeconds % 60}s remaining`;
            }

            if (barInner) barInner.style.width = `${percent}%`;
            if (percentEl) percentEl.textContent = `${percent}%`;
            if (speedEl) speedEl.textContent = `${speedMbps} MB/s`;
            if (etaEl) etaEl.textContent = etaText;
            
            const completedCount = uploadedChunkSet.size;
            if (chunksEl) {
                chunksEl.textContent = `${completedCount} / ${totalChunks} chunks • ${formatBytes(totalUploadedBytes)} / ${formatBytes(file.size)} • parallel ${adaptiveState.currentConcurrency}`;
            }
        };

        const uploadChunk = (index, uploadId, chunkRetryCount, chunkTimeoutCount) => {
            return new Promise((resolve, reject) => {
                const start = index * CHUNK_SIZE;
                const end = Math.min(start + CHUNK_SIZE, file.size);
                const chunk = file.slice(start, end);
                const startedAt = nowMs();

                const xhr = new XMLHttpRequest();
                xhr.open('POST', '/api/upload/chunk');
                xhr.timeout = estimateChunkTimeoutMs(chunk.size);
                
                const formData = new FormData();
                formData.append('upload_id', uploadId);
                formData.append('chunk_index', index);
                formData.append('file', chunk, `part_${index}`);
                formData.append('client_concurrency', String(adaptiveState.currentConcurrency));
                formData.append('chunk_retry_count', String(chunkRetryCount || 0));
                formData.append('chunk_timeout_count', String(chunkTimeoutCount || 0));
                if (adaptiveState.emaThroughputBps > 0) {
                    formData.append('chunk_throughput_mbps', (adaptiveState.emaThroughputBps / (1024 * 1024)).toFixed(3));
                }

                xhr.upload.onprogress = (e) => {
                    if (e.lengthComputable) {
                        activeChunks[index] = e.loaded;
                        updateUI();
                    }
                };

                xhr.onload = () => {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        const chunkSize = end - start;
                        completedBytes += chunkSize;
                        delete activeChunks[index];
                        updateUI();
                        resolve({
                            bytes: chunkSize,
                            durationMs: Math.max(1, nowMs() - startedAt)
                        });
                        return;
                    }

                    const status = Number(xhr.status) || 0;
                    const is429 = status === 429;
                    const is5xx = status >= 500;
                    reject({
                        retryable: is429 || is5xx,
                        errorType: is429 ? 'http_429' : (is5xx ? 'http_5xx' : 'http_4xx'),
                        message: `Chunk ${index + 1} failed with status ${status}`
                    });
                };

                xhr.onerror = () => reject({
                    retryable: true,
                    errorType: 'network',
                    message: `Chunk ${index + 1} network error`
                });
                xhr.ontimeout = () => reject({
                    retryable: true,
                    errorType: 'timeout',
                    message: `Chunk ${index + 1} request timeout`
                });
                xhr.send(formData);
            });
        };

        const uploadChunkWithRetry = async (index, uploadId) => {
            let chunkRetryCount = 0;
            let chunkTimeoutCount = 0;
            for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                try {
                    const result = await uploadChunk(index, uploadId, chunkRetryCount, chunkTimeoutCount);
                    onChunkSuccess(result.bytes, result.durationMs);
                    return;
                } catch (err) {
                    delete activeChunks[index];
                    updateUI();

                    const errorType = classifyUploadError(err);
                    const retryable = !!(err && err.retryable);
                    const errMessage = (err && err.message) ? err.message : `Chunk ${index + 1} failed`;

                    applyFailureBackpressure(errorType);
                    if (errorType === 'timeout') {
                        chunkTimeoutCount += 1;
                        adaptiveState.totalTimeouts += 1;
                    }

                    if (!retryable || attempt >= MAX_RETRIES) {
                        throw new Error(errMessage);
                    }

                    chunkRetryCount += 1;
                    adaptiveState.totalRetries += 1;
                    if (etaEl) {
                        etaEl.textContent = `Retrying chunk ${index + 1} (${attempt}/${MAX_RETRIES}) • parallel ${adaptiveState.currentConcurrency}`;
                    }
                    await sleep(retryDelayMs(attempt, errorType));
                }
            }
        };

        try {
            let uploadId = localStorage.getItem(uploadSessionKey);

            // 1. Try to resume existing upload session.
            if (uploadId) {
                try {
                    const statusData = await fetchJsonWithRetry(
                        `/api/upload/status?upload_id=${encodeURIComponent(uploadId)}`,
                        undefined,
                        'Upload status',
                        3
                    );
                    const sameFile = statusData.filename === file.name && Number(statusData.total_size) === Number(file.size);
                    const sameChunkProfile =
                        Number(statusData.chunk_size) === Number(CHUNK_SIZE) &&
                        Number(statusData.total_chunks) === Number(totalChunks);
                    if (sameFile && sameChunkProfile) {
                        const uploadedChunks = Array.isArray(statusData.uploaded_chunks) ? statusData.uploaded_chunks : [];
                        uploadedChunkSet = new Set(uploadedChunks.map((v) => Number(v)).filter(Number.isInteger));
                    } else {
                        uploadId = null;
                        localStorage.removeItem(uploadSessionKey);
                    }
                } catch (_) {
                    uploadId = null;
                    localStorage.removeItem(uploadSessionKey);
                }
            }

            // 2. Create new upload session if resume is unavailable.
            if (!uploadId) {
                const initData = new FormData();
                initData.append('filename', file.name);
                initData.append('total_size', String(file.size));
                initData.append('chunk_size', String(CHUNK_SIZE));
                initData.append('total_chunks', String(totalChunks));

                const initPayload = await fetchJsonWithRetry(
                    '/api/upload/init',
                    { method: 'POST', body: initData },
                    'Upload initialization'
                );
                uploadId = initPayload.upload_id;
                localStorage.setItem(uploadSessionKey, uploadId);
                persistServerProfile(initPayload);

                const serverMaxParallel = Number(initPayload && initPayload.max_parallel_chunks);
                if (serverMaxParallel > 0) {
                    adaptiveState.maxConcurrency = clamp(Math.round(serverMaxParallel), adaptiveState.minConcurrency, 8);
                    adaptiveState.currentConcurrency = clamp(
                        adaptiveState.currentConcurrency,
                        adaptiveState.minConcurrency,
                        adaptiveState.maxConcurrency
                    );
                }
            }

            // Restore completed bytes from resumed chunks.
            completedBytes = Array.from(uploadedChunkSet).reduce((sum, idx) => sum + getChunkSize(idx), 0);
            updateUI();

            // 3. Upload missing chunks with dynamic bounded concurrency
            const chunkIndices = Array.from({ length: totalChunks }, (_, i) => i)
                .filter((idx) => !uploadedChunkSet.has(idx));
            const pool = new Set();
            
            for (const index of chunkIndices) {
                while (pool.size >= adaptiveState.currentConcurrency) {
                    await Promise.race(pool);
                }
                const promise = uploadChunkWithRetry(index, uploadId)
                    .then(() => {
                        uploadedChunkSet.add(index);
                        updateUI();
                    })
                    .finally(() => pool.delete(promise));
                pool.add(promise);
            }
            await Promise.all(pool);

            // 4. Complete upload
            if (etaEl) etaEl.textContent = 'Finalizing...';
            const completeData = new FormData();
            completeData.append('upload_id', uploadId);
            completeData.append('type', 't_pose');
            
            const finalData = await fetchJsonWithRetry(
                '/api/upload/complete',
                { method: 'POST', body: completeData },
                'Upload finalization'
            );
            localStorage.removeItem(uploadSessionKey);
            window.location.href = `/task?id=${finalData.task_id}`;

        } catch (error) {
            console.error('Upload error:', error);
            alert(`Upload failed. The file was not fully uploaded. ${error.message}`);
            if (overlay) overlay.classList.add('hidden');
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

        try {
            // Homepage preview should show top liked by default
            const resp = await fetch('/api/gallery?per_page=10&sort=likes');
            const data = await resp.json();
            const items = (data && data.items) ? data.items : [];
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
                    return `<a href="${taskUrl}" style="display:block; border-radius:12px; overflow:hidden;">
                        <div style="position:relative; width:100%; aspect-ratio: 9 / 16; background:#111;">
                            <img src="${thumbUrl}" 
                                 style="width:100%; height:100%; object-fit: cover;" 
                                 alt="" 
                                 onerror="this.src='/static/images/icons/gallery.svg'; this.style.padding='2rem'; this.style.opacity='0.3';"/>
                        </div>
                    </a>`;
                }).join('');
            }

        } catch (e) {
            console.error('Failed to load gallery:', e);
            grid.innerHTML = `<div class="card" style="padding: 1rem; color: var(--text-muted)">-</div>`;
        }
    },

    async loadVisitorCount() {
        const el = document.getElementById('visitor-counter');
        if (!el) return;
        
        try {
            const response = await fetch('/api/system/activity');
            const data = await response.json();
            el.textContent = data.count || 0;
            el.title = 'Unique visitors in last 24h';
        } catch (error) {
            console.error('Failed to load visitor count:', error);
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
        keywordsLoaded: false
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
        
        if (!results) return;

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
            const url = `/api/free3d/search?q=${encodeURIComponent(searchQuery)}&topK=50`;
            const response = await fetch(url);
            const data = await response.json();

            status?.classList.add('hidden');
            this.free3dState.isSearching = false;

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
            results.innerHTML = `<div class="free3d-no-results">Search error. Please try again.</div>`;
            results.classList.remove('hidden');
        }
    },

    /**
     * Render Free3D search results
     */
    renderFree3DResults(models) {
        const results = document.getElementById('free3d-results');
        if (!results) return;

        const baseUrl = 'https://free3d.online';

        results.innerHTML = models.map(model => {
            // Use our proxy for images to bypass referrer restrictions
            const previewPath = model.previewSmallUrl; // e.g. /data/{guid}/{guid}_preview.jpg
            const previewUrl = `/api/free3d/image/${model.guid}/${model.guid}_preview.jpg`;
            const glbUrl = baseUrl + model.glbUrl;
            const title = model.title || 'Untitled';

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
     * Create a new OneClick task from a Free3D model
     */
    async createTaskFromFree3D(glbUrl, title) {
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

        try {
            const response = await fetch('/api/task/create', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (response.ok) {
                window.location.href = `/task?id=${data.task_id}`;
            } else {
                if (response.status === 401) {
                    alert(t('error_login_required'));
                    window.location.href = '/auth/login';
                } else if (response.status === 402) {
                    window.location.href = '/buy-credits';
                } else {
                    alert(data.detail || t('error_generic'));
                }
            }
        } catch (error) {
            console.error('Failed to create task:', error);
            alert(t('error_generic'));
        }
    }
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => App.init());

