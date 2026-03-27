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
        taskSubmitInProgress: false
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
    setConvertFormBusy(isBusy, mode) {
        const overlay = document.getElementById('convert-form-busy');
        const card = document.querySelector('.convert-form-card');
        const textEl = document.getElementById('convert-form-busy-text');
        if (!overlay) return;

        this.state.taskSubmitInProgress = !!isBusy;

        if (isBusy) {
            const key =
                mode === 'upload' ? 'upload_progress_uploading' : 'upload_progress_creating_task';
            if (textEl) {
                textEl.setAttribute('data-i18n', key);
                textEl.textContent = typeof t === 'function' ? t(key) : '';
            }
            overlay.classList.remove('hidden');
            overlay.setAttribute('aria-busy', 'true');
            card?.classList.add('form-is-busy');
        } else {
            overlay.classList.add('hidden');
            overlay.setAttribute('aria-busy', 'false');
            card?.classList.remove('form-is-busy');
        }
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
        this.setConvertFormBusy(true, busyMode);

        // Disable button (visible on link tab)
        if (startBtn) {
            startBtn.disabled = true;
            startBtn.textContent = typeof t === 'function' ? t('upload_progress_btn') : 'Please wait…';
        }

        try {
            const response = await fetch('/api/task/create', {
                method: 'POST',
                body: formData
            });
            
            const data = await response.json();
            
            if (response.ok) {
                // Redirect to task page
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
            console.error('Submit error:', error);
            alert(t('error_generic'));
        } finally {
            this.setConvertFormBusy(false);
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
                    return `<a href="${taskUrl}" style="display:block; border-radius:12px; overflow:hidden;">
                        <div style="position:relative; width:100%; aspect-ratio: 9 / 16; background:#111;">
                            <img src="${thumbUrl}" style="width:100%; height:100%; object-fit: cover;" alt="" />
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
                    window.location.href = '/buy-credits';
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

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => App.init());

