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
            if (this.state.loginRequired) {
                startBtn.textContent = t('btn_login_continue');
                startBtn.onclick = () => window.location.href = '/auth/login';
            } else if (this.state.creditsRemaining <= 0) {
                // Logged-in users with 0 credits should be redirected to Buy Credits page (no front-end credit logic)
                startBtn.textContent = t('nav_buy');
                startBtn.disabled = false;
                startBtn.onclick = () => window.location.href = '/buy-credits';
            } else {
                startBtn.textContent = t('btn_start');
                startBtn.disabled = false;
                startBtn.onclick = () => this.submitTask();
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
                const target = tab.getAttribute('data-tab');
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
        
        if (!zone || !input) return;
        
        // Click to upload
        zone.addEventListener('click', () => input.click());
        
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
        
        const linkInput = document.getElementById('link-input');
        const startBtn = document.getElementById('start-btn');
        
        let formData = new FormData();
        
        if (this.state.activeTab === 'upload' && this.state.selectedFile) {
            formData.append('source', 'upload');
            formData.append('file', this.state.selectedFile);
        } else if (this.state.activeTab === 'link' && linkInput?.value) {
            formData.append('source', 'link');
            formData.append('input_url', linkInput.value);
        } else {
            alert(t('error_no_file'));
            return;
        }
        
        formData.append('type', 't_pose');
        
        // Disable button
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

        try {
            // Homepage preview should show top liked by default
            const resp = await fetch('/api/gallery?per_page=8&sort=likes');
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

            grid.innerHTML = items.map(it => {
                const taskUrl = `/task?id=${it.task_id}`;
                const videoUrl = it.video_url;
                const thumbUrl = it.thumbnail_url || `/api/thumb/${it.task_id}`;
                const timeAgo = it.time_ago || '';
                const likeCount = (typeof it.like_count === 'number') ? it.like_count : 0;
                const liked = !!it.liked_by_me;
                return `
                    <a href="${taskUrl}" class="card" data-task-id="${it.task_id}" data-video-url="${videoUrl}" style="display:block; padding: 0.75rem; text-decoration:none;">
                        <div class="media" style="position:relative; width:100%; aspect-ratio: 9 / 16; overflow:hidden; border-radius: var(--radius-sm); background:#000;">
                            <img src="${thumbUrl}" loading="lazy" style="position:absolute; inset:0; width:100%; height:100%; object-fit: cover; z-index:2;" alt="" />
                            <button class="like-btn ${liked ? 'liked' : ''}" type="button" data-like-task="${it.task_id}" style="position:absolute; top:0.5rem; right:0.5rem; z-index:10;">
                                <span class="like-heart">♥</span>
                                <span class="like-count">${likeCount}</span>
                            </button>
                        </div>
                        <div style="margin-top: 0.5rem; display:flex; justify-content: space-between; gap: 0.75rem; align-items: center;">
                            <div style="color: var(--text-secondary); font-size: 0.875rem;">${timeAgo}</div>
                            <div class="btn btn-ghost" style="padding: 0.25rem 0.5rem; font-size: 0.75rem;">View</div>
                        </div>
                    </a>
                `;
            }).join('');

            // Like buttons handler
            grid.querySelectorAll('[data-like-task]').forEach(btn => {
                btn.addEventListener('click', async (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    const userInfo = document.getElementById('user-info');
                    const isAuthed = userInfo && !userInfo.classList.contains('hidden');
                    if (!isAuthed) {
                        window.location.href = '/auth/login';
                        return;
                    }
                    const taskId = btn.getAttribute('data-like-task');
                    try {
                        const r = await fetch(`/api/gallery/${taskId}/like`, { method: 'POST' });
                        if (r.status === 401) { window.location.href = '/auth/login'; return; }
                        const d = await r.json();
                        btn.classList.toggle('liked', !!d.liked_by_me);
                        const cnt = btn.querySelector('.like-count');
                        if (cnt) cnt.textContent = String(d.like_count ?? 0);
                    } catch (err) {
                        console.error('Like failed:', err);
                    }
                });
            });

            // Fade-in lazy thumbnails (styles.css keeps lazy images at opacity:0 until .loaded)
            const imgs = grid.querySelectorAll('img[loading="lazy"]');
            imgs.forEach(img => {
                const markLoaded = () => img.classList.add('loaded');
                img.addEventListener('load', markLoaded, { once: true });
                img.addEventListener('error', markLoaded, { once: true });
                if (img.complete) {
                    // cached image may already be complete
                    markLoaded();
                }
            });

            // Lazy video: create/load on hover AND autoplay when visible (IntersectionObserver)
            grid.querySelectorAll('a[data-task-id]').forEach(card => {
                const media = card.querySelector('.media');
                const thumb = media?.querySelector('img');
                const videoUrl = card.getAttribute('data-video-url');

                const startVideo = async () => {
                    if (!media || !videoUrl) return;
                    if (media.querySelector('video')) return;
                    const v = document.createElement('video');
                    v.src = videoUrl;
                    v.muted = true;
                    v.playsInline = true;
                    v.preload = 'metadata';
                    v.style.position = 'absolute';
                    v.style.inset = '0';
                    v.style.width = '100%';
                    v.style.height = '100%';
                    v.style.objectFit = 'cover';
                    v.style.zIndex = '1';
                    media.appendChild(v);
                    // Keep thumb visible until the video actually starts producing frames
                    if (thumb) {
                        thumb.style.transition = 'opacity 0.15s';
                        thumb.style.opacity = '1';
                    }
                    v.addEventListener('playing', () => { if (thumb) thumb.style.opacity = '0'; }, { once: true });
                    v.addEventListener('loadeddata', () => { if (thumb) thumb.style.opacity = '0'; }, { once: true });
                    try { await v.play(); } catch (_) { /* ignore */ }
                };

                const stopVideo = () => {
                    const v = media?.querySelector('video');
                    if (!v) return;
                    try { v.pause(); } catch (_) {}
                    try { v.removeAttribute('src'); v.load(); } catch (_) {}
                    v.remove();
                    if (thumb) {
                        thumb.style.opacity = '1';
                    }
                };

                card.addEventListener('mouseenter', startVideo);
                card.addEventListener('mouseleave', stopVideo);
            });

            // Autoplay previews when they enter viewport (muted inline videos).
            // Limits concurrent playback to keep CPU low.
            const MAX_AUTOPLAY = 2;
            const playing = new Set();

            const ensureAutoplayBudget = () => {
                while (playing.size > MAX_AUTOPLAY) {
                    const first = playing.values().next().value;
                    if (!first) break;
                    try { first.pause(); } catch (_) {}
                    try { first.removeAttribute('src'); first.load(); } catch (_) {}
                    try { first.remove(); } catch (_) {}
                    playing.delete(first);
                }
            };

            if ('IntersectionObserver' in window) {
                const observer = new IntersectionObserver((entries) => {
                    entries.forEach(entry => {
                        const card = entry.target;
                        const media = card.querySelector('.media');
                        const thumb = media?.querySelector('img');
                        if (!media) return;

                        const videoUrl = card.getAttribute('data-video-url');
                        const existing = media.querySelector('video');

                        if (entry.isIntersecting) {
                            // Start autoplay if not already playing
                            if (!existing && videoUrl) {
                                const v = document.createElement('video');
                                v.src = videoUrl;
                                v.muted = true;
                                v.playsInline = true;
                                v.loop = true;
                                v.preload = 'metadata';
                                v.style.position = 'absolute';
                                v.style.inset = '0';
                                v.style.width = '100%';
                                v.style.height = '100%';
                                v.style.objectFit = 'cover';
                                v.style.zIndex = '1';
                                media.appendChild(v);
                                if (thumb) {
                                    thumb.style.transition = 'opacity 0.15s';
                                    thumb.style.opacity = '1';
                                }
                                v.addEventListener('playing', () => { if (thumb) thumb.style.opacity = '0'; }, { once: true });
                                v.addEventListener('loadeddata', () => { if (thumb) thumb.style.opacity = '0'; }, { once: true });
                                playing.add(v);
                                ensureAutoplayBudget();
                                v.play().catch(() => { /* ignore autoplay block */ });
                            }
                        } else {
                            // Stop when leaving viewport
                            if (existing) {
                                try { existing.pause(); } catch (_) {}
                                try { existing.removeAttribute('src'); existing.load(); } catch (_) {}
                                existing.remove();
                                playing.delete(existing);
                                if (thumb) thumb.style.opacity = '1';
                            }
                        }
                    });
                }, { root: null, threshold: 0.55 });

                grid.querySelectorAll('a[data-task-id]').forEach(card => observer.observe(card));
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
            
            container.innerHTML = data.tasks.map(task => `
                <a href="/task?id=${task.task_id}" class="history-item">
                    <div class="history-item-info">
                        <span class="history-item-status ${task.status}"></span>
                        <span>${task.status === 'done' ? t('task_status_done') : 
                               task.status === 'processing' ? `${task.progress}%` : 
                               t('task_status_' + task.status)}</span>
                    </div>
                    <span class="history-item-date">${this.formatDate(task.created_at)}</span>
                </a>
            `).join('');
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
     * Create a new AutoRig task from a Free3D model
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

