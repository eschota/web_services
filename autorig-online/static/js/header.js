/**
 * Reusable Site Header Component
 * Generates consistent header and Free3D search across all pages
 */

const GOOGLE_ICON_SVG = `<svg viewBox="0 0 24 24" width="18" height="18">
    <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
    <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
    <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
    <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
</svg>`;

const DROPDOWN_ARROW_SVG = `<svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
    <path d="M3 4.5L6 7.5L9 4.5"/>
</svg>`;

/**
 * Render the site header
 * @param {Object} options - Configuration options
 * @param {boolean} options.showSearch - Whether to show Free3D search (default: true)
 * @param {boolean} options.showNav - Whether to show navigation links (default: true)
 * @param {boolean} options.showCredits - Whether to show credits badge (default: true)
 * @returns {string} HTML string
 */
function renderSiteHeader(options = {}) {
    const { 
        showSearch = true, 
        showNav = true, 
        showCredits = true 
    } = options;
    
    const navHtml = showNav ? `
        <nav class="nav">
            <a href="/guides" class="nav-link" data-i18n="nav_guides">Guides</a>
            <a href="/gallery" class="nav-link" data-i18n="nav_gallery">Gallery</a>
            <a href="/buy-credits" class="nav-link" data-i18n="nav_buy">Buy</a>
            <a href="/developers" class="nav-link" data-i18n="nav_api">API</a>
        </nav>
    ` : '';
    
    const creditsHtml = showCredits ? `
        <div class="credits-badge">
            <span id="credits-label" data-i18n="credits_free">Free conversions left</span>
            <span class="count" id="credits-count">0</span>
        </div>
    ` : '';
    
    return `
    <header class="header">
        <div class="container">
            <div class="header-inner">
                <a href="/" class="logo">
                    <img src="/static/images/logo/autorig-logo.png" 
                         srcset="/static/images/logo/autorig-logo.png 1x, /static/images/logo/autorig-logo@2x.png 2x"
                         alt="Autorig.Online" 
                         class="logo-img" 
                         height="120">
                </a>

                ${navHtml}
                
                <div class="header-actions">
                    ${creditsHtml}
                    
                    <!-- Language Selector -->
                    <div class="lang-selector">
                        <button class="lang-btn">
                            <span>EN</span>
                            ${DROPDOWN_ARROW_SVG}
                        </button>
                        <div class="lang-dropdown">
                            <button class="lang-option" data-lang="en">English</button>
                            <button class="lang-option" data-lang="ru">Русский</button>
                            <button class="lang-option" data-lang="zh">中文</button>
                            <button class="lang-option" data-lang="hi">हिंदी</button>
                        </div>
                    </div>
                    
                    <!-- Theme Toggle -->
                    <button class="theme-toggle" id="theme-toggle" title="Toggle theme">🌙</button>
                    
                    <!-- Login Button -->
                    <a href="/auth/login" class="btn btn-google" id="login-btn">
                        ${GOOGLE_ICON_SVG}
                        <span data-i18n="btn_login">Sign in with Google</span>
                    </a>
                    
                    <!-- User Info (hidden when not logged in) -->
                    <div id="user-info" class="hidden flex items-center gap-2">
                        <img class="user-avatar" src="" alt="" style="width:32px;height:32px;border-radius:50%;">
                        <span class="user-name"></span>
                        <a href="/auth/logout" class="btn btn-ghost" data-i18n="btn_logout">Sign out</a>
                    </div>
                </div>
            </div>
        </div>
    </header>
    ${showSearch ? renderFree3DSearch() : ''}
    `;
}

/**
 * Render the Free3D search section
 * @returns {string} HTML string
 */
function renderFree3DSearch() {
    return `
    <section class="free3d-search">
        <div class="container">
            <div class="free3d-search-row">
                <label for="free3d-search-input" class="free3d-search-label" data-i18n="free3d_search_label">Search Free 3D Models</label>
                <select id="free3d-category-select" class="free3d-category-select">
                    <option value="characters" selected data-i18n="free3d_cat_characters">Characters</option>
                    <option value="animals" data-i18n="free3d_cat_animals">Animals</option>
                    <option value="vehicles" data-i18n="free3d_cat_vehicles">Vehicles</option>
                    <option value="weapons" data-i18n="free3d_cat_weapons">Weapons</option>
                    <option value="props" data-i18n="free3d_cat_props">Props</option>
                    <option value="all" data-i18n="free3d_cat_all">All</option>
                </select>
                <div class="free3d-input-group">
                    <input type="text" 
                           id="free3d-search-input" 
                           class="free3d-search-input" 
                           data-i18n-placeholder="free3d_search_placeholder"
                           placeholder="girl, robot, warrior..."
                           autocomplete="off">
                    <button type="button" class="free3d-randomize-btn" id="free3d-randomize-btn" title="Random search">🎲</button>
                </div>
                <span class="free3d-search-status hidden" id="free3d-search-status" data-i18n="free3d_searching">Searching...</span>
            </div>
            <div class="free3d-results hidden" id="free3d-results">
                <!-- Horizontal scrollable preview strip populated by JS -->
            </div>
        </div>
    </section>
    `;
}

/**
 * Initialize header functionality (call after rendering)
 * - Theme toggle
 * - Language selector
 * - User auth state
 * - Credits display
 */
async function initSiteHeader() {
    // Theme toggle
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        const savedTheme = localStorage.getItem('theme') || 'dark';
        document.body.classList.toggle('light', savedTheme === 'light');
        themeToggle.textContent = savedTheme === 'light' ? '☀️' : '🌙';
        
        themeToggle.addEventListener('click', () => {
            const isLight = document.body.classList.toggle('light');
            localStorage.setItem('theme', isLight ? 'light' : 'dark');
            themeToggle.textContent = isLight ? '☀️' : '🌙';
        });
    }
    
    // Language selector
    const langBtn = document.querySelector('.lang-btn');
    const langDropdown = document.querySelector('.lang-dropdown');
    const langOptions = document.querySelectorAll('.lang-option');
    
    if (langBtn && langDropdown) {
        // Redundant setup removed - now handled by I18n.setupSelector()
        // which is called in task.html after injection.
    }
    
    // Update login button to include return URL
    const loginBtn = document.getElementById('login-btn');
    if (loginBtn) {
        const currentUrl = window.location.pathname + window.location.search;
        loginBtn.href = '/auth/login?next=' + encodeURIComponent(currentUrl);
    }
    
    // Fetch and display user/credits info
    try {
        const resp = await fetch('/auth/me');
        if (resp.ok) {
            const data = await resp.json();
            const loginBtn = document.getElementById('login-btn');
            const userInfo = document.getElementById('user-info');
            const creditsCount = document.getElementById('credits-count');
            const creditsLabel = document.getElementById('credits-label');
            
            if (data.user) {
                // User is logged in
                if (loginBtn) loginBtn.classList.add('hidden');
                if (userInfo) {
                    userInfo.classList.remove('hidden');
                    const avatar = userInfo.querySelector('.user-avatar');
                    const name = userInfo.querySelector('.user-name');
                    if (avatar && data.user.picture) avatar.src = data.user.picture;
                    if (name) name.textContent = data.user.name || data.user.email;
                }
                if (creditsCount) creditsCount.textContent = data.user.balance_credits || 0;
                if (creditsLabel) {
                    creditsLabel.textContent = 'Credits';
                    creditsLabel.setAttribute('data-i18n', 'credits_balance');
                }
            } else if (data.anon && data.anon.free_remaining !== undefined) {
                // Anonymous user
                if (creditsCount) creditsCount.textContent = data.anon.free_remaining;
                if (creditsLabel) {
                    creditsLabel.textContent = 'Free conversions left';
                    creditsLabel.setAttribute('data-i18n', 'credits_free');
                }
            } else if (typeof data.credits_remaining === 'number') {
                // Fallback for anonymous response
                if (creditsCount) creditsCount.textContent = data.credits_remaining;
            }
        }
    } catch (e) {
        console.warn('[Header] Failed to fetch user info:', e);
    }
}

/**
 * Initialize Free3D search functionality (call after rendering)
 * @param {Function} onModelSelect - Callback when a model is selected
 */
function initFree3DSearch(onModelSelect) {
    const searchInput = document.getElementById('free3d-search-input');
    const categorySelect = document.getElementById('free3d-category-select');
    const resultsContainer = document.getElementById('free3d-results');
    const statusSpan = document.getElementById('free3d-search-status');
    const randomizeBtn = document.getElementById('free3d-randomize-btn');
    
    if (!searchInput || !resultsContainer) return;
    
    let searchTimeout = null;
    
    async function performSearch(query, category = 'characters') {
        if (!query || query.length < 2) {
            resultsContainer.classList.add('hidden');
            return;
        }
        
        if (statusSpan) {
            statusSpan.classList.remove('hidden');
        }
        
        try {
            const resp = await fetch(`/api/free3d/search?q=${encodeURIComponent(query)}&category=${category}`);
            if (!resp.ok) throw new Error('Search failed');
            
            const data = await resp.json();
            renderSearchResults(data.results || []);
        } catch (e) {
            console.error('[Free3D] Search error:', e);
            resultsContainer.innerHTML = '<div style="padding: 1rem; color: var(--text-muted);">Search failed</div>';
            resultsContainer.classList.remove('hidden');
        } finally {
            if (statusSpan) statusSpan.classList.add('hidden');
        }
    }
    
    function renderSearchResults(results) {
        if (!results.length) {
            resultsContainer.innerHTML = '<div style="padding: 1rem; color: var(--text-muted);">No models found</div>';
            resultsContainer.classList.remove('hidden');
            return;
        }
        
        resultsContainer.innerHTML = results.map(model => `
            <div class="free3d-result-item" data-url="${model.glb_url || model.url}" data-name="${model.name || ''}">
                <img src="${model.thumb || '/static/images/placeholder-thumb.svg'}" alt="${model.name || 'Model'}" loading="lazy">
                <div class="free3d-result-name">${model.name || 'Untitled'}</div>
            </div>
        `).join('');
        
        resultsContainer.classList.remove('hidden');
        
        // Add click handlers
        resultsContainer.querySelectorAll('.free3d-result-item').forEach(item => {
            item.addEventListener('click', () => {
                const url = item.dataset.url;
                const name = item.dataset.name;
                if (onModelSelect && url) {
                    onModelSelect(url, name);
                } else if (url) {
                    // Default: navigate to task page with URL
                    window.location.href = `/?url=${encodeURIComponent(url)}`;
                }
            });
        });
    }
    
    // Debounced search on input
    searchInput.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            const query = searchInput.value.trim();
            const category = categorySelect ? categorySelect.value : 'characters';
            performSearch(query, category);
        }, 300);
    });
    
    // Category change triggers search
    if (categorySelect) {
        categorySelect.addEventListener('change', () => {
            const query = searchInput.value.trim();
            if (query) performSearch(query, categorySelect.value);
        });
    }
    
    // Randomize button
    if (randomizeBtn) {
        const randomQueries = ['robot', 'warrior', 'girl', 'zombie', 'soldier', 'knight', 'ninja', 'monster', 'alien', 'dragon'];
        randomizeBtn.addEventListener('click', () => {
            const randomQuery = randomQueries[Math.floor(Math.random() * randomQueries.length)];
            searchInput.value = randomQuery;
            const category = categorySelect ? categorySelect.value : 'characters';
            performSearch(randomQuery, category);
        });
    }
    
    // Enter key triggers search
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            clearTimeout(searchTimeout);
            const query = searchInput.value.trim();
            const category = categorySelect ? categorySelect.value : 'characters';
            performSearch(query, category);
        }
    });
}

// Export for global use
window.SiteHeader = {
    render: renderSiteHeader,
    renderSearch: renderFree3DSearch,
    init: initSiteHeader,
    initSearch: initFree3DSearch
};
