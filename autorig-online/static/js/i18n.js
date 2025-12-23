/**
 * AutoRig Online - Internationalization (i18n)
 * Handles language switching and translations
 */

const I18n = {
    currentLang: 'en',
    translations: {},
    availableLanguages: ['en', 'ru', 'zh', 'hi'],
    
    /**
     * Initialize i18n system
     */
    async init() {
        // Get saved language or detect from browser
        const savedLang = localStorage.getItem('autorig_lang');
        if (savedLang && this.availableLanguages.includes(savedLang)) {
            this.currentLang = savedLang;
        } else {
            // Detect from browser
            const browserLang = navigator.language.split('-')[0];
            if (this.availableLanguages.includes(browserLang)) {
                this.currentLang = browserLang;
            }
        }

        // Load translations
        await this.loadTranslations(this.currentLang);

        // Apply translations to page
        this.applyTranslations();

        // Setup language selector if exists
        this.setupSelector();
    },
    
    /**
     * Load translations for a language
     */
    async loadTranslations(lang) {
        try {
            // NOTE: /static/ is served with long-lived immutable caching in nginx.
            // For translations we want updates to propagate immediately, so bypass cache.
            const response = await fetch(`/static/i18n/${lang}.json`, { cache: 'no-store' });
            if (response.ok) {
                this.translations = await response.json();
            } else {
                console.error(`Failed to load translations for ${lang}`);
                // Fallback to English
                if (lang !== 'en') {
                    await this.loadTranslations('en');
                }
            }
        } catch (error) {
            console.error('Error loading translations:', error);
        }
    },
    
    /**
     * Get translation for a key
     */
    t(key, replacements = {}) {
        let text = this.translations[key] || key;
        
        // Apply replacements
        Object.entries(replacements).forEach(([k, v]) => {
            text = text.replace(`{${k}}`, v);
        });
        
        return text;
    },
    
    /**
     * Apply translations to all elements with data-i18n attribute
     */
    applyTranslations() {
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            el.textContent = this.t(key);
        });
        
        // Also update placeholders
        document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
            const key = el.getAttribute('data-i18n-placeholder');
            el.placeholder = this.t(key);
        });
        
        // Update title attributes
        document.querySelectorAll('[data-i18n-title]').forEach(el => {
            const key = el.getAttribute('data-i18n-title');
            el.title = this.t(key);
        });

        // Update guide links based on current language
        this.updateGuideLinks();
    },

    /**
     * Update guide links based on current language
     */
    updateGuideLinks() {
        const guides = [
            'mixamo-alternative',
            'rig-glb-unity',
            'rig-fbx-unreal',
            't-pose-vs-a-pose',
            'glb-vs-fbx',
            'auto-rig-obj',
            'animation-retargeting'
        ];

        guides.forEach(guide => {
            const links = document.querySelectorAll(`a[href="/${guide}"]`);
            links.forEach(link => {
                if (this.currentLang !== 'en') {
                    link.href = `/${guide}-${this.currentLang}`;
                } else {
                    link.href = `/${guide}`;
                }
            });
        });
    },
    
    /**
     * Switch language
     */
    async switchLanguage(lang) {
        if (!this.availableLanguages.includes(lang)) return;
        
        this.currentLang = lang;
        localStorage.setItem('autorig_lang', lang);
        
        await this.loadTranslations(lang);
        this.applyTranslations();
        
        // Update selector UI
        this.updateSelector();
        
        // Dispatch event for dynamic content
        window.dispatchEvent(new CustomEvent('languageChanged', { detail: { lang } }));
    },
    
    /**
     * Setup language selector dropdown
     */
    setupSelector() {
        const btn = document.querySelector('.lang-btn');
        const dropdown = document.querySelector('.lang-dropdown');
        
        if (!btn || !dropdown) return;
        
        // Toggle dropdown
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            dropdown.classList.toggle('show');
        });
        
        // Close on outside click
        document.addEventListener('click', () => {
            dropdown.classList.remove('show');
        });
        
        // Language options
        dropdown.querySelectorAll('.lang-option').forEach(option => {
            option.addEventListener('click', () => {
                const lang = option.getAttribute('data-lang');
                this.switchLanguage(lang);
                dropdown.classList.remove('show');
            });
        });
        
        this.updateSelector();
    },
    
    /**
     * Update selector UI
     */
    updateSelector() {
        const btn = document.querySelector('.lang-btn span');
        if (btn) {
            btn.textContent = this.currentLang.toUpperCase();
        }
        
        document.querySelectorAll('.lang-option').forEach(option => {
            const lang = option.getAttribute('data-lang');
            option.classList.toggle('active', lang === this.currentLang);
        });
    }
};

// Export for use
window.I18n = I18n;
window.t = (key, replacements) => I18n.t(key, replacements);

