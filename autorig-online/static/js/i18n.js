/**
 * AutoRig Online - Internationalization (i18n)
 *
 * Languages: en, ru, zh, hi, fa (Persian, right-to-left).
 *
 * Contract for every page, widget and agent-written UI:
 *   - Every user-visible string goes through I18n.t(key, {placeholder: value}).
 *     Raw server/internal text is never shown to a user.
 *   - After a switch, window receives 'languageChanged' {detail: {lang, dir}}.
 *   - Language order: the URL language (/fa/..., /ru/...) > the visitor's explicit
 *     choice (localStorage + cookie "autorig_lang") > the server's detection
 *     (Accept-Language, handed over in window.__AUTORIG_LANG__) > navigator.languages > en.
 *   - The explicit choice is also stored on the account / anonymous session through
 *     POST /api/me/language, so agents read the user's language from the API.
 */
(function () {
    'use strict';

    var LANGS = ['en', 'ru', 'zh', 'hi', 'fa'];
    var META = {
        en: { name: 'English', native: 'English', dir: 'ltr', locale: 'en-US' },
        ru: { name: 'Russian', native: 'Русский', dir: 'ltr', locale: 'ru-RU' },
        zh: { name: 'Chinese', native: '中文', dir: 'ltr', locale: 'zh-CN' },
        hi: { name: 'Hindi', native: 'हिंदी', dir: 'ltr', locale: 'hi-IN' },
        fa: { name: 'Persian', native: 'فارسی', dir: 'rtl', locale: 'fa-IR' }
    };
    // Languages whose pages are served under /<lang>/... by the backend.
    var PREFIX_LANGS = ['ru', 'zh', 'hi', 'fa'];
    // Guide articles that exist as separate per-language pages (/guide-ru ...).
    var GUIDE_VARIANT_LANGS = ['ru', 'zh', 'hi'];
    // Pages whose main content is translated through data-i18n: the whole page
    // follows the language direction. Elsewhere only the shared chrome does.
    // /task is chrome-only until the V3 task page declares data-i18n-scope="page".
    var FULL_PAGE_PATHS = ['/', '/gallery', '/buy-credits', '/how-it-works', '/dashboard',
        '/payment/success', '/developers', '/guides'];
    var STORAGE_KEY = 'autorig_lang';
    var COOKIE_KEY = 'autorig_lang';
    var RTL_CSS_HREF = '/static/css/rtl.css?v=72f3adeba2';
    var SAFE_TAG_RE = /<(\/?)(strong|b|em|i|br|code|small)\s*\/?>/gi;
    var LATIN_RE = /[A-Za-z]{2,}/;
    // The page's own language as served, read before this script changes <html lang>.
    var PAGE_LANG = (function () {
        try {
            var h = document.documentElement;
            return String(h.getAttribute('data-page-lang') || h.getAttribute('lang') || '').toLowerCase();
        } catch (e) {
            return '';
        }
    })();
    var RTL_CHAR_RE = /[֐-ࣿיִ-﷿ﹰ-﻿]/;

    function primary(tag) {
        var p = String(tag || '').trim().toLowerCase().replace('_', '-').split('-')[0];
        if (p === 'pes' || p === 'prs' || p === 'per') return 'fa';
        return p;
    }

    function readCookie(name) {
        try {
            var parts = String(document.cookie || '').split(';');
            for (var i = 0; i < parts.length; i++) {
                var kv = parts[i].trim().split('=');
                if (kv[0] === name) return decodeURIComponent(kv.slice(1).join('='));
            }
        } catch (e) {}
        return '';
    }

    function writeCookie(name, value) {
        try {
            var secure = location.protocol === 'https:' ? '; Secure' : '';
            document.cookie = name + '=' + encodeURIComponent(value) +
                '; Path=/; Max-Age=31536000; SameSite=Lax' + secure;
        } catch (e) {}
    }

    function storageGet(key) {
        try { return localStorage.getItem(key); } catch (e) { return null; }
    }

    function storageSet(key, value) {
        try { localStorage.setItem(key, value); } catch (e) {}
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    // Translations may carry a few inline tags (<strong> in How It Works).
    // Everything else is escaped, so a translation can never inject markup.
    function safeHtml(text) {
        return escapeHtml(text).replace(/&lt;(\/?)(strong|b|em|i|br|code|small)\s*\/?&gt;/gi,
            function (_m, slash, tag) { return '<' + slash + tag.toLowerCase() + '>'; });
    }

    function hasSafeTags(text) {
        SAFE_TAG_RE.lastIndex = 0;
        return SAFE_TAG_RE.test(String(text || ''));
    }

    function pathWithoutLangPrefix(pathname) {
        var m = String(pathname || '/').match(/^\/(ru|zh|hi|fa)(\/.*)?$/);
        if (!m) return { lang: '', path: pathname || '/' };
        return { lang: m[1], path: m[2] || '/' };
    }

    var I18n = {
        currentLang: 'en',
        source: 'default',
        translations: {},
        fallback: {},
        availableLanguages: LANGS.slice(),
        languageMeta: META,
        _selectorBound: false,
        _initPromise: null,

        isAvailable: function (lang) {
            return LANGS.indexOf(lang) !== -1;
        },

        dir: function (lang) {
            var m = META[lang || this.currentLang];
            return m ? m.dir : 'ltr';
        },

        isRtl: function (lang) {
            return this.dir(lang) === 'rtl';
        },

        localeTag: function (lang) {
            var m = META[lang || this.currentLang];
            return m ? m.locale : 'en-US';
        },

        nativeName: function (lang) {
            var m = META[lang || this.currentLang];
            return m ? m.native : String(lang || '');
        },

        formatNumber: function (value, lang) {
            var n = Number(value);
            if (!isFinite(n)) return String(value);
            try { return n.toLocaleString(this.localeTag(lang)); } catch (e) { return String(value); }
        },

        /** Pre-formatted numbers ("15 979", "1,234") in the script of the current language. */
        localizeDigits: function (value, lang) {
            var l = lang || this.currentLang;
            var s = String(value);
            if (l !== 'fa') return s;
            return s.replace(/(\d)[\s\u00a0\u202f\u2009,](?=\d{3}(?!\d))/g, '$1\u066c')
                .replace(/\d/g, function (d) { return String.fromCharCode(0x06f0 + Number(d)); });
        },

        /** The language the server rendered this page in (null on plain static pages). */
        serverBoot: function () {
            var b = window.__AUTORIG_LANG__;
            return b && typeof b === 'object' ? b : null;
        },

        savedChoice: function () {
            var s = primary(storageGet(STORAGE_KEY));
            if (this.isAvailable(s)) return s;
            var c = primary(readCookie(COOKIE_KEY));
            return this.isAvailable(c) ? c : '';
        },

        browserLanguages: function () {
            var list = [];
            try {
                (navigator.languages && navigator.languages.length ? navigator.languages : [navigator.language])
                    .forEach(function (x) { if (x) list.push(String(x)); });
            } catch (e) {}
            return list;
        },

        detectLanguage: function () {
            var boot = this.serverBoot();
            var urlLang = pathWithoutLangPrefix(location.pathname).lang;
            if (urlLang && this.isAvailable(urlLang)) return { lang: urlLang, source: 'url' };
            if (boot && boot.source === 'url' && this.isAvailable(boot.lang)) {
                return { lang: boot.lang, source: 'url' };
            }
            var saved = this.savedChoice();
            if (saved) return { lang: saved, source: 'explicit' };
            if (boot && this.isAvailable(boot.lang) && boot.source && boot.source !== 'default') {
                return { lang: boot.lang, source: boot.source };
            }
            var browser = this.browserLanguages();
            for (var i = 0; i < browser.length; i++) {
                var p = primary(browser[i]);
                if (this.isAvailable(p)) return { lang: p, source: 'browser' };
            }
            return { lang: 'en', source: 'default' };
        },

        /**
         * Initialize: detect, load translations, apply them and the text direction.
         * Safe to call more than once (pages and app.js both call it).
         */
        init: function () {
            var self = this;
            if (this._initPromise) {
                return this._initPromise.then(function () {
                    self.applyTranslations();
                    self.setupSelector();
                });
            }
            var found = this.detectLanguage();
            this.currentLang = found.lang;
            this.source = found.source;
            // A choice made before cookies existed lives only in localStorage:
            // copy it so the server renders the next page in that language.
            if (found.source === 'explicit' && primary(readCookie(COOKIE_KEY)) !== found.lang) {
                writeCookie(COOKIE_KEY, found.lang);
            }
            this.applyDirection();
            this._initPromise = this.loadTranslations(this.currentLang).then(function () {
                self.applyTranslations();
                self.setupSelector();
                self.reportLanguage(false);
            });
            return this._initPromise;
        },

        fetchJson: function (lang) {
            // /static/ is immutable-cached by nginx; revalidate so edits show at once.
            return fetch('/static/i18n/' + lang + '.json', { cache: 'no-cache' }).then(function (r) {
                if (!r.ok) throw new Error('HTTP ' + r.status);
                return r.json();
            });
        },

        loadTranslations: function (lang) {
            var self = this;
            var main = this.fetchJson(lang).then(function (data) {
                self.translations = data || {};
            }).catch(function (error) {
                console.error('Failed to load translations for ' + lang, error);
                self.translations = {};
            });
            var fb = (lang === 'en' || Object.keys(this.fallback).length)
                ? Promise.resolve()
                : this.fetchJson('en').then(function (data) { self.fallback = data || {}; }).catch(function () {});
            return Promise.all([main, fb]).then(function () {
                if (lang === 'en') self.fallback = self.translations;
            });
        },

        has: function (key) {
            return Object.prototype.hasOwnProperty.call(this.translations, key) ||
                Object.prototype.hasOwnProperty.call(this.fallback, key);
        },

        /** Translate a key. Missing keys fall back to English, then to the key itself. */
        t: function (key, replacements) {
            var text = this.translations[key];
            if (text === undefined || text === null) text = this.fallback[key];
            if (text === undefined || text === null) text = key;
            text = String(text);
            var self = this;
            if (replacements && typeof replacements === 'object') {
                Object.keys(replacements).forEach(function (k) {
                    var v = replacements[k];
                    if (typeof v === 'number' || (typeof v === 'string' && /^\d{1,15}$/.test(v))) {
                        v = self.formatNumber(v);
                    } else if (typeof v === 'string' && /\d/.test(v) && /^[\d\s.,\u00a0\u202f\u2009'+-]+$/.test(v)) {
                        v = self.localizeDigits(v);
                    }
                    text = text.split('{' + k + '}').join(String(v));
                });
            }
            return text;
        },

        /** Apply translations to all elements with data-i18n / -placeholder / -title / -aria-label. */
        applyTranslations: function (root) {
            var scope = root || document;
            var self = this;
            scope.querySelectorAll('[data-i18n]').forEach(function (el) {
                var key = el.getAttribute('data-i18n');
                if (!self.has(key)) return;           // keep the server text rather than show a key
                var value = self.t(key);
                if (hasSafeTags(value)) el.innerHTML = safeHtml(value);
                else el.textContent = value;
            });
            scope.querySelectorAll('[data-i18n-placeholder]').forEach(function (el) {
                var key = el.getAttribute('data-i18n-placeholder');
                if (self.has(key)) el.placeholder = self.t(key);
            });
            scope.querySelectorAll('[data-i18n-title]').forEach(function (el) {
                var key = el.getAttribute('data-i18n-title');
                if (self.has(key)) el.title = self.t(key);
            });
            scope.querySelectorAll('[data-i18n-aria-label]').forEach(function (el) {
                var key = el.getAttribute('data-i18n-aria-label');
                if (self.has(key)) el.setAttribute('aria-label', self.t(key));
            });
            this.updateGuideLinks();
            this.markLatinBlocks();
        },

        /** Whole page or only the shared chrome follows the language direction. */
        directionScope: function () {
            var html = document.documentElement;
            var declared = html.getAttribute('data-i18n-scope');
            if (declared === 'page' || declared === 'chrome') return declared;
            // A per-language article (lang="ru" guide) keeps its own language.
            if (!this.serverBoot() && PAGE_LANG && primary(PAGE_LANG) !== 'en') {
                return 'chrome';
            }
            var path = pathWithoutLangPrefix(location.pathname).path.replace(/\/+$/, '') || '/';
            return FULL_PAGE_PATHS.indexOf(path) !== -1 ? 'page' : 'chrome';
        },

        chromeElements: function () {
            return ['site-header', 'site-footer', 'ar-support-chat-root']
                .map(function (id) { return document.getElementById(id); })
                .filter(Boolean);
        },

        applyDirection: function () {
            var lang = this.currentLang;
            var dir = this.dir(lang);
            var html = document.documentElement;
            html.setAttribute('data-ui-lang', lang);
            if (this.directionScope() === 'page') {
                html.setAttribute('lang', lang);
                html.setAttribute('dir', dir);
                this.chromeElements().forEach(function (el) {
                    el.removeAttribute('dir');
                    el.removeAttribute('lang');
                });
            } else {
                if (html.getAttribute('dir') === 'rtl' && !html.hasAttribute('data-i18n-fixed')) {
                    html.setAttribute('dir', 'ltr');
                }
                this.chromeElements().forEach(function (el) {
                    el.setAttribute('dir', dir);
                    el.setAttribute('lang', lang);
                });
            }
            if (dir === 'rtl') this.ensureRtlAssets();
        },

        ensureRtlAssets: function () {
            if (document.getElementById('autorig-rtl-css') ||
                document.querySelector('link[href^="/static/css/rtl.css"]')) return;
            var link = document.createElement('link');
            link.id = 'autorig-rtl-css';
            link.rel = 'stylesheet';
            link.href = RTL_CSS_HREF;
            document.head.appendChild(link);
        },

        /**
         * In right-to-left pages, untranslated English blocks (model titles, code,
         * product names) keep their own left-to-right flow instead of having their
         * punctuation mirrored.
         */
        markLatinBlocks: function () {
            var rtl = this.isRtl() && this.directionScope() === 'page';
            document.querySelectorAll('[data-i18n-auto-ltr]').forEach(function (el) {
                if (!rtl) {
                    el.removeAttribute('dir');
                    el.removeAttribute('data-i18n-auto-ltr');
                }
            });
            if (!rtl) return;
            var sel = 'main p, main li, main h1, main h2, main h3, main h4, main h5, main h6, main td, main th,' +
                ' main figcaption, main label, main dt, main dd, main blockquote, main .btn, main a';
            document.querySelectorAll(sel).forEach(function (el) {
                if (el.hasAttribute('data-i18n') || el.querySelector('[data-i18n]') || el.closest('[data-i18n]')) return;
                if (el.hasAttribute('dir') && !el.hasAttribute('data-i18n-auto-ltr')) return;
                var text = el.textContent || '';
                if (LATIN_RE.test(text) && !RTL_CHAR_RE.test(text)) {
                    el.setAttribute('dir', 'ltr');
                    el.setAttribute('data-i18n-auto-ltr', '1');
                }
            });
        },

        /** Update guide links based on current language */
        updateGuideLinks: function () {
            var guides = [
                'mixamo-alternative',
                'rig-glb-unity',
                'rig-fbx-unreal',
                't-pose-vs-a-pose',
                'glb-vs-fbx',
                'auto-rig-obj',
                'animation-retargeting',
                'face-rig-animation',
                'image-to-rigged-3d-character'
            ];
            var lang = this.currentLang;
            guides.forEach(function (guide) {
                var selector = 'a[href="/' + guide + '"], a[data-guide-base="' + guide + '"]';
                document.querySelectorAll(selector).forEach(function (link) {
                    link.setAttribute('data-guide-base', guide);
                    link.href = GUIDE_VARIANT_LANGS.indexOf(lang) !== -1 ? '/' + guide + '-' + lang : '/' + guide;
                });
            });
        },

        /** The URL of this page in another language, or '' when the page has no prefix form. */
        urlForLanguage: function (lang) {
            var parsed = pathWithoutLangPrefix(location.pathname);
            if (!parsed.lang) return '';
            var base = parsed.path || '/';
            var prefix = (lang === 'en' || PREFIX_LANGS.indexOf(lang) === -1) ? '' : '/' + lang;
            var path = prefix + (base === '/' && prefix ? '/' : base);
            return path + location.search + location.hash;
        },

        /** Switch language (explicit choice: remembered and sent to the API). */
        switchLanguage: function (lang) {
            lang = primary(lang);
            if (!this.isAvailable(lang)) return Promise.resolve();
            var previousDir = this.dir(this.currentLang);
            this.currentLang = lang;
            this.source = 'explicit';
            storageSet(STORAGE_KEY, lang);
            writeCookie(COOKIE_KEY, lang);
            this.reportLanguage(true);
            var target = this.urlForLanguage(lang);
            if (target && target !== location.pathname + location.search + location.hash) {
                location.assign(target);
                return Promise.resolve();
            }
            var self = this;
            return this.loadTranslations(lang).then(function () {
                self.applyDirection();
                self.applyTranslations();
                self.updateSelector();
                if (previousDir !== self.dir(lang)) document.documentElement.classList.add('i18n-dir-switched');
                window.dispatchEvent(new CustomEvent('languageChanged', { detail: { lang: lang, dir: self.dir(lang) } }));
            });
        },

        /**
         * Tell the server the language: explicit choices are stored on the account or
         * anonymous session; otherwise only the browser's languages are reported once a day.
         */
        reportLanguage: function (explicit) {
            try {
                var day = new Date().toISOString().slice(0, 10);
                var stamp = this.currentLang + '|' + (explicit ? 'x' : 'd') + '|' + day;
                if (!explicit && storageGet('autorig_lang_reported') === stamp) return;
                var body = {
                    language: explicit ? this.currentLang : null,
                    explicit: !!explicit,
                    ui_language: this.currentLang,
                    browser_languages: this.browserLanguages().slice(0, 6)
                };
                fetch('/api/me/language', {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                    keepalive: true
                }).then(function (r) {
                    if (r.ok) storageSet('autorig_lang_reported', stamp);
                }).catch(function () {});
            } catch (e) {}
        },

        /** Setup language selector dropdown */
        setupSelector: function () {
            var btn = document.querySelector('.lang-btn');
            var dropdown = document.querySelector('.lang-dropdown');
            if (!btn || !dropdown) return;
            this.ensureSelectorOptions(dropdown);
            if (!this._selectorBound) {
                this._selectorBound = true;
                var self = this;
                btn.addEventListener('click', function (e) {
                    e.stopPropagation();
                    dropdown.classList.toggle('show');
                });
                document.addEventListener('click', function () {
                    dropdown.classList.remove('show');
                });
                dropdown.addEventListener('click', function (e) {
                    var option = e.target && e.target.closest ? e.target.closest('.lang-option') : null;
                    if (!option) return;
                    e.stopPropagation();
                    dropdown.classList.remove('show');
                    self.switchLanguage(option.getAttribute('data-lang'));
                });
            }
            this.updateSelector();
        },

        /** Older cached header markup may lack newer languages: add them. */
        ensureSelectorOptions: function (dropdown) {
            LANGS.forEach(function (lang) {
                var opt = dropdown.querySelector('.lang-option[data-lang="' + lang + '"]');
                if (!opt) {
                    opt = document.createElement('button');
                    opt.type = 'button';
                    opt.className = 'lang-option';
                    opt.setAttribute('data-lang', lang);
                    dropdown.appendChild(opt);
                }
                opt.textContent = META[lang].native;
                opt.setAttribute('lang', lang);
                opt.setAttribute('dir', META[lang].dir);
            });
        },

        /** Update selector UI */
        updateSelector: function () {
            var btn = document.querySelector('.lang-btn span');
            if (btn) btn.textContent = this.currentLang.toUpperCase();
            var lang = this.currentLang;
            var label = this.has('lang_label') ? this.t('lang_label') : 'Language';
            var button = document.querySelector('.lang-btn');
            if (button) button.setAttribute('aria-label', label + ': ' + this.nativeName(lang));
            document.querySelectorAll('.lang-option').forEach(function (option) {
                option.classList.toggle('active', option.getAttribute('data-lang') === lang);
            });
        }
    };

    // Export for use
    window.I18n = I18n;
    window.t = function (key, replacements) { return I18n.t(key, replacements); };
})();
