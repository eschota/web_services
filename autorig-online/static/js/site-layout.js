/**
 * Site-wide layout bootstrap: inject #site-header and #site-footer from header.js / footer.js.
 * Configure via <body data-layout-free3d-ribbon="1" data-layout-free3d-init="none|task" data-layout-active-path="...">
 * - data-layout-free3d-ribbon: include Free3D ribbon HTML (same as SiteHeader.showSearch)
 * - data-layout-free3d-init: "task" runs SiteHeader.initSearch (task page); "none" leaves ribbon to app.js on home
 * - data-layout-active-path: optional; omit to use location.pathname; "none" disables active nav highlight
 */
(function () {
    function readOptions() {
        const body = document.body;
        if (!body) {
            return { showSearch: false, initMode: 'none', activePath: typeof location !== 'undefined' ? location.pathname : '' };
        }
        const ribbon = body.getAttribute('data-layout-free3d-ribbon');
        const showSearch = ribbon === '1' || ribbon === 'true';
        const initMode = body.getAttribute('data-layout-free3d-init') || 'none';
        let activePath = body.getAttribute('data-layout-active-path');
        if (activePath === null) {
            activePath = typeof location !== 'undefined' ? location.pathname : '';
        }
        return { showSearch, initMode, activePath };
    }

    function isWebAppMode() {
        try {
            return new URLSearchParams(window.location.search).get('mode') === 'webapp';
        } catch (e) {
            return false;
        }
    }

    function bootstrapTaskSearch() {
        if (!window.SiteHeader || typeof SiteHeader.initSearch !== 'function') return;
        SiteHeader.initSearch({
            defaultCategory: 'characters',
            autoRandomOnInit: false,
            type: 1,
            onModelSelect: (modelUrl, modelName, modelPageUrl) => {
                const destination = modelPageUrl || 'https://free3d.online/';
                window.location.href = destination;
            }
        });
    }

    function bootstrap(extra) {
        const merged = { ...readOptions(), ...(extra || {}) };
        const showSearch = merged.showSearch;
        const initMode = merged.initMode;
        let activePath = merged.activePath;
        if (activePath === null) {
            activePath = typeof location !== 'undefined' ? location.pathname : '';
        }

        const headerEl = document.getElementById('site-header');
        const footerEl = document.getElementById('site-footer');
        const webapp = isWebAppMode();

        if (footerEl && window.SiteFooter && typeof SiteFooter.render === 'function') {
            footerEl.innerHTML = SiteFooter.render();
        }

        if (webapp && headerEl) {
            headerEl.innerHTML = '';
        } else if (headerEl && window.SiteHeader && typeof SiteHeader.render === 'function') {
            headerEl.innerHTML = SiteHeader.render({
                showSearch: !!showSearch,
                activePath: activePath === 'none' ? 'none' : activePath
            });
            if (typeof SiteHeader.init === 'function') {
                SiteHeader.init();
            }
            if (initMode === 'task') {
                bootstrapTaskSearch();
            }
        }

        (function loadSupportChat() {
            try {
                const b = document.body;
                if (!b || b.getAttribute('data-support-chat-off') === '1') return;
            } catch (e) {}
            if (window.__siteLayoutSupportChat === false) return;

            if (window.SupportChat && typeof window.SupportChat.init === 'function') {
                try {
                    window.SupportChat.init();
                } catch (e2) {}
                return;
            }
            if (document.querySelector('script[data-support-chat-js="1"]')) return;

            const s = document.createElement('script');
            s.src = '/static/js/support-chat.js?v=20260430-sup5';
            s.async = true;
            s.setAttribute('data-support-chat-js', '1');
            s.onload = function () {
                try {
                    if (window.SupportChat && typeof window.SupportChat.init === 'function') {
                        window.SupportChat.init();
                    }
                } catch (e3) {}
            };
            document.head.appendChild(s);
        })();
    }

    window.SiteLayout = {
        bootstrap
    };

    if (window.__siteLayoutAutoBootstrap === false) {
        return;
    }

    function runAuto() {
        if (!document.getElementById('site-header') && !document.getElementById('site-footer')) return;
        bootstrap();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', runAuto);
    } else {
        runAuto();
    }
})();

