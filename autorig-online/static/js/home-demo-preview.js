(function () {
    function startHomeDemoPreview() {
        const video = document.getElementById('home-demo-video');
        const card = video ? video.closest('.demo-preview-card') : null;
        if (!video || !card) return;

        const src = video.getAttribute('data-src');
        if (!src || card.dataset.demoState === 'loading') return;
        card.dataset.demoState = 'loading';

        fetch(src, { credentials: 'same-origin', cache: 'force-cache' })
            .then((response) => {
                if (!response.ok) throw new Error('Demo video failed to load');
                return response.blob();
            })
            .then((blob) => {
                const objectUrl = URL.createObjectURL(blob);
                const cleanup = () => URL.revokeObjectURL(objectUrl);
                video.addEventListener('error', cleanup, { once: true });
                window.addEventListener('pagehide', cleanup, { once: true });
                video.src = objectUrl;
                video.load();
                return video.play();
            })
            .then(() => {
                card.classList.add('is-video-ready');
                card.dataset.demoState = 'ready';
            })
            .catch(() => {
                card.dataset.demoState = 'poster';
            });
    }

    // Gallery perf (2026-10-15): fetch the demo only when its card is on screen, and not on data saver.
    function whenVisible() {
        const video = document.getElementById('home-demo-video');
        const conn = navigator.connection || {};
        if (!video || conn.saveData || /(^|-)2g$/.test(String(conn.effectiveType || ''))) return;
        if (!('IntersectionObserver' in window)) { startHomeDemoPreview(); return; }
        const io = new IntersectionObserver((entries) => {
            if (entries.some((e) => e.isIntersecting)) { io.disconnect(); startHomeDemoPreview(); }
        }, { threshold: 0.25 });
        io.observe(video);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', whenVisible);
    } else {
        whenVisible();
    }
})();
