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

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', startHomeDemoPreview);
    } else {
        startHomeDemoPreview();
    }
})();
