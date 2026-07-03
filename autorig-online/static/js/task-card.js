/**
 * TaskCard - Reusable task card component for AutoRig Online
 * Video autoplay on hover, thumbnail fallback
 */

const TaskCard = {
    /**
     * Format author display name (hide @ and domain from email)
     */
    formatAuthorName(nickname, email) {
        if (nickname) return nickname;
        if (!email) return null;
        const atIndex = email.indexOf('@');
        return atIndex > 0 ? email.substring(0, atIndex) : email;
    },

    /**
     * Render a task card HTML
     */
    render(item, options = {}) {
        const currentSort = options.currentSort || 'date';

        const taskUrl = `/task?id=${item.task_id}`;
        const mediaVersion = encodeURIComponent(
            String(item.guid || item.updated_at || item.video_url || item.version || 'ready')
        );
        const versionTaskMediaUrl = (url) => {
            const raw = String(url || '');
            if (!mediaVersion) return raw;
            const isTaskMedia = (
                raw.startsWith('/api/video/')
                || raw.startsWith('/api/thumb/')
                || raw.startsWith('/thumb/')
                || raw.includes('/api/video/')
                || raw.includes('/api/thumb/')
            );
            if (!isTaskMedia) return raw;
            return `${raw}${raw.includes('?') ? '&' : '?'}v=${mediaVersion}`;
        };
        const thumbUrl = versionTaskMediaUrl(item.thumbnail_url || `/api/thumb/${item.task_id}`);
        const videoUrl = versionTaskMediaUrl(item.video_url || `/api/video/${item.task_id}`);
        const salesCount = (typeof item.sales_count === 'number') ? item.sales_count : 0;

        const authorDisplay = this.formatAuthorName(item.author_nickname, item.author_email);
        const authorEmail = item.author_email || null;

        // Author badge (top-left) - use span, not <a> to avoid nested links (invalid HTML)
        const authorHtml = authorEmail
            ? `<span class="tc-author" data-author="${authorEmail}" data-sort="${currentSort}" title="${authorEmail}">${authorDisplay}</span>`
            : '';

        // Sales badge (only if > 0)
        const salesHtml = salesCount > 0
            ? `<span class="tc-badge" title="Sales"><span>$</span><span>${salesCount}</span></span>`
            : '';

        // Version badge (bottom-left, only if > 1)
        const version = item.version || 1;
        const versionHtml = version > 1
            ? `<span class="tc-version" title="Version">v${version}</span>`
            : '';

        const rigKey = (typeof item.rig_icon_key === 'string' && item.rig_icon_key)
            ? item.rig_icon_key
            : 'humanoid';
        const rigIconSrc = (typeof resolveRigIconUrl === 'function')
            ? resolveRigIconUrl(rigKey)
            : `/static/Icons_png/${rigKey === 'humanoid' ? 'Human' : (rigKey.charAt(0).toUpperCase() + rigKey.slice(1))}.png?v=rigicons1`;
        const rigIconHtml = `<span class="tc-rig-icon" title="Rig type"><img src="${rigIconSrc}" alt="" width="64" height="64" loading="lazy" decoding="async" aria-hidden="true"></span>`;
        const badgesHtml = salesHtml ? `<div class="tc-badges">${salesHtml}</div>` : '';

        return `<a href="${taskUrl}" class="tc-card" data-task-id="${item.task_id}"><div class="tc-media"><img class="tc-thumb" src="${thumbUrl}" alt="" onload="this.classList.add('loaded')"><video class="tc-video" src="${videoUrl}" muted loop playsinline preload="none"></video>${authorHtml}${versionHtml}${rigIconHtml}${badgesHtml}</div></a>`;
    },

    /**
     * Navigate to author's gallery
     */
    navigateToAuthor(authorEmail, sort) {
        if (typeof GalleryPage !== 'undefined' && GalleryPage.updateUrl) {
            GalleryPage.author = authorEmail;
            GalleryPage.page = 1;
            GalleryPage.updateUrl();
            GalleryPage.load();
        } else {
            window.location.href = `/gallery?author=${encodeURIComponent(authorEmail)}&sort=${sort}`;
        }
    },

    /**
     * Attach SPA navigation to author badges and video autoplay
     */
    attachHandlers(container, options = {}) {
        if (!container) return;

        // Author navigation
        container.querySelectorAll('.tc-author[data-author]').forEach(el => {
            el.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                const author = el.getAttribute('data-author');
                const sort = el.getAttribute('data-sort') || 'date';
                if (author) TaskCard.navigateToAuthor(author, sort);
            });
        });

        // Video autoplay on hover
        this.setupAutoplay(container);
    },

    /**
     * Setup video autoplay on hover for task cards
     */
    setupAutoplay(container) {
        if (!container) container = document;

        container.querySelectorAll('.tc-card').forEach(card => {
            const video = card.querySelector('.tc-video');
            const thumb = card.querySelector('.tc-thumb');
            if (!video || !thumb) return;

            card.addEventListener('mouseenter', () => {
                video.play().catch(() => {});
                video.style.opacity = '1';
                thumb.style.opacity = '0';
            });

            card.addEventListener('mouseleave', () => {
                video.pause();
                video.currentTime = 0;
                video.style.opacity = '0';
                thumb.style.opacity = '1';
            });
        });
    }
};
