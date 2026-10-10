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
        
        // Privacy (2026-10-11): the badge carries the public handle and name, never an e-mail address.
        const escapeHtml = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, (c) => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
        const authorDisplay = item.author_name || item.author_nickname || null;
        const authorHandle = /^[0-9a-f]{10}$/.test(String(item.author_handle || '')) ? item.author_handle : null;
        
        // Author badge (top-left) - use span, not <a> to avoid nested links (invalid HTML)
        const authorHtml = authorHandle
            ? `<span class="tc-author" data-author="${authorHandle}" data-sort="${currentSort}" title="${escapeHtml(authorDisplay)}">${escapeHtml(authorDisplay)}</span>`
            : '';
        
        // Sales badge (only if > 0)
        const salesHtml = salesCount > 0 
            ? `<span class="tc-badge" title="Sales"><span>💰</span><span>${salesCount}</span></span>` 
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
        
        return `<a href="${taskUrl}" class="tc-card" data-task-id="${item.task_id}"><div class="tc-media"><img class="tc-thumb" src="${thumbUrl}" alt="" onload="this.classList.add('loaded')" onerror="this.onerror=null;this.src='/static/images/poster-missing.svg';this.classList.add('loaded')"><video class="tc-video" src="${videoUrl}" muted loop playsinline preload="none"></video>${authorHtml}${versionHtml}${rigIconHtml}${badgesHtml}</div></a>`;
    },
    
    /**
     * Navigate to author's gallery
     */
    navigateToAuthor(authorHandle, sort) {
        // the author's page: all their public models (server-rendered, /author/<handle>)
        if (!/^[0-9a-f]{10}$/.test(String(authorHandle || ''))) return;
        const lang = (window.__AUTORIG_LANG__ && window.__AUTORIG_LANG__.lang) || 'en';
        const prefix = ['ru', 'zh', 'hi', 'fa'].includes(lang) ? `/${lang}` : '';
        window.location.href = `${prefix}/author/${authorHandle}`;
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
