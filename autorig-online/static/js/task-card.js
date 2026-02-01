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
        const currentSort = options.currentSort || 'likes';
        
        const taskUrl = `/task?id=${item.task_id}`;
        const thumbUrl = item.thumbnail_url || `/api/thumb/${item.task_id}`;
        const videoUrl = item.video_url || `/api/video/${item.task_id}`;
        const likeCount = (typeof item.like_count === 'number') ? item.like_count : 0;
        const salesCount = (typeof item.sales_count === 'number') ? item.sales_count : 0;
        const liked = !!item.liked_by_me;
        
        const authorDisplay = this.formatAuthorName(item.author_nickname, item.author_email);
        const authorEmail = item.author_email || null;
        
        // Author badge (top-left) - use span, not <a> to avoid nested links (invalid HTML)
        const authorHtml = authorEmail 
            ? `<span class="tc-author" data-author="${authorEmail}" data-sort="${currentSort}" title="${authorEmail}">${authorDisplay}</span>` 
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
        
        return `<a href="${taskUrl}" class="tc-card" data-task-id="${item.task_id}"><div class="tc-media"><img class="tc-thumb" src="${thumbUrl}" alt="" onload="this.classList.add('loaded')"><video class="tc-video" src="${videoUrl}" muted loop playsinline preload="none"></video>${authorHtml}${versionHtml}<div class="tc-badges"><button class="tc-like ${liked ? 'liked' : ''}" data-like-task="${item.task_id}" onclick="event.preventDefault();event.stopPropagation();TaskCard.toggleLike(this,'${item.task_id}')"><span>♥</span><span class="tc-like-count">${likeCount}</span></button>${salesHtml}</div></div></a>`;
    },
    
    /**
     * Toggle like on a task
     */
    async toggleLike(btn, taskId) {
        // Check auth
        const userInfo = document.getElementById('user-info');
        const isAuthed = userInfo && !userInfo.classList.contains('hidden');
        if (!isAuthed) {
            window.location.href = '/auth/login';
            return;
        }
        
        try {
            const r = await fetch(`/api/gallery/${taskId}/like`, { method: 'POST' });
            if (r.status === 401) {
                window.location.href = '/auth/login';
                return;
            }
            const d = await r.json();
            btn.classList.toggle('liked', !!d.liked_by_me);
            const cnt = btn.querySelector('.tc-like-count');
            if (cnt) cnt.textContent = String(d.like_count ?? 0);
        } catch (err) {
            console.error('Like failed:', err);
        }
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
                const sort = el.getAttribute('data-sort') || 'likes';
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
