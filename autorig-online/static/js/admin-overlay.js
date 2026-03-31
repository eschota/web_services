/**
 * Admin queue monitor overlay (opened from header «АДМИНКА»). Requires admin session cookie.
 */
(function () {
    'use strict';

    function api(path, options) {
        return fetch(path, Object.assign({ credentials: 'same-origin' }, options || {}));
    }

    function esc(s) {
        if (s == null || s === '') return '';
        const d = document.createElement('div');
        d.textContent = String(s);
        return d.innerHTML;
    }

    function escAttr(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;');
    }

    let selectedTaskId = null;
    let lastCards = [];

    function ensureRoot() {
        var root = document.getElementById('admin-overlay-root');
        if (root) return root;
        root = document.createElement('div');
        root.id = 'admin-overlay-root';
        root.setAttribute('aria-hidden', 'true');
        root.innerHTML =
            '<div class="admin-overlay-backdrop" data-close="1"></div>' +
            '<div class="admin-overlay-panel" role="dialog" aria-modal="true" aria-label="Admin queue">' +
            '<div class="admin-overlay-head">' +
            '<h2>Очередь задач</h2>' +
            '<div class="admin-overlay-actions">' +
            '<button type="button" id="admin-ov-refresh" title="Обновить список">Обновить</button>' +
            '<button type="button" id="admin-ov-sel10" title="Отметить первые 10 карточек">Выбрать 10</button>' +
            '<button type="button" id="admin-ov-bulk-rc" title="Сбросить restart_count у отмеченных">Сброс retry (выбранные)</button>' +
            '<button type="button" id="admin-ov-bulk-rq" class="danger" title="Вернуть в очередь (created)">Requeue (выбранные)</button>' +
            '<button type="button" id="admin-ov-recent24" title="Все задачи за 24ч">Retry=0 за 24ч</button>' +
            '</div>' +
            '<button type="button" class="admin-overlay-close" id="admin-ov-close" aria-label="Закрыть">×</button>' +
            '</div>' +
            '<div class="admin-overlay-body">' +
            '<div class="admin-queue-scroll">' +
            '<div class="admin-queue-toolbar"><span id="admin-ov-count" style="font-size:0.8rem;color:var(--text-muted,#9aa3b2)"></span></div>' +
            '<div id="admin-card-grid" class="admin-card-grid"></div>' +
            '</div>' +
            '<aside class="admin-detail" id="admin-detail-panel">' +
            '<h3>Selected</h3>' +
            '<div id="admin-detail-inner" class="admin-detail-empty">Выберите карточку</div>' +
            '</aside>' +
            '</div>' +
            '</div>';
        document.body.appendChild(root);
        wireRoot(root);
        return root;
    }

    function wireRoot(root) {
        root.querySelector('[data-close="1"]').addEventListener('click', close);
        root.querySelector('#admin-ov-close').addEventListener('click', close);
        root.querySelector('#admin-ov-refresh').addEventListener('click', loadQueue);
        root.querySelector('#admin-ov-sel10').addEventListener('click', selectFirst10);
        root.querySelector('#admin-ov-bulk-rc').addEventListener('click', bulkRestartCount);
        root.querySelector('#admin-ov-bulk-rq').addEventListener('click', bulkRequeue);
        root.querySelector('#admin-ov-recent24').addEventListener('click', bulkRecent24);
        document.addEventListener('keydown', onKeyDown);
    }

    function onKeyDown(e) {
        if (e.key === 'Escape') {
            var root = document.getElementById('admin-overlay-root');
            if (root && root.classList.contains('is-open')) close();
        }
    }

    function open() {
        var root = ensureRoot();
        root.classList.add('is-open');
        root.setAttribute('aria-hidden', 'false');
        document.body.classList.add('admin-overlay-open');
        loadQueue();
    }

    function close() {
        var root = document.getElementById('admin-overlay-root');
        if (!root) return;
        root.classList.remove('is-open');
        root.setAttribute('aria-hidden', 'true');
        document.body.classList.remove('admin-overlay-open');
    }

    function statusClass(st) {
        var s = (st || '').toLowerCase();
        if (s === 'created' || s === 'processing' || s === 'error' || s === 'done') return s;
        return 'created';
    }

    async function loadQueue() {
        var grid = document.getElementById('admin-card-grid');
        var countEl = document.getElementById('admin-ov-count');
        if (!grid) return;
        grid.innerHTML = '<div style="padding:12px;color:var(--text-muted)">Загрузка…</div>';
        try {
            var urls = [
                '/api/admin/tasks?status=processing&per_page=50&sort_desc=true&sort_by=created_at',
                '/api/admin/tasks?status=created&per_page=50&sort_desc=true&sort_by=created_at',
            ];
            var responses = await Promise.all(urls.map(function (u) {
                return api(u);
            }));
            var r1 = responses[0];
            var r2 = responses[1];
            if (!r1.ok || !r2.ok) {
                grid.innerHTML =
                    '<div style="color:#f88">Нет доступа (admin) или ошибка API: ' +
                    r1.status +
                    ' / ' +
                    r2.status +
                    '</div>';
                return;
            }
            var jp = await r1.json();
            var jc = await r2.json();
            var map = new Map();
            (jp.tasks || []).forEach(function (t) {
                map.set(t.task_id, t);
            });
            (jc.tasks || []).forEach(function (t) {
                map.set(t.task_id, t);
            });
            lastCards = Array.from(map.values()).sort(function (a, b) {
                return new Date(b.created_at) - new Date(a.created_at);
            });
            if (countEl) countEl.textContent = 'В очереди / работе: ' + lastCards.length + ' карточек';
            renderCards(lastCards);
        } catch (e) {
            grid.innerHTML = '<div style="color:#f88">' + esc(String(e)) + '</div>';
        }
    }

    function renderCards(tasks) {
        var grid = document.getElementById('admin-card-grid');
        if (!grid) return;
        if (!tasks.length) {
            grid.innerHTML = '<div style="color:var(--text-muted)">Нет задач в created/processing</div>';
            return;
        }
        var html = tasks
            .map(function (t) {
                var id = t.task_id;
                var pct = Math.max(0, Math.min(100, t.progress | 0));
                var st = statusClass(t.status);
                var taskUrl = '/task?id=' + encodeURIComponent(id);
                var wtip = t.worker_api ? escAttr(t.worker_api) : '';
                var itip = t.input_url ? escAttr(t.input_url) : '';
                var etip = t.error_message ? escAttr(t.error_message) : '';
                return (
                    '<div class="admin-card" data-task-id="' +
                    escAttr(id) +
                    '" tabindex="0">' +
                    '<input type="checkbox" class="admin-card-cb" data-task-id="' +
                    escAttr(id) +
                    '" title="Выбрать" />' +
                    '<div class="admin-card-status ' +
                    st +
                    '">' +
                    esc(t.status) +
                    '</div>' +
                    '<div class="admin-card-progress-wrap"><div class="admin-card-progress-bar" style="width:' +
                    pct +
                    '%"></div></div>' +
                    '<div style="font-size:0.7rem;color:#9aa3b2">' +
                    pct +
                    '% · ' +
                    (t.ready_count | 0) +
                    '/' +
                    (t.total_count | 0) +
                    ' · r' +
                    (t.restart_count | 0) +
                    ' · ' +
                    esc(t.pipeline_kind || '—') +
                    '</div>' +
                    '<div class="admin-card-id">' +
                    esc(id.slice(0, 8)) +
                    '…</div>' +
                    '<a class="admin-card-link" href="' +
                    taskUrl +
                    '" target="_blank" rel="noopener" onclick="event.stopPropagation()">Открыть страницу</a>' +
                    '<div class="admin-card-icons">' +
                    (t.worker_api
                        ? '<a href="' +
                          escAttr(t.worker_api) +
                          '" target="_blank" rel="noopener" title="' +
                          wtip +
                          '" onclick="event.stopPropagation()">🔧</a>'
                        : '<span title="Нет worker_api">🔧</span>') +
                    (t.input_url
                        ? '<a href="' +
                          escAttr(t.input_url) +
                          '" target="_blank" rel="noopener" title="' +
                          itip +
                          '" onclick="event.stopPropagation()">📎</a>'
                        : '<span title="Нет input">📎</span>') +
                    (t.error_message
                        ? '<span title="' + etip + '">⚠️</span>'
                        : '') +
                    '</div>' +
                    '</div>'
                );
            })
            .join('');
        grid.innerHTML = html;
        grid.querySelectorAll('.admin-card').forEach(function (card) {
            card.addEventListener('click', function (e) {
                if (e.target.classList && e.target.classList.contains('admin-card-cb')) return;
                e.preventDefault();
                selectCard(card.dataset.taskId);
            });
        });
        grid.querySelectorAll('.admin-card-cb').forEach(function (cb) {
            cb.addEventListener('click', function (e) {
                e.stopPropagation();
            });
        });
        document.querySelectorAll('.admin-card').forEach(function (c) {
            c.classList.toggle('is-selected', c.dataset.taskId === selectedTaskId);
        });
    }

    function selectCard(taskId) {
        selectedTaskId = taskId;
        document.querySelectorAll('.admin-card').forEach(function (c) {
            c.classList.toggle('is-selected', c.dataset.taskId === taskId);
        });
        loadDetail(taskId);
    }

    async function loadDetail(taskId) {
        var inner = document.getElementById('admin-detail-inner');
        if (!inner) return;
        inner.innerHTML = 'Загрузка…';
        try {
            var r = await api('/api/admin/task/' + encodeURIComponent(taskId) + '/inspect');
            if (!r.ok) {
                inner.innerHTML = '<div class="admin-detail-empty">Ошибка ' + r.status + '</div>';
                return;
            }
            var d = await r.json();
            inner.innerHTML =
                '<div class="admin-detail-row"><label>ID</label><div>' +
                esc(d.task_id) +
                '</div></div>' +
                '<div class="admin-detail-row"><label>Status</label><div>' +
                esc(d.status) +
                ' · ' +
                esc(d.progress) +
                '%</div></div>' +
                '<div class="admin-detail-row"><label>Pipeline</label><div>' +
                esc(d.pipeline_kind) +
                ' · ' +
                esc(d.input_type || '') +
                '</div></div>' +
                '<div class="admin-detail-row"><label>Owner</label><div>' +
                esc(d.owner_type) +
                ' ' +
                esc(d.owner_id) +
                '</div></div>' +
                '<div class="admin-detail-row"><label>restart_count</label><div>' +
                esc(d.restart_count) +
                '</div></div>' +
                '<div class="admin-detail-row"><label>guid</label><div>' +
                esc(d.guid || '—') +
                '</div></div>' +
                '<div class="admin-detail-row"><label>worker_task_id</label><div>' +
                esc(d.worker_task_id || '—') +
                '</div></div>' +
                '<div class="admin-detail-row"><label>worker_api</label><div>' +
                (d.worker_api
                    ? '<a href="' +
                      escAttr(d.worker_api) +
                      '" target="_blank" rel="noopener">' +
                      esc(d.worker_api) +
                      '</a>'
                    : '—') +
                '</div></div>' +
                '<div class="admin-detail-row"><label>progress_page</label><div>' +
                (d.progress_page
                    ? '<a href="' +
                      escAttr(d.progress_page) +
                      '" target="_blank" rel="noopener">' +
                      esc(d.progress_page.slice(0, 64)) +
                      '…</a>'
                    : '—') +
                '</div></div>' +
                '<div class="admin-detail-row"><label>input_url</label><div>' +
                (d.input_url
                    ? '<a href="' +
                      escAttr(d.input_url) +
                      '" target="_blank" rel="noopener">' +
                      esc(d.input_url.slice(0, 80)) +
                      '…</a>'
                    : '—') +
                '</div></div>' +
                '<div class="admin-detail-row"><label>last_progress_at</label><div>' +
                esc(d.last_progress_at || '—') +
                '</div></div>' +
                '<div class="admin-detail-row"><label>error</label><div>' +
                esc(d.error_message || '—') +
                '</div></div>' +
                '<div class="admin-detail-actions">' +
                '<button type="button" id="admin-dtl-rc">Сбросить restart_count</button>' +
                '<button type="button" id="admin-dtl-rq" class="danger">Requeue → created</button>' +
                '</div>';
            var b1 = inner.querySelector('#admin-dtl-rc');
            var b2 = inner.querySelector('#admin-dtl-rq');
            if (b1)
                b1.addEventListener('click', function () {
                    postBulkRestart([taskId]);
                });
            if (b2)
                b2.addEventListener('click', function () {
                    postBulkRequeue([taskId]);
                });
        } catch (e) {
            inner.innerHTML = '<div class="admin-detail-empty">' + esc(String(e)) + '</div>';
        }
    }

    function getSelectedIds() {
        var ids = [];
        document.querySelectorAll('.admin-card-cb:checked').forEach(function (cb) {
            ids.push(cb.dataset.taskId);
        });
        return ids;
    }

    function selectFirst10() {
        var cbs = document.querySelectorAll('.admin-card-cb');
        cbs.forEach(function (cb, i) {
            cb.checked = i < 10;
        });
    }

    async function postBulkRestart(ids) {
        if (!ids.length) {
            alert('Нет выбранных задач');
            return;
        }
        var r = await api('/api/admin/tasks/bulk-restart-count', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ task_ids: ids }),
        });
        var j = await r.json().catch(function () {
            return {};
        });
        if (!r.ok) {
            alert('Ошибка: ' + r.status);
            return;
        }
        alert('Обновлено строк: ' + (j.affected | 0));
        loadQueue();
        if (selectedTaskId) loadDetail(selectedTaskId);
    }

    async function postBulkRequeue(ids) {
        if (!ids.length) {
            alert('Нет выбранных задач');
            return;
        }
        if (!confirm('Вернуть ' + ids.length + ' задач(и) в очередь (created)?')) return;
        var r = await api('/api/admin/tasks/bulk-requeue', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ task_ids: ids }),
        });
        var j = await r.json().catch(function () {
            return {};
        });
        if (!r.ok) {
            alert('Ошибка: ' + r.status);
            return;
        }
        alert('Requeue: ' + (j.affected | 0));
        loadQueue();
        if (selectedTaskId) loadDetail(selectedTaskId);
    }

    function bulkRestartCount() {
        postBulkRestart(getSelectedIds());
    }

    function bulkRequeue() {
        postBulkRequeue(getSelectedIds());
    }

    async function bulkRecent24() {
        if (
            !confirm(
                'Установить restart_count=0 для ВСЕХ задач, созданных за последние 24 часа? Это массовая операция.'
            )
        )
            return;
        var r = await api('/api/admin/tasks/bulk-restart-count-recent', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ hours: 24 }),
        });
        var j = await r.json().catch(function () {
            return {};
        });
        if (!r.ok) {
            alert('Ошибка: ' + r.status);
            return;
        }
        alert('Обновлено строк: ' + (j.affected | 0));
        loadQueue();
        if (selectedTaskId) loadDetail(selectedTaskId);
    }

    window.AdminOverlay = {
        open: open,
        close: close,
        refresh: loadQueue,
    };
})();
