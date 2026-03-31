/**
 * Admin queue monitor overlay (opened from header «АДМИНКА»). Requires admin session cookie.
 */
(function () {
    'use strict';

    function api(path, options) {
        return fetch(path, Object.assign({ credentials: 'same-origin', cache: 'no-store' }, options || {}));
    }

    /** Сообщение для пользователя вместо сырого TypeError: Failed to fetch */
    function humanFetchError(e) {
        if (typeof location !== 'undefined' && location.protocol === 'file:') {
            return 'Откройте сайт по адресу https://… на сервере, а не как file:// — иначе браузер не выполнит запросы к API.';
        }
        var msg = String(e && e.message != null ? e.message : e);
        if (msg.indexOf('Failed to fetch') !== -1 || msg.indexOf('NetworkError') !== -1) {
            return 'Сервер не ответил (нет соединения с API). Убедитесь, что бэкенд запущен и страница открыта с того же сайта; не смешивайте http и https.';
        }
        return msg;
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

    function formatBytes(n) {
        if (n == null || n === '') return '—';
        var x = Number(n);
        if (!isFinite(x) || x < 0) return '—';
        if (x < 1024) return x + ' B';
        var u = ['B', 'KB', 'MB', 'GB'];
        var i = 0;
        var v = x;
        while (v >= 1024 && i < u.length - 1) {
            v /= 1024;
            i++;
        }
        return (i === 0 ? String(x) : i === 1 ? v.toFixed(1) : v.toFixed(2)) + ' ' + u[i];
    }

    /** Elapsed time from server-provided age_seconds (UTC clock on server). */
    function formatAgeSeconds(sec) {
        if (sec == null || sec === '' || !isFinite(Number(sec))) return '—';
        var s = Math.max(0, Math.floor(Number(sec)));
        var h = Math.floor(s / 3600);
        var m = Math.floor((s % 3600) / 60);
        if (s < 60) return '<1 мин';
        if (h === 0) return m + ' мин';
        var d = Math.floor(h / 24);
        var rh = h % 24;
        if (d >= 1) return d + 'д ' + rh + 'ч ' + m + 'м';
        return h + 'ч ' + m + 'м';
    }

    function resolveAgeSeconds(t) {
        if (t.age_seconds != null && t.age_seconds !== '') return t.age_seconds;
        if (t.created_at) {
            var c = new Date(t.created_at).getTime();
            if (!isNaN(c)) return Math.floor((Date.now() - c) / 1000);
        }
        return null;
    }

    var ICON = '/static/icons/admin/';

    function imgIcon(file, title, cls) {
        return (
            '<img class="admin-svg-img' +
            (cls ? ' ' + cls : '') +
            '" src="' +
            ICON +
            file +
            '" width="18" height="18" alt="" title="' +
            escAttr(title) +
            '" />'
        );
    }

    var selectedTaskId = null;
    var lastCards = [];

    /** @type {{ status: string, pipeline: string, sortBy: string, sortDesc: boolean, page: number, perPage: number, q: string }} */
    var listState = {
        status: 'all',
        pipeline: '',
        sortBy: 'created_at',
        sortDesc: true,
        page: 1,
        perPage: 30,
        q: '',
    };

    var lastListMeta = { total: 0, page: 1, per_page: 30 };
    var searchDebounceTimer = null;

    function syncListStateFromDom() {
        var st = document.getElementById('admin-filter-status');
        var pl = document.getElementById('admin-filter-pipeline');
        var sb = document.getElementById('admin-sort-by');
        var pp = document.getElementById('admin-per-page');
        if (st) listState.status = st.value;
        if (pl) listState.pipeline = pl.value;
        if (sb) listState.sortBy = sb.value;
        if (pp) listState.perPage = parseInt(pp.value, 10) || 30;
        var qel = document.getElementById('admin-search-q');
        if (qel) listState.q = qel.value.trim();
    }

    function buildListUrl() {
        syncListStateFromDom();
        var params = new URLSearchParams();
        params.set('page', String(listState.page));
        params.set('per_page', String(listState.perPage));
        params.set('sort_by', listState.sortBy);
        params.set('sort_desc', listState.sortDesc ? 'true' : 'false');
        if (listState.status && listState.status !== 'all') {
            params.set('status', listState.status);
        }
        if (listState.pipeline) {
            params.set('pipeline_kind', listState.pipeline);
        }
        if (listState.q) {
            params.set('query', listState.q);
        }
        return '/api/admin/tasks?' + params.toString();
    }

    function updatePagerUI() {
        var prev = document.getElementById('admin-page-prev');
        var next = document.getElementById('admin-page-next');
        var label = document.getElementById('admin-page-label');
        var sortBtn = document.getElementById('admin-sort-dir');
        if (sortBtn) {
            sortBtn.innerHTML = imgIcon(
                listState.sortDesc ? 'arrow-down.svg' : 'arrow-up.svg',
                listState.sortDesc ? 'По убыванию' : 'По возрастанию'
            );
            sortBtn.title = listState.sortDesc ? 'По убыванию' : 'По возрастанию';
        }
        if (!label) return;
        var total = lastListMeta.total;
        var page = lastListMeta.page;
        var pp = lastListMeta.per_page;
        var start = total === 0 ? 0 : (page - 1) * pp + 1;
        var end = Math.min(page * pp, total);
        label.textContent = start + '–' + end + ' из ' + total + ' · стр. ' + page;
        if (prev) prev.disabled = page <= 1;
        if (next) next.disabled = page * pp >= total;
    }

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
            '<h2>Задачи</h2>' +
            '<div class="admin-overlay-actions">' +
            '<button type="button" class="admin-toolbar-ico" id="admin-ov-refresh" title="Обновить список" aria-label="Обновить">' +
            '<img src="' +
            ICON +
            'refresh.svg" width="20" height="20" alt="" /></button>' +
            '<button type="button" class="admin-toolbar-ico" id="admin-ov-sel10" title="Выбрать первые 10" aria-label="Выбрать 10">' +
            '<img src="' +
            ICON +
            'select.svg" width="20" height="20" alt="" /></button>' +
            '<button type="button" class="admin-toolbar-ico" id="admin-ov-bulk-rc" title="Сбросить restart у выбранных" aria-label="Сброс retry">' +
            '<img src="' +
            ICON +
            'reset.svg" width="20" height="20" alt="" /></button>' +
            '<button type="button" class="admin-toolbar-ico danger" id="admin-ov-bulk-rq" title="Requeue выбранных в created" aria-label="Requeue">' +
            '<img src="' +
            ICON +
            'requeue.svg" width="20" height="20" alt="" /></button>' +
            '<button type="button" class="admin-toolbar-ico" id="admin-ov-recent24" title="restart_count=0 за 24ч" aria-label="Retry за 24ч">' +
            '<img src="' +
            ICON +
            'calendar.svg" width="20" height="20" alt="" /></button>' +
            '</div>' +
            '<button type="button" class="admin-overlay-close" id="admin-ov-close" title="Закрыть" aria-label="Закрыть">' +
            '<img src="' +
            ICON +
            'close.svg" width="20" height="20" alt="" /></button>' +
            '</div>' +
            '<div class="admin-overlay-body">' +
            '<div class="admin-queue-scroll">' +
            '<div class="admin-queue-filters">' +
            '<label class="admin-filter-label">Статус<select id="admin-filter-status">' +
            '<option value="all" selected>Все</option>' +
            '<option value="created,processing">Активные (очередь + работа)</option>' +
            '<option value="created">created</option>' +
            '<option value="processing">processing</option>' +
            '<option value="done">done</option>' +
            '<option value="error">error</option>' +
            '</select></label>' +
            '<label class="admin-filter-label">Pipeline<select id="admin-filter-pipeline">' +
            '<option value="" selected>Все</option>' +
            '<option value="rig">rig</option>' +
            '<option value="convert">convert</option>' +
            '</select></label>' +
            '<label class="admin-filter-label">Сортировка<select id="admin-sort-by">' +
            '<option value="created_at" selected>дата создания</option>' +
            '<option value="updated_at">дата обновления</option>' +
            '<option value="id">id задачи</option>' +
            '<option value="pipeline_kind">тип pipeline</option>' +
            '<option value="status">статус</option>' +
            '<option value="progress">progress</option>' +
            '</select></label>' +
            '<button type="button" class="admin-sort-dir-btn" id="admin-sort-dir" title="Направление сортировки">' +
            '<img src="' +
            ICON +
            'arrow-down.svg" width="18" height="18" alt="" /></button>' +
            '<label class="admin-filter-label">На стр.<select id="admin-per-page">' +
            '<option value="10">10</option>' +
            '<option value="20">20</option>' +
            '<option value="30" selected>30</option>' +
            '<option value="50">50</option>' +
            '<option value="100">100</option>' +
            '</select></label>' +
            '<label class="admin-filter-label admin-filter-search">Поиск<input type="search" id="admin-search-q" placeholder="id / owner / email" autocomplete="off" /></label>' +
            '</div>' +
            '<div class="admin-filter-quick" role="group" aria-label="Быстрый фильтр">' +
            '<button type="button" class="admin-chip" data-status="all" title="Все статусы">Все</button>' +
            '<button type="button" class="admin-chip" data-status="created,processing" title="Очередь и работа">Активные</button>' +
            '<button type="button" class="admin-chip" data-status="created" title="created">Создано</button>' +
            '<button type="button" class="admin-chip" data-status="processing" title="processing">В работе</button>' +
            '<button type="button" class="admin-chip" data-status="done" title="done">Готово</button>' +
            '<button type="button" class="admin-chip" data-status="error" title="error">Ошибка</button>' +
            '</div>' +
            '<div class="admin-filter-quick admin-filter-pipeline-row" role="group" aria-label="Pipeline">' +
            '<span class="admin-filter-quick-label">Pipeline</span>' +
            '<button type="button" class="admin-chip admin-chip-pipe" data-pipeline="" title="Все типы">Все</button>' +
            '<button type="button" class="admin-chip admin-chip-pipe" data-pipeline="rig" title="rig">rig</button>' +
            '<button type="button" class="admin-chip admin-chip-pipe" data-pipeline="convert" title="convert">convert</button>' +
            '</div>' +
            '<div class="admin-queue-toolbar">' +
            '<span id="admin-ov-count" class="admin-ov-count"></span>' +
            '<div class="admin-pager">' +
            '<button type="button" class="admin-pager-ico" id="admin-page-prev" title="Предыдущая" aria-label="Назад">' +
            '<img src="' +
            ICON +
            'chevron-left.svg" width="18" height="18" alt="" /></button>' +
            '<span id="admin-page-label"></span>' +
            '<button type="button" class="admin-pager-ico" id="admin-page-next" title="Следующая" aria-label="Вперёд">' +
            '<img src="' +
            ICON +
            'chevron-right.svg" width="18" height="18" alt="" /></button>' +
            '</div>' +
            '</div>' +
            '<div id="admin-card-grid" class="admin-card-grid"></div>' +
            '</div>' +
            '<aside class="admin-detail" id="admin-detail-panel">' +
            '<h3 class="admin-detail-title">Выбрано</h3>' +
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

        var st = root.querySelector('#admin-filter-status');
        var pl = root.querySelector('#admin-filter-pipeline');
        var sb = root.querySelector('#admin-sort-by');
        var pp = root.querySelector('#admin-per-page');
        if (st)
            st.addEventListener('change', function () {
                listState.page = 1;
                loadQueue();
            });
        if (pl)
            pl.addEventListener('change', function () {
                listState.page = 1;
                loadQueue();
            });
        if (sb)
            sb.addEventListener('change', function () {
                loadQueue();
            });
        if (pp)
            pp.addEventListener('change', function () {
                listState.page = 1;
                loadQueue();
            });
        root.querySelector('#admin-sort-dir').addEventListener('click', function () {
            listState.sortDesc = !listState.sortDesc;
            loadQueue();
        });
        root.querySelectorAll('.admin-chip:not(.admin-chip-pipe)').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var st = btn.getAttribute('data-status');
                var sel = document.getElementById('admin-filter-status');
                if (sel && st != null) sel.value = st;
                listState.page = 1;
                loadQueue();
            });
        });
        root.querySelectorAll('.admin-chip-pipe').forEach(function (btn) {
            btn.addEventListener('click', function () {
                var pl = btn.getAttribute('data-pipeline');
                var sel = document.getElementById('admin-filter-pipeline');
                if (sel) sel.value = pl != null ? pl : '';
                listState.page = 1;
                loadQueue();
            });
        });
        var sq = root.querySelector('#admin-search-q');
        if (sq) {
            sq.addEventListener('input', function () {
                clearTimeout(searchDebounceTimer);
                searchDebounceTimer = setTimeout(function () {
                    listState.page = 1;
                    loadQueue();
                }, 350);
            });
            sq.addEventListener('keydown', function (e) {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    clearTimeout(searchDebounceTimer);
                    listState.page = 1;
                    loadQueue();
                }
            });
        }
        root.querySelector('#admin-page-prev').addEventListener('click', function () {
            if (listState.page > 1) {
                listState.page -= 1;
                loadQueue();
            }
        });
        root.querySelector('#admin-page-next').addEventListener('click', function () {
            syncListStateFromDom();
            if (listState.page * listState.perPage < lastListMeta.total) {
                listState.page += 1;
                loadQueue();
            }
        });

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

    function updateFilterChips() {
        syncListStateFromDom();
        document.querySelectorAll('.admin-chip:not(.admin-chip-pipe)').forEach(function (btn) {
            var v = btn.getAttribute('data-status') || '';
            btn.classList.toggle('is-active', v === listState.status);
        });
        document.querySelectorAll('.admin-chip-pipe').forEach(function (btn) {
            var v = btn.getAttribute('data-pipeline');
            var cur = listState.pipeline || '';
            var btnPl = v != null ? String(v) : '';
            btn.classList.toggle('is-active', btnPl === cur);
        });
    }

    async function loadQueue() {
        var grid = document.getElementById('admin-card-grid');
        var countEl = document.getElementById('admin-ov-count');
        if (!grid) return;
        syncListStateFromDom();
        grid.innerHTML = '<div style="padding:12px;color:var(--text-muted)">Загрузка…</div>';
        try {
            var url = buildListUrl();
            var r = await api(url);
            if (!r.ok) {
                grid.innerHTML =
                    '<div style="color:#f88">Нет доступа (admin) или ошибка API: HTTP ' + r.status + '</div>';
                return;
            }
            var data;
            try {
                data = await r.json();
            } catch (je) {
                grid.innerHTML =
                    '<div style="color:#f88">Ответ /api/admin/tasks не JSON — проверьте прокси и бэкенд.</div>';
                return;
            }
            lastCards = data.tasks || [];
            lastListMeta = {
                total: data.total | 0,
                page: data.page | 1,
                per_page: data.per_page | listState.perPage,
            };
            listState.page = lastListMeta.page;
            if (countEl) {
                countEl.textContent =
                    (listState.status === 'all' ? '∗' : listState.status) +
                    (listState.pipeline ? ' · ' + listState.pipeline : '') +
                    ' · ' +
                    listState.sortBy +
                    (listState.sortDesc ? '↓' : '↑');
            }
            updatePagerUI();
            updateFilterChips();
            renderCards(lastCards);
        } catch (e) {
            grid.innerHTML = '<div style="color:#f88">' + esc(humanFetchError(e)) + '</div>';
        }
    }

    function renderCards(tasks) {
        var grid = document.getElementById('admin-card-grid');
        if (!grid) return;
        if (!tasks.length) {
            grid.innerHTML =
                '<div style="color:var(--text-muted);padding:8px 0">Нет задач по текущему фильтру</div>';
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
                var thumbInner = t.poster_url
                    ? '<img class="admin-card-poster" src="' +
                      escAttr(t.poster_url) +
                      '" alt="" loading="lazy"/>'
                    : '<div class="admin-card-thumb-ph"></div>';
                var oid = t.owner_id || '';
                var userInline;
                if (t.owner_email) {
                    var em = t.owner_email;
                    var shortEm = em.length > 20 ? em.slice(0, 18) + '…' : em;
                    userInline =
                        '<span class="admin-card-user-inline" title="' +
                        escAttr(em) +
                        '"><img src="' +
                        ICON +
                        'user.svg" width="12" height="12" alt="" />' +
                        esc(shortEm) +
                        '</span>';
                } else {
                    userInline =
                        '<span class="admin-card-user-inline admin-card-user-anon" title="' +
                        escAttr(oid) +
                        '"><img src="' +
                        ICON +
                        'user-anon.svg" width="12" height="12" alt="" />anon</span>';
                }
                return (
                    '<div class="admin-card" data-task-id="' +
                    escAttr(id) +
                    '" tabindex="0">' +
                    '<input type="checkbox" class="admin-card-cb" data-task-id="' +
                    escAttr(id) +
                    '" title="Выбрать" />' +
                    '<div class="admin-card-inner">' +
                    '<div class="admin-card-thumb">' +
                    thumbInner +
                    '</div>' +
                    '<div class="admin-card-main admin-card-main--' +
                    st +
                    '">' +
                    '<div class="admin-card-strip" aria-hidden="true"></div>' +
                    '<div class="admin-card-line admin-card-line1">' +
                    '<span class="admin-card-st-dot admin-card-st-dot--' +
                    st +
                    '" title="' +
                    escAttr('Статус: ' + t.status) +
                    '"></span>' +
                    '<span class="admin-card-age-txt" title="' +
                    escAttr(
                        'Прошло с создания записи · ' +
                            String(t.created_at || '') +
                            ' (UTC) · age_seconds с сервера'
                    ) +
                    '">' +
                    formatAgeSeconds(resolveAgeSeconds(t)) +
                    '</span></div>' +
                    '<div class="admin-card-line admin-card-line2">' +
                    '<div class="admin-card-progress-wrap"><div class="admin-card-progress-bar" style="width:' +
                    pct +
                    '%"></div></div>' +
                    '<span class="admin-card-meta-txt">' +
                    pct +
                    '% · ' +
                    (t.ready_count | 0) +
                    '/' +
                    (t.total_count | 0) +
                    ' · r' +
                    (t.restart_count | 0) +
                    ' · ' +
                    esc(t.pipeline_kind || '—') +
                    '</span></div>' +
                    '<div class="admin-card-line admin-card-line3">' +
                    userInline +
                    '<span class="admin-card-size" title="Размер входа">' +
                    formatBytes(t.input_bytes) +
                    '</span>' +
                    '<div class="admin-card-icons">' +
                    '<a class="admin-card-ico-mini" href="' +
                    taskUrl +
                    '" target="_blank" rel="noopener" title="Страница задачи" onclick="event.stopPropagation()">' +
                    imgIcon('external.svg', 'Страница') +
                    '</a>' +
                    (t.worker_api
                        ? '<a class="admin-card-ico-mini" href="' +
                          escAttr(t.worker_api) +
                          '" target="_blank" rel="noopener" title="' +
                          wtip +
                          '" onclick="event.stopPropagation()">' +
                          imgIcon('wrench.svg', 'Worker') +
                          '</a>'
                        : '<span class="admin-ico-off admin-card-ico-mini">' +
                          imgIcon('wrench.svg', 'Нет worker') +
                          '</span>') +
                    (t.input_url
                        ? '<a class="admin-card-ico-mini" href="' +
                          escAttr(t.input_url) +
                          '" target="_blank" rel="noopener" title="' +
                          itip +
                          '" onclick="event.stopPropagation()">' +
                          imgIcon('clip.svg', 'Input') +
                          '</a>'
                        : '<span class="admin-ico-off admin-card-ico-mini">' +
                          imgIcon('clip.svg', 'Нет input') +
                          '</span>') +
                    (t.error_message
                        ? '<span class="admin-card-err-ico" title="' +
                          etip +
                          '">' +
                          imgIcon('alert.svg', 'Ошибка') +
                          '</span>'
                        : '') +
                    '</div></div></div></div></div>'
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
                inner.innerHTML = '<div class="admin-detail-empty">Ошибка HTTP ' + r.status + '</div>';
                return;
            }
            var d;
            try {
                d = await r.json();
            } catch (je) {
                inner.innerHTML =
                    '<div class="admin-detail-empty">Ответ inspect не JSON — проверьте прокси.</div>';
                return;
            }
            var stCls = statusClass(d.status);
            var errShort =
                d.error_message && d.error_message.length > 140
                    ? d.error_message.slice(0, 137) + '…'
                    : d.error_message || '';
            var posterTop =
                d.poster_url ?
                    '<div class="admin-detail-poster-wrap"><img class="admin-detail-poster" src="' +
                    escAttr(d.poster_url) +
                    '" alt="" loading="lazy"/></div>'
                : '';
            var ageDetail =
                '<div class="admin-detail-age" title="' +
                escAttr('Создано (UTC): ' + String(d.created_at || '') + ' · возраст по серверу') +
                '">' +
                '<img class="admin-detail-age-ico" src="' +
                ICON +
                'clock.svg" width="18" height="18" alt="" />' +
                '<span class="admin-detail-age-txt">' +
                formatAgeSeconds(resolveAgeSeconds(d)) +
                '</span></div>';
            var emailRow = d.owner_email
                ? '<div class="admin-detail-email-row" title="' +
                  escAttr(d.owner_email) +
                  '"><img class="admin-detail-email-ico" src="' +
                  ICON +
                  'user.svg" width="16" height="16" alt="" /><span class="admin-detail-email">' +
                  esc(d.owner_email) +
                  '</span></div>'
                : '<div class="admin-detail-email-row admin-detail-anon" title="' +
                  escAttr(String(d.owner_id || '')) +
                  '"><img class="admin-detail-email-ico" src="' +
                  ICON +
                  'user-anon.svg" width="16" height="16" alt="" /><span class="admin-detail-email">anon</span></div>';
            inner.innerHTML =
                '<div class="admin-detail-compact">' +
                posterTop +
                ageDetail +
                '<div class="admin-detail-head">' +
                '<span class="admin-card-status ' +
                stCls +
                '">' +
                esc(d.status) +
                '</span>' +
                '<span class="admin-detail-pct" title="Прогресс">' +
                esc(d.progress) +
                '%</span>' +
                '<span class="admin-detail-pipe" title="Pipeline · input type">' +
                esc(d.pipeline_kind || '—') +
                ' · ' +
                esc(d.input_type || '—') +
                '</span>' +
                '</div>' +
                '<div class="admin-detail-primary">' +
                emailRow +
                '<div class="admin-detail-size-line"><span title="Размер входа">' +
                formatBytes(d.input_bytes) +
                '</span></div>' +
                '</div>' +
                '<div class="admin-detail-meta">' +
                '<span class="admin-detail-pill" title="restart_count">r' +
                esc(d.restart_count) +
                '</span>' +
                (d.last_progress_at
                    ? '<span class="admin-detail-pill admin-detail-pill-time" title="' +
                      escAttr(String(d.last_progress_at)) +
                      '">' +
                      imgIcon('clock.svg', String(d.last_progress_at)) +
                      '</span>'
                    : '') +
                '</div>' +
                '<div class="admin-detail-icon-row" role="toolbar" aria-label="Ссылки">' +
                (d.worker_api
                    ? '<a class="admin-detail-icon-btn" href="' +
                      escAttr(d.worker_api) +
                      '" target="_blank" rel="noopener" title="Worker API">' +
                      imgIcon('wrench.svg', 'Worker API') +
                      '</a>'
                    : '<span class="admin-detail-icon-btn is-disabled" title="Нет worker API">' +
                      imgIcon('wrench.svg', 'Нет worker API') +
                      '</span>') +
                (d.progress_page
                    ? '<a class="admin-detail-icon-btn" href="' +
                      escAttr(d.progress_page) +
                      '" target="_blank" rel="noopener" title="Прогресс воркера">' +
                      imgIcon('chart.svg', 'Прогресс воркера') +
                      '</a>'
                    : '<span class="admin-detail-icon-btn is-disabled" title="Нет progress page">' +
                      imgIcon('chart.svg', 'Нет progress page') +
                      '</span>') +
                (d.input_url
                    ? '<a class="admin-detail-icon-btn" href="' +
                      escAttr(d.input_url) +
                      '" target="_blank" rel="noopener" title="Input URL">' +
                      imgIcon('clip.svg', 'Input URL') +
                      '</a>'
                    : '<span class="admin-detail-icon-btn is-disabled" title="Нет input URL">' +
                      imgIcon('clip.svg', 'Нет input URL') +
                      '</span>') +
                '<button type="button" class="admin-detail-icon-btn" id="admin-dtl-copy" title="Копировать task id">' +
                imgIcon('clipboard.svg', 'Копировать task id') +
                '</button>' +
                (d.guid
                    ? '<span class="admin-detail-icon-btn is-static" title="GUID: ' +
                      escAttr(d.guid) +
                      '">' +
                      imgIcon('folder.svg', 'GUID') +
                      '</span>'
                    : '<span class="admin-detail-icon-btn is-disabled" title="Нет GUID">' +
                      imgIcon('folder.svg', 'Нет GUID') +
                      '</span>') +
                (d.worker_task_id
                    ? '<span class="admin-detail-icon-btn is-static" title="worker_task_id: ' +
                      escAttr(d.worker_task_id) +
                      '">' +
                      imgIcon('cog.svg', 'worker_task_id') +
                      '</span>'
                    : '<span class="admin-detail-icon-btn is-disabled" title="Нет worker_task_id">' +
                      imgIcon('cog.svg', 'Нет worker_task_id') +
                      '</span>') +
                '</div>' +
                (d.error_message
                    ? '<div class="admin-detail-error" title="' +
                      escAttr(d.error_message) +
                      '"><span class="admin-detail-error-ico">' +
                      imgIcon('alert.svg', 'Ошибка') +
                      '</span><span class="admin-detail-error-txt">' +
                      esc(errShort) +
                      '</span></div>'
                    : '') +
                '<div class="admin-detail-actions">' +
                '<button type="button" class="admin-detail-icon-btn admin-detail-action" id="admin-dtl-rc" title="Сбросить restart_count">' +
                imgIcon('reset-action.svg', 'Сбросить restart_count') +
                '</button>' +
                '<button type="button" class="admin-detail-icon-btn admin-detail-action is-danger" id="admin-dtl-rq" title="Requeue → created">' +
                imgIcon('requeue-action.svg', 'Requeue → created') +
                '</button>' +
                '</div>' +
                '</div>';
            var b1 = inner.querySelector('#admin-dtl-rc');
            var b2 = inner.querySelector('#admin-dtl-rq');
            var bCopy = inner.querySelector('#admin-dtl-copy');
            if (bCopy) {
                bCopy.addEventListener('click', function (ev) {
                    ev.preventDefault();
                    var id = d.task_id;
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        navigator.clipboard.writeText(id);
                    }
                });
            }
            if (b1)
                b1.addEventListener('click', function () {
                    postBulkRestart([taskId]);
                });
            if (b2)
                b2.addEventListener('click', function () {
                    postBulkRequeue([taskId]);
                });
        } catch (e) {
            inner.innerHTML = '<div class="admin-detail-empty">' + esc(humanFetchError(e)) + '</div>';
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
