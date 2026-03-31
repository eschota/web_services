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

    /** Порт из worker_api (api-converter-glb и т.п.) */
    function extractPortFromWorkerApi(raw) {
        if (!raw || typeof raw !== 'string') return null;
        var s = raw.trim();
        var m = s.match(/:(\d{2,5})(?:\/|$|\?|#)/);
        if (m) return parseInt(m[1], 10);
        try {
            var u = new URL(s.indexOf('://') === -1 ? 'http://' + s : s);
            if (u.port) return parseInt(u.port, 10);
        } catch (e) {}
        return null;
    }

    /**
     * Метка конвертера по порту (маппинг api-converter-glb: f1…f13).
     */
    var CONVERTER_BY_PORT = {
        5132: { short: 'F1', hint: 'конвертер F1, порт 5132' },
        5279: { short: 'F2', hint: 'конвертер F2, порт 5279' },
        5131: { short: 'F7', hint: 'конвертер F7, порт 5131' },
        5533: { short: 'F11', hint: 'конвертер F11, порт 5533' },
        5267: { short: 'F13', hint: 'конвертер F13, порт 5267' },
    };

    /** @returns {{ short: string, title: string } | null} */
    function converterBadgeFromWorkerApi(workerApi) {
        if (!workerApi || typeof workerApi !== 'string') return null;
        var port = extractPortFromWorkerApi(workerApi);
        if (port == null) return null;
        var row = CONVERTER_BY_PORT[port];
        if (!row) return null;
        return {
            short: row.short,
            title: row.hint + ' · ' + workerApi.trim(),
        };
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
    /** id задач с отмеченным чекбоксом (в т.ч. после автообновления) */
    var bulkCheckedIds = new Set();
    var queuePollTimer = null;
    var QUEUE_POLL_MS = 10000;

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
            '<button type="button" class="admin-toolbar-ico danger" id="admin-ov-bulk-rq" title="Requeue: created, сброс состояния, restart_count и глобального таймаута (обновление времени)" aria-label="Requeue">' +
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
            '<div class="admin-queue-two-cols">' +
            '<div class="admin-queue-col-left">' +
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
            '</div>' +
            '<div class="admin-queue-col-right">' +
            '<div class="admin-stats-panel">' +
            '<div class="admin-stats-title">Сводка</div>' +
            '<div id="admin-ov-stats-inner" class="admin-ov-stats-inner">—</div>' +
            '<div class="admin-disk-panel">' +
            '<div class="admin-stats-title">Диск</div>' +
            '<div id="admin-disk-chart" class="admin-disk-chart">—</div>' +
            '<div id="admin-disk-cache-cap" class="admin-disk-cache-cap"></div>' +
            '<div class="admin-disk-actions">' +
            '<button type="button" class="admin-disk-btn" id="admin-disk-refresh" title="Пересчитать размеры каталогов">Обновить</button>' +
            '<button type="button" class="admin-disk-btn admin-disk-btn-danger" id="admin-disk-cleanup" title="Запустить фоновую очистку до min free (как в API)">Очистка сейчас</button>' +
            '</div></div>' +
            '<button type="button" class="admin-stats-reset" id="admin-ov-stats-reset" title="Обнулить счётчики периода (завершения и сумму длительностей) в БД">Сбросить счётчики периода</button>' +
            '</div></div></div>' +
            '<div class="admin-bulk-sel" role="toolbar" aria-label="Быстрый выбор">' +
            '<span class="admin-bulk-sel-label">Выбор</span>' +
            '<button type="button" class="admin-sel-pill admin-sel-all" id="admin-sel-all" title="Отметить все задачи на этой странице">все</button>' +
            '<button type="button" class="admin-sel-pill admin-sel-none" id="admin-sel-none" title="Снять отметки на этой странице">снять</button>' +
            '<button type="button" class="admin-sel-pill admin-sel-inv" id="admin-sel-inv" title="Инвертировать отметки на странице">инверт</button>' +
            '<button type="button" class="admin-sel-pill admin-sel-err" id="admin-sel-err" title="Оставить выбранными только error">ошибки</button>' +
            '<button type="button" class="admin-sel-pill admin-sel-done" id="admin-sel-done" title="Оставить выбранными только done">готово</button>' +
            '<button type="button" class="admin-sel-pill admin-sel-act" id="admin-sel-act" title="Оставить выбранными только очередь и работу">в работе</button>' +
            '</div>' +
            '<div class="admin-bulk-actions" role="toolbar" aria-label="Действия с выбранными задачами">' +
            '<span class="admin-bulk-sel-label">Действия</span>' +
            '<span class="admin-bulk-meta">выбрано: <strong id="admin-bulk-count">0</strong></span>' +
            '<button type="button" class="admin-action-pill admin-action-rq" id="admin-bulk-bar-rq" title="Requeue: created, сброс состояния, restart_count и глобального таймаута (обновление времени)">requeue</button>' +
            '<button type="button" class="admin-action-pill admin-action-del" id="admin-bulk-bar-del" title="Удалить задачи и файлы на сервере (безвозвратно)">удалить</button>' +
            '<button type="button" class="admin-action-pill admin-action-copy" id="admin-bulk-bar-copy" title="Скопировать id отмеченных в буфер">копировать id</button>' +
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

        root.querySelector('#admin-sel-all').addEventListener('click', function (e) {
            e.preventDefault();
            selectAllVisible();
        });
        root.querySelector('#admin-sel-none').addEventListener('click', function (e) {
            e.preventDefault();
            selectNoneVisible();
        });
        root.querySelector('#admin-sel-inv').addEventListener('click', function (e) {
            e.preventDefault();
            invertVisible();
        });
        root.querySelector('#admin-sel-err').addEventListener('click', function (e) {
            e.preventDefault();
            selectVisibleByStatuses(['error']);
        });
        root.querySelector('#admin-sel-done').addEventListener('click', function (e) {
            e.preventDefault();
            selectVisibleByStatuses(['done']);
        });
        root.querySelector('#admin-sel-act').addEventListener('click', function (e) {
            e.preventDefault();
            selectVisibleByStatuses(['created', 'processing']);
        });

        root.querySelector('#admin-bulk-bar-rq').addEventListener('click', function (e) {
            e.preventDefault();
            bulkRequeue();
        });
        root.querySelector('#admin-bulk-bar-del').addEventListener('click', function (e) {
            e.preventDefault();
            bulkDelete();
        });
        root.querySelector('#admin-bulk-bar-copy').addEventListener('click', function (e) {
            e.preventDefault();
            bulkCopySelectedIds();
        });

        root.querySelector('#admin-ov-stats-reset').addEventListener('click', function (e) {
            e.preventDefault();
            if (
                !confirm(
                    'Обнулить счётчики периода в БД (число завершений и сумму длительностей)?'
                )
            )
                return;
            api('/api/admin/overlay-metrics/reset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: '{}',
            }).then(function (r) {
                if (r.ok) loadOverlayMetrics();
                else alert('Ошибка: ' + r.status);
            });
        });

        var dr = root.querySelector('#admin-disk-refresh');
        var dc = root.querySelector('#admin-disk-cleanup');
        if (dr) dr.addEventListener('click', function (e) { e.preventDefault(); loadDiskStats(); });
        if (dc)
            dc.addEventListener('click', function (e) {
                e.preventDefault();
                runAdminDiskCleanup();
            });

        root.addEventListener('click', function (e) {
            var t = e.target;
            if (t && t.id === 'admin-task-cache-max-save') {
                e.preventDefault();
                saveTaskCacheMaxGb();
            }
        });

        document.addEventListener('visibilitychange', onVisibilityRefreshQueue);

        document.addEventListener('keydown', onKeyDown);
    }

    function onVisibilityRefreshQueue() {
        if (document.hidden) return;
        var root = document.getElementById('admin-overlay-root');
        if (!root || !root.classList.contains('is-open')) return;
        loadQueue({ silent: true });
    }

    function onKeyDown(e) {
        if (e.key === 'Escape') {
            var root = document.getElementById('admin-overlay-root');
            if (root && root.classList.contains('is-open')) close();
        }
    }

    function startQueuePoll() {
        stopQueuePoll();
        queuePollTimer = setInterval(function () {
            if (typeof document !== 'undefined' && document.hidden) return;
            loadQueue({ silent: true });
        }, QUEUE_POLL_MS);
    }

    function stopQueuePoll() {
        if (queuePollTimer != null) {
            clearInterval(queuePollTimer);
            queuePollTimer = null;
        }
    }

    function open() {
        var root = ensureRoot();
        root.classList.add('is-open');
        root.setAttribute('aria-hidden', 'false');
        document.body.classList.add('admin-overlay-open');
        loadQueue();
        startQueuePoll();
    }

    function close() {
        stopQueuePoll();
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

    /**
     * @param {{ silent?: boolean }} [opts] — silent: автообновление без «Загрузка…» и без затирания сетки при ошибке
     */
    async function loadQueue(opts) {
        opts = opts || {};
        var silent = !!opts.silent;
        var grid = document.getElementById('admin-card-grid');
        var countEl = document.getElementById('admin-ov-count');
        if (!grid) return;
        syncListStateFromDom();
        if (!silent) {
            grid.innerHTML = '<div style="padding:12px;color:var(--text-muted)">Загрузка…</div>';
        }
        try {
            var url = buildListUrl();
            var r = await api(url);
            if (!r.ok) {
                if (!silent) {
                    grid.innerHTML =
                        '<div style="color:#f88">Нет доступа (admin) или ошибка API: HTTP ' + r.status + '</div>';
                }
                return;
            }
            var data;
            try {
                data = await r.json();
            } catch (je) {
                if (!silent) {
                    grid.innerHTML =
                        '<div style="color:#f88">Ответ /api/admin/tasks не JSON — проверьте прокси и бэкенд.</div>';
                }
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
            loadOverlayMetrics();
        } catch (e) {
            if (!silent) {
                grid.innerHTML = '<div style="color:#f88">' + esc(humanFetchError(e)) + '</div>';
            }
        }
    }

    function renderDiskBreakdown(data) {
        var el = document.getElementById('admin-disk-chart');
        if (!el) return;
        var d = data && data.disk;
        var b = data && data.breakdown_gb;
        if (!d || !b) {
            el.innerHTML = '<span style="color:#f88">Нет данных</span>';
            return;
        }
        var rows = [
            { k: 'task_cache', label: 'Кэш задач (static/tasks)' },
            { k: 'glb_cache', label: 'GLB кэш' },
            { k: 'static_assets', label: 'Статика (без кэшей)' },
            { k: 'uploads', label: 'Загрузки' },
            { k: 'videos', label: 'Видео /var/autorig/videos' },
        ];
        if (b.database_sqlite != null && b.database_sqlite > 0) {
            rows.push({ k: 'database_sqlite', label: 'БД SQLite' });
        }
        rows.push({
            k: 'other_on_disk',
            label: 'Прочее на диске /',
            title:
                'Остаток на разделе / после учёта static, uploads, videos и SQLite: логи, /opt, docker, снимки и т.д. Не относится к лимиту кэша задач.',
        });
        var maxGb = 0.001;
        rows.forEach(function (r) {
            var v = Number(b[r.k] || 0);
            if (v > maxGb) maxGb = v;
        });
        var panel = document.querySelector('.admin-disk-panel');
        if (panel) {
            panel.classList.toggle('admin-disk-panel--low', Number(d.free_gb) < 2.5);
        }
        var freeLine =
            '<div class="admin-disk-summary">' +
            '<div class="admin-disk-summary-line">Свободно: <strong class="admin-disk-free-val">' +
            esc(String(d.free_gb)) +
            '</strong> <span class="admin-disk-unit">GB</span></div>' +
            '<div class="admin-disk-summary-line">Занято: <strong>' +
            esc(String(d.used_gb)) +
            '</strong> GB <span class="admin-disk-of">из</span> ' +
            esc(String(d.total_gb)) +
            ' GB</div></div>';
        var bars = rows
            .map(function (r) {
                var gb = Number(b[r.k] || 0);
                var pct = Math.min(100, (gb / maxGb) * 100);
                var tip = r.title != null ? r.title : r.label;
                return (
                    '<div class="admin-disk-bar-row">' +
                    '<span class="admin-disk-bar-label" title="' +
                    escAttr(tip) +
                    '">' +
                    esc(r.label) +
                    '</span>' +
                    '<div class="admin-disk-bar-track"><i style="width:' +
                    pct.toFixed(1) +
                    '%"></i></div>' +
                    '<span class="admin-disk-bar-val">' +
                    (gb >= 0.01 ? gb.toFixed(2) : gb.toFixed(3)) +
                    ' GB</span></div>'
                );
            })
            .join('');
        el.innerHTML = freeLine + bars;
        var capEl = document.getElementById('admin-disk-cache-cap');
        if (capEl) {
            var s = data.settings || {};
            var maxGb =
                data.task_cache_max_gb != null
                    ? data.task_cache_max_gb
                    : s.task_cache_max_gb != null
                      ? s.task_cache_max_gb
                      : 10;
            capEl.innerHTML =
                '<div class="admin-disk-cache-cap-row">' +
                '<label class="admin-disk-cache-cap-label" for="admin-task-cache-max-gb" title="Суммарный размер static/tasks. При превышении при создании новой задачи удаляются целые каталоги старых задач (не трогаем created/processing).">Лимит кэша задач (GB)</label>' +
                '<input type="number" class="admin-disk-cache-cap-input" id="admin-task-cache-max-gb" min="0.1" step="0.1" value="' +
                escAttr(String(maxGb)) +
                '" />' +
                '<button type="button" class="admin-disk-btn" id="admin-task-cache-max-save">Сохранить</button>' +
                '</div>';
        }
    }

    async function saveTaskCacheMaxGb() {
        var inp = document.getElementById('admin-task-cache-max-gb');
        if (!inp) return;
        var v = parseFloat(inp.value);
        if (!isFinite(v) || v <= 0) {
            alert('Укажите положительное число GB');
            return;
        }
        try {
            var r = await api('/api/admin/settings/task-cache-max', {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ task_cache_max_gb: v }),
            });
            if (!r.ok) {
                alert('Ошибка: ' + r.status);
                return;
            }
            loadDiskStats();
        } catch (e) {
            alert(esc(humanFetchError(e)));
        }
    }

    async function loadDiskStats() {
        var el = document.getElementById('admin-disk-chart');
        if (!el) return;
        el.innerHTML = '<span class="admin-disk-loading">Считаю размеры…</span>';
        try {
            var r = await api('/api/admin/disk-stats');
            if (!r.ok) {
                el.innerHTML = '<span style="color:#f88">disk-stats: ' + r.status + '</span>';
                return;
            }
            var data = await r.json();
            renderDiskBreakdown(data);
        } catch (e) {
            el.innerHTML = '<span style="color:#f88">' + esc(humanFetchError(e)) + '</span>';
        }
    }

    async function runAdminDiskCleanup() {
        if (
            !confirm(
                'Запустить очистку диска до целевого свободного места (MIN_FREE_SPACE)? Удалятся старые файлы задач и/или сироты по правилам сервера.'
            )
        )
            return;
        try {
            var r = await api('/api/admin/cleanup', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: '{}',
            });
            var j = await r.json().catch(function () {
                return {};
            });
            if (!r.ok) {
                alert('Ошибка очистки: ' + r.status);
                return;
            }
            alert(
                'Очистка завершена.\nОсвобождено: ~' +
                    (j.freed_gb != null ? j.freed_gb : '?') +
                    ' GB\nСвободно сейчас: ~' +
                    (j.final_free_gb != null ? j.final_free_gb : '?') +
                    ' GB'
            );
            loadDiskStats();
        } catch (e) {
            alert(esc(humanFetchError(e)));
        }
    }

    function formatDurationSeconds(sec) {
        if (sec == null || sec === '' || !isFinite(Number(sec))) return '—';
        var s = Number(sec);
        if (s < 60) return Math.round(s) + ' сек';
        if (s < 3600) return (s / 60).toFixed(1) + ' мин';
        return (s / 3600).toFixed(1) + ' ч';
    }

    async function loadOverlayMetrics() {
        var inner = document.getElementById('admin-ov-stats-inner');
        if (!inner) return;
        try {
            var r = await api('/api/admin/overlay-metrics');
            if (!r.ok) {
                inner.innerHTML = '<span style="color:#f88">Нет доступа к метрикам</span>';
                return;
            }
            var m = await r.json();
            var ts = m.tasks_by_status || {};
            var rating =
                m.rating_percent != null && m.rating_percent !== ''
                    ? String(m.rating_percent) + '%'
                    : '—';
            var avg = formatDurationSeconds(m.session_avg_seconds);
            inner.innerHTML =
                '<div class="admin-stat-row"><span class="admin-stat-k">Рейтинг (done / (done+err))</span>' +
                '<span class="admin-stat-v">' +
                esc(rating) +
                '</span></div>' +
                '<div class="admin-stat-row"><span class="admin-stat-k">Статусы в БД</span>' +
                '<span class="admin-stat-v">c ' +
                (ts.created | 0) +
                ' · pr ' +
                (ts.processing | 0) +
                ' · ✓ ' +
                (ts.done | 0) +
                ' · ✗ ' +
                (ts.error | 0) +
                '</span></div>' +
                '<div class="admin-stat-row"><span class="admin-stat-k">Всего задач</span>' +
                '<span class="admin-stat-v">' +
                (m.total_tasks | 0) +
                '</span></div>' +
                '<div class="admin-stat-sub">Период (счётчик сбрасывается кнопкой)</div>' +
                '<div class="admin-stat-row"><span class="admin-stat-k">Завершено за период</span>' +
                '<span class="admin-stat-v">' +
                (m.session_completed | 0) +
                '</span></div>' +
                '<div class="admin-stat-row"><span class="admin-stat-k">Средняя длительность (период)</span>' +
                '<span class="admin-stat-v">' +
                esc(avg) +
                '</span></div>';
            loadDiskStats();
        } catch (e) {
            inner.innerHTML = '<span style="color:#f88">' + esc(humanFetchError(e)) + '</span>';
        }
    }

    function renderCards(tasks) {
        var grid = document.getElementById('admin-card-grid');
        if (!grid) return;
        if (!tasks.length) {
            grid.innerHTML =
                '<div style="color:var(--text-muted);padding:8px 0">Нет задач по текущему фильтру</div>';
            updateBulkSelectionCount();
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
                var conv = converterBadgeFromWorkerApi(t.worker_api);
                var convHtml = conv
                    ? '<span class="admin-card-conv" title="' + escAttr(conv.title) + '">' + esc(conv.short) + '</span>'
                    : '';
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
                    '" data-task-status="' +
                    escAttr(st) +
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
                    '<span class="admin-card-line1-left">' +
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
                    '</span></span>' +
                    convHtml +
                    '</div>' +
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
            var tid = cb.dataset.taskId;
            cb.checked = bulkCheckedIds.has(tid);
            cb.addEventListener('click', function (e) {
                e.stopPropagation();
            });
            cb.addEventListener('change', function () {
                if (cb.checked) bulkCheckedIds.add(tid);
                else bulkCheckedIds.delete(tid);
                updateBulkSelectionCount();
            });
        });
        document.querySelectorAll('.admin-card').forEach(function (c) {
            c.classList.toggle('is-selected', c.dataset.taskId === selectedTaskId);
        });
        updateBulkSelectionCount();
    }

    function updateBulkSelectionCount() {
        var el = document.getElementById('admin-bulk-count');
        if (el) el.textContent = String(bulkCheckedIds.size);
    }

    function selectAllVisible() {
        lastCards.forEach(function (t) {
            bulkCheckedIds.add(t.task_id);
        });
        document.querySelectorAll('#admin-card-grid .admin-card-cb').forEach(function (cb) {
            cb.checked = true;
        });
        updateBulkSelectionCount();
    }

    function selectNoneVisible() {
        lastCards.forEach(function (t) {
            bulkCheckedIds.delete(t.task_id);
        });
        document.querySelectorAll('#admin-card-grid .admin-card-cb').forEach(function (cb) {
            cb.checked = false;
        });
        updateBulkSelectionCount();
    }

    function invertVisible() {
        lastCards.forEach(function (t) {
            var id = t.task_id;
            if (bulkCheckedIds.has(id)) bulkCheckedIds.delete(id);
            else bulkCheckedIds.add(id);
        });
        document.querySelectorAll('#admin-card-grid .admin-card-cb').forEach(function (cb) {
            cb.checked = bulkCheckedIds.has(cb.dataset.taskId);
        });
        updateBulkSelectionCount();
    }

    /** @param {string[]} statuses — нормализованные имена: created, processing, error, done */
    function selectVisibleByStatuses(statuses) {
        var want = {};
        statuses.forEach(function (s) {
            want[s] = true;
        });
        lastCards.forEach(function (t) {
            var id = t.task_id;
            var st = statusClass(t.status);
            if (want[st]) bulkCheckedIds.add(id);
            else bulkCheckedIds.delete(id);
        });
        document.querySelectorAll('#admin-card-grid .admin-card-cb').forEach(function (cb) {
            cb.checked = bulkCheckedIds.has(cb.dataset.taskId);
        });
        updateBulkSelectionCount();
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
                '<button type="button" class="admin-detail-icon-btn admin-detail-action is-danger" id="admin-dtl-rq" title="Requeue: created, сброс состояния, restart_count и глобального таймаута (обновление времени)">' +
                imgIcon('requeue-action.svg', 'Requeue') +
                '</button>' +
                '</div>' +
                '</div>';
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
            if (b2)
                b2.addEventListener('click', function () {
                    postBulkRequeue([taskId]);
                });
        } catch (e) {
            inner.innerHTML = '<div class="admin-detail-empty">' + esc(humanFetchError(e)) + '</div>';
        }
    }

    /** Все отмеченные id (включая другие страницы), не только видимые чекбоксы в DOM */
    function getSelectedIds() {
        return Array.from(bulkCheckedIds);
    }

    function clearBulkSelection() {
        bulkCheckedIds.clear();
        document.querySelectorAll('#admin-card-grid .admin-card-cb').forEach(function (cb) {
            cb.checked = false;
        });
        updateBulkSelectionCount();
        selectedTaskId = null;
        document.querySelectorAll('#admin-card-grid .admin-card').forEach(function (c) {
            c.classList.remove('is-selected');
        });
        var inner = document.getElementById('admin-detail-inner');
        if (inner) inner.innerHTML = '<div class="admin-detail-empty">Выберите карточку</div>';
    }

    function bulkCopySelectedIds() {
        var ids = getSelectedIds();
        if (!ids.length) {
            alert('Нет выбранных задач');
            return;
        }
        var text = ids.join('\n');
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text);
        } else {
            alert(text);
        }
    }

    function selectFirst10() {
        lastCards.forEach(function (t, i) {
            if (i < 10) bulkCheckedIds.add(t.task_id);
            else bulkCheckedIds.delete(t.task_id);
        });
        document.querySelectorAll('#admin-card-grid .admin-card-cb').forEach(function (cb) {
            cb.checked = bulkCheckedIds.has(cb.dataset.taskId);
        });
        updateBulkSelectionCount();
    }

    async function postBulkRequeue(ids) {
        if (!ids.length) {
            alert('Нет выбранных задач');
            return;
        }
        if (
            !confirm(
                'Вернуть ' +
                    ids.length +
                    ' задач(и) в очередь (created): сброс состояния, restart_count, глобального таймаута и обновление времени создания?'
            )
        )
            return;
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
        clearBulkSelection();
        loadQueue();
    }

    function bulkRequeue() {
        postBulkRequeue(getSelectedIds());
    }

    async function postBulkDelete(ids) {
        if (!ids.length) {
            alert('Нет выбранных задач');
            return;
        }
        if (
            !confirm(
                'Безвозвратно удалить ' +
                    ids.length +
                    ' задач(и) вместе с файлами на сервере?'
            )
        )
            return;
        var r = await api('/api/admin/tasks/bulk-delete', {
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
        alert('Удалено: ' + (j.affected | 0));
        clearBulkSelection();
        loadQueue();
    }

    function bulkDelete() {
        postBulkDelete(getSelectedIds());
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
        clearBulkSelection();
        loadQueue();
    }

    window.AdminOverlay = {
        open: open,
        close: close,
        refresh: loadQueue,
    };
})();
