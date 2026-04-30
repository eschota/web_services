/**
 * Site-wide support chat (FastAPI + Telegram forum topics).
 * Override API origin: window.__AUTORIG_API_BASE__ = "https://api.example.com" (no trailing slash)
 * Or: <body data-api-base="https://api.example.com">
 * Opt out: data-support-chat-off="1" or window.__siteLayoutSupportChat = false
 */
(function () {
    var STORAGE_VISITOR = 'autorig_support_visitor_v1';
    var STORAGE_SESSION = 'autorig_support_session_v1';
    var STORAGE_CURSOR_PREFIX = 'autorig_support_poll_cursor_v1_';

    /** Standard chat bubble (SVG), not emoji — avoids wrong OS glyphs */
    var SVG_CHAT_BUBBLE =
        '<svg class="ar-support-chat-bubble-svg" viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
        '<path fill="currentColor" d="M20 3H4a2 2 0 00-2 2v14a2 2 0 002 2h13l5 4V5a2 2 0 00-2-2z"/>' +
        '</svg>';

    var SVG_SEND_PLANE =
        '<svg class="ar-support-send-plane" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path fill="currentColor" d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>';

    var SVG_SEND_SPINNER =
        '<svg class="ar-support-send-spinner-svg" viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
        '<circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2.25" stroke-linecap="round" stroke-dasharray="40 70"/></svg>';

    var SVG_SEND_OK =
        '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" d="M5 12.5 9.5 17 19 8"/></svg>';

    var SVG_SEND_ERR =
        '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" d="M7 7l10 10M17 7L7 17"/></svg>';

    var OPERATOR_GRAVATAR =
        'https://www.gravatar.com/avatar/9248a22dd0eca093b2361264d8b1b882?d=identicon&s=96';

    var PACK = {
        en: {
            support_chat_title: 'Support',
            support_chat_disclaimer:
                'You are chatting with a live human operator (Escho) — replies are never from AI or scripted bots.',
            support_chat_placeholder: 'Type a message…',
            support_chat_send: 'Send',
            support_chat_send_aria: 'Send message',
            support_chat_phase_session: 'Preparing session…',
            support_chat_phase_message: 'Sending to operator…',
            support_chat_phase_sync: 'Syncing chat…',
            support_chat_phase_done: 'Sent',
            support_chat_operator_label: 'Escho · Operator',
            support_chat_you_label: 'You',
            support_chat_support_disabled: 'Support is disabled on the server.',
            support_chat_telegram_missing:
                'Support is temporarily unavailable. Please try again later.',
            support_chat_ready_thread: '',
            support_chat_linked: '',
            support_chat_error_api_connect:
                'Could not reach Support API (routing or old server). Reload; ops must expose /api/support-chat/* on FastAPI. Try GET /api/support-chat/health',
            support_chat_error_session_missing:
                'Session expired — reopen the chat.',
            support_chat_arialabel_open: 'Open support chat',
            support_chat_arialabel_panel: 'Support chat dialog',
        },
        ru: {
            support_chat_title: 'Поддержка',
            support_chat_disclaimer:
                'Вы общаетесь с живым оператором (Escho) — ответ приходит человеком, не ИИ и не ботами.',
            support_chat_placeholder: 'Введите сообщение…',
            support_chat_send: 'Отправить',
            support_chat_send_aria: 'Отправить сообщение',
            support_chat_phase_session: 'Готовим сессию…',
            support_chat_phase_message: 'Отправка оператору…',
            support_chat_phase_sync: 'Обновление чата…',
            support_chat_phase_done: 'Отправлено',
            support_chat_operator_label: 'Escho · Оператор',
            support_chat_you_label: 'Вы',
            support_chat_support_disabled: 'Поддержка отключена на сервере.',
            support_chat_telegram_missing:
                'Поддержка временно недоступна. Попробуйте позже.',
            support_chat_ready_thread: '',
            support_chat_linked: '',
            support_chat_error_api_connect:
                'API поддержки недоступен (маршрут или старый деплой). Обновите страницу; проверьте GET /api/support-chat/health',
            support_chat_error_session_missing: 'Сессия устарела — откройте чат снова.',
            support_chat_arialabel_open: 'Открыть чат поддержки',
            support_chat_arialabel_panel: 'Диалог поддержки',
        },
        zh: {
            support_chat_title: '客服',
            support_chat_disclaimer:
                '您正在与真人运营者（Escho）对话 — 回复来自人工，不是 AI 或自动脚本。',
            support_chat_placeholder: '输入消息…',
            support_chat_send: '发送',
            support_chat_send_aria: '发送消息',
            support_chat_phase_session: '正在准备会话…',
            support_chat_phase_message: '正在发送给客服…',
            support_chat_phase_sync: '正在同步…',
            support_chat_phase_done: '已发送',
            support_chat_operator_label: 'Escho · 客服',
            support_chat_you_label: '您',
            support_chat_support_disabled: '服务器已关闭客服入口。',
            support_chat_telegram_missing: '客服暂时不可用。请稍后再试。',
            support_chat_ready_thread: '',
            support_chat_linked: '',
            support_chat_error_api_connect:
                '无法连接客服接口。请刷新并确认 /api/support-chat/* 转发到 FastAPI；可试 GET /api/support-chat/health',
            support_chat_error_session_missing: '会话已失效 — 请重新打开聊天。',
            support_chat_arialabel_open: '打开客服聊天',
            support_chat_arialabel_panel: '客服聊天窗口',
        },
        hi: {
            support_chat_title: 'सहायता',
            support_chat_disclaimer:
                'आप जीवित ऑपरेटर (Escho) से चैट कर रहे हैं — जवाब मानव देते हैं, AI या बॉट नहीं।',
            support_chat_placeholder: 'संदेश लिखें…',
            support_chat_send: 'भेजें',
            support_chat_send_aria: 'संदेश भेजें',
            support_chat_phase_session: 'सत्र तैयार हो रहा है…',
            support_chat_phase_message: 'ऑपरेटर को भेजा जा रहा है…',
            support_chat_phase_sync: 'चैट सिंक हो रही है…',
            support_chat_phase_done: 'भेज दिया',
            support_chat_operator_label: 'Escho · ऑपरेटर',
            support_chat_you_label: 'आप',
            support_chat_support_disabled: 'सर्वर पर सहायता बंद है।',
            support_chat_telegram_missing:
                'सहायता अभी अस्थायी रूप से उपलब्ध नहीं है। कृपया बाद में प्रयास करें।',
            support_chat_ready_thread: '',
            support_chat_linked: '',
            support_chat_error_api_connect:
                'सहायता API नहीं मिला। रीफ़्रेश करें; GET /api/support-chat/health जाँचें',
            support_chat_error_session_missing:
                'सत्र समाप्त — चैट दोबारा खोलें।',
            support_chat_arialabel_open: 'सहायता चैट खोलें',
            support_chat_arialabel_panel: 'सहायता चैट विंडो',
        },
    };

    function lang() {
        try {
            if (window.I18n && window.I18n.currentLang) {
                var c = String(window.I18n.currentLang).toLowerCase();
                if (PACK[c]) return c;
            }
        } catch (e) {}
        var n = (navigator.language || 'en').split('-')[0].toLowerCase();
        return PACK[n] ? n : 'en';
    }

    function translation(key) {
        try {
            if (window.I18n && window.I18n.translations && window.I18n.translations[key]) {
                return String(window.I18n.translations[key]);
            }
        } catch (e2) {}
        var L = PACK[lang()] || PACK.en;
        return String((L[key] != null ? L[key] : PACK.en[key]) || key);
    }

    function tr(key, replacements) {
        var text = translation(key);
        if (replacements && typeof replacements === 'object') {
            Object.keys(replacements).forEach(function (k) {
                text = text.replace(
                    new RegExp('\\{\\s*' + k + '\\s*\\}', 'g'),
                    String(replacements[k])
                );
            });
        }
        return text;
    }

    function isWebApp() {
        try {
            return new URLSearchParams(window.location.search).get('mode') === 'webapp';
        } catch (e) {
            return false;
        }
    }

    function randomId() {
        if (window.crypto && typeof window.crypto.randomUUID === 'function') {
            return window.crypto.randomUUID();
        }
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = (Math.random() * 16) | 0;
            var v = c === 'x' ? r : (r & 0x3) | 0x8;
            return v.toString(16);
        });
    }

    function getVisitorId() {
        try {
            var v = localStorage.getItem(STORAGE_VISITOR);
            if (v && v.length >= 8) return v;
            v = randomId();
            localStorage.setItem(STORAGE_VISITOR, v);
            return v;
        } catch (e) {
            return randomId();
        }
    }

    function getStoredSessionId() {
        try {
            var x = parseInt(localStorage.getItem(STORAGE_SESSION) || '', 10);
            return isNaN(x) ? null : x;
        } catch (e) {
            return null;
        }
    }

    function setStoredSessionId(id) {
        try {
            if (id != null) localStorage.setItem(STORAGE_SESSION, String(id));
            else localStorage.removeItem(STORAGE_SESSION);
        } catch (e) {
            /* noop */
        }
    }

    function cursorKey(sessionIdInt) {
        return STORAGE_CURSOR_PREFIX + String(sessionIdInt || '0');
    }

    function getPollCursor(sessionIdInt) {
        try {
            var x = parseInt(localStorage.getItem(cursorKey(sessionIdInt)) || '0', 10);
            return isNaN(x) ? 0 : x;
        } catch (e) {
            return 0;
        }
    }

    function setPollCursor(sessionIdInt, msgIdInt) {
        try {
            localStorage.setItem(cursorKey(sessionIdInt), String(msgIdInt));
        } catch (e) {
            /* noop */
        }
    }

    function apiBaseRaw() {
        try {
            if (
                typeof window.__AUTORIG_API_BASE__ === 'string' &&
                window.__AUTORIG_API_BASE__.trim()
            ) {
                return window.__AUTORIG_API_BASE__.trim().replace(/\/+$/, '');
            }
        } catch (e3) {}
        try {
            var b = document.body && document.body.getAttribute('data-api-base');
            if (b && b.trim()) return b.trim().replace(/\/+$/, '');
        } catch (e4) {}
        try {
            var h = document.documentElement && document.documentElement.getAttribute('data-api-base');
            if (h && h.trim()) return h.trim().replace(/\/+$/, '');
        } catch (e5) {}
        return '';
    }

    function endpoint(path) {
        var base = apiBaseRaw();
        if (!base) return path;
        return base + path;
    }

    function apiJson(relPath, opts) {
        opts = opts || {};
        opts.credentials = opts.credentials || 'same-origin';
        opts.headers = opts.headers || {};
        if (!opts.headers['Content-Type'] && opts.body) {
            opts.headers['Content-Type'] = 'application/json';
        }
        var url = endpoint(relPath);
        return fetch(url, opts).then(function (res) {
            return res.text().then(function (text) {
                var j = null;
                if (text) {
                    try {
                        j = JSON.parse(text);
                    } catch (e6) {
                        j = null;
                    }
                } else j = {};
                if (!res.ok) {
                    var d =
                        j && j.detail !== undefined && j.detail !== null
                            ? j.detail
                            : null;
                    if (Array.isArray(d)) {
                        d = d
                            .map(function (x) {
                                if (x && typeof x === 'object' && x.msg) return String(x.msg);
                                try {
                                    return JSON.stringify(x);
                                } catch (e7) {
                                    return String(x);
                                }
                            })
                            .join('; ');
                    }
                    if (d == null || String(d).trim() === '') {
                        d =
                            text && text.trim()
                                ? text.trim().slice(0, 400)
                                : 'HTTP ' + res.status;
                    }
                    var err = new Error(String(d));
                    err.status = res.status;
                    err.payload = j;
                    throw err;
                }
                return j || {};
            });
        });
    }

    function el(tag, cls, txt) {
        var n = document.createElement(tag);
        if (cls) n.className = cls;
        if (txt != null && txt !== '') n.textContent = txt;
        return n;
    }

    function initOnce() {
        if (document.getElementById('ar-support-chat-root')) return;

        try {
            if (document.body && document.body.getAttribute('data-support-chat-off') === '1') {
                return;
            }
        } catch (e8) {}

        var state = {
            sessionIdInt: getStoredSessionId(),
            visitorId: getVisitorId(),
            pollTimer: null,
            openBool: false,
            afterIdInt: 0,
            unreadInt: 0,
        };

        var root = el('div', 'ar-support-chat');
        root.id = 'ar-support-chat-root';
        if (isWebApp()) root.classList.add('ar-support-chat--webapp');

        var bubble = el('button', 'ar-support-chat-bubble', '');
        bubble.type = 'button';

        var bubbleIcon = el('span', 'ar-support-chat-bubble-icon');
        bubbleIcon.innerHTML = SVG_CHAT_BUBBLE;
        bubble.appendChild(bubbleIcon);

        var badge = el('span', 'ar-support-chat-badge', '');
        bubble.appendChild(badge);

        var panel = el('div', 'ar-support-chat-panel');
        panel.setAttribute('role', 'dialog');

        var head = el('div', 'ar-support-chat-head');
        var title = el('div', 'ar-support-chat-title', translation('support_chat_title'));
        var closeBtn = el('button', 'ar-support-chat-close', '×');
        closeBtn.type = 'button';
        head.appendChild(title);
        head.appendChild(closeBtn);

        var disclaimer = el('div', 'ar-support-chat-disclaimer', '');
        disclaimer.textContent = translation('support_chat_disclaimer');

        var meta = el('div', 'ar-support-chat-meta', '');
        var log = el('div', 'ar-support-chat-log');

        var compose = el('div', 'ar-support-chat-compose');
        var sendPhase = el('div', 'ar-support-chat-send-phase');
        sendPhase.id = 'ar-support-chat-send-phase';
        sendPhase.setAttribute('aria-live', 'polite');
        var inpRowInner = el('div', 'ar-support-chat-inputrow-inner');
        var ta = document.createElement('textarea');
        ta.className = 'ar-support-chat-textarea';
        ta.rows = 2;
        ta.placeholder = translation('support_chat_placeholder');
        var send = el('button', 'ar-support-chat-send ar-support-chat-send--idle', '');
        send.type = 'button';
        send.innerHTML =
            '<span class="ar-support-send-layers">' +
            '<span class="ar-support-send-layer ar-support-send-layer--idle">' +
            SVG_SEND_PLANE +
            '</span>' +
            '<span class="ar-support-send-layer ar-support-send-layer--busy">' +
            SVG_SEND_SPINNER +
            '</span>' +
            '<span class="ar-support-send-layer ar-support-send-layer--ok">' +
            SVG_SEND_OK +
            '</span>' +
            '<span class="ar-support-send-layer ar-support-send-layer--err">' +
            SVG_SEND_ERR +
            '</span>' +
            '</span>';
        inpRowInner.appendChild(ta);
        inpRowInner.appendChild(send);
        compose.appendChild(sendPhase);
        compose.appendChild(inpRowInner);

        panel.appendChild(head);
        panel.appendChild(disclaimer);
        panel.appendChild(meta);
        panel.appendChild(log);
        panel.appendChild(compose);

        root.appendChild(bubble);
        root.appendChild(panel);
        document.body.appendChild(root);

        function applyAriaAndLabels() {
            bubble.setAttribute(
                'aria-label',
                tr('support_chat_arialabel_open')
            );
            panel.setAttribute(
                'aria-label',
                tr('support_chat_arialabel_panel')
            );
            title.textContent = tr('support_chat_title');
            disclaimer.textContent = tr('support_chat_disclaimer');
            ta.placeholder = tr('support_chat_placeholder');
            send.setAttribute('aria-label', tr('support_chat_send_aria'));
        }

        applyAriaAndLabels();
        window.addEventListener('languageChanged', applyAriaAndLabels);

        var sendChromeTimer = null;
        function clearSendChromeTimer() {
            if (sendChromeTimer) {
                window.clearTimeout(sendChromeTimer);
                sendChromeTimer = null;
            }
        }

        /** mode: idle | busy | ok | err */
        function setSendChrome(mode, phaseMessage) {
            clearSendChromeTimer();
            send.classList.remove(
                'ar-support-chat-send--idle',
                'ar-support-chat-send--busy',
                'ar-support-chat-send--ok',
                'ar-support-chat-send--err'
            );
            send.classList.add('ar-support-chat-send--' + mode);
            sendPhase.textContent = phaseMessage || '';
            var busyBool = mode === 'busy';
            send.disabled = busyBool;
            ta.disabled = busyBool;
        }

        var sendingLocks = false;

        function finishSendChromeToIdle(delayMs) {
            clearSendChromeTimer();
            sendChromeTimer = window.setTimeout(function () {
                setSendChrome('idle', '');
                sendChromeTimer = null;
                sendingLocks = false;
            }, delayMs || 0);
        }

        function setMeta(t) {
            meta.textContent = t || '';
        }

        function updateBadge() {
            if (state.unreadInt > 0 && !state.openBool) {
                badge.style.display = 'block';
                badge.textContent = String(state.unreadInt > 9 ? '9+' : state.unreadInt);
            } else {
                badge.style.display = 'none';
            }
        }

        function appendMsgRow(item) {
            var dir = item.direction_string || '';
            var mod = dir === 'admin' ? 'admin' : dir === 'user' ? 'user' : 'system';

            var row = el('div', 'ar-support-chat-msg ar-support-chat-msg--' + mod);

            var inner = el('div', 'ar-support-chat-msg-inner');

            var whoLabel =
                dir === 'admin'
                    ? tr('support_chat_operator_label')
                    : dir === 'user'
                      ? tr('support_chat_you_label')
                      : 'System';

            if (dir === 'admin') {
                var av = document.createElement('img');
                av.className = 'ar-support-chat-avatar';
                av.src = OPERATOR_GRAVATAR;
                av.alt = '';
                av.width = 36;
                av.height = 36;
                inner.appendChild(av);
            }

            var col = el('div', 'ar-support-chat-msg-col');
            var hdr = el('div', 'ar-support-chat-msg-hdr', '');
            hdr.textContent =
                whoLabel +
                ' · ' +
                String(item.created_at_string || '').replace('T', ' ').slice(0, 19);

            var body = el('div', 'ar-support-chat-msg-body', '');
            body.textContent = item.body_text_string || '';
            col.appendChild(hdr);
            col.appendChild(body);
            inner.appendChild(col);
            row.appendChild(inner);

            log.appendChild(row);
            log.scrollTop = log.scrollHeight;
            var mid = parseInt(item.id_int, 10);
            if (!isNaN(mid) && mid > state.afterIdInt) state.afterIdInt = mid;
        }

        function applyPollPayload(data, renderBool) {
            var list = data && data.messages ? data.messages : [];
            var sid = parseInt(state.sessionIdInt, 10) || 0;
            var prevCursor = renderBool ? 0 : getPollCursor(sid);
            var maxInBatch = prevCursor;
            list.forEach(function (m) {
                var mid = parseInt(m.id_int, 10);
                mid = isNaN(mid) ? 0 : mid;
                if (mid > maxInBatch) maxInBatch = mid;
                if (renderBool) appendMsgRow(m);
                else if (mid > prevCursor && m.direction_string === 'admin') state.unreadInt++;
            });
            if (maxInBatch > prevCursor) {
                setPollCursor(sid, maxInBatch);
            }
            if (renderBool) {
                state.unreadInt = 0;
            }
            updateBadge();
        }

        function mapNetworkError(err) {
            var st = err && err.status;
            var m = ((err && err.message) || '').trim();
            if (st === 404 || /^not found$/i.test(m)) return tr('support_chat_error_api_connect');
            if (st === 503) return tr('support_chat_telegram_missing');
            return m || tr('support_chat_error_api_connect');
        }

        function ensureSession(showErr) {
            var urlPg = String(window.location.href || '').slice(0, 4096);
            return apiJson('/api/support-chat/session', {
                method: 'POST',
                body: JSON.stringify({
                    visitor_id_string: state.visitorId,
                    page_url_string: urlPg,
                }),
            })
                .then(function (j) {
                    state.sessionIdInt = j.session_id_int;
                    setStoredSessionId(state.sessionIdInt);

                    if (!j.support_configured_bool) setMeta(tr('support_chat_telegram_missing'));
                    else setMeta('');

                    return j;
                })
                .catch(function (e) {
                    if (showErr) setMeta(mapNetworkError(e));
                    throw e;
                });
        }

        function pollMessages(renderBool) {
            if (!state.sessionIdInt || !state.visitorId) return Promise.resolve();
            var sid = parseInt(state.sessionIdInt, 10) || 0;
            var after = renderBool ? 0 : getPollCursor(sid);
            var q =
                '?visitor_id_string=' +
                encodeURIComponent(state.visitorId) +
                '&session_id_int=' +
                encodeURIComponent(String(state.sessionIdInt)) +
                '&after_id_int=' +
                encodeURIComponent(String(after));
            return apiJson('/api/support-chat/messages' + q).then(function (data) {
                applyPollPayload(data, renderBool);
            }).catch(function () {});
        }

        function startPoll() {
            stopPoll();
            state.pollTimer = window.setInterval(function () {
                pollMessages(!!state.openBool);
            }, 4500);
        }

        function stopPoll() {
            if (state.pollTimer) {
                window.clearInterval(state.pollTimer);
                state.pollTimer = null;
            }
        }

        function openPanel() {
            state.openBool = true;
            panel.style.display = 'flex';
            root.classList.add('ar-support-chat--open');
            stopPoll();
            log.innerHTML = '';
            state.afterIdInt = 0;
            state.unreadInt = 0;
            updateBadge();
            ensureSession(true)
                .then(function () {
                    return pollMessages(true);
                })
                .catch(function () {});
            startPoll();
            ta.focus();
        }

        function closePanel() {
            state.openBool = false;
            panel.style.display = 'none';
            root.classList.remove('ar-support-chat--open');
            stopPoll();
            startPoll();
        }

        bubble.addEventListener('click', function () {
            if (state.openBool) closePanel();
            else openPanel();
        });
        closeBtn.addEventListener('click', closePanel);

        function doSend() {
            if (sendingLocks) return;
            var txt = ta.value.trim();
            if (!txt) return;

            sendingLocks = true;
            clearSendChromeTimer();
            setSendChrome('busy', tr('support_chat_phase_session'));

            apiJson('/api/support-chat/session', {
                method: 'POST',
                body: JSON.stringify({
                    visitor_id_string: state.visitorId,
                    page_url_string: String(window.location.href || '').slice(0, 4096),
                }),
            })
                .then(function (sess) {
                    sendPhase.textContent = tr('support_chat_phase_message');
                    state.sessionIdInt = sess.session_id_int;
                    setStoredSessionId(state.sessionIdInt);
                    return apiJson('/api/support-chat/message', {
                        method: 'POST',
                        body: JSON.stringify({
                            visitor_id_string: state.visitorId,
                            session_id_int: state.sessionIdInt,
                            message_text_string: txt,
                        }),
                    });
                })
                .then(function () {
                    sendPhase.textContent = tr('support_chat_phase_sync');
                    ta.value = '';
                    return pollMessages(!!state.openBool);
                })
                .then(function () {
                    setSendChrome('ok', tr('support_chat_phase_done'));
                    send.disabled = false;
                    ta.disabled = false;
                    finishSendChromeToIdle(900);
                })
                .catch(function (e) {
                    var msg = (((e && e.message) || '') + '').toLowerCase();
                    if (e && e.status === 404 && msg.indexOf('support session not found') === -1) {
                        setMeta(tr('support_chat_error_api_connect'));
                    } else if (e && e.status === 503) {
                        setMeta(tr('support_chat_telegram_missing'));
                    } else if (msg.indexOf('support session not found') !== -1) {
                        setStoredSessionId(null);
                        state.sessionIdInt = null;
                        setMeta(tr('support_chat_error_session_missing'));
                    } else {
                        setMeta(
                            e && e.message ? String(e.message) : tr('support_chat_error_api_connect')
                        );
                    }
                    setSendChrome('err', '');
                    send.disabled = false;
                    ta.disabled = false;
                    finishSendChromeToIdle(1700);
                });
        }

        send.addEventListener('click', doSend);

        ta.addEventListener('keydown', function (ev) {
            if (ev.key !== 'Enter') return;
            if (ev.shiftKey || ev.altKey || ev.ctrlKey || ev.metaKey) return;
            ev.preventDefault();
            doSend();
        });

        panel.style.display = 'none';

        ensureSession(false)
            .then(function () {
                startPoll();
            })
            .catch(function () {});
    }

    window.SupportChat = {
        init: initOnce,
    };
})();
