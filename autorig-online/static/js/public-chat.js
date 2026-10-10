/* AutoRig public chat widget (Public chat · V3, 2026-10-10).
 * Owner: «и публичный чат посредине сделай внизу, чтобы все могли переписываться».
 *
 * A strip under the task viewer (bottom centre of #tv3-stage): a pill with the unread count, the last
 * message and who is online; it opens a panel with two rooms, everyone on the site and this model.
 * Server: /api/public-chat/* (autorig-public-chat.service, deploy/public-chat/public_chat.py), live
 * updates over Server-Sent Events. Every visible string goes through I18n.t('pchat_*'); the English
 * below is only the fallback. User text is only ever inserted as text nodes.
 */
(function () {
  'use strict';
  if (window.AutorigPublicChat) return;

  var API = '/api/public-chat';
  var UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  var BOT_UA = /\b(?:bot|crawler|spider)\b|googlebot|bingbot|yandex|slurp|bingpreview|mediapartners|facebookexternalhit|embedly|telegrambot/i;
  var MAX_DOM = 220;

  var EN = {
    pchat_title: 'Public chat',
    pchat_open: 'Open the public chat',
    pchat_close: 'Collapse',
    pchat_room_general: 'Everyone on AutoRig',
    pchat_room_task: 'Chat of this model',
    pchat_online: '{count} online',
    pchat_placeholder: 'Message everyone…',
    pchat_placeholder_task: 'Message about this model…',
    pchat_send: 'Send',
    pchat_emoji: 'Emoji',
    pchat_reply: 'Reply',
    pchat_replying: 'Reply to {name}',
    pchat_cancel: 'Cancel',
    pchat_translate: 'Translate into my language',
    pchat_translating: 'Translating…',
    pchat_translated: 'Translated',
    pchat_report: 'Report',
    pchat_reported: 'Reported. Thank you!',
    pchat_delete: 'Delete',
    pchat_mute: 'Mute for 1 hour',
    pchat_ban: 'Ban the author',
    pchat_restore: 'Restore',
    pchat_confirm: 'Are you sure?',
    pchat_empty: 'No messages yet. Say hi!',
    pchat_empty_task: 'Nobody has written about this model yet.',
    pchat_you: 'You',
    pchat_rename: 'Change my chat name',
    pchat_rename_guest: 'Sign in to choose your name',
    pchat_save: 'Save',
    pchat_astra_typing: 'Astra is typing…',
    pchat_astra_hint: 'Write @Astra to ask the AI',
    pchat_ai: 'AI',
    pchat_admin_badge: 'AutoRig team',
    pchat_verified: 'Signed in',
    pchat_new: '{count} new',
    pchat_removed: 'Message removed',
    pchat_model: '3D model',
    pchat_rules: 'Be kind. No ads, spam or adult content. Messages are public.',
    pchat_moderation: 'Moderation',
    pchat_kill_off: 'Hide the chat on the whole site',
    pchat_kill_on: 'Show the chat on the whole site',
    pchat_hidden_sitewide: 'The chat is hidden for visitors',
    pchat_clear_room: 'Clear this room',
    pchat_reports: 'Reported and hidden messages',
    pchat_back: 'Back',
    pchat_load_older: 'Older messages',
    pchat_language: 'Language: {lang}',
    pchat_err_rate_limited: 'Too fast. Wait a few seconds.',
    pchat_err_too_long: 'The message is too long.',
    pchat_err_empty: 'Type a message first.',
    pchat_err_links_not_allowed: 'Links to other sites are not allowed here.',
    pchat_err_too_many_links: 'Too many links in one message.',
    pchat_err_duplicate: 'You have already sent this.',
    pchat_err_repetitive: 'This looks like spam.',
    pchat_err_blocked_words: 'This message breaks the chat rules.',
    pchat_err_muted: 'You can write again at {time}.',
    pchat_err_banned: 'You cannot write in this chat.',
    pchat_err_chat_disabled: 'The chat is closed right now.',
    pchat_err_identity_required: 'Reload the page and try again.',
    pchat_err_room_unavailable: 'This room is not available.',
    pchat_err_guests_read_only: 'Sign in to write in the chat.',
    pchat_err_bad_name: 'This name is not allowed.',
    pchat_err_offline: 'No connection. Reconnecting…',
    pchat_err_generic: 'Something went wrong. Try again.',
    pchat_err_own_message: 'This is your own message.',
    pchat_err_busy: 'Busy right now. Try again in a minute.',
    pchat_err_chat_unavailable: 'The chat is restarting. One moment…',
    pchat_err_signin_required: 'Sign in to choose your name.',
    pchat_orig_show: 'Show original',
    pchat_tr_show: 'Show translation',
    pchat_translated_from: 'Translated from {lang}',
    pchat_author_open: "Open {name}'s models"
  };

  // ------------------------------------------------------------------ i18n
  function t(key, repl) {
    var i18n = window.I18n;
    if (i18n && typeof i18n.has === 'function' && i18n.has(key)) return i18n.t(key, repl);
    var text = EN[key] || key;
    if (repl) Object.keys(repl).forEach(function (k) { text = text.split('{' + k + '}').join(String(repl[k])); });
    return text;
  }
  function uiLang() {
    var lang = window.I18n && window.I18n.currentLang;
    if (!lang) { try { lang = localStorage.getItem('autorig_lang'); } catch (e) { lang = ''; } }
    return String(lang || navigator.language || 'en').slice(0, 2).toLowerCase();
  }
  function uiDir() {
    var i18n = window.I18n;
    if (i18n && typeof i18n.dir === 'function') return i18n.dir();
    return uiLang() === 'fa' ? 'rtl' : 'ltr';
  }
  function locale() {
    var i18n = window.I18n;
    return (i18n && typeof i18n.localeTag === 'function' && i18n.localeTag()) || uiLang();
  }
  function langName(code) {
    try { return new Intl.DisplayNames([locale()], { type: 'language' }).of(code) || code; } catch (e) { return code; }
  }

  // ------------------------------------------------------------------ icons (static, trusted markup)
  var I = {
    chat: '<svg viewBox="0 0 24 24" aria-hidden="true"><path class="pc-b1" d="M4 5.5h11a2 2 0 0 1 2 2V13a2 2 0 0 1-2 2H9l-4 3v-3H4a2 2 0 0 1-2-2V7.5a2 2 0 0 1 2-2z"/><path class="pc-b2" d="M19 9h1a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-1v2.5L15.5 18H12"/><circle class="pc-d d1" cx="6.5" cy="10.3" r="1"/><circle class="pc-d d2" cx="9.5" cy="10.3" r="1"/><circle class="pc-d d3" cx="12.5" cy="10.3" r="1"/></svg>',
    globe: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8.5"/><path d="M3.5 12h17M12 3.5c2.6 2.4 3.9 5.2 3.9 8.5s-1.3 6.1-3.9 8.5c-2.6-2.4-3.9-5.2-3.9-8.5s1.3-6.1 3.9-8.5z"/></svg>',
    cube: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l8 4.5v9L12 21l-8-4.5v-9z"/><path d="M12 12l8-4.5M12 12v9M12 12L4 7.5"/></svg>',
    down: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 9.5l6 6 6-6"/></svg>',
    up: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 14.5l6-6 6 6"/></svg>',
    send: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 12l16-7.5-6.5 16-2.6-6.9z"/><path d="M10.9 13.6l4.6-4.6"/></svg>',
    smile: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8.5"/><path d="M8.5 14.2c.9 1.3 2.1 2 3.5 2s2.6-.7 3.5-2"/><circle cx="9" cy="10" r=".9" class="pc-fill"/><circle cx="15" cy="10" r=".9" class="pc-fill"/></svg>',
    reply: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M10 7L5 12l5 5"/><path d="M5.5 12H14a5 5 0 0 1 5 5v1"/></svg>',
    translate: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h9M8.5 4v2c0 4-2 7-4.5 8.5M6.5 10c1 2 2.8 3.6 5 4.5"/><path d="M13 20l4-9 4 9M14.4 17h5.2"/></svg>',
    flag: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 21V4M6 4.5h11l-2 4 2 4H6"/></svg>',
    trash: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 7h14M10 7V5h4v2M7 7l1 12h8l1-12M10.5 10.5v5M13.5 10.5v5"/></svg>',
    mute: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9.5h3.5L12 6v12l-4.5-3.5H4z"/><path d="M16 9.5l4 5M20 9.5l-4 5"/></svg>',
    ban: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M6.4 6.4l11.2 11.2"/></svg>',
    restore: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4.5 12a7.5 7.5 0 1 0 2.2-5.3"/><path d="M4.5 4.5v4h4"/></svg>',
    shield: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l7 3v5.5c0 4.3-3 7.9-7 9.5-4-1.6-7-5.2-7-9.5V6z"/><path d="M9 12l2 2 4-4"/></svg>',
    power: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3.5v8"/><path d="M7.2 6.6a7 7 0 1 0 9.6 0"/></svg>',
    broom: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 4l6 6M15.5 8.5L9 15"/><path d="M9 15c-2.5-1-5 0-5.5 5 5-.5 6-3 5.5-5z"/><path d="M5 18.5l1.5-1.5"/></svg>',
    pencil: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 19l1-4.5L15.5 5a2.1 2.1 0 0 1 3 3L9 17.5z"/><path d="M13.5 7l3 3"/></svg>',
    check: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12.5L9.5 17 19 7.5"/></svg>',
    x: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 7l10 10M17 7L7 17"/></svg>',
    spark: '<svg viewBox="0 0 24 24" aria-hidden="true"><path class="pc-fill" d="M12 2.5l1.9 5.6 5.6 1.9-5.6 1.9L12 17.5l-1.9-5.6L4.5 10l5.6-1.9z"/><path class="pc-fill" d="M18.5 15l.8 2.2 2.2.8-2.2.8-.8 2.2-.8-2.2-2.2-.8 2.2-.8z"/></svg>',
    verified: '<svg viewBox="0 0 24 24" aria-hidden="true"><path class="pc-fill" d="M12 2.8l2.3 1.7 2.9-.1.9 2.7 2.3 1.7-.9 2.7.9 2.7-2.3 1.7-.9 2.7-2.9-.1L12 21.2l-2.3-1.7-2.9.1-.9-2.7-2.3-1.7.9-2.7-.9-2.7 2.3-1.7.9-2.7 2.9.1z"/><path class="pc-tick" d="M8.6 12.2l2.2 2.2 4.6-4.6"/></svg>',
    open: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 5h5v5M19 5l-8 8M10 6H6a1 1 0 0 0-1 1v11a1 1 0 0 0 1 1h11a1 1 0 0 0 1-1v-4"/></svg>',
    dots: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle class="pc-fill" cx="6" cy="12" r="1.6"/><circle class="pc-fill" cx="12" cy="12" r="1.6"/><circle class="pc-fill" cx="18" cy="12" r="1.6"/></svg>',
    person: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="8" r="3.4"/><path d="M5 20c.8-3.7 3.5-5.6 7-5.6s6.2 1.9 7 5.6"/></svg>'
  };

  // ------------------------------------------------------------------ flags (16x11, simplified)
  function stripes(colors, vertical) {
    var n = colors.length, out = '';
    colors.forEach(function (c, i) {
      out += vertical
        ? '<rect x="' + (16 / n * i) + '" y="0" width="' + (16 / n + 0.05) + '" height="11" fill="' + c + '"/>'
        : '<rect x="0" y="' + (11 / n * i) + '" width="16" height="' + (11 / n + 0.05) + '" fill="' + c + '"/>';
    });
    return out;
  }
  function star(cx, cy, r, fill) {
    var pts = [];
    for (var i = 0; i < 10; i++) {
      var a = -Math.PI / 2 + i * Math.PI / 5, rr = i % 2 ? r * 0.42 : r;
      pts.push((cx + rr * Math.cos(a)).toFixed(2) + ',' + (cy + rr * Math.sin(a)).toFixed(2));
    }
    return '<polygon points="' + pts.join(' ') + '" fill="' + fill + '"/>';
  }
  var FLAG = {
    en: '<rect width="16" height="11" fill="#012169"/><path d="M0 0L16 11M16 0L0 11" stroke="#fff" stroke-width="2.2"/><path d="M0 0L16 11M16 0L0 11" stroke="#c8102e" stroke-width="0.9"/><path d="M8 0v11M0 5.5h16" stroke="#fff" stroke-width="3.4"/><path d="M8 0v11M0 5.5h16" stroke="#c8102e" stroke-width="2"/>',
    ru: stripes(['#fff', '#1c57a5', '#d52b1e']),
    fa: stripes(['#239f40', '#fff', '#da0000']) + '<circle cx="8" cy="5.5" r="1.1" fill="none" stroke="#da0000" stroke-width=".6"/>',
    zh: '<rect width="16" height="11" fill="#de2910"/>' + star(3.4, 3, 1.9, '#ffde00') + star(6.6, 1.4, 0.55, '#ffde00') + star(7.5, 2.8, 0.55, '#ffde00') + star(7.5, 4.6, 0.55, '#ffde00') + star(6.6, 5.9, 0.55, '#ffde00'),
    hi: stripes(['#ff9933', '#fff', '#138808']) + '<circle cx="8" cy="5.5" r="1.25" fill="none" stroke="#000080" stroke-width=".5"/>',
    de: stripes(['#000', '#dd0000', '#ffce00']),
    fr: stripes(['#0055a4', '#fff', '#ef4135'], true),
    it: stripes(['#009246', '#fff', '#ce2b37'], true),
    es: '<rect width="16" height="11" fill="#aa151b"/><rect y="2.75" width="16" height="5.5" fill="#f1bf00"/>',
    uk: stripes(['#0057b7', '#ffd700']),
    pl: stripes(['#fff', '#dc143c']),
    id: stripes(['#ce1126', '#fff']),
    nl: stripes(['#ae1c28', '#fff', '#21468b']),
    ja: '<rect width="16" height="11" fill="#fff"/><circle cx="8" cy="5.5" r="3" fill="#bc002d"/>',
    tr: '<rect width="16" height="11" fill="#e30a17"/><circle cx="6.3" cy="5.5" r="2.8" fill="#fff"/><circle cx="7" cy="5.5" r="2.25" fill="#e30a17"/>' + star(10, 5.5, 1.2, '#fff'),
    pt: '<rect width="16" height="11" fill="#da291c"/><rect width="6.4" height="11" fill="#046a38"/><circle cx="6.4" cy="5.5" r="1.8" fill="#ffe900"/>',
    vi: '<rect width="16" height="11" fill="#da251d"/>' + star(8, 5.7, 3, '#ffff00'),
    kk: '<rect width="16" height="11" fill="#00afca"/><circle cx="8" cy="5" r="2" fill="#fec50c"/>',
    ko: '<rect width="16" height="11" fill="#fff"/><circle cx="8" cy="5.5" r="2.6" fill="#0047a0"/><path d="M5.4 5.5a2.6 2.6 0 0 1 5.2 0a1.3 1.3 0 0 1-2.6 0a1.3 1.3 0 0 0-2.6 0z" fill="#cd2e3a"/>'
  };
  function flagNode(code) {
    code = String(code || '').toLowerCase();
    var span = document.createElement('span');
    span.className = 'pc-flag';
    span.title = t('pchat_language', { lang: langName(code || 'en') });
    if (FLAG[code]) {
      span.innerHTML = '<svg viewBox="0 0 16 11" aria-hidden="true">' + FLAG[code] + '</svg>';
    } else {
      span.className += ' pc-flag-code';
      span.textContent = (code || '?').toUpperCase().slice(0, 3);
    }
    return span;
  }

  // ------------------------------------------------------------------ dom helpers
  function el(tag, cls, attrs) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (attrs) Object.keys(attrs).forEach(function (k) {
      if (attrs[k] === null || attrs[k] === undefined) return;
      if (k === 'html') node.innerHTML = attrs[k]; // trusted icon markup only
      else if (k === 'text') node.textContent = attrs[k];
      else node.setAttribute(k, attrs[k]);
    });
    return node;
  }
  function iconButton(cls, icon, key, extra) {
    var b = el('button', 'pc-ib ' + (cls || ''), { type: 'button', html: icon, 'data-i18n-title': key });
    b.title = t(key);
    b.setAttribute('aria-label', t(key));
    if (extra) Object.keys(extra).forEach(function (k) { b.setAttribute(k, extra[k]); });
    return b;
  }
  function store(key, value) {
    try {
      if (value === undefined) return localStorage.getItem(key);
      localStorage.setItem(key, value);
    } catch (e) { /* private mode */ }
    return null;
  }
  function hue(tag) {
    var n = parseInt(String(tag || '0').slice(0, 6), 16);
    return isNaN(n) ? 220 : n % 360;
  }

  // ------------------------------------------------------------------ state
  var S = {
    taskId: '', rooms: ['general'], active: 'general', enabled: true, me: { kind: 'none' }, features: {},
    msgs: { general: [] }, byId: {}, gone: {}, tr: {}, trWanted: {}, online: {}, unread: {}, oldest: {},
    lastId: 0, es: null, esTimer: 0, hiddenTimer: 0, open: false, replyTo: null, typing: {}, typingTimer: {},
    adminView: false, config: null, cid: '', sending: false, started: false, loaded: {},
    orig: {}, pend: {}, trErr: {}, avUrl: {}
  };
  var RTL_LANGS = ['fa', 'ar', 'he', 'ur', 'ps', 'yi', 'dv', 'ckb', 'sd', 'ug'];
  var E = {};

  function roomKey(room) { return room === 'general' ? 'general' : 'task'; }
  function normLang(tag) {
    tag = String(tag || '').toLowerCase().replace('_', '-').split('-')[0];
    tag = { iw: 'he', nb: 'no', nn: 'no', 'in': 'id', tl: 'fil' }[tag] || tag;
    return /^[a-z]{2,3}$/.test(tag) ? tag : '';
  }
  function readerLang() {
    // The language the visitor reads in = their browser locale (owner 2026-10-10), then what the site API knows
    // for them (/api/me/language through the chat state), then the interface language. The interface language
    // they picked in the menu never decides it.
    var list = (navigator.languages && navigator.languages.length) ? navigator.languages : [navigator.language];
    return normLang(list && list[0]) || normLang(S.me && S.me.lang) || normLang(S.features && S.features.reader_lang) ||
      normLang(uiLang()) || 'en';
  }
  function myLang() { return readerLang(); }
  function authorHref(handle) {
    var ui = uiLang();
    return (['ru', 'zh', 'hi', 'fa'].indexOf(ui) >= 0 ? '/' + ui : '') + '/author/' + handle;
  }
  function readOnly() {
    var me = S.me || {};
    if (me.kind === 'none' || me.admin) return null;
    if (me.banned) return 'banned';
    if (me.muted_until && me.muted_until * 1000 > Date.now()) return 'muted';
    if (!S.enabled) return 'chat_disabled';
    if (me.kind === 'guest' && S.features.guests_can_post === false) return 'guests_read_only';
    return null;
  }
  function isMine(m) { return !!(S.me && S.me.tag && m.author && m.author.tag === S.me.tag); }

  // ------------------------------------------------------------------ api
  function api(path, body) {
    var opts = { credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' } };
    if (body !== undefined) {
      opts.method = 'POST';
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    return fetch(API + path, opts).then(function (r) {
      return r.text().then(function (txt) {
        var doc = null;
        try { doc = txt ? JSON.parse(txt) : null; } catch (e) { doc = null; }
        if (!r.ok) {
          var code = (doc && doc.detail && doc.detail.error_string) || (r.status === 429 ? 'rate_limited' : 'generic');
          var error = new Error(code);
          error.code = code;
          error.detail = doc && doc.detail;
          throw error;
        }
        return doc || {};
      });
    });
  }
  function errorText(error) {
    var code = (error && error.code) || 'generic';
    if (code === 'muted' && error.detail && error.detail.until) {
      var time = new Date(error.detail.until * 1000);
      return t('pchat_err_muted', { time: time.toLocaleTimeString(locale(), { hour: '2-digit', minute: '2-digit' }) });
    }
    var key = 'pchat_err_' + code;
    return (EN[key] || (window.I18n && window.I18n.has && window.I18n.has(key))) ? t(key) : t('pchat_err_generic');
  }

  // ------------------------------------------------------------------ build
  function build(stage) {
    var strip = el('div', 'pc-strip', { id: 'pc-strip' });
    var pill = el('button', 'pc-pill', { type: 'button', 'aria-expanded': 'false', 'aria-controls': 'pc-panel' });
    pill.innerHTML = '<span class="pc-ic">' + I.chat + '<b class="pc-badge" hidden></b></span>' +
      '<span class="pc-prev"><span class="pc-prev-flag"></span><b class="pc-prev-name"></b><span class="pc-prev-text"></span></span>' +
      '<span class="pc-on" hidden><i></i><b></b></span><span class="pc-chev">' + I.up + '</span>';
    strip.appendChild(pill);

    var panel = el('div', 'pc-panel', { id: 'pc-panel', role: 'dialog', hidden: '' });
    var head = el('div', 'pc-head');
    var rooms = el('div', 'pc-rooms', { role: 'tablist' });
    var rGeneral = iconButton('pc-room', I.globe + '<i class="pc-dot"></i>', 'pchat_room_general', { 'data-room': 'general', role: 'tab' });
    var rTask = iconButton('pc-room', I.cube + '<i class="pc-dot"></i>', 'pchat_room_task', { 'data-room': 'task', role: 'tab', hidden: '' });
    rooms.appendChild(rGeneral);
    rooms.appendChild(rTask);
    var online = el('span', 'pc-online', { html: '<i></i><b></b>' });
    var me = el('button', 'pc-me', { type: 'button' });
    me.innerHTML = '<span class="pc-av pc-av-me"></span><span class="pc-me-name"></span><span class="pc-me-edit">' + I.pencil + '</span>';
    var admin = iconButton('pc-admin', I.shield, 'pchat_moderation', { hidden: '' });
    var close = iconButton('pc-x', I.down, 'pchat_close');
    head.appendChild(rooms);
    head.appendChild(online);
    head.appendChild(el('span', 'pc-sp'));
    head.appendChild(me);
    head.appendChild(admin);
    head.appendChild(close);

    var adminBar = el('div', 'pc-adminbar', { hidden: '' });
    var kill = iconButton('pc-kill', I.power, 'pchat_kill_off');
    var clear = iconButton('pc-clear', I.broom, 'pchat_clear_room');
    var reports = iconButton('pc-reports', I.flag, 'pchat_reports');
    var banner = el('span', 'pc-banner', { 'data-i18n': 'pchat_hidden_sitewide', text: t('pchat_hidden_sitewide'), hidden: '' });
    adminBar.appendChild(kill);
    adminBar.appendChild(clear);
    adminBar.appendChild(reports);
    adminBar.appendChild(banner);

    var list = el('div', 'pc-list', { role: 'log', 'aria-live': 'polite' });
    var older = el('button', 'pc-older', { type: 'button', hidden: '', html: I.up });
    older.title = t('pchat_load_older');
    var empty = el('div', 'pc-empty', { hidden: '' });
    empty.innerHTML = '<span class="pc-empty-ic">' + I.chat + '</span><span class="pc-empty-t"></span><span class="pc-empty-h">' + I.spark + '<span></span></span>';
    var jump = el('button', 'pc-jump', { type: 'button', hidden: '' });
    jump.innerHTML = I.down + '<b></b>';
    var typing = el('div', 'pc-typing', { hidden: '' });
    typing.innerHTML = '<span class="pc-av pc-av-astra">' + I.spark + '</span><span class="pc-typing-dots"><i></i><i></i><i></i></span><span class="pc-typing-t"></span>';
    var note = el('div', 'pc-note', { role: 'status', hidden: '' });
    var replyBar = el('div', 'pc-replybar', { hidden: '' });
    replyBar.innerHTML = '<span class="pc-replybar-ic">' + I.reply + '</span><span class="pc-replybar-t"></span>';
    var replyX = iconButton('pc-replybar-x', I.x, 'pchat_cancel');
    replyBar.appendChild(replyX);

    var form = el('form', 'pc-form', { autocomplete: 'off' });
    var emoBtn = iconButton('pc-emo-btn', I.smile, 'pchat_emoji');
    var input = el('textarea', 'pc-input', { rows: '1', maxlength: '500', dir: 'auto', enterkeyhint: 'send' });
    var send = el('button', 'pc-send', { type: 'submit', html: I.send });
    send.title = t('pchat_send');
    send.setAttribute('aria-label', t('pchat_send'));
    form.appendChild(emoBtn);
    form.appendChild(input);
    form.appendChild(send);
    var emo = el('div', 'pc-emo', { hidden: '' });
    ['👍', '❤️', '🔥', '😂', '😮', '🙏', '👏', '🎉', '💯', '✨', '😍', '🤩', '😎', '🤔', '😅', '🥳', '👋', '🙂',
     '😢', '🫡', '🤖', '🦾', '💃', '🕺', '🎮', '🎬', '🎨', '🧩', '⚙️', '🚀', '⭐', '🏆', '💡', '👀', '✅', '❌',
     '❓', '🧠', '🐉', '🦄'].forEach(function (ch) {
      emo.appendChild(el('button', 'pc-emo-i', { type: 'button', text: ch }));
    });

    panel.appendChild(head);
    panel.appendChild(adminBar);
    panel.appendChild(older);
    panel.appendChild(list);
    panel.appendChild(empty);
    panel.appendChild(jump);
    panel.appendChild(typing);
    panel.appendChild(note);
    panel.appendChild(replyBar);
    panel.appendChild(emo);
    panel.appendChild(form);
    strip.appendChild(panel);
    stage.appendChild(strip);

    E = { stage: stage, strip: strip, pill: pill, panel: panel, rGeneral: rGeneral, rTask: rTask, online: online,
      me: me, admin: admin, close: close, adminBar: adminBar, kill: kill, clear: clear, reports: reports,
      banner: banner, list: list, older: older, empty: empty, jump: jump, typing: typing, note: note,
      replyBar: replyBar, replyX: replyX, form: form, emoBtn: emoBtn, input: input, send: send, emo: emo,
      badge: pill.querySelector('.pc-badge'), prevFlag: pill.querySelector('.pc-prev-flag'),
      prevName: pill.querySelector('.pc-prev-name'), prevText: pill.querySelector('.pc-prev-text'),
      pillOn: pill.querySelector('.pc-on') };
    wire();
    labels();
  }

  // ------------------------------------------------------------------ labels / direction
  function labels() {
    if (!E.strip) return;
    var dir = uiDir();
    E.strip.setAttribute('dir', dir);
    E.strip.setAttribute('lang', uiLang());
    E.pill.setAttribute('aria-label', t('pchat_open'));
    E.pill.title = t('pchat_title');
    E.panel.setAttribute('aria-label', t('pchat_title'));
    [E.rGeneral, E.rTask, E.admin, E.close, E.kill, E.clear, E.reports, E.emoBtn, E.replyX].forEach(function (b) {
      var key = b.getAttribute('data-i18n-title');
      if (key) { b.title = t(key); b.setAttribute('aria-label', t(key)); }
    });
    E.kill.title = t(S.enabled ? 'pchat_kill_off' : 'pchat_kill_on');
    E.kill.setAttribute('aria-label', E.kill.title);
    E.banner.textContent = t('pchat_hidden_sitewide');
    E.send.title = t('pchat_send');
    E.send.setAttribute('aria-label', t('pchat_send'));
    E.older.title = t('pchat_load_older');
    E.older.setAttribute('aria-label', t('pchat_load_older'));
    var ro = readOnly();
    E.input.placeholder = ro ? errorText({ code: ro, detail: { until: S.me && S.me.muted_until } })
      : t(S.active === 'general' ? 'pchat_placeholder' : 'pchat_placeholder_task');
    E.input.disabled = !!ro;
    E.send.disabled = !!ro;
    E.emoBtn.disabled = !!ro;
    E.input.title = t('pchat_rules');
    E.typing.querySelector('.pc-typing-t').textContent = t('pchat_astra_typing');
    E.empty.querySelector('.pc-empty-t').textContent = t(S.active === 'general' ? 'pchat_empty' : 'pchat_empty_task');
    E.empty.querySelector('.pc-empty-h span').textContent = t('pchat_astra_hint');
    meChip();
    online();
    pill();
    rerenderTimes();
  }

  function meChip() {
    var me = S.me || {};
    var name = me.name || t('pchat_you');
    E.me.querySelector('.pc-me-name').textContent = name;
    var av = E.me.querySelector('.pc-av-me');
    av.textContent = name.slice(0, 1).toUpperCase();
    av.classList.remove('pc-av-img');
    av.style.setProperty('--h', hue(me.tag));
    if (me.av) avatarImg(av, me.av);
    E.me.classList.toggle('can', !!me.can_rename);
    E.me.title = me.can_rename ? t('pchat_rename') : t('pchat_rename_guest');
    E.me.hidden = !me.name;
    E.admin.hidden = !me.admin;
  }

  // ------------------------------------------------------------------ rendering messages
  var URL_RE = /(https?:\/\/[^\s<>"']+|www\.[^\s<>"']+)/gi;
  var MENTION_RE = /(@astra\b|@[\w؀-ۿЀ-ӿ]{2,24})/gi;

  function appendText(parent, text) {
    var last = 0;
    String(text).replace(URL_RE, function (url, _g, offset) {
      if (offset > last) appendMentions(parent, text.slice(last, offset));
      var clean = url.replace(/[.,;:!?)»\]}'"]+$/, '');
      var tail = url.slice(clean.length);
      var href = /^https?:/i.test(clean) ? clean : 'https://' + clean;
      var own = /^https?:\/\/(www\.)?autorig\.online(\/|$|\?)/i.test(href);
      var a = el('a', 'pc-link', { href: href, dir: 'ltr' });
      a.textContent = clean.length > 64 ? clean.slice(0, 61) + '…' : clean;
      if (!own) {
        a.target = '_blank';
        a.rel = 'nofollow ugc noopener noreferrer';
      }
      parent.appendChild(a);
      if (tail) parent.appendChild(document.createTextNode(tail));
      last = offset + url.length;
      return url;
    });
    if (last < text.length) appendMentions(parent, text.slice(last));
  }
  function appendMentions(parent, text) {
    var last = 0;
    text.replace(MENTION_RE, function (m, _g, offset) {
      if (offset > last) parent.appendChild(document.createTextNode(text.slice(last, offset)));
      parent.appendChild(el('span', 'pc-mention', { text: m }));
      last = offset + m.length;
      return m;
    });
    if (last < text.length) parent.appendChild(document.createTextNode(text.slice(last)));
  }

  function fmtTime(ts) {
    var d = new Date(ts * 1000), now = new Date();
    var same = d.toDateString() === now.toDateString();
    try {
      return d.toLocaleString(locale(), same ? { hour: '2-digit', minute: '2-digit' }
        : { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
    } catch (e) { return d.toISOString().slice(11, 16); }
  }
  function rerenderTimes() {
    if (!E.list) return;
    E.list.querySelectorAll('time[data-ts]').forEach(function (n) { n.textContent = fmtTime(Number(n.getAttribute('data-ts'))); });
  }

  function avatarImg(av, url) {
    var img = el('img', '', { src: url, alt: '', width: '26', height: '26', decoding: 'async' });
    img.addEventListener('error', function () { img.remove(); av.classList.remove('pc-av-img'); }, { once: true });
    av.classList.add('pc-av-img');
    av.appendChild(img);
  }
  function avatar(m) {
    var kind = m.author && m.author.kind;
    if (kind === 'astra') return el('span', 'pc-av pc-av-astra', { html: I.spark });
    var name = (m.author && m.author.name) || '?';
    var handle = m.author && m.author.handle;
    var av = el('span', 'pc-av' + (kind === 'admin' ? ' pc-av-admin' : ''), { text: name.slice(0, 1).toUpperCase() });
    av.style.setProperty('--h', hue(m.author && m.author.tag));
    if (!handle) return av;
    av.setAttribute('data-h', handle);
    var url = S.avUrl[handle] || m.author.av;
    if (url) avatarImg(av, url);
    var link = el('a', 'pc-av-link', { href: authorHref(handle) });
    link.title = t('pchat_author_open', { name: name });
    link.setAttribute('aria-label', link.title);
    link.appendChild(av);
    return link;
  }

  // What a reader sees of a message: the translation into their language when there is one (the original one
  // tap away), the original while it is being translated.
  function viewInfo(m) {
    var rl = readerLang();
    var key = m.id + ':' + rl;
    var kind = m.author && m.author.kind;
    var needs = !!(S.features.translate !== false && m.lang && m.lang !== rl && kind !== 'system');
    var tr = S.tr[key];
    var same = tr && tr.replace(/\s+/g, ' ').trim() === String(m.text || '').replace(/\s+/g, ' ').trim();
    var has = !!tr && !same;
    return {
      rl: rl, key: key, needs: needs, has: has,
      useTr: has && !S.orig[m.id],
      pending: needs && !tr && !S.trErr[key] && (!!S.pend[key] || S.features.auto_translate !== false)
    };
  }
  function shownText(m) {
    var v = viewInfo(m);
    return v.useTr ? S.tr[v.key] : (m.text || '');
  }
  function trBar(m, v) {
    var b = el('button', 'pc-trbar' + (v.useTr ? '' : ' pc-trbar-orig'), { type: 'button', 'data-act': 'orig' });
    b.appendChild(el('span', 'pc-trbar-ic', { html: I.translate }));
    b.appendChild(el('span', 'pc-trbar-t', {
      text: v.useTr ? t('pchat_translated_from', { lang: langName(m.lang) }) + ' · ' + t('pchat_orig_show') : t('pchat_tr_show')
    }));
    return b;
  }

  function renderMsg(m) {
    var kind = (m.author && m.author.kind) || 'guest';
    var row = el('div', 'pc-msg pc-k-' + kind + (isMine(m) ? ' pc-mine' : ''), { 'data-id': m.id });
    row.appendChild(avatar(m));
    var body = el('div', 'pc-body');
    var meta = el('div', 'pc-meta');
    var handle = m.author && m.author.handle;
    var name = handle ? el('a', 'pc-name pc-name-link', { dir: 'auto', href: authorHref(handle), text: (m.author && m.author.name) || '?' })
      : el('b', 'pc-name', { dir: 'auto', text: (m.author && m.author.name) || '?' });
    if (handle) name.title = t('pchat_author_open', { name: (m.author && m.author.name) || '' });
    name.style.setProperty('--h', hue(m.author && m.author.tag));
    meta.appendChild(name);
    if (kind === 'astra') meta.appendChild(el('em', 'pc-ai', { text: t('pchat_ai') }));
    else if (kind === 'admin') meta.appendChild(el('span', 'pc-badge-admin', { html: I.shield, title: t('pchat_admin_badge') }));
    else if (kind === 'user') meta.appendChild(el('span', 'pc-ver', { html: I.verified, title: t('pchat_verified') }));
    meta.appendChild(flagNode(m.lang || (m.author && m.author.lang)));
    meta.appendChild(el('time', 'pc-time', { 'data-ts': m.ts, text: fmtTime(m.ts) }));
    body.appendChild(meta);
    if (m.reply && m.reply_to) {
      var q = el('button', 'pc-quote', { type: 'button', 'data-goto': m.reply_to });
      q.appendChild(el('span', 'pc-quote-ic', { html: I.reply }));
      q.appendChild(el('b', '', { dir: 'auto', text: m.reply.name || '' }));
      q.appendChild(el('span', 'pc-quote-t', { dir: 'auto', text: S.gone[m.reply_to] ? t('pchat_removed') : (m.reply.text || '') }));
      body.appendChild(q);
    }
    var view = viewInfo(m);
    var text = el('div', 'pc-text' + (view.pending ? ' pc-text-pend' : ''), { dir: 'auto' });
    if (view.useTr) {
      text.setAttribute('dir', RTL_LANGS.indexOf(view.rl) >= 0 ? 'rtl' : 'ltr');
      text.setAttribute('lang', view.rl);
    }
    appendText(text, shownText(m));
    body.appendChild(text);
    if (view.has) body.appendChild(trBar(m, view));
    (m.cards || []).forEach(function (c) {
      if (!c || c.type !== 'task' || !UUID.test(String(c.id || ''))) return;
      var card = el('a', 'pc-card', { href: '/task?id=' + c.id });
      var thumb = el('span', 'pc-card-th', { html: I.cube });
      if (c.thumb && /^\/thumb\/[0-9a-f-]{36}$/.test(c.thumb)) {
        var img = el('img', '', { src: c.thumb, alt: '', loading: 'lazy' });
        img.addEventListener('error', function () { img.remove(); }, { once: true });
        thumb.appendChild(img);
      }
      card.appendChild(thumb);
      var info = el('span', 'pc-card-i');
      info.appendChild(el('b', '', { dir: 'auto', text: c.title || t('pchat_model') }));
      info.appendChild(el('small', '', { text: t('pchat_model') + ' · ' + String(c.id).slice(0, 8) }));
      card.appendChild(info);
      card.appendChild(el('span', 'pc-card-go', { html: I.open }));
      body.appendChild(card);
    });
    row.appendChild(body);

    var acts = el('div', 'pc-acts');
    if (kind !== 'system') acts.appendChild(iconButton('', I.reply, 'pchat_reply', { 'data-act': 'reply' }));
    if (view.needs) {
      acts.appendChild(iconButton('pc-act-tr' + (view.has && !view.useTr ? ' pc-on' : ''), I.translate,
        view.has ? (view.useTr ? 'pchat_orig_show' : 'pchat_tr_show') : 'pchat_translate', { 'data-act': 'tr' }));
    }
    if (!isMine(m) && kind !== 'system') acts.appendChild(iconButton('', I.flag, 'pchat_report', { 'data-act': 'report' }));
    if (S.me && S.me.admin) {
      acts.appendChild(iconButton('pc-act-bad', I.trash, 'pchat_delete', { 'data-act': 'del' }));
      if (kind === 'guest' || kind === 'user') {
        acts.appendChild(iconButton('pc-act-bad', I.mute, 'pchat_mute', { 'data-act': 'mute' }));
        acts.appendChild(iconButton('pc-act-bad', I.ban, 'pchat_ban', { 'data-act': 'ban' }));
      }
    }
    row.appendChild(acts);
    return row;
  }

  function refreshRow(id) {
    var m = S.byId[id];
    if (!m || !E.list || S.adminView) return;
    var node = E.list.querySelector('.pc-msg[data-id="' + id + '"]');
    if (!node) return;
    var fresh = renderMsg(m);
    if (node.classList.contains('pc-act-on')) fresh.classList.add('pc-act-on');
    var keep = nearBottom();
    node.replaceWith(fresh);
    if (keep && E.list.lastElementChild === fresh) toBottom();
  }

  function nearBottom() {
    var l = E.list;
    return l.scrollHeight - l.scrollTop - l.clientHeight < 80;
  }
  function toBottom() { E.list.scrollTop = E.list.scrollHeight; }

  function renderList() {
    if (!E.list) return;
    E.list.textContent = '';
    if (S.adminView) return renderAdminView();
    var msgs = S.msgs[S.active] || [];
    msgs.slice(-MAX_DOM).forEach(function (m) { E.list.appendChild(renderMsg(m)); });
    E.empty.hidden = msgs.length > 0;
    E.older.hidden = !(msgs.length >= 30 && !S.oldest[S.active]);
    toBottom();
    E.jump.hidden = true;
  }

  function addMsg(m, fromStream) {
    if (!m || !m.id || S.byId[m.id] || S.gone[m.id]) return false;
    var room = m.room;
    if (S.rooms.indexOf(room) === -1) return false;
    var arr = S.msgs[room] || (S.msgs[room] = []);
    S.byId[m.id] = m;
    if (m.tr) Object.keys(m.tr).forEach(function (lang) { S.tr[m.id + ':' + lang] = m.tr[lang]; });
    if (m.author && m.author.handle && m.author.av) S.avUrl[m.author.handle] = m.author.av;
    if (!arr.length || arr[arr.length - 1].id < m.id) arr.push(m);
    else { arr.push(m); arr.sort(function (a, b) { return a.id - b.id; }); }
    if (arr.length > 400) arr.splice(0, arr.length - 400).forEach(function (x) { delete S.byId[x.id]; });
    if (m.id > S.lastId) S.lastId = m.id;
    var visible = S.open && !S.adminView && room === S.active && document.visibilityState === 'visible';
    if (fromStream && !isMine(m)) {
      if (!visible) S.unread[room] = (S.unread[room] || 0) + 1;
      flash();
    }
    if (visible) store('pchat_seen_' + roomKey(room), String(m.id));
    if (S.open && !S.adminView && room === S.active) {
      var stick = nearBottom() || isMine(m);
      var last = E.list.lastElementChild;
      var node = renderMsg(m);
      if (last && Number(last.getAttribute('data-id')) > m.id) { renderList(); return true; }
      E.list.appendChild(node);
      while (E.list.children.length > MAX_DOM) E.list.removeChild(E.list.firstElementChild);
      E.empty.hidden = true;
      if (stick) toBottom();
      else if (fromStream) {
        S.jumpCount = (S.jumpCount || 0) + 1;
        E.jump.hidden = false;
        E.jump.querySelector('b').textContent = t('pchat_new', { count: S.jumpCount });
      }
    }
    if (fromStream && m.author && m.author.kind === 'astra') setTyping(room, false);
    pill();
    dots();
    return true;
  }

  function removeMsgs(ids) {
    (ids || []).forEach(function (id) {
      id = Number(id);
      S.gone[id] = true;
      var m = S.byId[id];
      if (m) {
        var arr = S.msgs[m.room] || [];
        var i = arr.indexOf(m);
        if (i >= 0) arr.splice(i, 1);
        delete S.byId[id];
      }
      if (E.list) {
        var node = E.list.querySelector('.pc-msg[data-id="' + id + '"]');
        if (node && !S.adminView) node.remove();
        E.list.querySelectorAll('.pc-quote[data-goto="' + id + '"] .pc-quote-t').forEach(function (q) { q.textContent = t('pchat_removed'); });
      }
    });
    if (E.empty) E.empty.hidden = (S.msgs[S.active] || []).length > 0 || S.adminView;
    pill();
  }

  // ------------------------------------------------------------------ pill / dots / online
  function pill() {
    if (!E.pill) return;
    var msgs = S.msgs[S.active] || [];
    var last = msgs[msgs.length - 1];
    var total = 0;
    S.rooms.forEach(function (r) { total += S.unread[r] || 0; });
    E.badge.hidden = !total;
    E.badge.textContent = total > 99 ? '99+' : String(total);
    E.pill.classList.toggle('pc-has-unread', total > 0);
    E.prevFlag.textContent = '';
    if (last) {
      E.prevFlag.appendChild(flagNode(last.lang || (last.author && last.author.lang)));
      E.prevName.textContent = (last.author && last.author.name) || '';
      E.prevName.style.setProperty('--h', hue(last.author && last.author.tag));
      E.prevText.textContent = String(shownText(last)).replace(/\s+/g, ' ');
      E.prevText.setAttribute('dir', 'auto');
    } else {
      E.prevName.textContent = '';
      E.prevText.textContent = t('pchat_title');
    }
    E.pill.classList.toggle('pc-quiet', !last);
  }
  function dots() {
    if (!E.rGeneral) return;
    E.rGeneral.classList.toggle('pc-unread', !!S.unread.general && S.active !== 'general');
    var task = S.rooms[1];
    E.rTask.classList.toggle('pc-unread', !!(task && S.unread[task]) && S.active !== task);
  }
  function online() {
    if (!E.online) return;
    var n = S.online[S.active] || 0;
    var label = t('pchat_online', { count: n });
    E.online.querySelector('b').textContent = window.I18n && window.I18n.formatNumber ? window.I18n.formatNumber(n) : String(n);
    E.online.title = label;
    E.online.hidden = !n;
    var all = S.online.general || 0;
    E.pillOn.hidden = !all;
    E.pillOn.querySelector('b').textContent = window.I18n && window.I18n.formatNumber ? window.I18n.formatNumber(all) : String(all);
    E.pillOn.title = t('pchat_online', { count: all });
  }
  function flash() {
    E.pill.classList.remove('pc-hot');
    void E.pill.offsetWidth;
    E.pill.classList.add('pc-hot');
  }
  function setTyping(room, on) {
    clearTimeout(S.typingTimer[room]);
    S.typing[room] = !!on;
    if (on) S.typingTimer[room] = setTimeout(function () { setTyping(room, false); }, 25000);
    var show = !!S.typing[S.active];
    if (E.typing) {
      E.typing.hidden = !show || !S.open;
      E.pill.classList.toggle('pc-typing-on', !!(S.typing.general || (S.rooms[1] && S.typing[S.rooms[1]])));
    }
  }

  // ------------------------------------------------------------------ notes
  var noteTimer = 0;
  function showNote(text, sticky) {
    clearTimeout(noteTimer);
    E.note.textContent = text;
    E.note.hidden = !text;
    if (text && !sticky) noteTimer = setTimeout(function () { E.note.hidden = true; }, 5000);
  }

  // ------------------------------------------------------------------ open / close / rooms
  function openPanel(on) {
    S.open = !!on;
    E.panel.hidden = !S.open;
    E.pill.setAttribute('aria-expanded', S.open ? 'true' : 'false');
    E.strip.classList.toggle('pc-open', S.open);
    store('pchat_open', S.open ? '1' : '0');
    if (S.open) {
      sizePanel();
      S.unread[S.active] = 0;
      markSeen(S.active);
      ensureHistory(S.active).then(function () { renderList(); });
      renderList();
      setTyping(S.active, S.typing[S.active]);
      if (!matchMedia('(pointer:coarse)').matches) setTimeout(function () { E.input.focus(); }, 30);
      ensureIdentity();
    } else {
      E.emo.hidden = true;
    }
    pill();
    dots();
  }
  function setRoom(room) {
    if (S.rooms.indexOf(room) === -1) return;
    S.active = room;
    S.adminView = false;
    store('pchat_room', roomKey(room));
    E.rGeneral.classList.toggle('on', room === 'general');
    E.rGeneral.setAttribute('aria-selected', room === 'general' ? 'true' : 'false');
    E.rTask.classList.toggle('on', room !== 'general');
    E.rTask.setAttribute('aria-selected', room !== 'general' ? 'true' : 'false');
    S.unread[room] = 0;
    S.replyTo = null;
    E.replyBar.hidden = true;
    markSeen(room);
    labels();
    ensureHistory(room).then(function () { if (S.active === room) renderList(); });
    renderList();
    online();
    dots();
    setTyping(room, S.typing[room]);
  }
  function markSeen(room) {
    var arr = S.msgs[room] || [];
    if (arr.length) store('pchat_seen_' + roomKey(room), String(arr[arr.length - 1].id));
  }
  function sizePanel() {
    var vp = E.stage.querySelector('.tv3-viewport');
    var h = vp ? vp.clientHeight : 520;
    E.panel.style.setProperty('--pc-max-h', Math.max(260, h - 12) + 'px');
  }

  // ------------------------------------------------------------------ data loading
  function ensureHistory(room) {
    if (S.loaded[room]) return S.loaded[room];
    S.loaded[room] = api('/messages?room=' + encodeURIComponent(room) + '&limit=60&lang=' + readerLang()).then(function (doc) {
      (doc.messages || []).forEach(function (m) { addMsg(m, false); });
      if (doc.online) Object.keys(doc.online).forEach(function (r) { S.online[r] = doc.online[r]; });
      var seen = Number(store('pchat_seen_' + roomKey(room)) || 0);
      var arr = S.msgs[room] || [];
      if (!seen && arr.length) store('pchat_seen_' + roomKey(room), String(arr[arr.length - 1].id));
      else if (!(S.open && S.active === room)) {
        S.unread[room] = arr.filter(function (m) { return m.id > seen && !isMine(m); }).length;
      }
      online();
      pill();
      dots();
    }).catch(function () { S.loaded[room] = null; });
    return S.loaded[room];
  }
  function loadOlder() {
    var arr = S.msgs[S.active] || [];
    if (!arr.length) return;
    var first = arr[0].id;
    var before = E.list.scrollHeight;
    api('/messages?room=' + encodeURIComponent(S.active) + '&before=' + first + '&limit=40&lang=' + readerLang()).then(function (doc) {
      var got = doc.messages || [];
      if (got.length < 40) S.oldest[S.active] = true;
      got.forEach(function (m) { addMsg(m, false); });
      renderList();
      E.list.scrollTop = E.list.scrollHeight - before;
    }).catch(function () {});
  }

  var identityPromise = null;
  function ensureIdentity() {
    if (S.me && S.me.kind !== 'none') return Promise.resolve(S.me);
    if (identityPromise) return identityPromise;
    // The site gives every visitor an anon_id cookie through its own language API; the chat only reads it.
    identityPromise = fetch('/api/me/language', { credentials: 'same-origin', cache: 'no-store' })
      .catch(function () {}).then(refreshState).then(function () { identityPromise = null; return S.me; });
    return identityPromise;
  }
  function refreshState() {
    var q = S.taskId ? '?task=' + S.taskId : '';
    return api('/state' + q).then(applyState);
  }
  function applyState(doc) {
    S.enabled = !!doc.enabled;
    S.me = doc.me || { kind: 'none' };
    S.features = doc.features || {};
    S.config = doc.config || null;
    var taskRoom = (doc.rooms || []).filter(function (r) { return r.id !== 'general' && r.available; })[0];
    S.rooms = ['general'];
    if (taskRoom) S.rooms.push(taskRoom.id);
    (doc.rooms || []).forEach(function (r) { S.online[r.id] = r.online || 0; });
    E.rTask.hidden = !taskRoom;
    var visible = S.enabled || (S.me && S.me.admin);
    E.strip.hidden = !visible;
    E.strip.classList.toggle('pc-off', !S.enabled);
    E.banner.hidden = S.enabled;
    E.kill.classList.toggle('pc-act-bad', !S.enabled);
    if (S.rooms.indexOf(S.active) === -1) S.active = 'general';
    E.form.classList.toggle('pc-ro', !!readOnly());
    labels();
    return doc;
  }

  // ------------------------------------------------------------------ stream
  function connect() {
    if (S.es || BOT_UA.test(navigator.userAgent || '')) return;
    if (!S.enabled && !(S.me && S.me.admin)) return;
    var url = API + '/stream?rooms=' + encodeURIComponent(S.rooms.join(',')) + '&cid=' + S.cid + '&lang=' + readerLang() + (S.lastId ? '&after=' + S.lastId : '');
    var es;
    try { es = new EventSource(url); } catch (e) { return; }
    S.es = es;
    es.addEventListener('msg', function (e) { try { addMsg(JSON.parse(e.data), true); } catch (x) {} });
    es.addEventListener('del', function (e) { try { removeMsgs(JSON.parse(e.data).ids); } catch (x) {} });
    es.addEventListener('tr', function (e) { try { onTranslation(JSON.parse(e.data)); } catch (x) {} });
    es.addEventListener('av', function (e) { try { onAvatar(JSON.parse(e.data)); } catch (x) {} });
    es.addEventListener('typing', function (e) {
      try { var d = JSON.parse(e.data); if (d.who === 'astra') setTyping(d.room, d.on); } catch (x) {}
    });
    es.addEventListener('online', function (e) {
      try { var d = JSON.parse(e.data); Object.keys(d).forEach(function (r) { S.online[r] = d[r]; }); online(); } catch (x) {}
    });
    es.addEventListener('config', function (e) {
      try { if ('enabled' in JSON.parse(e.data)) refreshState().then(function () { if (!S.enabled && !S.me.admin) disconnect(); }); } catch (x) {}
    });
    es.onopen = function () { E.strip.classList.remove('pc-offline'); if (E.note.dataset.offline) { showNote(''); delete E.note.dataset.offline; } };
    es.onerror = function () {
      if (es.readyState === 2) {
        // 204 (chat switched off) or a hard failure: ask the server again later instead of hammering it
        S.es = null;
        clearTimeout(S.esTimer);
        S.esTimer = setTimeout(function () { refreshState().then(connect, connect); }, 60000);
      } else {
        E.strip.classList.add('pc-offline');
      }
    };
  }
  function disconnect() {
    clearTimeout(S.esTimer);
    if (S.es) { S.es.close(); S.es = null; }
  }

  // ------------------------------------------------------------------ actions
  function submit() {
    if (S.sending) return;
    var text = E.input.value.replace(/\s+$/, '');
    if (!text.trim()) { showNote(t('pchat_err_empty')); return; }
    S.sending = true;
    E.send.classList.add('pc-busy');
    ensureIdentity().then(function () {
      return api('/messages', { room: S.active, text: text, lang: uiLang(), reply_to: S.replyTo || undefined });
    }).then(function (doc) {
      E.input.value = '';
      autosize();
      S.replyTo = null;
      E.replyBar.hidden = true;
      E.emo.hidden = true;
      showNote('');
      if (doc.message) addMsg(doc.message, false);
      toBottom();
    }).catch(function (error) {
      if (error && error.code === 'identity_required') { S.me = { kind: 'none' }; }
      showNote(errorText(error));
    }).then(function () {
      S.sending = false;
      E.send.classList.remove('pc-busy');
    });
  }

  function act(button) {
    var row = button.closest('.pc-msg');
    var id = row && Number(row.getAttribute('data-id'));
    var m = S.byId[id] || (S.adminView && S.adminMsgs && S.adminMsgs[id]);
    var what = button.getAttribute('data-act');
    if (!id || !what) return;
    if (what === 'reply' && m) {
      S.replyTo = id;
      E.replyBar.hidden = false;
      E.replyBar.querySelector('.pc-replybar-t').textContent = t('pchat_replying', { name: (m.author && m.author.name) || '' });
      E.input.focus();
      return;
    }
    if (what === 'tr') return translate(id);
    if (what === 'orig') { S.orig[id] = !S.orig[id]; refreshRow(id); return; }
    if (what === 'report') {
      return ensureIdentity().then(function () { return api('/report', { id: id }); }).then(function (doc) {
        button.classList.add('pc-done');
        showNote(t('pchat_reported'));
        if (doc.hidden) removeMsgs([id]);
      }).catch(function (error) { showNote(errorText(error)); });
    }
    if (what === 'del' || what === 'mute' || what === 'ban' || what === 'restore') {
      if ((what === 'ban' || what === 'del') && !window.confirm(t('pchat_confirm'))) return;
      var path = { del: '/admin/delete', mute: '/admin/mute', ban: '/admin/ban', restore: '/admin/restore' }[what];
      api(path, { id: id, minutes: 60 }).then(function () {
        if (what === 'restore') { row.remove(); return; }
        if (what === 'del' || what === 'ban') removeMsgs([id]);
        button.classList.add('pc-done');
      }).catch(function (error) { showNote(errorText(error)); });
    }
  }

  function translate(id) {
    var m = S.byId[id];
    if (!m) return;
    var lang = readerLang(), key = id + ':' + lang;
    if (S.tr[key]) { S.orig[id] = !S.orig[id]; refreshRow(id); return; }
    delete S.trErr[key];
    S.pend[key] = true;
    refreshRow(id);
    api('/translate', { id: id, lang: lang }).then(function (doc) {
      if (doc.state === 'done' && doc.text) onTranslation({ id: id, lang: lang, text: doc.text });
    }).catch(function (error) {
      delete S.pend[key];
      S.trErr[key] = true;
      refreshRow(id);
      showNote(errorText(error));
    });
  }
  function onTranslation(d) {
    if (!d || !d.id) return;
    var key = d.id + ':' + d.lang;
    delete S.pend[key];
    if (d.text) { S.tr[key] = d.text; delete S.trErr[key]; }
    else S.trErr[key] = true;
    if (d.lang !== readerLang()) return;
    refreshRow(d.id);
    var arr = S.msgs[S.active] || [];
    if (arr.length && arr[arr.length - 1].id === Number(d.id)) pill();
  }
  function onAvatar(d) {
    if (!d || !d.handle) return;
    if (d.v) S.avUrl[d.handle] = '/api/avatar/' + d.handle + '?s=64&v=' + d.v;
    else delete S.avUrl[d.handle];
    if (E.list) E.list.querySelectorAll('.pc-av[data-h="' + d.handle + '"]').forEach(function (av) {
      av.querySelectorAll('img').forEach(function (i) { i.remove(); });
      av.classList.remove('pc-av-img');
      if (d.v) avatarImg(av, S.avUrl[d.handle]);
    });
    if (S.me && S.me.handle === d.handle) { S.me.av = d.v ? S.avUrl[d.handle] : null; meChip(); }
  }

  function rename() {
    if (!S.me || !S.me.can_rename) return;
    if (E.me.querySelector('input')) return;
    var box = el('span', 'pc-rename');
    var input = el('input', '', { maxlength: '24', dir: 'auto', value: S.me.name || '' });
    input.value = S.me.name || '';
    var ok = iconButton('pc-ok', I.check, 'pchat_save');
    box.appendChild(input);
    box.appendChild(ok);
    E.me.hidden = true;
    E.me.parentNode.insertBefore(box, E.me);
    input.focus();
    input.select();
    function done(save) {
      if (!box.parentNode) return;
      if (!save) { box.remove(); E.me.hidden = false; return; }
      api('/me', { name: input.value.trim() }).then(function (doc) {
        S.me = doc.me || S.me;
        box.remove();
        E.me.hidden = false;
        meChip();
      }).catch(function (error) { showNote(errorText(error)); });
    }
    ok.addEventListener('click', function () { done(true); });
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') { e.preventDefault(); done(true); }
      if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); done(false); }
    });
  }

  // admin view: reported and hidden messages
  function renderAdminView() {
    E.empty.hidden = true;
    E.older.hidden = true;
    var back = el('button', 'pc-back', { type: 'button', html: I.down });
    back.title = t('pchat_back');
    back.appendChild(el('span', '', { text: t('pchat_reports') }));
    E.list.appendChild(back);
    back.addEventListener('click', function () { S.adminView = false; renderList(); });
    api('/admin/reports').then(function (doc) {
      S.adminMsgs = {};
      (doc.messages || []).forEach(function (m) {
        S.adminMsgs[m.id] = m;
        var row = renderMsg(m);
        row.classList.add('pc-adm', 'pc-st-' + m.state);
        var info = el('div', 'pc-adm-info', { text: [m.state, m.state_reason, m.reports ? ('⚑' + m.reports) : ''].filter(Boolean).join(' · ') });
        row.querySelector('.pc-body').appendChild(info);
        var acts = row.querySelector('.pc-acts');
        acts.textContent = '';
        if (m.state !== 'visible') acts.appendChild(iconButton('', I.restore, 'pchat_restore', { 'data-act': 'restore' }));
        else acts.appendChild(iconButton('pc-act-bad', I.trash, 'pchat_delete', { 'data-act': 'del' }));
        if (m.author && (m.author.kind === 'guest' || m.author.kind === 'user')) {
          acts.appendChild(iconButton('pc-act-bad', I.mute, 'pchat_mute', { 'data-act': 'mute' }));
          acts.appendChild(iconButton('pc-act-bad', I.ban, 'pchat_ban', { 'data-act': 'ban' }));
        }
        E.list.appendChild(row);
      });
    }).catch(function (error) { showNote(errorText(error)); });
  }

  // ------------------------------------------------------------------ input helpers
  function autosize() {
    E.input.style.height = 'auto';
    E.input.style.height = Math.min(E.input.scrollHeight, 104) + 'px';
  }
  function insertAtCursor(text) {
    var i = E.input, start = i.selectionStart || i.value.length, end = i.selectionEnd || i.value.length;
    i.value = i.value.slice(0, start) + text + i.value.slice(end);
    i.selectionStart = i.selectionEnd = start + text.length;
    i.focus();
    autosize();
  }

  // ------------------------------------------------------------------ events
  function wire() {
    E.pill.addEventListener('click', function () { openPanel(!S.open); });
    E.close.addEventListener('click', function () { openPanel(false); });
    E.rGeneral.addEventListener('click', function () { setRoom('general'); });
    E.rTask.addEventListener('click', function () { if (S.rooms[1]) setRoom(S.rooms[1]); });
    E.me.addEventListener('click', rename);
    E.admin.addEventListener('click', function () { E.adminBar.hidden = !E.adminBar.hidden; });
    E.kill.addEventListener('click', function () {
      if (!window.confirm(t('pchat_confirm'))) return;
      api('/admin/config', { enabled: !S.enabled }).then(refreshState).catch(function (error) { showNote(errorText(error)); });
    });
    E.clear.addEventListener('click', function () {
      if (!window.confirm(t('pchat_confirm'))) return;
      api('/admin/clear', { room: S.active }).then(function () {
        removeMsgs((S.msgs[S.active] || []).map(function (m) { return m.id; }));
      }).catch(function (error) { showNote(errorText(error)); });
    });
    E.reports.addEventListener('click', function () { S.adminView = true; renderList(); });
    E.older.addEventListener('click', loadOlder);
    E.list.addEventListener('scroll', function () {
      if (nearBottom()) { E.jump.hidden = true; S.jumpCount = 0; }
    }, { passive: true });
    E.jump.addEventListener('click', function () { toBottom(); E.jump.hidden = true; S.jumpCount = 0; });
    E.list.addEventListener('click', function (e) {
      var b = e.target.closest('button[data-act]');
      if (b) { e.preventDefault(); act(b); return; }
      var q = e.target.closest('.pc-quote');
      if (q) {
        var target = E.list.querySelector('.pc-msg[data-id="' + q.getAttribute('data-goto') + '"]');
        if (target) { target.scrollIntoView({ block: 'center', behavior: 'smooth' }); target.classList.add('pc-flash'); setTimeout(function () { target.classList.remove('pc-flash'); }, 1400); }
        return;
      }
      var row = e.target.closest('.pc-msg');
      if (row && !e.target.closest('a')) {
        E.list.querySelectorAll('.pc-msg.pc-act-on').forEach(function (n) { if (n !== row) n.classList.remove('pc-act-on'); });
        row.classList.toggle('pc-act-on');
      }
    });
    E.replyX.addEventListener('click', function () { S.replyTo = null; E.replyBar.hidden = true; });
    E.emoBtn.addEventListener('click', function () { E.emo.hidden = !E.emo.hidden; });
    E.emo.addEventListener('click', function (e) {
      var b = e.target.closest('.pc-emo-i');
      if (b) insertAtCursor(b.textContent);
    });
    E.form.addEventListener('submit', function (e) { e.preventDefault(); submit(); });
    E.input.addEventListener('input', autosize);
    E.input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); submit(); }
      if (e.key === 'Escape') { e.preventDefault(); if (!E.emo.hidden) E.emo.hidden = true; else openPanel(false); }
    });
    E.panel.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && e.target !== E.input) { openPanel(false); E.pill.focus(); }
    });
    addEventListener('resize', function () { if (S.open) sizePanel(); }, { passive: true });
    addEventListener('languageChanged', function () { labels(); if (S.open) renderList(); });
    document.addEventListener('visibilitychange', function () {
      clearTimeout(S.hiddenTimer);
      if (document.visibilityState === 'hidden') {
        S.hiddenTimer = setTimeout(disconnect, 120000);
      } else {
        if (!S.es) connect();
        if (S.open) { S.unread[S.active] = 0; markSeen(S.active); pill(); }
      }
    });
    addEventListener('pagehide', disconnect);
  }

  // ------------------------------------------------------------------ start
  function start() {
    if (S.started) return;
    var stage = document.getElementById('tv3-stage');
    if (!stage) return;
    S.started = true;
    var params = new URLSearchParams(location.search);
    var id = String(params.get('id') || '').trim().toLowerCase();
    S.taskId = UUID.test(id) ? id : '';
    try {
      S.cid = sessionStorage.getItem('pchat_cid') || Math.random().toString(36).slice(2, 12);
      sessionStorage.setItem('pchat_cid', S.cid);
    } catch (e) { S.cid = Math.random().toString(36).slice(2, 12); }
    build(stage);
    E.strip.hidden = true;
    refreshState().then(function () {
      var saved = store('pchat_room');
      if (saved === 'task' && S.rooms[1]) S.active = S.rooms[1];
      setRoom(S.active);
      return Promise.all(S.rooms.map(ensureHistory));
    }).then(function () {
      pill();
      dots();
      // live updates once the page (and the viewer) had a moment to load
      setTimeout(function () { if (document.visibilityState === 'visible') connect(); }, 1500);
      if (store('pchat_open') === '1' && !matchMedia('(max-width: 640px)').matches) openPanel(true);
    }).catch(function () {
      E.strip.hidden = true;
      setTimeout(function () { S.started = false; E.strip.remove(); start(); }, 60000);
    });
  }

  function boot() {
    var i18n = window.I18n;
    var ready = null;
    try { ready = i18n && typeof i18n.init === 'function' ? i18n.init() : null; } catch (e) { ready = null; }
    if (ready && typeof ready.then === 'function') ready.then(function () { labels(); if (S.open) renderList(); }, function () {});
    start();
  }

  window.AutorigPublicChat = { open: function () { openPanel(true); }, close: function () { openPanel(false); }, state: S };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
