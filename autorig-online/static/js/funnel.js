// Conversion funnel (owner 2026-10-12: «нам нужна конверсия, она сейчас ведётся на сайте?»).
// window.autorigFunnel.track(event, {task_id, ...}) sends one funnel step to GA4 (gtag) and to our own log
// (/api/funnel/event, aggregated by /api/admin/funnel). The visitor's first-touch source is kept in the cookie
// ar_src: utm_source, else the referrer's host, else "direct".
(function () {
  if (window.autorigFunnel) return;
  const EVENTS = ['landing', 'upload_start', 'task_created', 'task_create_failed', 'download_click', 'paywall_view', 'checkout_start', 'purchase', 'sign_up_prompt'];
  function readCookie(name) {
    const m = document.cookie.match(new RegExp('(?:^|; )' + name + '=([^;]*)'));
    return m ? decodeURIComponent(m[1]) : '';
  }
  function firstTouch() {
    let src = readCookie('ar_src');
    if (src) return src;
    const q = new URLSearchParams(location.search);
    src = q.get('utm_source') || '';
    if (!src && document.referrer) {
      try {
        const host = new URL(document.referrer).hostname.replace(/^www\./, '');
        if (host && host !== location.hostname.replace(/^www\./, '')) src = host;
      } catch (e) { /* no referrer */ }
    }
    src = (src || 'direct').toLowerCase().replace(/[^a-z0-9._-]+/g, '').slice(0, 40) || 'direct';
    try { document.cookie = `ar_src=${encodeURIComponent(src)}; path=/; max-age=${60 * 60 * 24 * 90}; samesite=lax; secure`; } catch (e) { /* ignore */ }
    return src;
  }
  const source = firstTouch();
  const sent = new Set();
  function track(event, params) {
    if (!EVENTS.includes(event)) return;
    const p = params || {};
    const key = `${event}:${p.task_id || ''}:${p.once || ''}`;
    if (p.once && sent.has(key)) return;
    sent.add(key);
    try { if (typeof window.gtag === 'function') window.gtag('event', event, { ...p, traffic_source: source }); } catch (e) { /* ignore */ }
    try {
      const body = JSON.stringify({ event, source, task_id: p.task_id || null, props: p.props || null });
      if (navigator.sendBeacon) navigator.sendBeacon('/api/funnel/event', new Blob([body], { type: 'application/json' }));
      else fetch('/api/funnel/event', { method: 'POST', body, headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', keepalive: true });
    } catch (e) { /* the funnel never breaks the page */ }
  }
  window.autorigFunnel = { track, source };
  if (location.pathname === '/' || /^\/(ru|zh|hi|fa)\/?$/.test(location.pathname)) track('landing', { once: 'home' });
})();
