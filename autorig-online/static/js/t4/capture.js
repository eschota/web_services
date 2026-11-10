// Preview capture page: /static/capture4.html?run=<run>. The farm's capture script opens it, waits for
// window.autorigCapture4.ready and asks for frames (shot): the same viewer the task page shows, one frame per call.
import { createViewer } from './viewer.js';

const q = new URLSearchParams(location.search);
const run = String(q.get('run') || '');
const cap = { ready: false, error: null, clips: [], clip: null, duration: 0, front: '+z', shot: null };
window.autorigCapture4 = cap;

const ORDER = [/^run(ning)?$/i, /run/i, /gallop/i, /trot/i, /^walk(ing)?$/i, /walk/i, /fly|flap|swim|slither|crawl|hop/i, /idle/i];

(async () => {
  try {
    if (!/^[0-9a-f]{20}$/.test(run)) throw new Error('no run');
    const base = `/api/mt/files/${run}/rig/`;
    const viewer = createViewer(document.getElementById('v'));
    const rig = await fetch(`${base}rig.json`).then((r) => (r.ok ? r.json() : null)).catch(() => null);
    await viewer.load(`${base}rigged.glb?view=2`, { rigged: true, intro: 'none' });
    const front = rig && rig.body && rig.body.forward_axis ? String(rig.body.forward_axis) : '+z';
    viewer.setFront(front);
    cap.front = front;
    cap.clips = viewer.clips;
    let pick = null;
    for (const re of ORDER) { pick = cap.clips.find((c) => re.test(c.name)); if (pick) break; }
    pick = pick || cap.clips[0] || null;
    cap.clip = pick ? pick.name : null;
    cap.duration = pick ? pick.duration : 0;
    cap.shot = (o) => viewer.shot(Object.assign({ clip: cap.clip }, o || {}));
    cap.ready = true;
  } catch (e) {
    cap.error = String((e && e.message) || e);
  }
})();
