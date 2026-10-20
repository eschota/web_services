// Viewer V3.1 stages on the task page (2026-10-17). Owner: «визуализация найденных предметов одежды, обычных
// предметов, бороды, волос, класс персонажа — всё это должно отображаться во вьювере по мере анализа интерактивно и
// последовательно, а при загрузке страницы проигрываться последовательно начиная с этапа вокселизации,
// позиционирования и т. п.» Spec: MT docs/v31-viewer-visualisation-spec-20261017.md, rig contract V3.1 §5.
//
// Over the Unity viewer of the task (#tv3-viewer, same origin) this draws the run's rig stages from its live log
// (GET /api/mt/runs/<run>/live, the reader of mt/live.py; with ?v31fixture=1 the V3.1 fixture of the run through the
// same reader, /v31/fixture): the voxel solid (a 2D voxel splat that is on screen within a second, long before the
// Unity build), the positioning (up / forward, the height ruler), every part / garment / held object in its colour
// with a legend (a click selects it, shows its numbers and offers the contract labels: POST
// /api/mt/runs/<run>/parts/<id>/label), the class card, the bones, the dominant weights, the pose opening and return
// (the arm chains and their voxels turn to the standard pose and back; contacts red before, green after), the
// layers, the clips and the QA cards. A finished run is replayed on load from the voxels on (≈1.5 s a step, a scrub
// bar, pause, skip); the environment scene waits for it (scene-gate.js autorigEnvHold) and loads after the replay or
// the skip. A run in progress is drawn as its events arrive. The person's «replay on load» is saved with their
// viewer prefs (/api/mt/v31/prefs). Runs without the V3.1 events still get the voxels, bones, weights, clips and
// checks they have; the Unity presentation (mt/static/live.js) is never driven from here, so it works as before.
// Off: ?v31=0. Icons with tooltips; every name goes through I18n (v31_* keys, en/ru/zh/hi/fa).
// The 3D side (V3.1 parts, spec MT docs/v31-part-entities-spec-20261017.md §D, 2026-10-17): a Unity build that answers
// live_ready_v31 (LiveLayerParts.cs) gets the part records too: LiveParts (colour, icon, mask, centroid, bone), the voxel
// colours while the parts are found, the part shader on the mesh from the bones on, and only the 3D icons once the
// animation is ready (LivePartsOff; a tool bar button cycles icons / full / none). Hover on an icon opens the part's card,
// a click (icon or mesh) pins it with the relabel buttons. A replay with the build in drives the voxels, bones and
// weights in 3D instead of the splat. A build without it never gets these messages.
const BUILD = 'v31-20261018.1';
const params = new URLSearchParams(location.search);
const OFF = params.get('v31') === '0';
const FIXTURE = params.get('v31fixture') === '1';
const SIM = Math.max(0, Math.min(50, Number(params.get('v31sim') || 0)));
const RUN_RE = /^[0-9a-f]{20}$/;
const T_OPEN = 0;                     // W.t timings: ms since the page was opened (navigation start)

const FB = {
  en: {
    v31_replay: 'Replay the rig stages', v31_pause: 'Pause', v31_play: 'Play', v31_skip: 'Skip to the result',
    v31_autoplay_on: 'Replay on load: on', v31_autoplay_off: 'Replay on load: off', v31_map: 'Parts map',
    v31_step_voxels: 'Voxels', v31_step_erosion: 'Erosion', v31_step_frame: 'Positioning', v31_step_parts: 'Parts',
    v31_step_class: 'Class', v31_step_bones: 'Bones', v31_step_weights: 'Weights', v31_step_pose: 'Skin pose',
    v31_step_layers: 'Layers', v31_step_clips: 'Clips', v31_step_qa: 'Checks', v31_step_done: 'Done',
    v31_parts: 'Parts', v31_class: 'Character class', v31_pose: 'Skin pose', v31_layers: 'Layers',
    v31_clips: 'Clips', v31_qa: 'Clip checks', v31_qa_external: 'External checks', v31_relabel: 'Relabel',
    v31_relabel_saved: 'Saved as analysis version {version}', v31_relabel_denied: 'Only the task owner or an administrator',
    v31_relabel_failed: 'Not saved', v31_up: 'Up', v31_forward: 'Forward', v31_height: 'Height',
    v31_voxels_n: 'Voxels', v31_volume: 'Volume', v31_confidence: 'Confidence', v31_bone: 'Bone', v31_source: 'Source',
    v31_symmetry: 'Mirror symmetry', v31_lobes: 'Limbs', v31_body_type: 'Body type', v31_pose_class: 'Pose',
    v31_opening: 'Opening', v31_contacts: 'Contacts', v31_contacts_before: 'before', v31_contacts_after: 'after',
    v31_no_layers: 'No separate layers yet', v31_edges4x: 'Edges over 4×', v31_stretch: 'Worst stretch',
    v31_stride: 'Stride', v31_elbows: 'Elbow width', v31_verdict_pass: 'Clean', v31_verdict_fail: 'To fix',
    v31_not_run: 'not run yet', v31_fixture: 'fixture', v31_side_L: 'left', v31_side_R: 'right',
    v31_gltf: 'glTF validator', v31_unity: 'Unity Humanoid', v31_blender: 'Blender', v31_props_none: 'No held objects',
    v31_label_body: 'body', v31_label_torso: 'torso', v31_label_head: 'head', v31_label_arm: 'arm', v31_label_leg: 'leg',
    v31_label_hand: 'hand', v31_label_foot: 'foot', v31_label_hair: 'hair', v31_label_beard: 'beard',
    v31_label_hat: 'hat', v31_label_glove: 'glove', v31_label_mitten: 'mitten', v31_label_coat: 'coat',
    v31_label_cloak: 'cloak', v31_label_skirt: 'skirt', v31_label_boot: 'boot', v31_label_wing: 'wing',
    v31_label_tail: 'tail', v31_label_ear: 'ear', v31_label_prop: 'held object',
    v31_label_face: 'face', v31_label_moustache: 'moustache', v31_label_neck: 'neck', v31_label_cape: 'cape',
    v31_label_sleeve: 'sleeve', v31_label_belt: 'belt', v31_label_finger: 'finger', v31_label_horn: 'horn',
    v31_behaviour: 'Behaviour', v31_chain: 'Chain', v31_sources: 'Sources', v31_qa_part: 'Part checks',
    v31_behaviour_bone: 'bone', v31_behaviour_chain: 'bone chain', v31_behaviour_cloth: 'cloth',
    v31_behaviour_tail: 'tail', v31_behaviour_prop: 'held object', v31_behaviour_static: 'static',
    v31_parts_mode_icons: 'Parts: icons only', v31_parts_mode_full: 'Parts: colours, shader and icons',
    v31_parts_mode_none: 'Parts: hidden',
    v31_cat_biped: 'Biped', v31_cat_humanoid: 'Humanoid', v31_cat_quadruped: 'Quadruped', v31_cat_equine: 'Equine',
    v31_cat_monster: 'Monster', v31_cat_serpent: 'Serpent', v31_cat_bird: 'Bird', v31_cat_multilimb: 'Many limbs',
    v31_cat_hands: 'Hands', v31_cat_vehicle: 'Vehicle', v31_cat_primate: 'Primate',
    v31_poseclass_T: 'T pose', v31_poseclass_A: 'A pose', v31_poseclass_hanging: 'arms hanging',
    v31_poseclass_contact: 'limbs touching', v31_poseclass_posed: 'posed frame',
    v31_body_heavy: 'heavy', v31_body_slim: 'slim', v31_body_normal: 'normal', v31_body_athletic: 'athletic', v31_body_stocky: 'stocky', v31_body_child: 'child', v31_body_chibi: 'chibi', v31_body_toy_mascot: 'toy mascot', v31_body_ultra_fat: 'very heavy', v31_body_slim_tall: 'slim and tall', v31_body_long_limbed: 'long-limbed', v31_body_realistic: 'realistic', v31_body_toy: 'toy',
  },
  ru: {
    v31_replay: 'Проиграть этапы рига', v31_pause: 'Пауза', v31_play: 'Играть', v31_skip: 'К результату',
    v31_autoplay_on: 'Проигрывать при загрузке: да', v31_autoplay_off: 'Проигрывать при загрузке: нет', v31_map: 'Карта частей',
    v31_step_voxels: 'Воксели', v31_step_erosion: 'Эрозия', v31_step_frame: 'Позиционирование', v31_step_parts: 'Части',
    v31_step_class: 'Класс', v31_step_bones: 'Кости', v31_step_weights: 'Веса', v31_step_pose: 'Поза скина',
    v31_step_layers: 'Слои', v31_step_clips: 'Клипы', v31_step_qa: 'Проверки', v31_step_done: 'Готово',
    v31_parts: 'Части', v31_class: 'Класс персонажа', v31_pose: 'Поза скина', v31_layers: 'Слои',
    v31_clips: 'Клипы', v31_qa: 'Проверка клипов', v31_qa_external: 'Внешние проверки', v31_relabel: 'Переименовать',
    v31_relabel_saved: 'Сохранено: версия анализа {version}', v31_relabel_denied: 'Только владелец задачи или администратор',
    v31_relabel_failed: 'Не сохранено', v31_up: 'Верх', v31_forward: 'Вперёд', v31_height: 'Рост',
    v31_voxels_n: 'Воксели', v31_volume: 'Объём', v31_confidence: 'Уверенность', v31_bone: 'Кость', v31_source: 'Источник',
    v31_symmetry: 'Зеркальная симметрия', v31_lobes: 'Конечности', v31_body_type: 'Телосложение', v31_pose_class: 'Поза',
    v31_opening: 'Раскрытие', v31_contacts: 'Касания', v31_contacts_before: 'до', v31_contacts_after: 'после',
    v31_no_layers: 'Отдельных слоёв пока нет', v31_edges4x: 'Рёбер длиннее 4×', v31_stretch: 'Худшее растяжение',
    v31_stride: 'Шаг', v31_elbows: 'Ширина локтей', v31_verdict_pass: 'Чисто', v31_verdict_fail: 'Исправить',
    v31_not_run: 'ещё не запускалась', v31_fixture: 'фикстура', v31_side_L: 'левая', v31_side_R: 'правая',
    v31_gltf: 'glTF-валидатор', v31_unity: 'Unity Humanoid', v31_blender: 'Blender', v31_props_none: 'Предметов в руках нет',
    v31_label_body: 'тело', v31_label_torso: 'торс', v31_label_head: 'голова', v31_label_arm: 'рука', v31_label_leg: 'нога',
    v31_label_hand: 'кисть', v31_label_foot: 'стопа', v31_label_hair: 'волосы', v31_label_beard: 'борода',
    v31_label_hat: 'шапка', v31_label_glove: 'перчатка', v31_label_mitten: 'варежка', v31_label_coat: 'пальто',
    v31_label_cloak: 'плащ', v31_label_skirt: 'юбка', v31_label_boot: 'сапог', v31_label_wing: 'крыло',
    v31_label_tail: 'хвост', v31_label_ear: 'ухо', v31_label_prop: 'предмет в руке',
    v31_label_face: 'лицо', v31_label_moustache: 'усы', v31_label_neck: 'шея', v31_label_cape: 'накидка',
    v31_label_sleeve: 'рукав', v31_label_belt: 'пояс', v31_label_finger: 'палец', v31_label_horn: 'рог',
    v31_behaviour: 'Поведение', v31_chain: 'Цепочка', v31_sources: 'Источники', v31_qa_part: 'Проверки части',
    v31_behaviour_bone: 'кость', v31_behaviour_chain: 'цепочка костей', v31_behaviour_cloth: 'ткань',
    v31_behaviour_tail: 'хвост', v31_behaviour_prop: 'предмет в руке', v31_behaviour_static: 'неподвижная',
    v31_parts_mode_icons: 'Части: только иконки', v31_parts_mode_full: 'Части: цвета, шейдер и иконки',
    v31_parts_mode_none: 'Части: скрыты',
    v31_cat_biped: 'Двуногий', v31_cat_humanoid: 'Гуманоид', v31_cat_quadruped: 'Четвероногий', v31_cat_equine: 'Лошадиные',
    v31_cat_monster: 'Монстр', v31_cat_serpent: 'Змей', v31_cat_bird: 'Птица', v31_cat_multilimb: 'Много конечностей',
    v31_cat_hands: 'Кисти', v31_cat_vehicle: 'Транспорт', v31_cat_primate: 'Примат',
    v31_poseclass_T: 'T-поза', v31_poseclass_A: 'A-поза', v31_poseclass_hanging: 'руки опущены',
    v31_poseclass_contact: 'конечности касаются', v31_poseclass_posed: 'кадр позы',
    v31_body_heavy: 'плотное', v31_body_slim: 'стройное', v31_body_normal: 'обычное', v31_body_athletic: 'атлетичное', v31_body_stocky: 'коренастое', v31_body_child: 'детское', v31_body_chibi: 'чиби', v31_body_toy_mascot: 'игрушка-маскот', v31_body_ultra_fat: 'очень полное', v31_body_slim_tall: 'высокое стройное', v31_body_long_limbed: 'длинноногое', v31_body_realistic: 'реалистичное', v31_body_toy: 'игрушка',
  },
};
// Viewer UX 2026-10-18 (owner: «сделай чтобы можно было убрать меню превью визуализации»): the presentation group
Object.assign(FB.en, { v31_pres_show: 'Show the stages panel', v31_pres_hide: 'Hide the stages panel · Esc' });
Object.assign(FB.ru, { v31_pres_show: 'Показать панель этапов', v31_pres_hide: 'Скрыть панель этапов · Esc', v31_relabel: 'Переименовать' });
const CONTRACT_LABELS = ['body', 'torso', 'head', 'arm', 'leg', 'hand', 'foot', 'hair', 'beard', 'hat', 'glove', 'mitten',
  'coat', 'cloak', 'skirt', 'boot', 'wing', 'tail', 'ear', 'prop'];
const STEP_COLOR = {
  vox: '#4cc9f0', erosion: '#7b5cff', frame: '#90e0ef', part: '#f4a261', cls: '#ffd166', bones: '#e9ecef',
  weights: '#ff6b6b', pose: '#06d6a0', layers: '#c77dff', clip: '#8ecae6', qa: '#95d5b2', qa_ext: '#95d5b2',
  judge: '#ffb703', relabel: '#f4a261', done: '#80ed99',
};
const PACE = { vox: 900, erosion: 1600, frame: 1400, part: 650, cls: 1500, bones: 1400, weights: 1500, pose: 2600,
  layers: 900, clip: 220, qa: 260, qa_ext: 900, judge: 900, relabel: 300, done: 600 };
const ICON = {
  vox: '<path d="M3 13h5v5H3zM8 13h5v5H8zM13 13h5v5h-5zM5.5 8h5v5h-5zM10.5 8h5v5h-5zM8 3h5v5H8z"/>',
  erosion: '<rect x="3" y="3" width="18" height="18" rx="2" stroke-dasharray="3 2"/><rect x="8" y="8" width="8" height="8" rx="1"/>',
  frame: '<path d="M6 20V5M6 5l-3 3M6 5l3 3M6 20h14M20 20l-3-3M20 20l-3 3"/><path d="M14 6v8M12 6h4M12 14h4"/>',
  part: '<path d="M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4z"/><path d="M13 16.5h7M16.5 13v7"/>',
  cls: '<rect x="3" y="5" width="18" height="14" rx="2"/><circle cx="9" cy="11" r="2.4"/><path d="M5.5 17c.8-2 2-3 3.5-3s2.7 1 3.5 3M14.5 9.5h4M14.5 13h3"/>',
  bones: '<circle cx="12" cy="4" r="1.7"/><circle cx="12" cy="11" r="1.5"/><circle cx="7" cy="20" r="1.5"/><circle cx="17" cy="20" r="1.5"/><path d="M12 5.7v3.8M10.8 12.2l-3 6.4M13.2 12.2l3 6.4M10.6 10.4l-3.8-1M13.4 10.4l3.8-1"/>',
  weights: '<path d="M12 3c3.5 4.5 6 7.6 6 11a6 6 0 0 1-12 0c0-3.4 2.5-6.5 6-11z"/><path d="M8.5 15.5a3.6 3.6 0 0 0 3.5 3"/>',
  pose: '<circle cx="12" cy="4.5" r="1.8"/><path d="M12 6.5v7M12 13.5l-3.5 7M12 13.5l3.5 7M12 8.5L5 12.5M12 8.5l7 4"/><path d="M4 7a9 9 0 0 1 3-3M20 7a9 9 0 0 0-3-3" stroke-dasharray="2 1.5"/>',
  layers: '<path d="M12 3l9 5-9 5-9-5z"/><path d="M3 12.5l9 5 9-5M3 16.5l9 5 9-5"/>',
  clip: '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="M10 9l5 3-5 3z"/>',
  qa: '<path d="M4 12.5l5 5L20 6.5"/>',
  qa_ext: '<path d="M14 4h6v6M20 4l-8 8"/><path d="M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"/>',
  judge: '<path d="M12 3v18M5 7h14M5 7l-3 7h6zM19 7l-3 7h6z"/>',
  relabel: '<path d="M4 20h4L19 9l-4-4L4 16z"/><path d="M13 7l4 4"/>',
  done: '<circle cx="12" cy="12" r="9"/><path d="M8 12.5l3 3 5-6"/>',
  play: '<path d="M8 5l11 7-11 7z" fill="currentColor"/>',
  pause: '<path d="M7 5h3.5v14H7zM13.5 5H17v14h-3.5z" fill="currentColor"/>',
  skip: '<path d="M5 5l9 7-9 7z" fill="currentColor"/><path d="M17 5v14" stroke-width="2.4"/>',
  replay: '<path d="M4 12a8 8 0 1 0 2.4-5.7"/><path d="M4 4v4.5h4.5"/><path d="M10.2 9.2l4.6 2.8-4.6 2.8z" fill="currentColor"/>',
  auto: '<path d="M4 12a8 8 0 0 1 14-5.3M20 12a8 8 0 0 1-14 5.3"/><path d="M18 3v4h-4M6 21v-4h4"/>',
  map: '<path d="M3 6l6-3 6 3 6-3v15l-6 3-6-3-6 3z"/><path d="M9 3v15M15 6v15"/>',
  close: '<path d="M6 6l12 12M18 6L6 18"/>',
  pres: '<rect x="3" y="4" width="18" height="16" rx="2.5"/><path d="M14 4v16"/><path d="M16.5 8h2M16.5 11h2M16.5 14h2"/><path d="M6 16.5h5" stroke-width="2.2"/>',
  parts_icons: '<circle cx="7" cy="7" r="3.2"/><circle cx="17" cy="7" r="3.2"/><circle cx="12" cy="17" r="3.2"/>',
  parts_full: '<path d="M12 3c4 0 6 2.6 6 5.6 0 2.4-1.4 3.6-3 4.4v7h-6v-7c-1.6-.8-3-2-3-4.4C6 5.6 8 3 12 3z"/><circle cx="12" cy="8.5" r="2.4" fill="currentColor"/>',
  parts_none: '<circle cx="7" cy="7" r="3.2"/><circle cx="17" cy="7" r="3.2"/><circle cx="12" cy="17" r="3.2"/><path d="M4 20L20 4"/>',
};
const STEP_NAME = { vox: 'voxels', erosion: 'erosion', frame: 'frame', part: 'parts', cls: 'class', bones: 'bones',
  weights: 'weights', pose: 'pose', layers: 'layers', clip: 'clips', qa: 'qa', qa_ext: 'qa', judge: 'qa',
  relabel: 'parts', done: 'done' };

function lang() {
  const l = (window.I18n && window.I18n.currentLang) || document.documentElement.lang || 'en';
  return String(l).slice(0, 2).toLowerCase();
}
function tr(key, rep) {
  const i18n = window.I18n;
  let text = '';
  if (i18n && typeof i18n.has === 'function' && i18n.has(key)) text = i18n.t(key, rep);
  else text = (FB[lang()] || FB.en)[key] || FB.en[key] || '';
  if (!text) return '';
  for (const [k, v] of Object.entries(rep || {})) text = text.split(`{${k}}`).join(String(v));
  return text;
}
const labelName = (l) => {
  const s = String(l || '');
  if (s.startsWith('prop:')) return `${tr('v31_label_prop')}: ${s.slice(5)}`;
  return tr(`v31_label_${s}`) || s;
};
const svg = (name, cls = '') => `<svg class="v31i ${cls}" viewBox="0 0 24 24" aria-hidden="true">${ICON[name] || ''}</svg>`;
const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const num = (v, d = 2) => (v == null || !isFinite(Number(v)) ? '—' : Number(v).toLocaleString(lang(), { maximumFractionDigits: d }));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ------------------------------------------------------------------------------------------------ voxels
const voxCache = new Map();
function parseArv(buf) {
  const dv = new DataView(buf);
  if (buf.byteLength < 32 || dv.getUint8(0) !== 65 || dv.getUint8(1) !== 82 || dv.getUint8(2) !== 86 || dv.getUint8(3) !== 49) return null;
  const n = dv.getUint32(28, true);
  if (32 + n * 4 > buf.byteLength) return null;
  const ox = dv.getFloat32(12, true), oy = dv.getFloat32(16, true), oz = dv.getFloat32(20, true), cell = dv.getFloat32(24, true);
  const b = new Uint8Array(buf, 32, n * 4);
  const pos = new Float32Array(n * 3), depth = new Uint8Array(n), core = new Uint8Array(n);
  let maxD = 0;
  for (let i = 0; i < n; i++) {
    pos[i * 3] = ox + (b[i * 4] + 0.5) * cell;
    pos[i * 3 + 1] = oy + (b[i * 4 + 1] + 0.5) * cell;
    pos[i * 3 + 2] = oz + (b[i * 4 + 2] + 0.5) * cell;
    const d = b[i * 4 + 3] & 127;
    depth[i] = d;
    core[i] = b[i * 4 + 3] >> 7;
    if (d !== 127 && d > maxD) maxD = d;
  }
  return { n, pos, depth, core, cell, maxD, origin: [ox, oy, oz] };
}
function loadVox(url) {
  if (!voxCache.has(url)) {
    voxCache.set(url, fetch(url, { credentials: 'same-origin' }).then((r) => (r.ok ? r.arrayBuffer() : null))
      .then((b) => (b ? parseArv(b) : null)).catch(() => null));
  }
  return voxCache.get(url);
}
const qkey = (x, y, z, cell) => `${Math.floor(x / cell)},${Math.floor(y / cell)},${Math.floor(z / cell)}`;

// ------------------------------------------------------------------------------------------------ the module
function start(viewerEl) {
  const viewport = viewerEl.parentElement;
  const root = document.createElement('div');
  root.className = 'v31';
  root.innerHTML = `
    <div class="v31-back"></div>
    <canvas class="v31-cv" aria-hidden="true"></canvas>
    <aside class="v31-panel" aria-live="polite">
      <div class="v31-panel-head">
        <button class="v31-btn v31-mapbtn" type="button">${svg('map')}</button>
        <span class="v31-now"></span>
        <button class="v31-btn v31-fold" type="button">${svg('close')}</button>
      </div>
      <div class="v31-cards"></div>
    </aside>
    <div class="v31-pop" hidden></div>
    <div class="v31-scrub" hidden>
      <button class="v31-btn v31-playbtn" type="button">${svg('pause')}</button>
      <div class="v31-track"><div class="v31-ticks"></div><i class="v31-fill"></i><b class="v31-thumb"></b></div>
      <span class="v31-stepname"></span>
      <button class="v31-btn v31-skipbtn" type="button">${svg('skip')}</button>
      <button class="v31-btn v31-autobtn" type="button">${svg('auto')}</button>
    </div>
    <div class="v31-toast" hidden></div>`;
  viewport.appendChild(root);
  const $ = (s) => root.querySelector(s);
  const cv = $('.v31-cv'), ctx = cv.getContext('2d');
  const panel = $('.v31-panel'), cards = $('.v31-cards'), pop = $('.v31-pop'), scrub = $('.v31-scrub');
  const track = $('.v31-track'), ticks = $('.v31-ticks'), fill = $('.v31-fill'), thumb = $('.v31-thumb');
  const playBtn = $('.v31-playbtn'), skipBtn = $('.v31-skipbtn'), autoBtn = $('.v31-autobtn');
  const nowEl = $('.v31-now'), stepName = $('.v31-stepname'), toast = $('.v31-toast');

  // the tool bar of the task page: «replay» (icons only)
  const tools = document.getElementById('tv3-tools');
  const replayTool = document.createElement('button');
  replayTool.className = 'tv3-tool v31-tool';
  replayTool.type = 'button';
  replayTool.hidden = true;
  replayTool.innerHTML = `<svg viewBox="0 0 24 24" aria-hidden="true">${ICON.replay}</svg>`;
  if (tools) tools.insertBefore(replayTool, document.getElementById('tv3-build') || null);
  const presTool = document.createElement('button');
  presTool.className = 'tv3-tool v31-tool v31-prestool on';
  presTool.type = 'button';
  presTool.hidden = true;
  presTool.innerHTML = `<svg viewBox="0 0 24 24" aria-hidden="true">${ICON.pres}</svg>`;
  if (tools) tools.insertBefore(presTool, document.getElementById('tv3-build') || null);
  const partsTool = document.createElement('button');
  partsTool.className = 'tv3-tool v31-tool v31-partstool';
  partsTool.type = 'button';
  partsTool.hidden = true;
  if (tools) tools.insertBefore(partsTool, document.getElementById('tv3-build') || null);

  const W = window.autorigV31 = { build: BUILD, run: null, steps: [], idx: 0, mode: 'wait', fixture: FIXTURE, t: {} };
  let run = '', FILES = '';
  let steps = W.steps;
  let idx = 0;              // steps applied
  let playing = false, replayMode = false, live = false, runDone = false, docked = false, collapsed = false;
  let S = null;             // the drawn state
  let selected = null, autoReplay = true, mapOn = false;
  let yaw = 0.5, dragYaw = null, stepT0 = performance.now(), stepKind = '';
  let boneLab = null;       // voxel index -> bone index (dominant weights picture)

  function fresh() {
    return { vox: null, voxRes: 0, erosion: false, frame: null, parts: new Map(), order: [], cls: null, bones: null,
      weights: null, pose: null, layers: null, clips: [], qa: new Map(), ext: null, judge: null, done: false,
      labels: {}, heightM: null };
  }
  S = fresh();

  // ---------------------------------------------------------------- events -> steps
  let lastBonesStep = null, lastWeights = null, voxStart = null;
  function push(step) { steps.push(step); return step; }
  function ingest(ev) {
    const k = `${ev.stage}/${ev.kind}`, d = ev.data || {}, p = ev.params || {}, key = ev.key || '';
    if (String(ev.kind || '').startsWith('v3.')) return;
    if (k === 'voxels/start') { voxStart = d; return; }
    if (k === 'voxels/level' && ev.url) {
      push({ k: 'vox', url: ev.url, res: d.res || p.res, fine: !!d.fine, count: d.count || p.count, t: ev.t });
      return;
    }
    if (k === 'voxels/erosion') { push({ k: 'erosion', steps: p.steps, core: p.core, t: ev.t }); return; }
    if (key === 'live_voxels') {
      const lf = steps.find((s) => s.k === 'frame' && s.legacy);
      if (lf) { lf.data = d; lf.legacy = false; } else push({ k: 'frame', data: d, t: ev.t });
      return;
    }
    if (k === 'voxels/done' && !steps.some((s) => s.k === 'frame')) {
      push({ k: 'frame', data: { res: voxStart && voxStart.dims, voxel_m: null }, legacy: true, t: ev.t });
      return;
    }
    if (key === 'live_parts' || key === 'live_garments' || key === 'live_props') {
      if (d.id) push({ k: 'part', group: key.slice(5), data: d, t: ev.t });
      return;
    }
    if (key === 'live_class') { push({ k: 'cls', data: d, t: ev.t }); return; }
    if (k === 'analysis/classify' && !steps.some((s) => s.k === 'cls' && !s.legacy)) {
      push({ k: 'cls', legacy: true, data: { category: p.body, what: p.what, hair: p.hair, props: p.props }, t: ev.t });
      return;
    }
    if (k === 'rig/fast_category') {
      const c = steps.find((s) => s.k === 'cls');
      if (c && c.legacy) c.data.registry = p.category;
      return;
    }
    if (k === 'rig/bones' || k === 'rig/weights') {
      if (!(d.bones || []).length) return;
      // a rig made again after the run (an edit, a re-skin) updates the bones and weights already shown
      const after = steps.some((s) => s.k === 'done');
      if (k === 'rig/bones') {
        if (after && lastBonesStep) lastBonesStep.bones = d.bones;
        else lastBonesStep = push({ k: 'bones', bones: d.bones, t: ev.t });
      } else {
        if (lastBonesStep) lastBonesStep.bones = d.bones;
        if (after && lastWeights) lastWeights.bones = d.bones;
        else lastWeights = push({ k: 'weights', bones: d.bones, t: ev.t });
      }
      return;
    }
    if (k === 'rig/preview' && ev.url && lastWeights) { lastWeights.img = ev.url; return; }
    if (key === 'live_pose') { push({ k: 'pose', data: d, t: ev.t }); return; }
    if (key === 'live_layers') { push({ k: 'layers', data: d, t: ev.t }); return; }
    if (k === 'retarget/clip') { push({ k: 'clip', name: p.name || d.name, n: p.n, total: p.total, t: ev.t }); return; }
    if (key === 'live_qa_clip') { push({ k: 'qa', data: d, t: ev.t }); return; }
    if (k === 'qa/clip' && !FIXTURE) {
      push({ k: 'qa', legacy: true, data: { clip: p.name, verdict: key === 'live_qa_clip_ok' ? 'pass' : 'fail', bad_frames: p.bad }, t: ev.t });
      return;
    }
    if (key === 'live_qa_external') { push({ k: 'qa_ext', data: d, t: ev.t }); return; }
    if (k === 'judge/verdict') { push({ k: 'judge', data: { score: p.score, fix: p.fix }, t: ev.t }); return; }
    if (k === 'analysis/relabel') { push({ k: 'relabel', data: d, t: ev.t }); return; }
    if (k === 'run/finish' || key === 'live_run_done' || key === 'live_run_needs_review') push({ k: 'done', t: ev.t });
  }

  // ---------------------------------------------------------------- one step onto the state
  function apply(st) {
    switch (st.k) {
      case 'vox': S.vox = st; S.erosion = false; break;
      case 'erosion': S.erosion = true; break;
      case 'frame': S.frame = st.data || {}; if (st.data && st.data.height_m) S.heightM = st.data.height_m; break;
      case 'part': if (!S.parts.has(st.data.id)) S.order.push(st.data.id); S.parts.set(st.data.id, { ...st.data, group: st.group }); break;
      case 'cls': S.cls = st.data; break;
      case 'bones': S.bones = st.bones; break;
      case 'weights': S.bones = st.bones; S.weights = st; boneLab = null; break;
      case 'pose': S.pose = st.data; break;
      case 'layers': S.layers = st.data; break;
      case 'clip': if (!S.clips.includes(st.name)) S.clips.push(st.name); break;
      case 'qa': S.qa.set(st.data.clip, st.data); break;
      case 'qa_ext': S.ext = st.data; break;
      case 'judge': S.judge = st.data; break;
      case 'relabel': if (st.data && st.data.id) S.labels[st.data.id] = st.data.label; break;
      case 'done': S.done = true; break;
      default:
    }
  }
  function seek(n) {
    n = Math.max(0, Math.min(steps.length, n));
    if (n < idx) {
      S = fresh();
      for (let i = 0; i < n; i++) apply(steps[i]);
      if (replay3d) { drv = {}; usendRaw('LiveReset', ''); delete uSent.vp; delete uSent.pm; }
    }
    else for (let i = idx; i < n; i++) apply(steps[i]);
    idx = n;
    W.idx = idx;
    const cur = steps[idx - 1];
    stepKind = cur ? cur.k : '';
    stepT0 = performance.now();
    if (cur) loadFor(cur);
    paint();
  }
  function loadFor(st) {
    if (st.k === 'vox' && !(Number(st.res) > 48)) {      // the splat draws up to the 48 level; the 96 (~155 KB) is Unity's
      loadVox(url(st.url)).then((v) => {
        if (v && !W.t.firstVoxel) { W.t.firstVoxel = Math.round(performance.now() - T_OPEN); draw(performance.now(), 0); }
      });
    }
    if (st.k === 'part' && st.data.mask_url) loadVox(url(st.data.mask_url)).then(() => { maskIndex = null; });
  }
  const url = (u) => (/^(https?:)?\//.test(u) ? u : FILES + u);

  // ---------------------------------------------------------------- the panel
  let lastCards = '';
  const gateIsOpen = () => !!(window.autorigSceneGate && window.autorigSceneGate.open);
  function stepLabel(k) { return tr(`v31_step_${STEP_NAME[k] || k}`); }
  function paint() {
    const cur = steps[idx - 1];
    nowEl.innerHTML = cur ? `${svg(cur.k)}<span>${esc(stepLabel(cur.k))}</span>` : '';
    stepName.textContent = cur ? stepLabel(cur.k) : '';
    const f = steps.length ? idx / steps.length : 0;
    fill.style.width = `${f * 100}%`;
    thumb.style.left = `${f * 100}%`;
    playBtn.innerHTML = svg(playing ? 'pause' : 'play');
    playBtn.title = tr(playing ? 'v31_pause' : 'v31_play');
    skipBtn.title = tr('v31_skip');
    autoBtn.title = tr(autoReplay ? 'v31_autoplay_on' : 'v31_autoplay_off');
    autoBtn.classList.toggle('on', autoReplay);
    replayTool.title = tr('v31_replay');
    replayTool.setAttribute('aria-label', tr('v31_replay'));
    $('.v31-mapbtn').title = tr('v31_map');
    $('.v31-fold').title = tr('v31_pres_hide');
    $('.v31-fold').setAttribute('aria-label', tr('v31_pres_hide'));
    presTool.title = tr(presHidden ? 'v31_pres_show' : 'v31_pres_hide');
    presTool.setAttribute('aria-label', presTool.title);
    const html = cardsHtml();
    if (html !== lastCards) {
      const n0 = cards.children.length;
      cards.innerHTML = html;
      lastCards = html;
      // a new card scrolls into view while the stages play
      if ((playing || live) && cards.children.length > n0) cards.lastElementChild.scrollIntoView({ block: 'nearest' });
    }
    root.classList.toggle('has-cards', !!html);
    // pictures of the cards (the weights preview, ~125 KB each) load once the scene gate is open: nothing heavy while
    // the presentation runs; the card shows its frame until then
    if (gateIsOpen()) cards.querySelectorAll('img[data-src]').forEach((im) => { im.src = im.dataset.src; im.removeAttribute('data-src'); });
    syncUnity();
    paintPartsTool();
  }
  function ticksHtml() {
    if (!steps.length) return '';
    return steps.map((s, i) => `<i style="left:${(i / steps.length) * 100}%;width:${100 / steps.length}%;background:${STEP_COLOR[s.k] || '#888'}" title="${esc(stepLabel(s.k))}"></i>`).join('');
  }
  function partLabel(p) { return S.labels[p.id] || p.label || p.id; }
  function cardsHtml() {
    const out = [];
    if (S.parts.size) {
      const rows = S.order.map((id) => {
        const p = S.parts.get(id);
        const side = p.side ? ` <small>${esc(tr(`v31_side_${p.side}`))}</small>` : '';
        const conf = Math.round(Number(p.confidence || 0) * 100);
        const fx = p.fixture && p.source === 'fixture' ? ` <em>${esc(tr('v31_fixture'))}</em>` : '';
        const relab = S.labels[p.id] && S.labels[p.id] !== p.label ? ' relab' : '';
        return `<li class="v31-part${selected === id ? ' sel' : ''}${relab}" data-part="${esc(id)}" title="${esc(tr('v31_relabel'))}">` +
          `<i style="background:${esc(p.color || '#ccc')}"></i><span>${esc(labelName(partLabel(p)))}${side}${fx}</span>` +
          `<b title="${esc(tr('v31_volume'))}">${num(p.volume_m3, 3)} m³</b><u title="${esc(tr('v31_confidence'))}"><s style="width:${conf}%"></s></u></li>`;
      }).join('');
      const nGarm = [...S.parts.values()].filter((p) => p.group === 'garments').length;
      const nProps = [...S.parts.values()].filter((p) => p.group === 'props').length;
      out.push(`<section class="v31-card"><h4>${svg('part')}<span>${esc(tr('v31_parts'))}</span><b>${S.parts.size - nGarm - nProps} · ${nGarm} · ${nProps}</b></h4><ul class="v31-parts">${rows}</ul>` +
        (nProps ? '' : `<p class="v31-mute">${esc(tr('v31_props_none'))}</p>`) + '</section>');
    }
    if (S.cls) {
      const c = S.cls, ev = c.evidence || {};
      const cat = tr(`v31_cat_${c.category}`) || c.category || '—';
      const pc = c.pose_class ? (tr(`v31_poseclass_${c.pose_class}`) || c.pose_class) : '';
      out.push(`<section class="v31-card v31-cls"><h4>${svg('cls')}<span>${esc(tr('v31_class'))}</span>${c.confidence != null ? `<b title="${esc(tr('v31_confidence'))}">${Math.round(c.confidence * 100)}%</b>` : ''}</h4>` +
        `<div class="v31-big">${esc(cat)}${c.registry && c.registry !== c.category ? ` <small>${esc(tr(`v31_cat_${c.registry}`) || c.registry)}</small>` : ''}</div>` +
        (c.what ? `<div class="v31-what">${esc(c.what)}</div>` : '') +
        `<dl>${c.body_type ? `<dt>${esc(tr('v31_body_type'))}</dt><dd>${esc(tr(`v31_body_${c.body_type}`) || c.body_type)}</dd>` : ''}${pc ? `<dt>${esc(tr('v31_pose_class'))}</dt><dd>${esc(pc)}</dd>` : ''}` +
        `${ev.symmetry_iou != null ? `<dt>${esc(tr('v31_symmetry'))}</dt><dd>${num(ev.symmetry_iou, 3)}</dd>` : ''}${ev.lobes != null ? `<dt>${esc(tr('v31_lobes'))}</dt><dd>${num(ev.lobes, 0)}</dd>` : ''}</dl></section>`);
    }
    if (S.weights && S.weights.img) {
      out.push(`<section class="v31-card"><h4>${svg('weights')}<span>${esc(tr('v31_step_weights'))}</span><b>${(S.bones || []).length}</b></h4><img class="v31-wimg" alt="" loading="lazy" decoding="async" data-src="${esc(url(S.weights.img))}"></section>`);
    }
    if (S.pose) {
      const p = S.pose, op = p.opening_deg || {};
      const bad = Number(p.contact_after || 0) > 0;
      out.push(`<section class="v31-card v31-posecard"><h4>${svg('pose')}<span>${esc(tr('v31_pose'))}</span><b class="v31-std">${esc(p.standard || '?')}</b></h4>` +
        `<dl>${Object.keys(op).map((b) => `<dt>${esc(tr('v31_opening'))} · ${esc(b)}</dt><dd>${num(op[b], 1)}°</dd>`).join('')}` +
        `<dt>${esc(tr('v31_contacts'))}</dt><dd><span class="bad">${num(p.contact_before, 0)}</span> ${esc(tr('v31_contacts_before'))} → <span class="${bad ? 'bad' : 'ok'}">${num(p.contact_after, 0)}</span> ${esc(tr('v31_contacts_after'))}</dd></dl></section>`);
    }
    if (S.layers) {
      const L = S.layers.layers || [];
      out.push(`<section class="v31-card"><h4>${svg('layers')}<span>${esc(tr('v31_layers'))}</span><b class="${L.length ? '' : 'warn'}">${L.length}</b></h4>` +
        (L.length ? `<ul class="v31-chips">${L.map((l) => { const bl = Array.isArray(l.bones) ? l.bones : (l.bones && typeof l.bones === 'object' ? Object.values(l.bones).flat().map(String) : (l.chain || [])); const bn = Array.isArray(l.bones) ? l.bones.length : (typeof l.bones === 'number' ? l.bones : bl.length); return `<li title="${esc(bl.join(', '))}">${esc(labelName(l.name || l.label || l.id))} <small>${bn}</small></li>`; }).join('')}</ul>`
          : `<p class="v31-mute warn">${esc(tr('v31_no_layers'))}</p>`) + '</section>');
    }
    if (S.clips.length) {
      out.push(`<section class="v31-card"><h4>${svg('clip')}<span>${esc(tr('v31_clips'))}</span><b>${S.clips.length}</b></h4><ul class="v31-chips">${S.clips.map((c) => {
        const q = S.qa.get(c);
        return `<li class="${q ? (q.verdict === 'pass' ? 'ok' : 'bad') : ''}">${esc(c)}</li>`;
      }).join('')}</ul></section>`);
    }
    if (S.qa.size) {
      const rows = [...S.qa.values()].map((q) => `<tr class="${q.verdict === 'pass' ? 'ok' : 'bad'}" title="${esc(tr(q.verdict === 'pass' ? 'v31_verdict_pass' : 'v31_verdict_fail'))}">` +
        `<td><i></i>${esc(q.clip)}</td><td>${num(q.edges_over_4x, 0)}</td><td>${num(q.worst_stretch, 2)}</td><td>${num(q.stride_H, 2)}</td><td>${num(q.elbow_width_sw, 2)}</td></tr>`).join('');
      const pass = [...S.qa.values()].filter((q) => q.verdict === 'pass').length;
      out.push(`<section class="v31-card"><h4>${svg('qa')}<span>${esc(tr('v31_qa'))}</span><b>${pass}/${S.qa.size}</b></h4><table class="v31-qa"><thead><tr><th></th>` +
        `<th title="${esc(tr('v31_edges4x'))}">4×</th><th title="${esc(tr('v31_stretch'))}">⇔</th><th title="${esc(tr('v31_stride'))}">⟿</th><th title="${esc(tr('v31_elbows'))}">⟷</th></tr></thead><tbody>${rows}</tbody></table></section>`);
    }
    if (S.ext) {
      const e = S.ext, g = e.gltf_validator || {}, u = e.unity_humanoid || {}, b = e.blender || {};
      const cell = (v, okv) => (v == null ? `<span class="v31-mute">${esc(tr('v31_not_run'))}</span>` : `<span class="${okv ? 'ok' : 'bad'}">${esc(num(v, 2))}</span>`);
      out.push(`<section class="v31-card"><h4>${svg('qa_ext')}<span>${esc(tr('v31_qa_external'))}</span></h4><dl>` +
        `<dt>${esc(tr('v31_gltf'))}</dt><dd>${cell(g.errors, g.errors === 0)}</dd>` +
        `<dt>${esc(tr('v31_unity'))}</dt><dd>${u.valid == null ? cell(null) : `<span class="${u.valid ? 'ok' : 'bad'}">${u.valid ? '✓' : '✗'}</span>`}</dd>` +
        `<dt>${esc(tr('v31_blender'))}</dt><dd>${cell(b.worst_stretch, Number(b.worst_stretch) < 4)}</dd></dl></section>`);
    }
    if (S.judge) {
      out.push(`<section class="v31-card"><h4>${svg('judge')}<span>${esc(stepLabel('judge'))}</span><b>${num(S.judge.score, 2)}</b></h4></section>`);
    }
    return out.join('');
  }

  // ---------------------------------------------------------------- relabel
  // the part's card: the numbers of its record (analysis/parts.json), then the contract labels to relabel it
  function srcHtml(src) {
    if (!src) return '—';
    if (typeof src !== 'object') return esc(src);
    const rows = Object.entries(src).filter(([, v]) => v != null && v !== false && v !== '');
    if (!rows.length) return '—';
    return rows.map(([k, v]) => `${esc(k)}${typeof v === 'number' ? ` ${Math.round(v * (v <= 1 ? 100 : 1))}${v <= 1 ? '%' : ''}` : ''}`).join(' · ');
  }
  function qaHtml(qa) {
    const rows = Object.entries(qa || {}).filter(([, v]) => v != null && typeof v !== 'object');
    return rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${typeof v === 'number' ? num(v, 3) : esc(v)}</dd>`).join('');
  }
  function openPop(id, x, y, opts) {
    const p = S.parts.get(id);
    if (!p) { pop.hidden = true; return; }
    const o = opts || {};
    const cur = partLabel(p);
    const bone = p.attached_to || p.attachment_bone || '';
    const chain = Array.isArray(p.chain) ? p.chain : [];
    const beh = p.behaviour ? (tr(`v31_behaviour_${p.behaviour}`) || p.behaviour) : '';
    const qa = qaHtml(p.qa);
    pop.innerHTML = `<h5><i style="background:${esc(p.color)}"></i>${esc(labelName(cur))}${p.side ? ` <small>${esc(tr(`v31_side_${p.side}`))}</small>` : ''}</h5><dl>` +
      `<dt>${esc(tr('v31_confidence'))}</dt><dd>${Math.round(Number(p.confidence || 0) * 100)}%</dd>` +
      `<dt>${esc(tr(typeof p.source === 'object' && p.source ? 'v31_sources' : 'v31_source'))}</dt><dd>${srcHtml(p.source)}</dd>` +
      `<dt>${esc(tr('v31_volume'))}</dt><dd>${num(p.volume_m3, 4)} m³</dd>` +
      (p.voxels != null ? `<dt>${esc(tr('v31_voxels_n'))}</dt><dd>${num(p.voxels, 0)}</dd>` : '') +
      `<dt>${esc(tr('v31_bone'))}</dt><dd>${esc(bone || '—')}</dd>` +
      (chain.length ? `<dt>${esc(tr('v31_chain'))}</dt><dd title="${esc(chain.join(', '))}">${chain.length}</dd>` : '') +
      (beh ? `<dt>${esc(tr('v31_behaviour'))}</dt><dd>${esc(beh)}</dd>` : '') + '</dl>' +
      (qa ? `<h6>${esc(tr('v31_qa_part'))}</h6><dl>${qa}</dl>` : '') +
      `<details class="v31-relabel-d"><summary title="${esc(tr('v31_relabel'))}">${svg('relabel')}<span>${esc(tr('v31_relabel'))}</span></summary>` +
      `<div class="v31-relabel">${CONTRACT_LABELS.map((l) => `<button type="button" data-label="${l}" class="${l === cur ? 'cur' : ''}">${esc(labelName(l))}</button>`).join('')}</div></details>`;
    pop.dataset.part = id;
    pop.dataset.hover = o.hover ? '1' : '';
    pop.hidden = false;
    placePop(x, y, o);
  }
  function placePop(x, y, o) {
    const r = root.getBoundingClientRect();
    const L = layout() || { barsTop: r.height - 16, colLeft: r.width - 16, panelBottom: 64, phone: false };
    pop.classList.toggle('sheet', L.phone);
    if (L.phone) {                                   // a phone: a bottom sheet over the bars' top
      pop.style.left = '8px'; pop.style.top = 'auto'; pop.style.bottom = `${Math.round(r.height - L.barsTop + 8)}px`;
      pop.style.maxHeight = `${Math.max(120, Math.round(L.barsTop - 72))}px`;
      return;
    }
    pop.style.bottom = 'auto';
    const pw = Math.min(260, r.width - 16);
    const maxRight = L.colLeft - 8, maxBottom = L.barsTop - 8;
    pop.style.maxHeight = `${Math.max(120, Math.min(220, maxBottom - 8))}px`;
    const ph = Math.min(pop.offsetHeight || 180, maxBottom - 8);
    let left = o.right && x - r.left + 22 + pw < maxRight ? x - r.left + 22 : x - r.left - pw - 12;
    let top = y - r.top - 20;
    if (maxRight - 8 < pw || maxBottom - 8 < 120) {  // no room beside the icon: under the panel
      left = L.colLeft; top = L.panelBottom + 8;
      pop.style.maxHeight = `${Math.max(100, maxBottom - top)}px`;
    }
    pop.style.left = `${Math.round(Math.max(8, Math.min(maxRight - pw, left)))}px`;
    pop.style.top = `${Math.round(Math.max(8, Math.min(maxBottom - ph, top)))}px`;
  }
  async function relabel(id, label) {
    let res = null;
    try {
      const r = await fetch(`/api/mt/runs/${run}/parts/${encodeURIComponent(id)}/label`, {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ label }),
      });
      res = { status: r.status, body: await r.json().catch(() => ({})) };
    } catch (e) { res = { status: 0, body: {} }; }
    W.lastRelabel = res;
    if (res.status === 200 && res.body.ok) {
      S.labels[id] = res.body.label;
      steps.push({ k: 'relabel', data: { id, label: res.body.label, version: res.body.version }, t: Date.now() / 1000, local: true });
      if (idx === steps.length - 1) idx = steps.length;
      ticks.innerHTML = ticksHtml();
      say(tr('v31_relabel_saved', { version: res.body.version }), 'ok');
    } else if (res.status === 403 || res.status === 401) say(tr('v31_relabel_denied'), 'bad');
    else say(`${tr('v31_relabel_failed')} (${res.status || 'net'})`, 'bad');
    pop.hidden = true;
    paint();
  }
  let toastT = 0;
  function say(text, cls) {
    toast.textContent = text;
    toast.className = `v31-toast ${cls || ''}`;
    toast.hidden = false;
    clearTimeout(toastT);
    toastT = setTimeout(() => { toast.hidden = true; }, 3200);
  }
  cards.addEventListener('click', (e) => {
    const li = e.target.closest('.v31-part');
    if (!li) return;
    const id = li.dataset.part;
    selected = selected === id && !pop.hidden ? null : id;
    if (selected) openPop(id, li.getBoundingClientRect().left, e.clientY); else pop.hidden = true;
    paint();
  });
  cards.addEventListener('mouseover', (e) => {
    const li = e.target.closest('.v31-part');
    usend('LivePartHover', { id: li ? li.dataset.part : '' }, 'hover');
  });
  cards.addEventListener('mouseleave', () => usend('LivePartHover', { id: '' }, 'hover'));
  pop.addEventListener('mouseenter', () => { clearTimeout(hoverHide); pop.dataset.hover = ''; });
  pop.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-label]');
    if (b && pop.dataset.part) relabel(pop.dataset.part, b.dataset.label);
  });
  document.addEventListener('keydown', onEsc);

  // ---------------------------------------------------------------- the presentation group (owner 2026-10-18: «сделай чтобы можно
  // было убрать меню превью визуализации, нафига оно там висит и его нельзя убрать»). The cards panel, the scrub bar and
  // the part card are one group: × on the panel, Esc and the toolbar icon hide it; the icon brings it back. On a finished
  // run it goes away 4 s after the replay. The person's explicit choice is remembered (this browser, and the person's
  // viewer settings v31_presentation through the build's SetSetting; scene-gate.js keeps v31_* out of the owner's
  // defaults). Every decision: console «[v31-ui] …».
  let presHidden = false, presAuto = 0, presPerson = null;     // presPerson: the remembered choice (true = hidden)
  try { const v = localStorage.getItem('autorig_v31_presentation'); if (v === 'hidden') presPerson = true; else if (v === 'shown') presPerson = false; } catch (e) { /* storage off */ }
  function uiLog(what) { try { console.info(`[v31-ui] ${what}`); } catch (e) { /* no console */ } }
  function setPres(hidden, why, person) {
    clearTimeout(presAuto);
    presHidden = !!hidden;
    root.classList.toggle('pres-off', presHidden);
    if (presHidden) { pop.hidden = true; selected = null; }
    presTool.classList.toggle('on', !presHidden);
    presTool.title = tr(presHidden ? 'v31_pres_show' : 'v31_pres_hide');
    presTool.setAttribute('aria-label', presTool.title);
    if (person) {
      presPerson = presHidden;
      try { localStorage.setItem('autorig_v31_presentation', presHidden ? 'hidden' : 'shown'); } catch (e) { /* storage off */ }
      try { const w = viewerEl.contentWindow; if (w && w.autorigUnity) w.autorigUnity.send('SetSetting', { v31_presentation: presHidden ? 0 : 1 }); } catch (e) { /* the viewer went away */ }
    }
    uiLog(`presentation ${presHidden ? 'hidden' : 'shown'} · ${why}${person ? ' (person)' : ''}`);
    layout();
  }
  function presAutoHide(ms, why) {
    clearTimeout(presAuto);
    presAuto = setTimeout(() => {
      if (presHidden || replayMode) return;
      if (root.matches(':hover') && (panel.matches(':hover') || scrub.matches(':hover') || pop.matches(':hover'))) { presAutoHide(2000, why.replace(/ \(.*$/, '') + ' (waited for the pointer)'); return; }
      setPres(true, `auto after the ${why}`, false);
    }, ms);
  }
  function onEsc(e) {
    if (e.key !== 'Escape' || e.defaultPrevented) return;
    const t = e.target, tag = t && t.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || (t && t.isContentEditable)) return;
    try { const d = viewerEl.contentDocument; if (d && d.body && d.body.classList.contains('bp')) return; } catch (err) { /* cross-origin */ }
    if (!root.classList.contains('on')) return;
    if (!pop.hidden) { pop.hidden = true; selected = null; paint(); return; }
    if (!presHidden) { if (replayMode) skip(); setPres(true, 'Esc', true); }
  }
  // the person's choice from the viewer settings (another device); this browser's choice is read above
  fetch('/api/mt/viewer/settings', { credentials: 'same-origin', cache: 'no-store' }).then((x) => (x.ok ? x.json() : null))
    .then((d) => {
      const v = d && d.settings ? d.settings.v31_presentation : undefined;
      if (v === 0 || v === 1) { presPerson = v === 0; if (presPerson && !presHidden) setPres(true, 'remembered (viewer settings)', false); }
    }).catch(() => {});
  if (presPerson === true) setPres(true, 'remembered (this browser)', false);

  // the bones are the viewer's (index.html api.bones: one flag, one owner); the replay only says when it presents
  const bonesQueue = [];
  function bonesCall(fn, ...args) {
    let w = null;
    try { w = viewerEl.contentWindow; } catch (e) { w = null; }
    const b = w && w.autorigUnity && w.autorigUnity.bones;
    if (b && typeof b[fn] === 'function') { try { b[fn](...args); } catch (e) { console.warn('[v31-ui] bones', e); } return; }
    bonesQueue.push([fn, args, Date.now()]);
  }
  setInterval(() => {
    if (!bonesQueue.length) return;
    let w = null;
    try { w = viewerEl.contentWindow; } catch (e) { w = null; }
    const b = w && w.autorigUnity && w.autorigUnity.bones;
    if (!b) { while (bonesQueue.length && Date.now() - bonesQueue[0][2] > 60000) bonesQueue.shift(); return; }
    bonesQueue.splice(0).forEach(([fn, args]) => { try { b[fn](...args); } catch (e) { console.warn('[v31-ui] bones', e); } });
  }, 500);

  // ---------------------------------------------------------------- one layout grid over the canvas (owner 2026-10-18: «сделай
  // нормальную верстку»): the right column holds the viewer's right toolbar and, beside it, the panel (≤ 260 px, scrolls
  // inside, never over the call window); the bottom centre holds the clip bar and above it the scrub (12 px gap, the
  // same width rule, moved left of the call window when they would meet); the card stays inside what is left.
  var lay = null;   // var: layout() is hoisted and ran before this line (ReferenceError at :684, verifier 2026-10-20)
  function layout() {
    const r = root.getBoundingClientRect();
    if (!r.width || !r.height) return lay;
    let d = null;
    try { d = viewerEl.contentDocument; } catch (e) { d = null; }
    const vr = viewerEl.getBoundingClientRect();
    const box = (el) => {
      if (!el) return null;
      const b = el.getBoundingClientRect();
      if (!b.width || !b.height) return null;
      const cs = el.ownerDocument.defaultView.getComputedStyle(el);
      if (cs.display === 'none' || cs.visibility === 'hidden' || Number(cs.opacity) === 0) return null;
      const ox = vr.left - r.left, oy = vr.top - r.top;
      return { l: ox + b.left, t: oy + b.top, r: ox + b.right, b: oy + b.bottom, w: b.width, h: b.height };
    };
    const vw = r.width, vh = r.height, phone = vw <= 480;
    const clipsB = d ? box(d.getElementById('clips')) : null;
    const fxB = d ? box(d.getElementById('fx')) : null;
    const callB = d ? box(d.querySelector('#call.on')) : null;
    // the scene strip (top right): its box is taller than what it draws, so the drawn children decide where it ends
    let scpB = null;
    if (d) {
      const sp = d.getElementById('scp');
      if (sp && box(sp)) [...sp.querySelectorAll('*')].forEach((el) => { const q = el.children.length ? null : box(el); if (q && q.h < 120) scpB = scpB ? { l: Math.min(scpB.l, q.l), r: Math.max(scpB.r, q.r), b: Math.max(scpB.b, q.b) } : q; });
    }
    root.classList.toggle('phone', phone);
    // the bottom centre
    let floor = vh - 12;
    if (clipsB) floor = Math.min(floor, clipsB.t);
    let sw = Math.min(560, Math.max(320, clipsB ? clipsB.w : 520), vw - 24);
    let cx = vw / 2;
    const sh = scrub.offsetHeight || 32;
    let bandTop = floor - 12 - sh;
    if (callB && callB.t < floor - 12 && callB.b > bandTop && callB.l < cx + sw / 2 + 12) {
      const room = callB.l - 12 - 12;                 // left of the call window
      if (room >= 220) {
        sw = Math.min(sw, room);
        cx = Math.max(12 + sw / 2, Math.min(cx, callB.l - 12 - sw / 2));
      } else {                                         // no room beside it (a phone): above it
        floor = Math.min(floor, callB.t);
        bandTop = floor - 12 - sh;
      }
    }
    scrub.style.left = `${Math.round(cx)}px`;
    scrub.style.width = `${Math.round(sw)}px`;
    scrub.style.bottom = `${Math.round(vh - floor + 12)}px`;
    const scrubShown = !scrub.hidden && !presHidden;
    const barsTop = scrubShown ? bandTop : floor;
    // the right column
    let pRight = fxB ? vw - fxB.l + 8 : 16;
    const pw0 = Math.min(260, panel.offsetWidth || 236);
    const top = Math.round(phone ? 56 : (scpB && scpB.r > vw - pRight - pw0 - 260 && scpB.l < vw - pRight ? Math.max(64, scpB.b + 8) : 64));
    let pBottom = barsTop - 12;
    const pw = Math.min(260, panel.offsetWidth || 236);
    if (callB && callB.t < pBottom && callB.l < vw - pRight && callB.r > vw - pRight - pw) {
      if (callB.t - 12 - top >= 160) pBottom = callB.t - 12;   // above the call window when there is room
      else pRight = vw - callB.l + 8;                           // else beside it
    }
    panel.style.right = `${Math.round(pRight)}px`;
    panel.style.top = `${top}px`;
    panel.style.maxHeight = `${Math.round(Math.max(phone ? 40 : 96, pBottom - top))}px`;
    const colLeft = vw - pRight - pw;
    lay = { W: vw, H: vh, phone, barsTop, floor, colLeft: presHidden || !root.classList.contains('has-cards') ? vw - pRight : colLeft,
      panelBottom: top + Math.min(panel.offsetHeight || 0, pBottom - top), clips: clipsB, call: callB, fx: fxB, scrub: { cx, sw } };
    W.layout = lay;
    // an open card follows the bars (they move when the scrub or the clip bar appear)
    if (!pop.hidden) {
      if (pop.classList.contains('sheet')) pop.style.bottom = `${Math.round(vh - barsTop + 8)}px`;
      else {
        const t = parseFloat(pop.style.top) || 0, maxT = barsTop - 8 - pop.offsetHeight;
        if (t > maxT) pop.style.top = `${Math.round(Math.max(8, maxT))}px`;
      }
    }
    return lay;
  }
  setInterval(layout, 600);
  window.addEventListener('resize', layout);
  // a phone: the panel is its head (the icon of the current stage); a tap on it opens the cards
  nowEl.addEventListener('click', () => { if (root.classList.contains('phone')) root.classList.toggle('cards-open'); });

  // ---------------------------------------------------------------- the 3D side (Unity, LiveLayerParts.cs)
  let u3d = false, uSent = {}, uState = null, replay3d = false, drv = {}, hoverHide = 0;
  let partsMode = 'icons';
  try { const m = localStorage.getItem('autorig_v31_parts_mode'); if (m === 'full' || m === 'none' || m === 'icons') partsMode = m; } catch (e) { /* storage off */ }
  const uwin = () => { try { return viewerEl.contentWindow; } catch (e) { return null; } };
  const abs = (u) => { try { return new URL(url(u), location.origin).href; } catch (e) { return u; } };
  function usendRaw(m, v) {
    const w = uwin();
    if (!u3d || !w || !w.autorigUnity || typeof w.autorigUnity.send !== 'function') return false;
    try { w.autorigUnity.send(m, v); } catch (e) { return false; }
    return true;
  }
  // a message is sent again only when it changed (paint() calls the sync often)
  function usend(m, v, key) {
    const k = key || m, sv = JSON.stringify(v);
    if (uSent[k] === sv) return;
    if (usendRaw(m, v)) uSent[k] = sv;
  }
  function onUnity(ev) {
    const d = ev.detail || {};
    let data = null;
    try { data = d.json ? JSON.parse(d.json) : null; } catch (e) { data = null; }
    if (d.name === 'live_ready_v31') {
      const first = !u3d;
      u3d = true;
      uSent = {};
      W.u3d = data;
      if (first) usendRaw('LiveV31Hello', '');
      paint();
    } else if (d.name === 'live_parts_state') {
      uState = data;
      W.uState = data;
      paintPartsTool();
    } else if (d.name === 'live_part_hover' && data) {
      const r = viewerEl.getBoundingClientRect();
      clearTimeout(hoverHide);
      if (data.id) {
        if (!pop.hidden && !pop.dataset.hover && pop.dataset.part !== data.id) return;   // a pinned card stays
        if (pop.hidden || pop.dataset.hover) openPop(data.id, r.left + data.x * r.width, r.top + data.y * r.height, { hover: true, right: true });
      } else if (!pop.hidden && pop.dataset.hover) hoverHide = setTimeout(() => { if (pop.dataset.hover) pop.hidden = true; }, 700);
    } else if (d.name === 'live_part_pick' && data && data.id) {
      const r = viewerEl.getBoundingClientRect();
      selected = data.selected || null;
      W.lastPick = data;
      if (selected) openPop(data.id, r.left + data.x * r.width, r.top + data.y * r.height, { right: true });
      else pop.hidden = true;
      paint();
    }
  }
  function hookUnity() {
    const w = uwin();
    if (!w || w.__v31hook) return;
    try { w.__v31hook = true; w.addEventListener('autorig-unity', onUnity); w.addEventListener('keydown', onEsc); } catch (e) { /* cross-origin */ }
  }
  hookUnity();
  setInterval(hookUnity, 500);
  viewerEl.addEventListener('load', () => { u3d = false; uSent = {}; drv = {}; hookUnity(); });

  function unityParts() {
    return S.order.map((id) => {
      const p = S.parts.get(id);
      const m = p.mask_url || p.mask_url_96;          // the 48 mask during the presentation (~3 KB a part), not the 96 (~20-70 KB)
      const o = { id, label: partLabel(p), color: p.color || '', priority: p.group === 'props' ? 2 : p.group === 'garments' ? 1 : 0 };
      if (p.icon) o.icon = p.icon;
      if (m) o.mask_url = abs(m);
      if (Array.isArray(p.centroid)) o.centroid = p.centroid;
      else if (p.bbox && p.bbox.min && p.bbox.max && !m) o.bbox = p.bbox;
      const bone = p.attached_to || p.attachment_bone;
      if (bone) o.bone = bone;
      if (Array.isArray(p.chain) && p.chain.length) o.bones = p.chain;
      return o;
    });
  }
  // what Unity shows of the parts: while the stages play (a 3D replay, a live run), the voxels in part colours until the
  // bones, then the part shader on the mesh; at rest the person's mode (icons only by default)
  function syncUnity() {
    if (!u3d) return;
    const parts = unityParts();
    usend('LiveParts', { parts });
    usend('LivePartSelect', { id: selected || '' }, 'sel');
    if (!parts.length) return;
    const presenting = replay3d || (live && !runDone && !replayMode);
    if (presenting) {
      usend('LivePartsOff', { mode: 'full', pin: true }, 'mode');
      usend('LiveShow', { layer: 'voxelparts', on: !S.bones }, 'vp');
      usend('LiveShow', { layer: 'parts', on: !!S.bones }, 'pm');
    } else {
      usend('LiveShow', { layer: 'voxelparts', on: false }, 'vp');
      usend('LiveShow', { layer: 'parts', on: true }, 'pm');
      usend('LivePartsOff', { mode: partsMode, pin: partsMode !== 'icons' }, 'mode');
    }
    drive3d();
  }
  // a 3D replay: the voxel levels, the bones and the weights go to Unity from the steps played so far
  function drive3d() {
    if (!replay3d || !u3d) return;
    const done = steps.slice(0, idx);
    let lastVox = null, lastBones = null;
    for (const st of done) { if (st.k === 'vox') lastVox = st; if (st.k === 'bones' || st.k === 'weights') lastBones = st; }
    const cur = done[done.length - 1];
    const voxUrl = lastVox ? abs(lastVox.url) : '';
    if (voxUrl && drv.vox !== voxUrl) { usendRaw('LiveVoxels', { url: voxUrl, res: lastVox.res, fine: !!lastVox.fine }); drv.vox = voxUrl; }
    const voxOn = !!lastVox && !S.bones;
    if (drv.voxOn !== voxOn) { usendRaw('LiveShow', { layer: 'voxels', on: voxOn }); drv.voxOn = voxOn; }
    if (lastBones && drv.bones !== lastBones) { usendRaw('LiveBones', { bones: lastBones.bones || [] }); usendRaw('LiveShow', { layer: 'bones', on: true }); drv.bones = lastBones; }
    const heat = !!(cur && cur.k === 'weights');
    if (drv.heat !== heat) { usendRaw('LiveWeights', { mode: heat ? 'dominant' : 'off' }); drv.heat = heat; }
  }
  function start3d() {
    replay3d = true;
    drv = {};
    uSent = {};
    usendRaw('LiveReset', '');
  }
  function end3d() {
    if (!replay3d) return;
    replay3d = false;
    usendRaw('LiveShow', { layer: 'voxels', on: false });
    usendRaw('LiveWeights', { mode: 'off' });
    drv = {};
    syncUnity();
  }
  function paintPartsTool() {
    const show = u3d && S.parts.size > 0;
    partsTool.hidden = !show;
    if (!show) return;
    const mode = uState && uState.mode && !replay3d ? uState.mode : partsMode;
    partsTool.innerHTML = `<svg viewBox="0 0 24 24" aria-hidden="true">${ICON[`parts_${mode}`] || ICON.parts_icons}</svg>`;
    partsTool.title = tr(`v31_parts_mode_${mode}`);
    partsTool.setAttribute('aria-label', partsTool.title);
    partsTool.classList.toggle('on', mode !== 'none');
  }
  partsTool.addEventListener('click', () => {
    const order = ['icons', 'full', 'none'];
    const cur = uState && uState.mode && !replay3d ? uState.mode : partsMode;
    partsMode = order[(order.indexOf(cur) + 1) % order.length];
    try { localStorage.setItem('autorig_v31_parts_mode', partsMode); } catch (e) { /* storage off */ }
    delete uSent.mode;
    if (replay3d || replayMode) skip();
    syncUnity();
    paintPartsTool();
  });

  // ---------------------------------------------------------------- the canvas (the voxel splat)
  let maskIndex = null;     // quantized position -> part id, from the loaded masks
  const maskData = new Map();
  async function buildMaskIndex() {
    const idx2 = new Map();
    let cell = 0;
    for (const id of S.order) {
      const p = S.parts.get(id);
      if (!p.mask_url) continue;
      const v = await loadVox(url(p.mask_url));
      if (!v) continue;
      cell = cell || v.cell;
      for (let i = 0; i < v.n; i++) {
        const key = qkey(v.pos[i * 3] - v.origin[0], v.pos[i * 3 + 1] - v.origin[1], v.pos[i * 3 + 2] - v.origin[2], v.cell);
        if (!idx2.has(key) || p.group !== 'parts') idx2.set(key, id);   // garments / props over the body part
      }
      maskData.set(id, { cell: v.cell, origin: v.origin });
    }
    return { map: idx2, cell, origin: maskData.size ? [...maskData.values()][0].origin : [0, 0, 0] };
  }
  let maskBuilding = false, maskFor = '';
  function partsKey() { return S.order.join('|'); }
  function voxColor(v, i, partId, partsOn) {
    if (partsOn && partId) {
      const p = S.parts.get(partId);
      return hex(p && p.color ? p.color : '#cccccc');
    }
    const d = v.depth[i];
    if (v.core[i] && S.erosion) return [255, 200, 70];
    const t = d === 127 || !v.maxD ? 0 : d / v.maxD;
    return [Math.round(76 + (158 - 76) * t), Math.round(217 + (82 - 217) * t), Math.round(255 + (255 - 255) * t)];
  }
  const hexCache = {};
  function hex(h) {
    if (hexCache[h]) return hexCache[h];
    const m = /^#?([0-9a-f]{6})$/i.exec(h || '');
    const n = m ? parseInt(m[1], 16) : 0xcccccc;
    return (hexCache[h] = [(n >> 16) & 255, (n >> 8) & 255, n & 255]);
  }
  function bonePalette(i) { const h = (i * 0.618034) % 1; return hsv(h, 0.62, 0.95); }
  function hsv(h, s, v) {
    const i = Math.floor(h * 6), f = h * 6 - i, p = v * (1 - s), q = v * (1 - f * s), t = v * (1 - (1 - f) * s);
    const [r, g, b] = [[v, t, p], [q, v, p], [p, v, t], [p, q, v], [t, p, v], [v, p, q]][i % 6];
    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
  }
  function nearestBones(v, bones) {
    const lab = new Int16Array(v.n);
    for (let i = 0; i < v.n; i++) {
      const x = v.pos[i * 3], y = v.pos[i * 3 + 1], z = v.pos[i * 3 + 2];
      let best = 1e9, bi = -1;
      for (let j = 0; j < bones.length; j++) {
        const a = bones[j].head, b = bones[j].tail;
        if (!a || !b) continue;
        const abx = b[0] - a[0], aby = b[1] - a[1], abz = b[2] - a[2];
        const L = abx * abx + aby * aby + abz * abz || 1e-9;
        let t = ((x - a[0]) * abx + (y - a[1]) * aby + (z - a[2]) * abz) / L;
        t = t < 0 ? 0 : t > 1 ? 1 : t;
        const dx = x - (a[0] + t * abx), dy = y - (a[1] + t * aby), dz = z - (a[2] + t * abz);
        const d = dx * dx + dy * dy + dz * dz;
        if (d < best) { best = d; bi = j; }
      }
      lab[i] = bi;
    }
    return lab;
  }
  // the pose opening: the arm chains turn about their shoulders in the frontal plane (and back)
  function armPivots() {
    const bones = S.bones || [];
    const by = Object.fromEntries(bones.map((b) => [b.name, b]));
    const out = [];
    for (const [side, sgn] of [['Left', 1], ['Right', -1]]) {
      const arm = by[`${side}Arm`];
      if (!arm) continue;
      const deg = Number((S.pose && S.pose.opening_deg && S.pose.opening_deg[`${side}Arm`]) || 0);
      const names = new Set();
      const walk = (n) => { names.add(n); bones.forEach((b) => { if (b.parent === n) walk(b.name); }); };
      walk(`${side}Arm`);
      out.push({ side, sgn, deg, pivot: arm.head, names });
    }
    return out;
  }
  function rotZ(p, pivot, a) {
    const x = p[0] - pivot[0], y = p[1] - pivot[1], c = Math.cos(a), s = Math.sin(a);
    return [pivot[0] + x * c - y * s, pivot[1] + x * s + y * c, p[2]];
  }

  // frame cost and rate of the presentation (window.autorigV31.perf(): draw ms p50 / p95, frames per second)
  const perfMs = [], perfAt = [];
  function perfNote(ms) {
    perfMs.push(ms); perfAt.push(performance.now());
    if (perfMs.length > 600) { perfMs.shift(); perfAt.shift(); }
  }
  W.perf = (windowMs = 5000) => {
    const now = performance.now();
    const ms = perfMs.filter((_, i) => now - perfAt[i] < windowMs).slice().sort((a, b) => a - b);
    const q = (f) => (ms.length ? Math.round(ms[Math.min(ms.length - 1, Math.floor(f * ms.length))] * 10) / 10 : null);
    return { frames: ms.length, fps: Math.round((ms.length / windowMs) * 10000) / 10, draw_p50_ms: q(0.5), draw_p95_ms: q(0.95), draw_max_ms: q(1) };
  };
  // the same frames without the display (a hidden tab has no requestAnimationFrame): n draws back to back
  W.bench = async (n = 120) => {
    perfMs.length = 0; perfAt.length = 0;
    const t0 = performance.now();
    for (let i = 0; i < n; i++) await draw(performance.now(), 1 / 60);
    const wall = performance.now() - t0;
    return { ...W.perf(1e9), frames: n, wall_ms: Math.round(wall), max_fps: Math.round((n / wall) * 10000) / 10 };
  };
  let lastFrame = 0, raf = 0, cvShown = null, cvRect = null;
  try { new ResizeObserver(() => { cvRect = null; }).observe(cv); } catch (e) { /* old browser: read every frame */ }
  function frame(now) {
    raf = requestAnimationFrame(frame);
    if (!root.isConnected || collapsed) return;
    const showCanvas = !docked || mapOn;
    if (cvShown !== showCanvas) { cvShown = showCanvas; cv.style.display = showCanvas ? '' : 'none'; }
    if (!showCanvas) return;
    if (now - lastFrame < 12) return;                    // every display frame (a draw costs ~3 ms), at most ~80 Hz
    const dt = Math.min(0.1, (now - lastFrame) / 1000);
    lastFrame = now;
    draw(now, dt);
  }
  async function draw(now, dt) {
    const r = cvRect || (cvRect = cv.getBoundingClientRect());
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const Wd = Math.max(1, Math.round(r.width * dpr)), Hd = Math.max(1, Math.round(r.height * dpr));
    if (cv.width !== Wd || cv.height !== Hd) { cv.width = Wd; cv.height = Hd; }
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.clearRect(0, 0, Wd, Hd);
    if (!S.vox) return;
    const fine = steps.slice(0, idx).filter((s) => s.k === 'vox').map((s) => s.url);
    // the 48 level is the splat (≈6 k cells); finer levels are drawn by Unity
    const pick = fine.find((u) => /vox_48\.bin$/.test(u)) && S.vox.res >= 48 ? fine.find((u) => /vox_48\.bin$/.test(u)) : S.vox.url;
    const v = await voxCache.get(url(pick));
    if (!v) { loadVox(url(pick)); return; }
    const tDraw = performance.now();
    if (dragYaw == null) yaw += dt * (docked ? 0.35 : 0.45);
    const pitch = 0.22;
    // bounds
    let minx = 1e9, miny = 1e9, minz = 1e9, maxx = -1e9, maxy = -1e9, maxz = -1e9;
    for (let i = 0; i < v.n; i++) {
      const x = v.pos[i * 3], y = v.pos[i * 3 + 1], z = v.pos[i * 3 + 2];
      if (x < minx) minx = x; if (x > maxx) maxx = x; if (y < miny) miny = y; if (y > maxy) maxy = y; if (z < minz) minz = z; if (z > maxz) maxz = z;
    }
    const cx = (minx + maxx) / 2, cy = (miny + maxy) / 2, cz = (minz + maxz) / 2;
    const span = Math.max(maxy - miny, Math.hypot(maxx - minx, maxz - minz)) || 1;
    const narrow = r.width < 720;
    const scale = Math.min(Wd * (docked ? 0.7 : narrow ? 0.5 : 0.45), Hd * (docked ? 0.78 : 0.58)) / span;
    const ox = Wd * (docked ? 0.5 : narrow ? 0.4 : 0.44), oy = Hd * (docked ? 0.5 : 0.47);
    const cyw = Math.cos(yaw), syw = Math.sin(yaw), cp = Math.cos(pitch), sp = Math.sin(pitch);
    const proj = (x, y, z) => {
      const X = (x - cx) * cyw + (z - cz) * syw, Z = -(x - cx) * syw + (z - cz) * cyw, Y = y - cy;
      const Y2 = Y * cp - Z * sp, Z2 = Y * sp + Z * cp;
      return [ox + X * scale, oy - Y2 * scale, Z2];
    };
    // parts
    const partsOn = S.parts.size > 0;
    if (partsOn && maskFor !== partsKey() && !maskBuilding) {
      maskBuilding = true;
      const key = partsKey();
      buildMaskIndex().then((m) => { maskIndex = m; maskFor = key; maskBuilding = false; }).catch(() => { maskBuilding = false; });
    }
    const weightsOn = S.weights && !partsOn ? true : (stepKind === 'weights' && performance.now() - stepT0 < (PACE.weights + 600));
    if (weightsOn && S.bones && !boneLab) boneLab = { v, lab: nearestBones(v, S.bones) };
    // pose animation: 0 -> 1 -> 0 over the step (open, hold, return); a docked view keeps the source pose
    let open = 0;
    if (S.pose && stepKind === 'pose') {
      const u = (performance.now() - stepT0) / PACE.pose;
      open = u < 0.35 ? u / 0.35 : u < 0.65 ? 1 : u < 1 ? 1 - (u - 0.65) / 0.35 : 0;
      open = open * open * (3 - 2 * open);
    }
    const pivots = open > 0 ? armPivots() : [];
    const armPart = { Left: 'arm_left', Right: 'arm_right' };
    // eroding: shells peel away over the step
    let minDepth = 0;
    if (stepKind === 'erosion') {
      const u = Math.min(1, (performance.now() - stepT0) / PACE.erosion);
      minDepth = Math.floor((u < 0.75 ? u / 0.75 : 1) * v.maxD);
      if (u >= 0.98) minDepth = 0;
    }
    const n = v.n, sx = new Float32Array(n), sy = new Float32Array(n), sz = new Float32Array(n), order = new Uint32Array(n);
    const ids = new Array(n);
    let cnt = 0;
    const mi = maskIndex && maskFor === partsKey() ? maskIndex : null;
    for (let i = 0; i < n; i++) {
      if (minDepth && (v.depth[i] === 127 || v.depth[i] < minDepth)) continue;
      let p = [v.pos[i * 3], v.pos[i * 3 + 1], v.pos[i * 3 + 2]];
      let id = null;
      if (mi) id = mi.map.get(qkey(p[0] - mi.origin[0], p[1] - mi.origin[1], p[2] - mi.origin[2], mi.cell)) || null;
      if (open > 0) {
        for (const pv of pivots) {
          const inArm = id ? (id === armPart[pv.side]) : (boneLab && S.bones[boneLab.lab[i]] && pv.names.has(S.bones[boneLab.lab[i]].name));
          if (inArm) p = rotZ(p, pv.pivot, pv.sgn * pv.deg * open * Math.PI / 180);
        }
      }
      const q = proj(p[0], p[1], p[2]);
      sx[i] = q[0]; sy[i] = q[1]; sz[i] = q[2]; ids[i] = id;
      order[cnt++] = i;
    }
    const ord = Array.from(order.subarray(0, cnt)).sort((a, b) => sz[a] - sz[b]);
    let zmin = 1e9, zmax = -1e9;
    for (const i of ord) { if (sz[i] < zmin) zmin = sz[i]; if (sz[i] > zmax) zmax = sz[i]; }
    const size = Math.max(1.2, v.cell * scale * 1.08);
    const appear = stepKind === 'part' ? Math.min(1, (performance.now() - stepT0) / 450) : 1;
    const newest = stepKind === 'part' ? steps[idx - 1].data.id : null;
    for (const i of ord) {
      let c;
      const id = ids[i];
      if (weightsOn && boneLab && boneLab.v === v && boneLab.lab[i] >= 0) c = bonePalette(boneLab.lab[i]);
      else c = voxColor(v, i, id, partsOn && !!mi);
      let k = 0.55 + 0.45 * ((sz[i] - zmin) / ((zmax - zmin) || 1));
      if (selected && id !== selected) k *= 0.28;
      if (newest && id === newest) k *= 0.6 + 0.8 * appear;
      ctx.fillStyle = `rgb(${Math.min(255, c[0] * k) | 0},${Math.min(255, c[1] * k) | 0},${Math.min(255, c[2] * k) | 0})`;
      ctx.fillRect(sx[i] - size / 2, sy[i] - size / 2, size, size);
    }
    // positioning: up / forward and the height ruler
    if (S.frame && !(docked && !mapOn)) {
      const gx = minx - (maxx - minx) * 0.18;
      const base = proj(gx, miny, cz), top = proj(cx, maxy, cz);
      const fwd = proj(gx, miny, cz + span * 0.3);
      const lw = Math.max(1.5, 2 * dpr);
      arrow(base, proj(gx, miny + (maxy - miny) * 0.3, cz), '#80ed99', lw, tr('v31_up'));
      arrow(base, fwd, '#4cc9f0', lw, tr('v31_forward'));
      const rx = ox + span * scale * 0.42, foot = proj(cx, miny, cz)[1];
      ctx.strokeStyle = 'rgba(255,255,255,.55)'; ctx.lineWidth = lw * 0.7;
      ctx.beginPath(); ctx.moveTo(rx, foot); ctx.lineTo(rx, top[1]); ctx.stroke();
      const hm = S.heightM;
      const tick = 10;
      for (let j = 0; j <= tick; j++) {
        const yy = foot + (top[1] - foot) * (j / tick);
        ctx.beginPath(); ctx.moveTo(rx, yy); ctx.lineTo(rx + (j % 5 ? 4 : 8) * dpr, yy); ctx.stroke();
      }
      ctx.fillStyle = '#fff'; ctx.font = `${600} ${11 * dpr}px system-ui,sans-serif`;
      ctx.fillText(hm ? `${num(hm, 2)} m` : 'H', rx + 10 * dpr, top[1] + 4 * dpr);
    }
    // bones
    if (S.bones && idx && ['bones', 'weights', 'pose', 'layers', 'clip', 'qa', 'qa_ext', 'judge', 'done', 'relabel', 'part', 'cls'].includes(stepKind) && S.bones.length && (!partsOn || stepKind !== 'part')) {
      const grow = stepKind === 'bones' ? Math.min(1, (performance.now() - stepT0) / 900) : 1;
      ctx.lineWidth = Math.max(1.5, 2.2 * dpr);
      for (const b of S.bones) {
        if (!b.head || !b.tail) continue;
        let h = b.head, t = b.tail;
        for (const pv of pivots) if (pv.names.has(b.name)) { h = rotZ(h, pv.pivot, pv.sgn * pv.deg * open * Math.PI / 180); t = rotZ(t, pv.pivot, pv.sgn * pv.deg * open * Math.PI / 180); }
        const a = proj(...h), e = proj(h[0] + (t[0] - h[0]) * grow, h[1] + (t[1] - h[1]) * grow, h[2] + (t[2] - h[2]) * grow);
        ctx.strokeStyle = 'rgba(255,255,255,.9)';
        ctx.beginPath(); ctx.moveTo(a[0], a[1]); ctx.lineTo(e[0], e[1]); ctx.stroke();
        ctx.fillStyle = '#ffd166';
        ctx.beginPath(); ctx.arc(a[0], a[1], 2.4 * dpr, 0, Math.PI * 2); ctx.fill();
      }
      // contacts of the pose: red before the opening, green once open
      if (S.pose && (stepKind === 'pose')) {
        const by = Object.fromEntries(S.bones.map((b) => [b.name, b]));
        for (const pv of armPivots()) {
          const hand = by[`${pv.side}Hand`];
          if (!hand) continue;
          const hp = rotZ(hand.head, pv.pivot, pv.sgn * pv.deg * open * Math.PI / 180);
          const q = proj(...hp);
          const touching = (S.pose.contacts || []).includes(pv.side[0]) || Number(S.pose.contact_before) > 0;
          ctx.strokeStyle = open > 0.6 || !touching ? '#80ed99' : '#ff4d4d';
          ctx.lineWidth = 2.5 * dpr;
          ctx.beginPath(); ctx.arc(q[0], q[1], (7 + 3 * Math.sin(performance.now() / 160)) * dpr, 0, Math.PI * 2); ctx.stroke();
        }
        ctx.fillStyle = '#fff'; ctx.font = `700 ${28 * dpr}px system-ui,sans-serif`;
        ctx.fillText(open > 0.6 ? (S.pose.standard || '') : '', ox - 9 * dpr, oy - span * scale * 0.56);
      }
    }
    if (!W.t.firstDraw) W.t.firstDraw = Math.round(performance.now() - T_OPEN);
    perfNote(performance.now() - tDraw);
  }
  function arrow(a, b, col, lw, label) {
    ctx.strokeStyle = col; ctx.fillStyle = col; ctx.lineWidth = lw;
    ctx.beginPath(); ctx.moveTo(a[0], a[1]); ctx.lineTo(b[0], b[1]); ctx.stroke();
    const ang = Math.atan2(b[1] - a[1], b[0] - a[0]), h = 9 * lw / 2;
    ctx.beginPath(); ctx.moveTo(b[0], b[1]);
    ctx.lineTo(b[0] - h * Math.cos(ang - 0.45), b[1] - h * Math.sin(ang - 0.45));
    ctx.lineTo(b[0] - h * Math.cos(ang + 0.45), b[1] - h * Math.sin(ang + 0.45)); ctx.fill();
    ctx.font = `600 ${5.5 * lw}px system-ui,sans-serif`;
    ctx.fillText(label, b[0] + 5, b[1] - 5);
  }
  cv.addEventListener('pointerdown', (e) => { dragYaw = { x: e.clientX, yaw }; cv.setPointerCapture(e.pointerId); });
  cv.addEventListener('pointermove', (e) => { if (dragYaw) yaw = dragYaw.yaw + (e.clientX - dragYaw.x) * 0.01; });
  cv.addEventListener('pointerup', () => { dragYaw = null; });

  // ---------------------------------------------------------------- the player
  let playToken = 0;
  async function play(from) {
    const token = ++playToken;
    playing = true;
    replayMode = true;
    bonesCall('presenting', 'v31', 'stage replay');
    clearTimeout(presAuto);
    // the build answers live_ready_v31 and has presented: the stages play in 3D, the panel stays docked
    if (u3d && unityReady()) { if (!replay3d) start3d(); setDocked(true); }
    else setDocked(false);
    scrub.hidden = false;
    if (from != null) seek(from);
    paint();
    while (playing && token === playToken && idx < steps.length) {
      seek(idx + 1);
      const cur = steps[idx - 1];
      let ms = PACE[cur.k] || 1000;
      const next = steps[idx];
      if (next && next.k === cur.k && (cur.k === 'vox' || cur.k === 'bones' || cur.k === 'weights')) ms = 350;   // repeated passes compressed
      await sleep(ms);
    }
    if (token !== playToken) return;
    if (idx >= steps.length) finishReplay();
  }
  function pause() { playing = false; playToken++; paint(); }
  function finishReplay() {
    playing = false;
    replayMode = false;
    end3d();
    paint();
    if (!live || runDone) setDocked(true);
    bonesCall('presentationEnd', 'v31', skipping ? 'skip' : 'replay end');
    // a finished run: the stages panel and the scrub go away 4 s after the replay (the toolbar icon brings them back),
    // unless the person chose to keep them
    if ((!live || runDone) && presPerson !== false && !presHidden) presAutoHide(4000, skipping ? 'skip' : 'replay end');
    replayDone(skipping ? 'skip' : 'replay_end');
    skipping = false;
  }
  let skipping = false;
  function skip() { playToken++; playing = false; skipping = true; seek(steps.length); finishReplay(); }
  function setDocked(on) {
    docked = on;
    root.classList.toggle('docked', on);
    root.classList.toggle('staging', !on);
    if (!on) { mapOn = false; root.classList.remove('mapon'); }
    replayTool.hidden = !steps.length;
    presTool.hidden = !steps.length;
    layout();
  }
  playBtn.addEventListener('click', () => { if (playing) pause(); else play(idx >= steps.length ? 0 : null); });
  skipBtn.addEventListener('click', skip);
  $('.v31-fold').addEventListener('click', () => { if (replayMode) skip(); setPres(true, 'close button', true); });
  presTool.addEventListener('click', () => setPres(!presHidden, 'toolbar icon', true));
  $('.v31-mapbtn').addEventListener('click', () => { if (!docked) return; mapOn = !mapOn; root.classList.toggle('mapon', mapOn); });
  replayTool.addEventListener('click', () => { collapsed = false; root.classList.remove('collapsed'); if (presHidden && presPerson !== true) setPres(false, 'replay icon', false); play(0); });
  autoBtn.addEventListener('click', () => {
    autoReplay = !autoReplay;
    paint();
    fetch('/api/mt/v31/prefs', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ replay: autoReplay }) }).catch(() => {});
    try { localStorage.setItem('autorig_v31_replay', autoReplay ? '1' : '0'); } catch (e) { /* storage off */ }
  });
  function scrubTo(e) {
    const r = track.getBoundingClientRect();
    const f = Math.max(0, Math.min(1, (e.clientX - r.left) / r.width));
    playToken++;
    playing = false;
    seek(Math.round(f * steps.length));
  }
  let scrubbing = false;
  track.addEventListener('pointerdown', (e) => { scrubbing = true; track.setPointerCapture(e.pointerId); scrubTo(e); });
  track.addEventListener('pointermove', (e) => { if (scrubbing) scrubTo(e); });
  track.addEventListener('pointerup', () => { scrubbing = false; });

  // ---------------------------------------------------------------- the scene gate
  // Owner 2026-10-17 (AGENTS «The Environment Scene Loads Only After The Rig Is Ready»): «сделай чтобы полноценная
  // сцена грузилась только после того как риг готов, а до этого чтобы была красивая и быстрая визуализация». One
  // flag, one owner (this page): scene-gate.js in the viewer waits while window.autorigEnvHold is set; it is set from
  // boot and cleared once, when
  //   open = (the replay is done or skipped, or there is none) AND (the rig is ready and animated, or the run was
  //          already finished when the page opened)
  // A live run's rig is ready at its live_rig_first_ready event plus the first clip the viewer plays (Unity «clip»);
  // a run that ends (done / needs_review / failed) counts as ready too. Hard cap: 120 s after rig-ready; a live run
  // with no event for 10 min opens it as stalled. Every decision goes to the console as [v31-gate] with its reason
  // (window.autorigV31.gateLog). Heavy work of the page waits behind it (window.autorigSceneGate.whenOpen).
  // A hold that someone else set (a vehicle waits for the person's 🌄, a picture-only view) is never ours to clear.
  const G = { open: false, replayDone: false, live: null, firstReady: false, clip: false, rigReady: false, capped: false,
    reason: '' };
  const gateLog = W.gateLog = [];
  const gateQueue = [];
  let capTimer = 0, lastEventAt = Date.now(), foreignLogged = false;
  function glog(what, why) {
    const row = { t: Math.round(performance.now()), what, why, replayDone: G.replayDone, live: G.live,
      firstReady: G.firstReady, clip: G.clip, rigReady: G.rigReady };
    gateLog.push(row);
    try { console.info('[v31-gate]', what, '·', why, JSON.stringify(row)); } catch (e) { /* no console */ }
  }
  function gateWin() { try { return viewerEl.contentWindow; } catch (e) { return null; } }
  function isVehicle(w) { try { return !!(w.autorigVehicle && w.autorigVehicle.state && w.autorigVehicle.state()); } catch (e) { return false; } }
  function onGateUnity(ev) {
    const d = ev.detail || {};
    if (d.name === 'clip' && !G.clip) { G.clip = true; glog('clip_playing', 'unity «clip» event'); rigCheck('first clip playing'); }
  }
  function ensureHold() {
    const w = gateWin();
    if (!w) return;
    try { if (!w.__v31gatehook) { w.__v31gatehook = true; w.addEventListener('autorig-unity', onGateUnity); } } catch (e) { return; }
    if (G.open || !w.__autorigSceneGate || w.__v31hold) return;
    if (w.autorigEnv && w.autorigEnv.gate) return;                  // the scene went already
    if (w.autorigEnvHold) {
      if (!foreignLogged) { foreignLogged = true; glog('hold_foreign', 'someone else holds the scene (vehicle / picture)'); }
      return;
    }
    w.autorigEnvHold = true;
    w.__v31hold = true;
    glog('hold', G.live === null ? 'page opened, the run not read yet' : (G.live ? 'live run' : 'replay'));
  }
  // the first clip plays: a Unity «clip» event, or the viewer state says the rigged model with its clips is in and
  // not paused (the showreel starts on load without an event)
  let vsTick = 0;
  function clipFromState() {
    if (G.clip || G.open || (vsTick++ % 4)) return;
    try {
      const u = gateWin() && gateWin().autorigUnity;
      const vs = u && typeof u.viewerState === 'function' ? u.viewerState() : null;
      if (vs && vs.ready && vs.rigged && (vs.clips || []).length && !vs.paused) {
        G.clip = true;
        glog('clip_playing', `viewer state: rigged, ${vs.clips.length} clips, playing`);
        rigCheck('first clip playing');
      }
    } catch (e) { /* the viewer is loading */ }
  }
  setInterval(() => {
    ensureHold();
    clipFromState();
    if (!G.open && G.live && Date.now() - lastEventAt > 10 * 60 * 1000) openGate('stalled_run: no event for 10 min');
  }, 120);
  function rigCheck(why) {
    if (G.rigReady || G.open) return;
    if (G.live === false || (G.firstReady && G.clip)) {
      G.rigReady = true;
      glog('rig_ready', why);
      clearTimeout(capTimer);
      capTimer = setTimeout(() => { G.capped = true; openGate('cap: 120 s after rig-ready'); }, 120000);
      gateEval(why);
    } else glog('rig_wait', why);
  }
  function replayDone(why) {
    if (G.replayDone) return;
    G.replayDone = true;
    glog('replay_done', why);
    gateEval(why);
  }
  function gateEval(why) {
    if (!G.open && G.replayDone && G.rigReady) openGate(why);
  }
  function openGate(why) {
    if (G.open) return;
    G.open = true;
    setTimeout(paint, 0);
    G.reason = why;
    clearTimeout(capTimer);
    glog('open', why);
    const w = gateWin();
    if (w && w.__v31hold) {
      w.__v31hold = false;
      if (!isVehicle(w)) w.autorigEnvHold = false;
      // «skip» loads the scene at once when the mesh is in (otherwise scene-gate.js goes as soon as it lands)
      try {
        if (/^skip/.test(why) && w.autorigEnv && w.autorigEnv.t && w.autorigEnv.t.mesh != null && !w.autorigEnv.gate
          && w.autorigUnity && typeof w.autorigUnity.envGo === 'function') w.autorigUnity.envGo('v31_skip');
      } catch (e) { /* the viewer went away */ }
    }
    if (!W.t.envRelease) W.t.envRelease = Math.round(performance.now() - T_OPEN);
    gateQueue.splice(0).forEach((q) => { glog('run_queued', q.label); try { q.fn(); } catch (e) { console.warn('[v31-gate]', e); } });
  }
  window.autorigSceneGate = {
    owner: 'task-v31-stages.js', state: () => ({ ...G }), log: gateLog,
    get open() { return G.open; },
    whenOpen(fn, label) { if (G.open) fn(); else { gateQueue.push({ fn, label: label || 'job' }); glog('queued', label || 'job'); } },
  };
  W.gate = () => ({ ...G });

  // ---------------------------------------------------------------- the stream
  async function follow() {
    let cursor = 0, first = true, quiet = 0;
    const t0 = Date.now() / 1000;
    for (;;) {
      if (!root.isConnected) return;
      let d = null;
      try {
        const u = FIXTURE ? `/api/mt/runs/${run}/v31/fixture?cursor=${cursor}${SIM ? `&sim=${SIM}&t0=${t0}` : ''}`
          : `/api/mt/runs/${run}/live?cursor=${cursor}&wait=${first ? 0 : (document.visibilityState === 'visible' ? 20 : 25)}`;
        const r = await fetch(u, { credentials: 'same-origin', cache: 'no-store' });
        if (r.status === 404) { if (first) { teardown(); return; } await sleep(5000); continue; }
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        d = await r.json();
      } catch (e) { await sleep(4000); continue; }
      if (d.reset) { cursor = 0; steps.length = 0; S = fresh(); idx = 0; continue; }
      cursor = d.cursor || cursor;
      const before = steps.length;
      (d.events || []).forEach(ingest);
      if ((d.events || []).length) lastEventAt = Date.now();
      if (!G.firstReady && (d.events || []).some((e) => e.key === 'live_rig_first_ready' || (e.stage === 'rig' && e.kind === 'first_ready'))) {
        G.firstReady = true;
        glog('first_ready', 'live_rig_first_ready event');
        if (G.live !== null) rigCheck('live_rig_first_ready');
      }
      const added = steps.length - before;
      runDone = !!d.done;
      ticks.innerHTML = ticksHtml();
      if (first) {
        first = false;
        W.t.firstEvents = Math.round(performance.now() - T_OPEN);
        if (!steps.length) {
          if (runDone) { teardown(); return; }
          G.live = true;                                 // a queued run: the presentation starts with its first event
          replayDone('live run, nothing to replay');
          rigCheck('live run, no event yet');
        }
        else begin(runDone);
      } else if (added && W.mode === 'wait') {
        begin(runDone);
      } else if (added && live && !replayMode) {
        seek(steps.length);                              // drawn as it arrives
        if (!replayTool.hidden || steps.length) replayTool.hidden = false;
      }
      if (runDone && live && !replayMode && !d.events.length) {
        live = false;
        setDocked(true);
        bonesCall('presentationEnd', 'v31', 'live run ended');
        if (!G.rigReady) { G.live = false; rigCheck('the run ended'); }
      }
      paint();
      if (d.done && !(d.events || []).length) {
        if (FIXTURE) return;
        quiet = quiet || Date.now();
        if (Date.now() - quiet > 15 * 60 * 1000) return;
        await sleep(document.visibilityState === 'visible' ? 5000 : 20000);
      } else {
        quiet = 0;
        if (SIM || (FIXTURE && !d.done)) await sleep(700);
      }
    }
  }
  function begin(done) {
    W.mode = done ? 'replay' : 'live';
    root.classList.add('on');
    replayTool.hidden = false;
    scrub.hidden = false;
    if (!done) {
      live = true;
      bonesCall('presenting', 'v31', 'live run');
      if (G.live === null) {
        G.live = true;
        replayDone('live run: drawn as it arrives, no replay');
        rigCheck('live run opened');
      }
      setDocked(unityReady());
      seek(steps.length);
      return;
    }
    G.live = false;
    rigCheck('the run had finished before the page opened');
    if (autoReplay) play(0);
    else { seek(steps.length); setDocked(true); replayDone('replay on load is off'); bonesCall('presentationEnd', 'v31', 'replay on load is off'); }
  }
  function unityReady() {
    try { const veil = viewerEl.contentDocument && viewerEl.contentDocument.getElementById('veil'); return !!(veil && veil.classList.contains('off')); } catch (e) { return false; }
  }
  // live: once the Unity build is in, the panel docks and Unity draws in 3D; the splat stays one click away
  setInterval(() => { if (live && !replayMode && !docked && unityReady()) setDocked(true); }, 1000);
  // the stage the person is looking at, for the viewer telemetry (static/js/viewer-telemetry.js)
  W.stageInfo = () => {
    const cur = steps[idx - 1];
    return { kind: cur ? (STEP_NAME[cur.k] || cur.k) : '', step: idx, steps: steps.length, docked, playing, replay: replayMode,
      live, replay3d: !!replay3d, map: mapOn, gate: G.open };
  };
  Object.assign(W, { seek: (n) => { playToken++; playing = false; seek(n); }, play: (n) => play(n), skip: () => skip(),
    state: () => S, kinds: () => steps.map((x) => x.k), unity: () => ({ u3d, replay3d, partsMode, uState, sent: { ...uSent } }) });
  function teardown() {
    cancelAnimationFrame(raf);
    root.remove();
    replayTool.remove();
    partsTool.remove();
    presTool.remove();
    bonesCall('presentationEnd', 'v31', 'no presentation for this run');
    openGate('no presentation for this run');
    W.mode = 'none';
  }

  async function boot(r) {
    run = r;
    W.run = r;
    FILES = `/api/mt/files/${run}/`;
    try { const v = localStorage.getItem('autorig_v31_replay'); if (v === '0') autoReplay = false; } catch (e) { /* storage off */ }
    fetch('/api/mt/v31/prefs', { credentials: 'same-origin', cache: 'no-store' }).then((x) => (x.ok ? x.json() : null))
      .then((p) => { if (p && p.person !== 'none' && typeof p.replay === 'boolean') { autoReplay = p.replay; paint(); } }).catch(() => {});
    fetch(`${FILES}analysis/scale.json`, { credentials: 'same-origin' }).then((x) => (x.ok ? x.json() : null))
      .then((s) => { if (s && !S.heightM) S.heightM = s.display_height_m || s.estimated_real_height_m || null; }).catch(() => {});
    fetch(`/api/mt/runs/${run}/parts/labels`, { credentials: 'same-origin', cache: 'no-store' }).then((x) => (x.ok ? x.json() : null))
      .then((l) => { if (l && l.labels) { Object.assign(S.labels, l.labels); paint(); } }).catch(() => {});
    // the voxel levels are fetched alongside the first log answer (their names are fixed by mt/live_voxels.py), so the
    // first voxel is drawn as soon as the log says so: one round trip less on a slow line
    [12, 24, 48].forEach((r) => loadVox(`${FILES}live/vox_${r}.bin`));
    ensureHold();                                        // held from the first moment; only the gate opens it
    raf = requestAnimationFrame(frame);
    follow();
    // keep the hold on a reloaded iframe (a new run, LoadRun) and let a person's first touch of the viewer skip

  }
  window.addEventListener('languageChanged', () => { ticks.innerHTML = ticksHtml(); paint(); });
  return { boot };
}

// wait for the viewer of the task and its run (an MT run in the iframe URL, or the classic task's live run)
(function wait() {
  if (OFF) return;
  const el = document.getElementById('tv3-viewer');
  if (!el) return;
  let mod = null, tries = 0;
  const tick = () => {
    tries += 1;
    let run = '';
    try { run = new URL(el.src || '', location.origin).searchParams.get('run') || ''; } catch (e) { run = ''; }
    if (run === 'task') { try { run = (el.contentWindow && el.contentWindow.autorigLive && el.contentWindow.autorigLive.run) || ''; } catch (e) { run = ''; } }
    if (RUN_RE.test(run)) {
      mod = start(el);
      mod.boot(run);
      return;
    }
    if (tries < 600) setTimeout(tick, tries < 50 ? 100 : 1000);
  };
  tick();
}());
