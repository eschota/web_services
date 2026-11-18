const PROFILE = new Set(['TECH_PLAN_P0082', 'BTI']);
const SCALES = new Set(['1:50', '1:100', '1:200']);
const SIDES = ['top', 'bottom', 'left', 'right'];
const WALL = new Set(['WALL_CAPITAL', 'WALL_PARTITION', 'VERANDA', 'AUX']);
const OPENING = new Set(['WINDOW_STD', 'DOOR_SINGLE', 'DOOR_DOUBLE', 'DOOR_GLAZED', 'ARCH']);
const SYMBOL = new Set(['STAIRS', 'STOVE', 'KITCHEN', 'COLUMN', 'SHAFT']);
const cleanText = (x, limit = 180) => typeof x === 'string' ? x.trim().slice(0, limit) : '';
const num = (x, min, max) => x != null && x !== '' && Number.isFinite(Number(x)) && Number(x) >= min && Number(x) <= max ? Number(x) : null;
const coord = x => num(x, -100000, 100000);
const box = x => Array.isArray(x) && x.length === 4 && x.every(n => coord(n) !== null) && +x[2] > +x[0] && +x[3] > +x[1] ? x.map(Number) : null;
const items = (x, limit) => Array.isArray(x) ? x.slice(0, limit).filter(v => v && typeof v === 'object' && !Array.isArray(v)) : [];
export const uid = prefix => prefix + '-' + crypto.randomUUID();
export function emptyFloor(index = 0) {
  return {floor_id: uid('floor'), title: 'Этаж ' + (index + 1), image: '', image_width: 1000,
    image_height: 760, source_bbox_px: [0, 0, 1000, 760], rooms: [], walls: []};
}
function cleanId(value, prefix, existing) {
  const id = /^[a-zA-Z0-9_-]{1,80}$/.test(value || '') && !existing.has(value) ? value : uid(prefix);
  existing.add(id);
  return id;
}
function cleanSideData(raw, convert) {
  return Object.fromEntries(SIDES.map(side => [side, convert(raw?.[side])]).filter(([, v]) => v != null));
}
function boundedList(x, max, name) {
  if (x != null && (!Array.isArray(x) || x.length > max)) throw new Error('Проект содержит некорректный список «' + name + '».');
  return items(x, max);
}
function cleanRoom(raw, index, ids) {
  const bounds = box(raw.bbox_px);
  const output = {
    id: cleanId(raw.id, 'room', ids), bbox_px: bounds || [50+210*index,100,240+210*index,290],
    schematic: !bounds || raw.schematic === true, is_heated: raw.is_heated !== false,
    raw_measurements_cm: cleanSideData(raw.raw_measurements_cm, x => num(x, 0.1, 100000)),
    raw_measurement_boxes_px: cleanSideData(raw.raw_measurement_boxes_px, box),
    measurement_confidences: cleanSideData(raw.measurement_confidences, x => num(x, 0, 1)),
    measurement_rotations_deg: cleanSideData(raw.measurement_rotations_deg, x => num(x,-180,180)),
    measurement_weights: cleanSideData(raw.measurement_weights, x => num(x, 0.01, 100)),
    wall_types: cleanSideData(raw.wall_types, x => WALL.has(x) ? x : null),
    wall_thickness_cm: cleanSideData(raw.wall_thickness_cm, x => num(x, 0, 1000)),
    confidence: num(raw.confidence, 0, 1) ?? 0, verified: raw.verified === true,
    openings: [], symbols: []
  };
  if (Array.isArray(raw.polygon_px) && raw.polygon_px.length >= 3 && raw.polygon_px.length <= 200 &&
      raw.polygon_px.every(p => Array.isArray(p) && p.length === 2 && p.every(v => coord(v) != null))) output.polygon_px = raw.polygon_px.map(p => p.map(Number));
  output.local_measurements_cm = items(raw.local_measurements_cm,60).filter(m=>num(m.value_cm,0.1,100000)!=null)
    .map(m=>({label:cleanText(m.label,90),value_cm:Number(m.value_cm),bbox_px:box(m.bbox_px)}));
  for (const o of boundedList(raw.openings, 100, 'Проёмы')) {
    if (!SIDES.includes(o.wall) || !OPENING.has(o.type)) continue;
    const op = {id: cleanId(o.id, 'opening', ids), wall: o.wall, type: o.type, width_cm: num(o.width_cm, 0.1, 10000), t: num(o.t, 0, 1) ?? 0.5, confidence: num(o.confidence, 0, 1) ?? 0, verified:o.verified===true,raw_label:cleanText(o.raw_label,60)};
    for (const field of ['offset_cm', 'offset_right_cm']) if (num(o[field], 0, 100000) != null) op[field] = Number(o[field]);
    for (const field of ['bbox_px','width_bbox_px']) if (box(o[field])) op[field] = box(o[field]);
    output.openings.push(op);
  }
  for (const s of boundedList(raw.symbols, 100, 'Условные знаки')) {
    if (SYMBOL.has(s.type)) output.symbols.push({id: cleanId(s.id, 'symbol', ids), type:s.type,
      u:num(s.u,0,1) ?? 0.5, v:num(s.v,0,1) ?? 0.5, width_cm:num(s.width_cm,1,10000), depth_cm:num(s.depth_cm,1,10000),bbox_px:box(s.bbox_px)});
  }
  return output;
}
// SVG project images load only through the browser's isolated image decoder.
// Re-encode to ordinary PNG before letting an imported document reach the editor.
async function safeImage(value) {
  if (!value) return '';
  if (typeof value !== 'string' || value.length > 32 * 1024 * 1024) throw new Error('Слишком большая подложка в проекте.');
  if (/^data:image\/(?:png|jpeg|webp);base64,[a-zA-Z0-9+/=\s]+$/.test(value)) return value;
  if (!/^data:image\/svg\+xml(?:;charset=utf-8)?(?:;base64)?,/i.test(value)) throw new Error('Неподдерживаемый формат подложки проекта.');
  const img = new Image();
  img.src = value;
  try { await img.decode(); } catch { throw new Error('Не удалось прочитать подложку.'); }
  if (!img.width || !img.height || img.width * img.height > 16000000) throw new Error('Недопустимый размер подложки.');
  const canvas = document.createElement('canvas');
  canvas.width = img.width;
  canvas.height = img.height;
  canvas.getContext('2d').drawImage(img, 0, 0);
  return canvas.toDataURL('image/png');
}
export async function readProject(raw, fallbackName) {
  if (!raw || !Array.isArray(raw.floors) || !raw.floors.length || raw.floors.length > 100) throw new Error('В проекте должен быть список этажей floors (от 1 до 100).');
  const meta = raw.project_meta || {};
  const project = {schema_version: 1, name: cleanText(raw.name || fallbackName) || 'Новый проект', project_meta:{
    scale:SCALES.has(meta.scale) ? meta.scale : '1:100', target_profile:PROFILE.has(meta.target_profile) ? meta.target_profile : 'TECH_PLAN_P0082',
    height_m:num(meta.height_m,0.2,100) ?? null,
    ...Object.fromEntries(['contract_number','customer','cadastral_number','address'].map(k => [k,cleanText(meta[k],350)]))
  },floors:[], dataset_corrections:[]};
  const floorIds = new Set(), images = new Map();
  for (const [i, f] of raw.floors.entries()) {
    if (!f || typeof f !== 'object') throw new Error('Некорректная запись этажа.');
    const ids = new Set();
    const rooms = boundedList(f.rooms, 300, 'Помещения').map((r,j) => cleanRoom(r,j,ids));
    const width = num(f.image_width, 1, 30000), height = num(f.image_height, 1, 30000);
    const srcBounds = box(f.source_bbox_px) || [0,0,width || Math.max(1000,...rooms.map(r=>r.bbox_px[2] + 40)),height || Math.max(760,...rooms.map(r=>r.bbox_px[3] + 40))];
    const source = f.image || '';
    if (!images.has(source)) images.set(source, await safeImage(source));
    project.floors.push({floor_id:cleanId(f.floor_id,'floor',floorIds),title:cleanText(f.title,80) || 'Этаж ' + (i+1),
      source_page:num(f.source_page,1,10000), source_bbox_px:srcBounds, image:images.get(source),
      cm_per_px:num(f.cm_per_px,0.0001,10000), height_m:num(f.height_m,0.2,100),
      training_group:/^[a-f0-9]{24}$/.test(f.training_group || '') ? f.training_group : '', page_bbox_px:box(f.page_bbox_px),
      exterior_alignments:boundedList(f.exterior_alignments,600,'Выравнивание фасада').filter(a=>SIDES.includes(a.side)&&ids.has(a.room_a)&&ids.has(a.room_b)).map(a=>({side:a.side,room_a:a.room_a,room_b:a.room_b})),
      image_width:width || srcBounds[2],image_height:height || srcBounds[3], rooms,
      walls: boundedList(f.walls,2000,'Стены').filter(w => [w.x1,w.y1,w.x2,w.y2].every(v => coord(v) != null)).map(w => ({
        id:cleanId(w.id,'wall',ids),x1:+w.x1,y1:+w.y1,x2:+w.x2,y2:+w.y2,type:WALL.has(w.type)?w.type:'WALL_PARTITION',thickness_cm:num(w.thickness_cm,0,1000) ?? 12,room_boundary:w.room_boundary === true
      }))
    });
  }
  if (Array.isArray(raw.dataset_corrections)) project.dataset_corrections = raw.dataset_corrections.slice(-4000).filter(e => e && typeof e === 'object' && JSON.stringify(e).length < 40000);
  return project;
}
let database;
async function db() {
  if (!database) database = new Promise((resolve,reject) => {
    const req = indexedDB.open('abris.engineering',1);
    req.onupgradeneeded = () => req.result.createObjectStore('workspace');
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  return database;
}
export async function draft(value) {
  const database = await db(), write = value !== undefined;
  return new Promise((resolve,reject) => {
    const tx = database.transaction('workspace',write ? 'readwrite' : 'readonly');
    const store = tx.objectStore('workspace');
    const req = write ? store.put(value,'current') : store.get('current');
    tx.oncomplete = () => resolve(req.result);
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}
export function saveFile(filename, data, type) {
  const url = URL.createObjectURL(data instanceof Blob ? data : new Blob([data], {type}));
  const link = document.createElement('a');
  link.href = url;
  link.download = filename.replace(/[<>:"/\\|?*\x00-\x1F]/g, '_');
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 60000);
}
