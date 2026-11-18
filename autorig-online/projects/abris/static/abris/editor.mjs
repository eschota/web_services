import { inPolygon, insertWallTopology, snapToWalls } from './topology.mjs';

const SVG = 'http://www.w3.org/2000/svg';
const SIDES = { top: 'Верх', bottom: 'Низ', left: 'Слева', right: 'Справа' };
const WALLS = {
  WALL_CAPITAL: 'Капитальная стена',
  WALL_PARTITION: 'Перегородка',
  VERANDA: 'Контур террасы',
  AUX: 'Вспомогательная линия',
};
const OPENINGS = {
  WINDOW_STD: 'Окно',
  DOOR_SINGLE: 'Дверь однопольная',
  DOOR_DOUBLE: 'Дверь двупольная',
  DOOR_GLAZED: 'Дверь остеклённая',
  ARCH: 'Арка / пустой проём',
};
const SYMBOLS = {
  STAIRS: ['Лестница', 90, 140, '≋'],
  STOVE: ['Печь', 65, 55, '⊞'],
  KITCHEN: ['Плита и мойка', 120, 60, '⊚'],
  COLUMN: ['Колонна', 15, 15, '□'],
  SHAFT: ['Шахта / дымоход', 38, 36, '⊠'],
};
const TOOLS = new Set(['select', 'room', 'wall', 'opening', 'symbol', 'pan']);
const clamp = (v, min, max) => Math.max(min, Math.min(max, v));
const num = (v, fallback = 0) => Number.isFinite(v) ? v : fallback;
const id = prefix => prefix + '-' + globalThis.crypto.randomUUID();
const coord = v => Math.round(v * 10) / 10;
const midpoint = (a, b) => (a + b) / 2;
const box = (a, b) => [Math.min(a.x,b.x), Math.min(a.y,b.y), Math.max(a.x,b.x), Math.max(a.y,b.y)];
const BOUNDS = [0, 0, 1000, 700];
const wallDefault = type => ({ WALL_CAPITAL: 25, WALL_PARTITION: 12, VERANDA: 3, AUX: 0 })[type] ?? 12;
const STATUS = { missing: 'Нужен промер по каждой оси', review: 'Проверьте промеры на абрисе',
  error: 'Невязка больше 10 см', ok: 'Промеры в допуске', schematic: 'Подтвердите положение на холсте',
  polygon: 'Контур из отрезков. Печать требует промеров по его граням.' };
const lowConfidence = value => num(value, 1) < .85;
const needsRoomCheck = room => lowConfidence(room.confidence) ||
  Object.keys(SIDES).some(side => room.raw_measurements_cm?.[side] > 0 && lowConfidence(room.measurement_confidences?.[side]));

function svg(name, attributes = {}, label) {
  const node = document.createElementNS(SVG, name);
  for (const [key, value] of Object.entries(attributes)) {
    if (value !== undefined && value !== null) node.setAttribute(key, value);
  }
  if (label !== undefined) node.textContent = label;
  return node;
}

function el(name, attributes = {}, label) {
  const node = document.createElement(name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
  if (label !== undefined) node.textContent = label;
  return node;
}

function info(parent, text) {
  parent.append(el('p', { class: 'abris-ed-note' }, text));
}

function heading(parent, title) {
  parent.append(el('h3', { class: 'abris-ed-heading' }, title));
}

function button(parent, label, action, className = '') {
  const node = el('button', { type: 'button', class: 'abris-ed-button ' + className }, label);
  node.addEventListener('click', action);
  parent.append(node);
  return node;
}

function field(parent, text, control) {
  const label = el('label', { class: 'abris-ed-field' });
  label.append(el('span', {}, text), control);
  parent.append(label);
  return control;
}

function numeric(parent, text, value, update, { min = 1, max = 100000, step = 0.1, current = false } = {}) {
  const input = el('input', { type: 'number', min, max, step, inputmode: 'decimal', placeholder: '—' });
  input.value = value ?? '';
  if (current) input.dataset.abrisCurrent = '';
  input.addEventListener('change', () => {
    if (input.checkValidity()) update(input.value === '' ? null : input.valueAsNumber);
    else input.reportValidity();
  });
  input.addEventListener('keydown', event => {
    if (event.key === 'Enter') {
      event.preventDefault();
      input.blur();
    }
  });
  return field(parent, text, input);
}

function options(parent, text, choices, value, update) {
  const select = el('select');
  for (const [key, title] of Object.entries(choices)) select.append(el('option', { value: key }, title));
  select.value = value;
  select.addEventListener('change', () => update(select.value));
  return field(parent, text, select);
}

function checked(parent, text, value, update) {
  const checkbox = el('input', { type: 'checkbox' });
  checkbox.checked = value;
  checkbox.addEventListener('change', () => update(checkbox.checked));
  const label = el('label', { class: 'abris-ed-check' });
  label.append(checkbox, el('span', {}, text));
  parent.append(label);
}

function roomSide(bounds, side) {
  const [l, t, r, b] = bounds;
  return {
    top: [l, t, r, t],
    bottom: [l, b, r, b],
    left: [l, t, l, b],
    right: [r, t, r, b],
  }[side];
}

function fromLine(line, t) {
  const [x1, y1, x2, y2] = line;
  return [x1 + (x2 - x1) * t, y1 + (y2 - y1) * t];
}

function distanceToLine(point, [x1, y1, x2, y2]) {
  const dx = x2 - x1;
  const dy = y2 - y1;
  const denominator = dx * dx + dy * dy;
  const t = denominator ? clamp(((point.x - x1) * dx + (point.y - y1) * dy) / denominator, 0, 1) : 0;
  return { t, distance: Math.hypot(point.x - (x1 + t * dx), point.y - (y1 + t * dy)) };
}

function rectangle([x1, y1, x2, y2], attributes) {
  return svg('rect', { x: x1, y: y1, width: x2 - x1, height: y2 - y1, ...attributes });
}

function roomShape(bounds, polygon, attributes) {
  return polygon?.length >= 3
    ? svg('polygon', { points:polygon.map(point => point.join(',')).join(' '), ...attributes })
    : rectangle(bounds, attributes);
}

function line([x1, y1, x2, y2], attributes) {
  return svg('line', { x1, y1, x2, y2, ...attributes });
}

function roomStatus(room) {
  if (room.polygon_px?.length >= 3) return 'polygon';
  if (room.schematic) return 'schematic';
  const { top, bottom, left, right } = room.raw_measurements_cm || {};
  if (!(top > 0 || bottom > 0) || !(left > 0 || right > 0)) return 'missing';
  const gap = Math.max(top > 0 && bottom > 0 ? Math.abs(top - bottom) : 0,
    left > 0 && right > 0 ? Math.abs(left - right) : 0);
  if (gap > 10) return 'error';
  if (gap > 3 || (!room.verified && needsRoomCheck(room))) return 'review';
  return 'ok';
}

function sourceImage(image, [x, y, x2, y2]) {
  if (typeof image !== 'string' || !/^data:image\/(jpeg|png|webp|svg\+xml)[;,]/i.test(image)) return null;
  return svg('image', { x, y, width: x2 - x, height: y2 - y, href: image, preserveAspectRatio: 'none' });
}

/** Mount an editor with scan coordinates that remain independent of the CAD solve. */
export function mountEditor({ stage, inspector, onChange, onSelect, onVision, notify = () => {} }) {
  let project;
  let floorIndex = -1;
  let tool = 'select';
  let opacity = 50;
  let vector = true;
  let selection;
  let recentMeasurement;
  let interaction;
  let view;
  let fitWidth;
  let visionBusy = false;

  stage.classList.add('abris-editor-stage');
  inspector.classList.add('abris-editor-inspector');
  const scene = svg('svg', { class: 'abris-ed-scene', 'aria-label': 'Холст абриса' });
  const raster = svg('g', { class: 'abris-ed-raster' });
  const vectors = svg('g');
  const transient = svg('g', { class: 'abris-ed-transient', 'pointer-events': 'none' });
  scene.append(raster, vectors, transient);
  stage.append(scene);

  const floor = () => project?.floors?.[floorIndex];
  const rooms = () => floor()?.rooms || [];
  const walls = () => floor()?.walls || [];
  const bounds = () => floor()?.source_bbox_px || BOUNDS;
  const imageBounds = () => {
    const [x, y, r, b] = bounds();
    const { image_width: width, image_height: height } = floor() || {};
    return r <= width && b <= height ? [0, 0, width, height] : [x, y, r, b];
  };
  const selectedRoom = () => rooms().find(room => room.id === selection?.roomId);
  const selectedWall = () => selection?.kind === 'free-wall' ? walls().find(wall => wall.id === selection.objectId) : undefined;
  const selectedOpening = () => selectedRoom()?.openings?.find(opening => opening.id === selection?.objectId);
  const selectedSymbol = () => selectedRoom()?.symbols?.find(symbol => symbol.id === selection?.objectId);
  scene.dataset.tool = tool;
  scene.hidden = true;

  function px(size) {
    const baseWidth = stage.clientWidth;
    return view && baseWidth ? size * view.width / baseWidth : size;
  }

  function roomNumber(room) {
    if (room.is_heated === false) return null;
    let index = 1;
    for (const entry of project?.floors || []) {
      for (const candidate of entry.rooms || []) {
        if (candidate === room) return index;
        if (candidate.is_heated !== false) index += 1;
      }
    }
    return index;
  }

  function drawMeasurements(group, room) {
    const [l, t, r, b] = room.bbox_px;
    const fontSize = px(11);
    const offset = px(7);
    if (r - l < px(55) || b - t < px(48)) return;
    for (const [side, value] of Object.entries(room.raw_measurements_cm || {})) {
      if (!(side in SIDES) || !(value > 0)) continue;
      const [x1,y1,x2,y2] = roomSide(room.bbox_px, side);
      const x = midpoint(x1, x2) + (side === 'left' ? offset : side === 'right' ? -offset : 0);
      const y = midpoint(y1, y2) + (side === 'top' ? offset : side === 'bottom' ? -offset : 0);
      let transform;
      if (side === 'left' || side === 'right') transform = 'rotate(-90 ' + x + ' ' + y + ')';
      const low = lowConfidence(room.measurement_confidences?.[side]) && !room.verified;
      group.append(svg('text', {
        x, y, transform, 'font-size': fontSize, 'data-ed-kind': 'measurement',
        'data-room': room.id, 'data-side': side, 'data-low': low ? 'yes' : 'no',
        'text-anchor': 'middle', 'dominant-baseline': side === 'top' ? 'hanging' : side === 'bottom' ? 'auto' : 'middle',
        class: 'abris-ed-measurement',
      }, value));
    }
  }

  function openingPosition(room, opening) {
    const side = opening.wall in SIDES ? opening.wall : 'top';
    const measure = num(room.raw_measurements_cm?.[side]);
    const width = num(opening.width_cm, 80);
    let center = num(opening.t, .5);
    if (measure > 0 && opening.offset_cm != null) center = (opening.offset_cm + width / 2) / measure;
    else if (measure > 0 && opening.offset_right_cm != null) center = 1 - (opening.offset_right_cm + width / 2) / measure;
    const lineCoord = roomSide(room.bbox_px, side);
    const fraction = measure > 0 ? clamp(width / measure / 2, .025, .5) : .12;
    return { side, lineCoord, fraction, center: clamp(center, fraction, 1 - fraction) };
  }

  function drawOpening(group, room, opening) {
    const { side, lineCoord, fraction, center } = openingPosition(room, opening);
    const active = selection?.kind === 'opening' && selection?.objectId === opening.id && selectedRoom() === room;
    const [cx, cy] = fromLine(lineCoord, center);
    const start = fromLine(lineCoord, center - fraction);
    const end = fromLine(lineCoord, center + fraction);
    const hit = svg('g', {
      class: 'abris-ed-opening',
      'data-ed-kind': 'opening', 'data-room': room.id, 'data-object': opening.id,
      'data-active': active ? 'yes' : 'no', 'data-low': lowConfidence(opening.confidence) && !opening.verified ? 'yes' : 'no',
    });
    const span = [...start, ...end];
    hit.append(line(span, { class: 'abris-ed-opening-track' }));
    hit.append(line(span, { class: 'abris-ed-opening-stripe' }));
    const d = px(6);
    if (opening.type === 'WINDOW_STD' || opening.type === 'DOOR_GLAZED') {
      const dx = side === 'left' || side === 'right' ? d * .55 : 0;
      const dy = dx ? 0 : d * .55;
      hit.append(line([start[0] + dx, start[1] + dy, end[0] + dx, end[1] + dy], { class: 'abris-ed-opening-stripe' }));
    }
    if (opening.type === 'DOOR_SINGLE' || opening.type === 'DOOR_DOUBLE') {
      const dx = side === 'left' ? d : side === 'right' ? -d : 0;
      const dy = side === 'top' ? d : side === 'bottom' ? -d : 0;
      hit.append(line([start[0], start[1], cx + dx, cy + dy], { class: 'abris-ed-opening-stripe' }));
    }
    group.append(hit);
  }

  function drawRoom(room) {
    if (!Array.isArray(room.bbox_px) || room.bbox_px.length !== 4) return;
    const active = selectedRoom() === room;
    const [l,t,r,b] = room.bbox_px;
    const polygon = room.polygon_px;
    let middleX = midpoint(l, r);
    let middleY = midpoint(t, b);
    if (polygon && !inPolygon([middleX,middleY],polygon)) {
      const average = polygon.reduce(([x,y], point) => [x + point[0]/polygon.length, y + point[1]/polygon.length], [0,0]);
      if (inPolygon(average, polygon)) [middleX,middleY] = average;
      else [middleX,middleY] = polygon[0];
    }
    const group = svg('g', { 'data-room': room.id, class: 'abris-ed-room', 'data-status': roomStatus(room), 'data-active': active ? 'yes' : 'no' });
    group.append(roomShape(room.bbox_px,polygon,{ class:'abris-ed-room-shape', 'data-ed-kind':'room' }));
    if (!polygon) {
      for (const side of Object.keys(SIDES)) {
        const selected = active && selection?.kind === 'side' && selection?.side === side;
        group.append(line(roomSide(room.bbox_px, side), {
          class: 'abris-ed-side', 'data-ed-kind': 'side', 'data-side': side,
          'data-wall-type': room.wall_types?.[side], 'data-active': selected ? 'yes' : 'no',
        }));
      }
      drawMeasurements(group, room);
      for (const opening of room.openings || []) drawOpening(group, room, opening);
    }
    const radius = Math.min(px(13), (r - l) / 5, (b - t) / 5);
    if (radius >= px(3)) {
      group.append(svg('circle', { cx: middleX, cy: middleY, r: radius, class: 'abris-ed-room-number', 'data-ed-kind': 'room' }));
      group.append(svg('text', {
        x: middleX, y: middleY, 'font-size': radius * .92,
        class: 'abris-ed-room-number-text', 'text-anchor': 'middle', 'dominant-baseline': 'central',
      }, roomNumber(room) ?? 'Т'));
    }
    for (const symbol of room.symbols || []) {
      const x = l + (r - l) * num(symbol.u, .5);
      const y = t + (b - t) * num(symbol.v, .5);
      const mark = svg('g', {
        'data-ed-kind': 'symbol', 'data-object': symbol.id, class: 'abris-ed-symbol',
        'data-active': active && selection?.kind === 'symbol' && selection?.objectId === symbol.id ? 'yes' : 'no',
      });
      mark.append(svg('circle', { cx: x, cy: y, r: px(9) }), svg('text', {
        x, y, 'font-size': px(13), 'text-anchor': 'middle', 'dominant-baseline': 'central',
      }, SYMBOLS[symbol.type]?.[3] ?? '◊'));
      group.append(mark);
    }
    if (active && tool === 'select' && !polygon) {
      for (const [corner, x, y] of [[0,l,t], [1,r,t], [2,r,b], [3,l,b]]) {
        const d = px(3.4);
        group.append(rectangle([x-d,y-d,x+d,y+d], {
          class: 'abris-ed-handle', 'data-ed-kind': 'handle', 'data-corner': corner,
        }));
      }
    }
    vectors.append(group);
  }

  function drawVectors() {
    vectors.replaceChildren();
    vectors.style.visibility = vector ? 'visible' : 'hidden';
    const active = selectedRoom();
    for (const room of rooms()) {
      if (room !== active) drawRoom(room);
    }
    if (active) drawRoom(active);
    for (const wall of walls()) {
      if (![wall.x1, wall.y1, wall.x2, wall.y2].every(Number.isFinite)) continue;
      vectors.append(line([wall.x1, wall.y1, wall.x2, wall.y2], {
        class: 'abris-ed-free-wall', 'data-ed-kind': 'free-wall', 'data-object': wall.id,
        'data-wall-type': wall.type, 'data-active': selectedWall() === wall ? 'yes' : 'no',
      }));
    }
  }

  function drawRaster() {
    const img = sourceImage(floor()?.image, imageBounds());
    raster.replaceChildren();
    if (img) raster.append(img);
    raster.style.opacity = opacity / 100;
  }

  function updateView() {
    if (!view) return;
    scene.setAttribute('viewBox', [view.x, view.y, view.width, view.height].join(' '));
    drawVectors();
    reportZoom();
  }

  function reportZoom() {
    if (view) stage.dispatchEvent(new CustomEvent('abris-zoom', { detail: 100 * fitWidth / view.width }));
  }

  function fit() {
    const [x1,y1,x2,y2] = bounds();
    const width = Math.max(10, x2 - x1) * 1.13;
    const height = Math.max(10, y2 - y1) * 1.13;
    const aspect = (stage.clientWidth || 900) / (stage.clientHeight || 650);
    const viewWidth = Math.max(width, height * aspect);
    const viewHeight = viewWidth / aspect;
    view = { x: midpoint(x1,x2) - viewWidth / 2, y: midpoint(y1,y2) - viewHeight / 2, width: viewWidth, height: viewHeight };
    fitWidth = viewWidth;
    updateView();
  }

  function position(event) {
    const matrix = scene.getScreenCTM();
    if (!matrix) return { x: 0, y: 0 };
    const p = new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse());
    return { x: p.x, y: p.y };
  }

  function zoom(factor, at) {
    if (!view || !Number.isFinite(factor) || factor <= 0) return;
    const width = clamp(view.width / factor, fitWidth / 12, fitWidth * 4);
    const ratio = width / view.width;
    const anchor = at || { x: view.x + view.width / 2, y: view.y + view.height / 2 };
    view = {
      x: anchor.x + (view.x - anchor.x) * ratio,
      y: anchor.y + (view.y - anchor.y) * ratio,
      width, height: view.height * ratio,
    };
    updateView();
  }

  function select(next, focus = false) {
    selection = next;
    drawVectors();
    drawInspector(focus);
    onSelect?.(selectedRoom()?.id ?? null);
  }

  function change(kind, detail, before, after, mutate) {
    if (JSON.stringify(before) === JSON.stringify(after)) return;
    const evidence = structuredClone({ before, after });
    mutate();
    drawVectors();
    onChange?.({ kind, floor_id: floor().floor_id, room_id: selectedRoom()?.id ?? null, ...detail, ...evidence });
  }

  function setValue(object, key, value, detail = {}) {
    change('edit', { field: key, ...detail }, object[key], value, () => { object[key] = value; });
  }

  function removeSelection() {
    const room = selectedRoom();
    const wall = selectedWall();
    const opening = selectedOpening();
    const symbol = selectedSymbol();
    if (opening) {
      change('delete', { entity: 'opening', object_id: opening.id }, opening, null, () => {
        room.openings = room.openings.filter(item => item !== opening);
      });
      select({ kind: 'room', roomId: room.id });
    } else if (symbol) {
      change('delete', { entity: 'symbol', object_id: symbol.id }, symbol, null, () => {
        room.symbols = room.symbols.filter(item => item !== symbol);
      });
      select({ kind: 'room', roomId: room.id });
    } else if (wall) {
      change('delete', { entity: 'wall', object_id: wall.id }, wall, null, () => {
        floor().walls = walls().filter(item => item !== wall);
      });
      select(null);
    } else if (room) {
      change('delete', { entity: 'room', object_id: room.id }, room, null, () => {
        floor().rooms = rooms().filter(item => item !== room);
      });
      select(null);
    }
  }

  function backToRoom(room) {
    button(inspector, '← ' + (roomNumber(room) ? 'Помещение ' + roomNumber(room) : 'Терраса'),
      () => select({ kind: 'room', roomId: room.id }), 'abris-ed-back');
  }

  function crop(room, side) {
    const image = sourceImage(floor()?.image, imageBounds());
    if (!image) return;
    let area = room.raw_measurement_boxes_px?.[side] || room.measurement_bboxes_px?.[side];
    if (!Array.isArray(area) || area.length !== 4 || !area.every(Number.isFinite)) {
      const [x1,y1,x2,y2] = roomSide(room.bbox_px, side);
      const width = room.bbox_px[2] - room.bbox_px[0];
      const height = room.bbox_px[3] - room.bbox_px[1];
      const cx = midpoint(x1,x2) + (side === 'left' ? width * .14 : side === 'right' ? -width * .14 : 0);
      const cy = midpoint(y1,y2) + (side === 'top' ? height * .14 : side === 'bottom' ? -height * .14 : 0);
      area = [cx-width*.25, cy-height*.24, cx+width*.25, cy+height*.24];
    }
    const [x,y,r,b] = area;
    if (r <= x || b <= y) return;
    const margin = Math.max(r-x, b-y) * .17;
    const fragment = svg('svg', {
      viewBox: [x-margin,y-margin,r-x+2*margin,b-y+2*margin].join(' '),
      class: 'abris-ed-crop', role: 'img', 'aria-label': 'Увеличенный фрагмент абриса',
    });
    fragment.append(image);
    inspector.append(fragment);
  }

  function measureField(parent, room, side, current = false) {
    return numeric(parent, SIDES[side] + ', см', room.raw_measurements_cm?.[side], value => {
      const before = room.raw_measurements_cm?.[side] ?? null;
      const bbox_px = room.raw_measurement_boxes_px?.[side] || room.measurement_bboxes_px?.[side];
      change('edit', { entity: 'measurement', field: side, bbox_px }, before, value, () => {
        (room.raw_measurements_cm ||= {})[side] = value;
        if (value != null) (room.measurement_confidences ||= {})[side] = 1;
      });
      const status = inspector.querySelector('.abris-ed-status');
      if (status) {
        status.dataset.status = roomStatus(room);
        status.textContent = STATUS[status.dataset.status];
      }
    }, { current });
  }

  function visionButton(room, side) {
    if (!onVision || !floor().image) return;
    const trigger = button(inspector, side ? 'Сверить промер через Vision' : 'Сверить промеры через Vision', async () => {
      if (visionBusy) return;
      visionBusy = true;
      trigger.disabled = true;
      trigger.setAttribute('aria-busy', 'true');
      try {
        await onVision(room, side);
      } catch (error) {
        notify(error instanceof Error ? error.message : 'Сверка промеров через Vision недоступна.');
      } finally {
        visionBusy = false;
        const current = inspector.querySelector('.abris-ed-vision');
        if (current) {
          current.disabled = false;
          current.removeAttribute('aria-busy');
        }
      }
    }, 'abris-ed-vision');
    trigger.disabled = visionBusy;
  }

  function roomInspector(room) {
    const number = roomNumber(room);
    heading(inspector, number ? 'Помещение ' + number : 'Терраса');
    const status = roomStatus(room);
    const chip = el('p', { class: 'abris-ed-status', 'data-status': status }, STATUS[status]);
    inspector.append(chip);
    if (room.polygon_px) {
      button(inspector, 'Удалить помещение', removeSelection, 'abris-ed-danger');
      return;
    }
    const grid = el('div', { class: 'abris-ed-grid' });
    inspector.append(grid);
    for (const side of Object.keys(SIDES)) measureField(grid, room, side);
    visionButton(room);
    if (needsRoomCheck(room) || room.verified || room.schematic) {
      if (room.verified && !room.schematic) info(inspector, 'Помещение проверено');
      else button(inspector, 'Подтвердить', () => {
        const current = roomStatus(room);
        if (current === 'missing' || current === 'error') return notify('Проверьте промеры перед подтверждением.');
        const before = { verified:room.verified, schematic:room.schematic };
        const after = { verified:true, schematic:false };
        change('edit', { entity:'room', field:'verified' }, before, after, () => Object.assign(room,after));
        drawInspector();
      }, 'abris-ed-confirm');
    }
    checked(inspector, 'Открытая терраса, без номера', room.is_heated === false, checkedValue => {
      setValue(room, 'is_heated', !checkedValue, { entity: 'room' });
      drawInspector();
    });
    const sideRow = el('div', { class: 'abris-ed-small-row', 'aria-label': 'Стены помещения' });
    for (const [side,label] of Object.entries(SIDES)) {
      button(sideRow, label, () => select({ kind: 'side', roomId: room.id, side }));
    }
    inspector.append(sideRow);
    const elements = [...(room.openings || []).map(item => ['opening', item, OPENINGS[item.type]]),
      ...(room.symbols || []).map(item => ['symbol', item, SYMBOLS[item.type]?.[0]])];
    if (elements.length) {
      heading(inspector, 'Элементы');
      const list = el('div', { class: 'abris-ed-items' });
      for (const [kind, item, title] of elements) {
        button(list, title || 'Элемент', () => select({ kind, roomId: room.id, objectId: item.id }));
      }
      inspector.append(list);
    }
    button(inspector, 'Удалить помещение', removeSelection, 'abris-ed-danger');
  }

  function wallInspector(room, wall) {
    const side = selection.side;
    if (room) backToRoom(room);
    heading(inspector, room ? 'Стена · ' + SIDES[side].toLowerCase() : 'Отрезок стены');
    const type = room ? room.wall_types?.[side] || 'WALL_CAPITAL' : wall.type || 'WALL_PARTITION';
    const thickness = room ? room.wall_thickness_cm?.[side] : wall.thickness_cm;
    const update = (key, value, oldValue) => {
      const detail = { entity: 'wall', side, object_id: wall?.id, field: key };
      if (room) {
        const prop = key === 'type' ? 'wall_types' : 'wall_thickness_cm';
        change('edit', detail, oldValue, value, () => { (room[prop] ||= {})[side] = value; });
      } else setValue(wall, key, value, detail);
    };
    options(inspector, 'Тип', WALLS, type, value => {
      update('type', value, type);
      drawInspector();
    });
    numeric(inspector, 'Толщина, см', thickness ?? wallDefault(type),
      value => update('thickness_cm', value, thickness), { min: 0, max: 300 });
    if (wall) button(inspector, 'Удалить стену', removeSelection, 'abris-ed-danger');
  }

  function openingInspector(room, opening) {
    backToRoom(room);
    heading(inspector, 'Проём');
    const detail = { entity: 'opening', object_id: opening.id };
    let confirm;
    const validOpening = () => opening.width_cm > 0 && Object.hasOwn(SIDES, opening.wall) && Object.hasOwn(OPENINGS, opening.type);
    const update = (key, value) => {
      change('edit', { ...detail, field:key }, opening[key], value, () => {
        opening[key] = value;
        opening.verified = false;
      });
      if (confirm) confirm.disabled = !validOpening();
      else if (lowConfidence(opening.confidence)) drawInspector();
    };
    options(inspector, 'Условный знак', OPENINGS, opening.type, value => update('type', value));
    options(inspector, 'Стена', SIDES, opening.wall, value => update('wall', value));
    const grid = el('div', { class: 'abris-ed-grid' });
    inspector.append(grid);
    numeric(grid, 'Ширина, см', opening.width_cm, value => update('width_cm', value), { max: 10000 });
    const fromEnd = opening.offset_cm == null && opening.offset_right_cm != null;
    const offset = fromEnd ? opening.offset_right_cm : opening.offset_cm;
    numeric(grid, fromEnd ? 'Отступ с конца, см' : 'Отступ, см', offset, value => {
      update(fromEnd ? 'offset_right_cm' : 'offset_cm', value);
    }, { min: 0, max: 10000 });
    if (opening.raw_label) info(inspector, 'Пометка на абрисе · ' + opening.raw_label);
    if (lowConfidence(opening.confidence)) {
      if (opening.verified) info(inspector, 'Проём проверен');
      else {
        confirm = button(inspector, 'Подтвердить проём', () => {
          if (!validOpening()) return;
          setValue(opening, 'verified', true, detail);
          drawInspector();
        }, 'abris-ed-confirm');
        confirm.disabled = !validOpening();
      }
    }
    button(inspector, 'Удалить проём', removeSelection, 'abris-ed-danger');
  }

  function symbolInspector(room, symbol) {
    backToRoom(room);
    heading(inspector, 'Условный знак');
    const labels = Object.fromEntries(Object.entries(SYMBOLS).map(([key, tuple]) => [key, tuple[0]]));
    const detail = { entity: 'symbol', object_id: symbol.id };
    options(inspector, 'Тип', labels, symbol.type, value => {
      const [, width, depth] = SYMBOLS[value];
      const before = { type: symbol.type, width_cm: symbol.width_cm, depth_cm: symbol.depth_cm };
      const after = { type: value, width_cm: width, depth_cm: depth };
      change('edit', { ...detail, field: 'type' }, before, after, () => Object.assign(symbol, after));
      drawInspector();
    });
    const grid = el('div', { class: 'abris-ed-grid' });
    inspector.append(grid);
    numeric(grid, 'Ширина, см', symbol.width_cm, value => setValue(symbol, 'width_cm', value, detail), { max: 2000 });
    numeric(grid, 'Глубина, см', symbol.depth_cm, value => setValue(symbol, 'depth_cm', value, detail), { max: 2000 });
    button(inspector, 'Удалить знак', removeSelection, 'abris-ed-danger');
  }

  function drawInspector(focus) {
    inspector.replaceChildren();
    const room = selectedRoom();
    const wall = selectedWall();
    if (!floor()) return;
    if (room && selection.kind === 'measurement') {
      backToRoom(room);
      heading(inspector, 'Промер · ' + SIDES[selection.side].toLowerCase());
      crop(room, selection.side);
      const input = measureField(inspector, room, selection.side, true);
      visionButton(room, selection.side);
      if (focus) {
        input.focus({ preventScroll: true });
        input.select();
      }
    } else if (wall || (room && selection.kind === 'side')) {
      wallInspector(room, wall);
    } else if (selectedOpening()) {
      openingInspector(room, selectedOpening());
    } else if (selectedSymbol()) {
      symbolInspector(room, selectedSymbol());
    } else if (room) {
      roomInspector(room);
    } else {
      info(inspector, rooms().length || walls().length
        ? 'Выберите помещение, стену или проём на холсте.'
        : 'Выберите инструмент «Помещение» и очертите первую комнату.');
      if (rooms().length) {
        const choices = Object.fromEntries(rooms().map(room => [room.id, roomNumber(room)
          ? 'Помещение ' + roomNumber(room) : 'Терраса']));
        options(inspector, 'Перейти к помещению', { '':'—', ...choices }, '', roomId => {
          if (roomId) select({ kind:'room', roomId });
        });
      }
    }
  }

  function targetInfo(target) {
    const mark = target.closest('[data-ed-kind]');
    return mark ? {
      kind: mark.dataset.edKind,
      roomId: mark.closest('[data-room]')?.dataset.room,
      side: mark.dataset.side,
      objectId: mark.dataset.object,
      corner: Number(mark.dataset.corner),
    } : null;
  }

  function nearbyRoom(point) {
    return [...rooms()].reverse().find(room => {
      if (room.polygon_px) return inPolygon([point.x,point.y],room.polygon_px);
      const [l,t,r,b] = room.bbox_px;
      return l <= point.x && point.x <= r && t <= point.y && point.y <= b;
    });
  }

  function insertOpening(point, roomHint) {
    const candidates = roomHint ? [roomHint] : rooms();
    let match;
    for (const room of candidates) {
      if (room.polygon_px) continue;
      for (const side of Object.keys(SIDES)) {
        const edge = distanceToLine(point, roomSide(room.bbox_px, side));
        if ((!match || edge.distance < match.distance) && edge.distance <= px(20)) match = { room, side, ...edge };
      }
    }
    if (!match) return notify('Нажмите на стену помещения.');
    const opening = { id: id('opening'), wall: match.side, type: 'WINDOW_STD', width_cm: 100, t: match.t, confidence: 1 };
    select({ kind: 'room', roomId: match.room.id });
    change('add', { entity: 'opening', object_id: opening.id }, null, opening, () => { (match.room.openings ||= []).push(opening); });
    select({ kind: 'opening', roomId: match.room.id, objectId: opening.id });
  }

  function insertSymbol(point) {
    const room = nearbyRoom(point);
    if (!room) return notify('Нажмите внутри помещения.');
    const [l,t,r,b] = room.bbox_px;
    const type = 'STAIRS';
    const [, width, depth] = SYMBOLS[type];
    const symbol = {
      id: id('symbol'), type, u: clamp((point.x - l) / (r - l), 0, 1),
      v: clamp((point.y-t) / (b-t), 0, 1), width_cm: width, depth_cm: depth,
    };
    select({ kind: 'room', roomId: room.id });
    change('add', { entity: 'symbol', object_id: symbol.id }, null, symbol, () => { (room.symbols ||= []).push(symbol); });
    select({ kind: 'symbol', roomId: room.id, objectId: symbol.id });
  }

  function snap(point, roomToSkip) {
    const distance = px(8);
    let x = point.x;
    let y = point.y;
    let dx = distance;
    let dy = distance;
    for (const room of rooms()) {
      if (room === roomToSkip) continue;
      for (const [vx,vy] of [[room.bbox_px[0],room.bbox_px[1]], [room.bbox_px[2],room.bbox_px[3]]]) {
        if (Math.abs(vx - point.x) < dx) {
          dx = Math.abs(vx - point.x);
          x = vx;
        }
        if (Math.abs(vy - point.y) < dy) {
          dy = Math.abs(vy - point.y);
          y = vy;
        }
      }
    }
    return { x: coord(x), y: coord(y) };
  }

  function snapWall(point) {
    const aligned = snap(point);
    const [x,y] = snapToWalls([aligned.x,aligned.y],walls(),px(8));
    return { x,y };
  }

  function pointerDown(event) {
    if (!floor() || interaction || (event.button !== 0 && event.button !== 1)) return;
    const target = targetInfo(event.target);
    if (inspector.contains(document.activeElement)) document.activeElement.blur();
    const point = position(event);
    const pan = tool === 'pan' || event.button === 1;
    if (!pan && tool === 'opening') return insertOpening(point, rooms().find(room => room.id === target?.roomId));
    if (!pan && tool === 'symbol') return insertSymbol(point);
    const start = tool === 'wall' ? snapWall(point) : tool === 'room' ? snap(point) : point;
    const selected = selectedRoom();
    const resize = target?.kind === 'handle' && tool === 'select';
    const move = target?.kind === 'room' && tool === 'select' && selected && target.roomId === selected.id;
    interaction = {
      pointer: event.pointerId, kind: pan ? 'pan' : resize ? 'resize' : move ? 'move' : tool,
      start, target, view: { ...view }, screenX: event.clientX, screenY: event.clientY,
      transform: scene.getScreenCTM(),
    };
    scene.setPointerCapture(event.pointerId);
    event.preventDefault();
  }

  function resized(point, action) {
    const room = rooms().find(item => item.id === action.target.roomId);
    if (!room) return null;
    const [l,t,r,b] = room.bbox_px;
    const corners = [[r,b],[l,b],[l,t],[r,t]];
    const [x,y] = corners[action.target.corner];
    const cursor = snap(point, room);
    const min = px(12);
    const left = action.target.corner === 0 || action.target.corner === 3;
    const top = action.target.corner < 2;
    const resizedPoint = { x: left ? Math.min(cursor.x,x-min) : Math.max(cursor.x,x+min),
      y: top ? Math.min(cursor.y,y-min) : Math.max(cursor.y,y+min) };
    return { room, bbox: box({x,y}, resizedPoint) };
  }

  function moved(point, action) {
    const room = rooms().find(item => item.id === action.target.roomId);
    if (!room) return null;
    const [l,t,r,b] = room.bbox_px;
    const anchor = snap({ x: l + point.x - action.start.x, y: t + point.y - action.start.y }, room);
    const polygon = room.polygon_px?.map(([x,y]) => [x + anchor.x - l, y + anchor.y - t]);
    return { room, bbox:[anchor.x,anchor.y,coord(r + anchor.x - l),coord(b + anchor.y - t)], polygon };
  }

  function pointerMove(event) {
    if (!interaction || interaction.pointer !== event.pointerId) return;
    const action = interaction;
    const point = position(event);
    const deltaX = event.clientX - action.screenX;
    const deltaY = event.clientY - action.screenY;
    if (Math.hypot(deltaX, deltaY) > 3) action.dragged = true;
    if (!action.dragged) return;
    transient.replaceChildren();
    if (action.kind === 'pan') {
      view.x = action.view.x - deltaX / (action.transform?.a || 1);
      view.y = action.view.y - deltaY / (action.transform?.d || 1);
      scene.setAttribute('viewBox', [view.x, view.y, view.width, view.height].join(' '));
    } else if (action.kind === 'room') {
      transient.append(rectangle(box(action.start, snap(point)), { class: 'abris-ed-ghost-room' }));
    } else if (action.kind === 'wall') {
      const end = wallEnd(point, action, event.shiftKey);
      transient.append(line([action.start.x, action.start.y, end.x, end.y], { class: 'abris-ed-ghost-wall' }));
    } else if (action.kind === 'resize' || action.kind === 'move') {
      const result = action.kind === 'resize' ? resized(point, action) : moved(point, action);
      if (result) transient.append(roomShape(result.bbox,result.polygon,{ class:'abris-ed-ghost-room' }));
    }
  }

  function wallEnd(point, action, orthogonal) {
    const end = snapWall(point);
    const dx = Math.abs(end.x - action.start.x);
    const dy = Math.abs(end.y - action.start.y);
    if (orthogonal || Math.min(dx,dy) < px(4)) {
      if (dx < dy) end.x = action.start.x;
      else end.y = action.start.y;
    }
    return end;
  }

  function pointerUp(event) {
    if (!interaction || interaction.pointer !== event.pointerId) return;
    const action = interaction;
    interaction = null;
    transient.replaceChildren();
    if (scene.hasPointerCapture(event.pointerId)) scene.releasePointerCapture(event.pointerId);
    const point = position(event);
    if (action.kind === 'select') {
      if (action.dragged) return;
      const target = action.target;
      const key = target?.kind === 'measurement' ? target.roomId + ':' + target.side : null;
      const focus = key && recentMeasurement?.key === key && event.timeStamp - recentMeasurement.time < 600;
      recentMeasurement = key && !focus ? { key, time: event.timeStamp } : null;
      select(target, focus);
    } else if (action.kind === 'room') {
      const bbox = box(action.start, snap(point));
      if (bbox[2] - bbox[0] < px(12) || bbox[3] - bbox[1] < px(12)) return;
      const room = { id: id('room'), bbox_px: bbox, raw_measurements_cm: {}, is_heated: true,
        confidence: 1, openings: [], symbols: [] };
      change('add', { entity: 'room', room_id: room.id }, null, room, () => { (floor().rooms ||= []).push(room); });
      select({ kind: 'room', roomId: room.id });
    } else if (action.kind === 'wall') {
      const end = wallEnd(point, action, event.shiftKey);
      if (Math.hypot(end.x - action.start.x, end.y - action.start.y) < px(8)) return;
      const wall = { id: id('wall'), x1: action.start.x, y1: action.start.y, x2: end.x, y2: end.y,
        type: 'WALL_PARTITION', thickness_cm: 12 };
      const found = insertWallTopology(floor(),wall,id,px(8));
      if (found.created.length) {
        const kind = found.replaced.length ? 'split-room' : 'close-room';
        const before = { rooms:found.replaced, walls:found.affectedWalls.filter(w => w !== wall) };
        const after = { rooms:found.created, walls:found.walls.filter(w => !walls().includes(w)) };
        change(kind, { entity:'room', room_id:found.created[0].id }, before, after, () => {
          floor().rooms = found.rooms;
          floor().walls = found.walls;
        });
        select({ kind:'room', roomId:found.created[0].id });
        notify(found.ambiguous ? 'Перегородка добавлена. Проверьте проёмы и новые промеры.' :
          kind === 'split-room' ? 'Помещение разделено. Добавьте новые промеры.' : 'Контур замкнут. Добавьте промеры.');
      } else {
        change('add', { entity:'wall', object_id:wall.id, room_id:null }, null, wall, () => { floor().walls = found.walls; });
        select({ kind:'free-wall', objectId:wall.id });
      }
    } else if (action.kind === 'resize' || action.kind === 'move') {
      if (!action.dragged) {
        if (action.kind === 'move') select(action.target);
        return;
      }
      const result = action.kind === 'resize' ? resized(point, action) : moved(point, action);
      if (result) change('edit', { entity: 'room', field: 'bbox_px' }, result.room.bbox_px, result.bbox, () => {
        result.room.bbox_px = result.bbox;
        result.room.verified = false;
        result.room.schematic = false;
        if (result.polygon) result.room.polygon_px = result.polygon;
      });
      drawInspector();
    }
  }

  function clearInteraction() {
    if (!interaction) return;
    const pointer = interaction.pointer;
    interaction = null;
    transient.replaceChildren();
    if (scene.hasPointerCapture(pointer)) scene.releasePointerCapture(pointer);
  }

  function toggleVector() {
    vector = !vector;
    vectors.style.visibility = vector ? 'visible' : 'hidden';
    return vector;
  }

  scene.addEventListener('pointerdown', pointerDown);
  scene.addEventListener('pointermove', pointerMove);
  scene.addEventListener('pointerup', pointerUp);
  scene.addEventListener('pointercancel', clearInteraction);
  scene.addEventListener('lostpointercapture', clearInteraction);
  scene.addEventListener('wheel', event => {
    if (!floor()) return;
    event.preventDefault();
    zoom(Math.exp(clamp(-event.deltaY * .002, -.27, .27)), position(event));
  }, { passive: false });
  const excluded = 'input, textarea, select, button, [contenteditable="true"], [role="dialog"]';
  document.addEventListener('keydown', event => {
    if (!floor() || stage.hidden || document.querySelector('dialog[open]') ||
      event.target.closest?.(excluded) || event.altKey || event.ctrlKey || event.metaKey) return;
    if (event.code === 'Space' && !event.repeat && stage.getBoundingClientRect().width > 0) {
      event.preventDefault();
      toggleVector();
    } else if (event.key === 'Escape') {
      clearInteraction();
      select(null);
    } else if (event.key === 'Delete' && selection && ['room','opening','symbol','free-wall'].includes(selection.kind)) {
      event.preventDefault();
      removeSelection();
    }
  });
  let previousWidth = 0;
  let previousHeight = 0;
  new ResizeObserver(() => {
    const width = stage.clientWidth;
    const height = stage.clientHeight;
    if (width && height && (width !== previousWidth || height !== previousHeight) && floor()) fit();
    if (width && height) {
      previousWidth = width;
      previousHeight = height;
    }
  }).observe(stage);

  return {
    render(data, index) {
      const changed = data !== project || index !== floorIndex;
      project = data;
      floorIndex = index;
      scene.hidden = !floor();
      if (!floor()) {
        selection = null;
        drawInspector();
        return;
      }
      if (changed) {
        clearInteraction();
        selection = null;
        fit();
      } else if (selection?.roomId && !selectedRoom()) {
        selection = null;
      }
      for (const room of rooms()) {
        room.id ||= id('room');
        for (const element of [...(room.openings || []), ...(room.symbols || [])]) element.id ||= id('element');
      }
      for (const wall of walls()) wall.id ||= id('wall');
      drawRaster();
      drawVectors();
      drawInspector();
      reportZoom();
    },
    setTool(value) {
      if (!TOOLS.has(value)) return;
      clearInteraction();
      tool = value;
      scene.dataset.tool = tool;
      drawVectors();
    },
    setOpacity(percent) {
      opacity = clamp(Number(percent) || 0, 0, 100);
      raster.style.opacity = opacity / 100;
    },
    selectRoom(roomId) {
      select(rooms().some(room => room.id === roomId) ? { kind: 'room', roomId } : null);
    },
    selectIssue(issue) {
      const roomId = issue?.room_id;
      const room = rooms().find(item => item.id === roomId);
      if (!room) return;
      const openingId = issue.opening_id || issue.object_id;
      if (room.openings?.some(item => item.id === openingId)) {
        select({ kind:'opening', roomId, objectId:openingId });
      } else if (room.symbols?.some(item => item.id === issue.object_id)) {
        select({ kind:'symbol', roomId, objectId:issue.object_id });
      } else if (Object.hasOwn(SIDES, issue.side)) {
        select({ kind:'measurement', roomId, side:issue.side }, true);
      } else {
        select({ kind:'room', roomId });
      }
    },
    zoom,
    fit,
    toggleVector,
  };
}
