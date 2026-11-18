const SIDES = { top: 'Верхняя', bottom: 'Нижняя', left: 'Левая', right: 'Правая' };
const MODEL = 'qwen35-9b-uncensored';
const MAX_CROP = 384;
const MAX_WAIT = 75000;
const PROMPT = 'Transcribe exactly one handwritten dimension, as seen in this tight image. Return one JSON object with only the key "digits". Its value must be a string of one to four ASCII digits prominently visible together, or null if uncertain, no digits are clear, or multiple numbers are present. No guessing, unit, conversion, or explanation.';
const clamp = (v, min, max) => Math.max(min, Math.min(max, v));
const sideExists = (side) => Object.hasOwn(SIDES, side);

function element(tag, properties = {}, text = '') {
  const node = document.createElement(tag);
  for (const [name, value] of Object.entries(properties)) node.setAttribute(name, value);
  node.textContent = text;
  return node;
}

function box(input) {
  return Array.isArray(input) && input.length === 4 && input.every(Number.isFinite)
    && input[2] > input[0] && input[3] > input[1] ? input : null;
}

function intersect(a, b) {
  return box([Math.max(a[0], b[0]), Math.max(a[1], b[1]), Math.min(a[2], b[2]), Math.min(a[3], b[3])]);
}

function sourceBounds(floor, image) {
  const width = floor.image_width || image.naturalWidth;
  const height = floor.image_height || image.naturalHeight;
  const extent = box(floor.source_bbox_px);
  return !extent || (extent[2] <= width && extent[3] <= height) ? [0, 0, width, height] : extent;
}

function sampleImage(ctx, image, bounds, area, dest) {
  const ratioX = image.naturalWidth / (bounds[2] - bounds[0]);
  const ratioY = image.naturalHeight / (bounds[3] - bounds[1]);
  ctx.drawImage(image, (area[0] - bounds[0]) * ratioX, (area[1] - bounds[1]) * ratioY,
    (area[2] - area[0]) * ratioX, (area[3] - area[1]) * ratioY, ...dest);
}

function tokenCanvas(image, scope, crop, rotation) {
  const w = crop[2] - crop[0];
  const h = crop[3] - crop[1];
  const angle = rotation * Math.PI / 180;
  const sin = Math.abs(Math.sin(angle)), cos = Math.abs(Math.cos(angle));
  const spanX = w*cos+h*sin, spanY=w*sin+h*cos;
  const scale = Math.min(6, (MAX_CROP - 20) / Math.max(spanX,spanY));
  const scaledW = Math.max(1, Math.floor(w * scale));
  const scaledH = Math.max(1, Math.floor(h * scale));
  const token = document.createElement('canvas');
  token.width = Math.min(MAX_CROP,Math.max(56,Math.ceil(spanX*scale)+20));
  token.height = Math.min(MAX_CROP,Math.max(56,Math.ceil(spanY*scale)+20));
  const ctx = token.getContext('2d', { willReadFrequently: true });
  ctx.fillStyle = '#fff';
  ctx.fillRect(0, 0, token.width, token.height);
  ctx.save();
  ctx.translate(token.width / 2, token.height / 2);
  ctx.rotate(rotation * Math.PI / 180);
  sampleImage(ctx, image, scope, crop, [-scaledW / 2, -scaledH / 2, scaledW, scaledH]);
  ctx.restore();
  const pixels = ctx.getImageData(0, 0, token.width, token.height);
  for (let i = 0; i < pixels.data.length; i += 4) {
    const tone = Math.min(pixels.data[i], pixels.data[i + 1], pixels.data[i + 2]);
    const contrast = tone > 222 ? 255 : clamp(Math.round((tone - 35) * 255 / 187), 0, 255);
    pixels.data[i] = contrast;
    pixels.data[i + 1] = contrast;
    pixels.data[i + 2] = contrast;
  }
  ctx.putImageData(pixels, 0, 0);
  return token;
}

function parseDimension(answer) {
  if (typeof answer !== 'string' || answer.length > 250) return null;
  const text = answer.trim().replace(/^```(?:json)?\s*([\s\S]*?)\s*```$/iu, '$1');
  const match = /^\s*\{\s*"digits"\s*:\s*(null|"[0-9]{1,4}")\s*\}\s*$/u.exec(text);
  if (!match || match[1] === 'null') return null;
  const value = Number(JSON.parse(match[1]));
  return value >= 15 && value <= 9999 ? value : null;
}

function delay(ms, signal) {
  return new Promise((resolve, reject) => {
    const cancel = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', cancel);
      resolve();
    }, ms);
    if (signal.aborted) cancel();
    else signal.addEventListener('abort', cancel, { once: true });
  });
}

async function responseData(url, options) {
  const response = await fetch(url, { ...options, cache: 'no-store' });
  if (response.status === 401 || response.status === 403) throw new Error('Для проверки на GPU-ферме войдите в AutoRig.');
  if (response.status === 429) throw new Error('Слишком много запросов. Попробуйте немного позже.');
  if (!response.ok) throw new Error('GPU-ферма сейчас недоступна. Попробуйте ещё раз.');
  try { return await response.json(); } catch { throw new Error('GPU-ферма вернула непонятный ответ.'); }
}

async function requestReading(image, signal) {
  let data = await responseData('/api/vision', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, signal,
    body: JSON.stringify({ image_url: image, prompt: PROMPT, model: MODEL, max_output_tokens: 256, wait_seconds: 12 }),
  });
  const id = data?.task_id_string;
  while (data?.success_bool !== false && !data?.finished_bool && data?.status_string !== 'completed') {
    if (!/^[\w.-]{1,150}$/u.test(id || '')) throw new Error('GPU-ферма не вернула номер задания.');
    await delay(1800, signal);
    data = await responseData(`/api/ai/status/${encodeURIComponent(id)}`, { signal });
  }
  if (data?.success_bool === false || data?.status_string === 'failed') throw new Error('GPU-ферме не удалось обработать фрагмент.');
  return parseDimension(data?.answer_string);
}

export async function openVision({ floor, room, side, apply }) {
  if (!/^data:image\/(?:png|jpeg|webp|svg\+xml)[;,]/iu.test(floor?.image || '') || !box(room?.bbox_px)) {
    throw new Error('Выберите помещение на загруженном абрисе.');
  }
  const image = new Image();
  image.src = floor.image;
  try { await image.decode(); } catch { throw new Error('Не удалось открыть скан для проверки.'); }
  const scope = sourceBounds(floor, image);
  const area = intersect(room.bbox_px, scope);
  if (!area || area[2] - area[0] < 18 || area[3] - area[1] < 18) throw new Error('У выбранного помещения слишком малая область рисунка.');

  const samples = room.raw_measurement_boxes_px || room.measurement_bboxes_px || {};
  let selectedSide = sideExists(side) ? side : Object.keys(SIDES).find(key => box(samples[key])) || 'top';
  let rotation = 0, crop = null, start = null, tentative = null, suggestion = null, request = null;
  const viewW = area[2] - area[0], viewH = area[3] - area[1];
  const maxW = Math.min(MAX_CROP, Math.floor(viewW * .34));
  const maxH = Math.min(MAX_CROP, Math.floor(viewH * .34));
  const accepted = (bounds) => Boolean(bounds && bounds[2] - bounds[0] >= 5 && bounds[2] - bounds[0] <= maxW + 0.0001
    && bounds[3] - bounds[1] >= 5 && bounds[3] - bounds[1] <= maxH + 0.0001);
  const withinRoom = (bounds) => box(bounds) ? intersect(bounds, area) : null;

  const dialog = element('dialog', { class: 'ab-dialog ab-vision', 'aria-labelledby': 'ab-vision-title' });
  const header = element('div', { class: 'ab-dialog-head' });
  const close = element('button', { type: 'button', class: 'ab-close', 'aria-label': 'Закрыть' }, '×');
  header.append(element('h2', { id: 'ab-vision-title' }, 'Проверка промера'), close);
  const sideMenu = element('select', { 'aria-label': 'Сторона помещения' });
  for (const [key, name] of Object.entries(SIDES)) sideMenu.append(element('option', { value: key }, name));
  const sideLabel = element('label', { class: 'ab-field ab-vision-side' }, 'Стена');
  sideLabel.append(sideMenu);
  const work = element('div', { class: 'ab-vision-work' });
  const mapFigure = element('figure', { class: 'ab-vision-area' });
  const map = element('canvas', { tabindex: '0', 'aria-label': 'Фрагмент помещения. Протяните рамку вокруг одного числа. Стрелки сдвигают рамку, Shift со стрелками меняет размер.' });
  const viewportScale = Math.min(640 / viewW, 440 / viewH, 2);
  map.width = Math.max(1, Math.round(viewW * viewportScale));
  map.height = Math.max(1, Math.round(viewH * viewportScale));
  mapFigure.append(map, element('figcaption', {}, 'Обведите один промер'));
  const preview = element('figure', { class: 'ab-vision-preview' });
  const tokenSlot = element('div', { class: 'ab-vision-token', 'aria-label': 'Изображение для отправки' });
  const helper = element('span', { class: 'ab-vision-empty' }, 'Фрагмент не выбран');
  const controls = element('div', { class: 'ab-vision-rotate' });
  const turnLeft = element('button', { type: 'button', title: 'Повернуть влево на 90°', 'aria-label': 'Повернуть влево на 90°' }, '↶');
  const turnRight = element('button', { type: 'button', title: 'Повернуть вправо на 90°', 'aria-label': 'Повернуть вправо на 90°' }, '↷');
  const degrees=element('input',{type:'number',min:'-180',max:'180',step:'1','aria-label':'Угол поворота, градусы',title:'Угол поворота, градусы'});
  controls.append(turnLeft,degrees,turnRight);
  preview.append(tokenSlot, element('figcaption', {}, 'Фрагмент для отправки'), controls);
  work.append(mapFigure, preview);
  const consent = element('p', { class: 'ab-vision-consent' }, 'На GPU-ферму AutoRig отправится только показанный фрагмент с цифрами.');
  const result = element('p', { class: 'ab-vision-result', role: 'status', 'aria-live': 'polite' });
  const footer = element('div', { class: 'ab-actions' });
  const send = element('button', { class: 'ab-primary', type: 'button' }, 'Прочитать цифры');
  const accept = element('button', { class: 'ab-primary', type: 'button', hidden: '' }, 'Применить');
  footer.append(send, accept);
  dialog.append(header, sideLabel, work, consent, result, footer);
  document.body.append(dialog);
  sideMenu.value = selectedSide;

  const sourceCanvas = document.createElement('canvas');
  sourceCanvas.width = map.width;
  sourceCanvas.height = map.height;
  const sourceContext = sourceCanvas.getContext('2d');
  sourceContext.fillStyle = '#fff';
  sourceContext.fillRect(0, 0, map.width, map.height);
  sampleImage(sourceContext, image, scope, area, [0, 0, map.width, map.height]);
  const ctx = map.getContext('2d');
  let token = null;

  function drawRoom() {
    ctx.drawImage(sourceCanvas, 0, 0);
    const bounds = tentative || crop;
    if (!bounds) return;
    const [x1, y1, x2, y2] = bounds;
    const x = (x1 - area[0]) * map.width / viewW;
    const y = (y1 - area[1]) * map.height / viewH;
    const w = (x2 - x1) * map.width / viewW;
    const h = (y2 - y1) * map.height / viewH;
    ctx.save();
    ctx.fillStyle = 'rgba(38,30,50,.36)';
    ctx.beginPath();
    ctx.rect(0, 0, map.width, map.height);
    ctx.rect(x, y, w, h);
    ctx.fill('evenodd');
    ctx.strokeStyle = '#4037cb';
    ctx.lineWidth = 2;
    ctx.strokeRect(x, y, w, h);
    ctx.restore();
  }

  function updateControls() {
    const locked = Boolean(request);
    send.disabled = locked || !token || !!start;
    sideMenu.disabled = locked;
    turnLeft.disabled = locked || !token;
    turnRight.disabled = locked || !token;
    degrees.disabled=locked || !token;
    degrees.value=String(rotation>180 ? rotation-360 : rotation);
    accept.hidden = suggestion === null || locked;
    map.setAttribute('aria-disabled', String(locked));
  }

  function updateCrop() {
    drawRoom();
    token = crop ? tokenCanvas(image, scope, crop, rotation) : null;
    tokenSlot.replaceChildren(token || helper);
    suggestion = null;
    result.textContent = '';
    updateControls();
  }

  function chooseSide() {
    selectedSide = sideMenu.value;
    const candidate = withinRoom(samples[selectedSide]);
    crop = accepted(candidate) ? candidate : null;
    const rotated = crop && crop[3] - crop[1] > (crop[2] - crop[0]) * 1.24;
    const saved = room.measurement_rotations_deg?.[selectedSide];
    rotation = Number.isFinite(saved) ? (saved+360)%360 : rotated ? (selectedSide === 'right' ? 270 : 90) : 0;
    updateCrop();
  }

  function point(event) {
    const rect = map.getBoundingClientRect();
    return [clamp(area[0] + (event.clientX - rect.left) * viewW / rect.width, area[0], area[2]),
      clamp(area[1] + (event.clientY - rect.top) * viewH / rect.height, area[1], area[3])];
  }

  function dragBounds(end) {
    const x = clamp(end[0], start[0] - maxW, start[0] + maxW);
    const y = clamp(end[1], start[1] - maxH, start[1] + maxH);
    return [Math.min(start[0], x), Math.min(start[1], y), Math.max(start[0], x), Math.max(start[1], y)];
  }

  close.onclick = () => dialog.close();
  dialog.addEventListener('close', () => {
    request?.abort();
    dialog.remove();
  }, { once: true });
  sideMenu.onchange = chooseSide;
  turnLeft.onclick = () => { rotation = (rotation + 270) % 360; updateCrop(); };
  turnRight.onclick = () => { rotation = (rotation + 90) % 360; updateCrop(); };
  degrees.onchange=()=>{ if (degrees.value && degrees.checkValidity()) { rotation=(degrees.valueAsNumber+360)%360; updateCrop(); } };
  map.addEventListener('pointerdown', event => {
    if (request || event.button !== 0 || start) return;
    map.setPointerCapture(event.pointerId);
    start = point(event);
    updateControls();
  });
  map.addEventListener('pointermove', event => {
    if (!start || !map.hasPointerCapture(event.pointerId)) return;
    tentative = dragBounds(point(event));
    drawRoom();
  });
  map.addEventListener('pointerup', event => {
    if (!start || !map.hasPointerCapture(event.pointerId)) return;
    const selection = dragBounds(point(event));
    start = null;
    tentative = null;
    if (accepted(selection)) {
      crop = selection;
      rotation = 0;
      updateCrop();
    } else {
      drawRoom();
      updateControls();
    }
  });
  map.addEventListener('lostpointercapture', () => {
    if (!start) return;
    start = null;
    tentative = null;
    drawRoom();
    updateControls();
  });
  map.addEventListener('keydown', event => {
    const moves = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    if (request || !Object.hasOwn(moves, event.key)) return;
    event.stopPropagation();
    event.preventDefault();
    const w = Math.max(5, Math.min(maxW, viewW / 5)), h = Math.max(5, Math.min(maxH, viewH / 5));
    const cx = (area[0] + area[2]) / 2, cy = (area[1] + area[3]) / 2;
    const current = crop || [cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2];
    const [dx, dy] = moves[event.key].map(offset => offset * (event.altKey ? 1 : 4));
    const next = event.shiftKey
      ? withinRoom([current[0], current[1], current[2] + dx, current[3] + dy])
      : withinRoom([current[0] + dx, current[1] + dy, current[2] + dx, current[3] + dy]);
    if (accepted(next)) { crop = next; updateCrop(); }
  });

  send.onclick = async () => {
    if (!token || request || start) return;
    const controller = new AbortController();
    request = controller;
    suggestion = null;
    updateControls();
    result.dataset.status = 'busy';
    result.textContent = 'GPU-ферма читает цифры…';
    const timeout = setTimeout(() => controller.abort(new DOMException('Timeout', 'TimeoutError')), MAX_WAIT);
    try {
      suggestion = await requestReading(token.toDataURL('image/png'), controller.signal);
      result.dataset.status = suggestion === null ? 'error' : 'ready';
      result.textContent = suggestion === null ? 'Уверенного ответа нет. Уточните рамку или поверните фрагмент.' : `Предложено ${suggestion} см. Сверьте число с рисунком.`;
    } catch (error) {
      if (!dialog.open) return;
      result.dataset.status = 'error';
      result.textContent = error.name === 'TimeoutError' ? 'Время ожидания ответа истекло. Попробуйте ещё раз.'
        : error instanceof TypeError ? 'Соединение с GPU-фермой недоступно.' : error.message;
    } finally {
      clearTimeout(timeout);
      request = null;
      updateControls();
    }
  };
  accept.onclick = () => {
    if (suggestion === null || request || !crop) return;
    apply({ side: selectedSide, value: suggestion, bbox_px: crop.map(value => Math.round(value)), rotation });
    dialog.close();
  };
  chooseSide();
  dialog.showModal();
}
