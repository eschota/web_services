import { createDemo } from './demo.mjs';
import { mountEditor } from './editor.mjs';
import * as cad from './cad.mjs';
import { readProject, emptyFloor, draft, saveFile } from './project.mjs';

const $ = id => document.getElementById(id);
const make = (tag, text, cls) => {
  const el = document.createElement(tag);
  if (text != null) el.textContent = text;
  if (cls) el.className = cls;
  return el;
};
let project = createDemo(), floorIndex = 0, view = 'scan', tool = 'select', isExample = true;
let solved, loading = false, sheetZoom = 1, storageWarning = false, noticeTimer;
const floor = () => project.floors[floorIndex];
function notify(message, hold = false) {
  clearTimeout(noticeTimer);
  $('status').textContent = message;
  const notice = $('notice');
  notice.textContent = message;
  notice.hidden = !hold;
  if (hold) noticeTimer = setTimeout(() => { notice.hidden = true; }, 16000);
}
function scheduleSave() {
  return draft({project,floorIndex,isExample}).catch(() => {
    if (!storageWarning) { storageWarning=true; notify('Автосохранение недоступно. Скачайте проект кнопкой «Сохранить».',true); }
  });
}
function changed(patch) {
  isExample = false;
  solved = null;
  if (patch) {
    project.dataset_corrections ||= [];
    project.dataset_corrections.push({floor_id:floor().floor_id, time:new Date().toISOString(), ...patch});
    if (project.dataset_corrections.length > 4000) project.dataset_corrections.shift();
  }
  drawFloors();
  scheduleSave();
}
const editor = mountEditor({stage:$('stage'), inspector:$('inspector'), notify,
  onSelect: () => {}, onChange: patch => { changed(patch); notify('Изменения внесены'); }, onVision:reviewWithVision});
async function reviewWithVision(room, side) {
  const sourceFloor=floor();
  try {
    const {openVision} = await import('./vision.mjs');
    await openVision({floor:sourceFloor,room,side,apply:result => {
      const key=result.side, n=Number(result.value);
      if (floor()!==sourceFloor || !sourceFloor.rooms.includes(room) || !['top','bottom','left','right'].includes(key) ||
          !Number.isInteger(n) || n<1 || n>9999) return;
      const before=room.raw_measurements_cm?.[key] ?? null;
      (room.raw_measurements_cm ||= {})[key]=n;
      (room.measurement_confidences ||= {})[key]=1;
      (room.raw_measurement_boxes_px ||= {})[key]=result.bbox_px;
      (room.measurement_rotations_deg ||= {})[key]=result.rotation===270 ? -90 : result.rotation || 0;
      changed({kind:'edit',entity:'measurement',room_id:room.id,field:key,before,after:n,
        source:'vision_review',bbox_px:result.bbox_px});
      editor.render(project,floorIndex); editor.selectRoom(room.id);
      notify('Подтверждён промер ' + n + ' см');
    }});
  } catch(e) { notify(e.message || 'Сверка через Vision временно недоступна.',true); }
}
$('stage').addEventListener('abris-zoom', e => {
  if (view === 'scan' && Number.isFinite(e.detail)) $('zoom-value').textContent = Math.round(e.detail) + '%';
});
function drawFloors() {
  const list = $('floors-list');
  list.replaceChildren();
  project.floors.forEach((f,i) => {
    const btn = make('button',null,'floor-item' + (i === floorIndex ? ' is-active' : ''));
    btn.type = 'button';
    btn.setAttribute('aria-current', i === floorIndex ? 'true' : 'false');
    btn.setAttribute('aria-selected', String(i === floorIndex));
    const n = make('span', String(i+1).padStart(2,'0'), 'floor-number');
    const info = make('span', null, 'floor-info');
    const count = f.rooms.filter(r => r.is_heated !== false).length;
    info.append(make('strong',f.title,'floor-title'),make('small','Помещений · ' + count,'floor-subtitle'));
    btn.append(n,info);
    btn.addEventListener('click',() => { floorIndex = i; render(); scheduleSave(); });
    list.append(btn);
  });
  $('room-count').hidden = true;
}
function drawSheet() {
  solved ||= cad.solve(project);
  const svg = cad.renderSheet(solved,floorIndex,project.project_meta);
  $('sheet').innerHTML = svg;
  $('sheet').style.setProperty('--sheet-zoom',sheetZoom);
  $('zoom-value').textContent = Math.round(sheetZoom * 100) + '%';
  showDiagnostics();
}
function issues() {
  return (solved?.issues || []).filter(i => !i.floor_id || i.floor_id === floor().floor_id);
}
function showDiagnostics() {
  const panel = $('inspector');
  panel.replaceChildren();
  panel.append(make('h3', 'Проверка чертежа'));
  const errors = issues();
  if (!errors.length) panel.append(make('p','Размеры согласованы. Лист готов к экспорту.'));
  else for (const issue of errors.slice(0,50)) {
    const btn = action(issue.message,() => {
      setView('scan');
      if (issue.room_id) editor.selectIssue(issue);
    },'ab-issue');
    btn.dataset.severity = issue.severity;
    panel.append(btn);
  }
}
function setView(next) {
  view = next;
  $('stage').hidden = view !== 'scan';
  $('sheet').hidden = view !== 'plan';
  document.querySelector('.tool-dock').hidden = view !== 'scan';
  document.querySelector('.opacity-control').hidden = view !== 'scan';
  $('view-scan').setAttribute('aria-pressed',String(view === 'scan'));
  $('view-plan').setAttribute('aria-pressed',String(view === 'plan'));
  if (view === 'plan') drawSheet();
  else editor.render(project,floorIndex);
}
function render() {
  $('project-name').value = project.name || 'Новый проект';
  $('scale').value = project.project_meta.scale || '1:100';
  $('profile').value = project.project_meta.target_profile || 'TECH_PLAN_P0082';
  drawFloors();
  setView(view);
}
function selectTool(name) {
  if (view !== 'scan') setView('scan');
  tool = name;
  editor.setTool(name);
  document.querySelectorAll('[data-tool]').forEach(btn => btn.setAttribute('aria-pressed',String(btn.dataset.tool === tool)));
}
function action(label, fn, cls) {
  const btn = make('button',label,cls);
  btn.type = 'button';
  btn.addEventListener('click',fn);
  return btn;
}
function modal(title) {
  const d = make('dialog',null,'ab-dialog');
  const head = make('div',null,'ab-dialog-head');
  const heading = make('h2',title);
  heading.id = 'ab-dialog-title';
  d.setAttribute('aria-labelledby',heading.id);
  const close = action('×',() => d.close(),'ab-close');
  close.setAttribute('aria-label','Закрыть');
  head.append(heading,close);
  d.append(head);
  d.addEventListener('click',e => { if (e.target === d) {
    const r = d.getBoundingClientRect();
    if (e.clientX < r.left || e.clientX > r.right || e.clientY < r.top || e.clientY > r.bottom) d.close();
  }});
  d.addEventListener('close',() => d.remove(),{once:true});
  document.body.append(d);
  d.showModal();
  return d;
}
function field(parent,label,value,type='text') {
  const row = make('label',null,'ab-field');
  const input = make('input');
  input.type = type;
  input.value = value ?? '';
  input.maxLength = 350;
  row.append(make('span',label),input);
  parent.append(row);
  return input;
}
async function exportTraining() {
  const samples=[];
  for (const f of project.floors) {
    if (!f.image) continue;
    const img = new Image(); img.src=f.image; await img.decode();
    for (const room of f.rooms) for (const side of ['top','bottom','left','right']) {
      const bounds = room.raw_measurement_boxes_px?.[side];
      const value = room.raw_measurements_cm?.[side];
      const confirmed = room.verified || room.measurement_confidences?.[side] === 1;
      if (!confirmed || !bounds || !Number.isInteger(value) || value <= 0) continue;
      const x=Math.max(0,Math.floor(bounds[0]-2)), y=Math.max(0,Math.floor(bounds[1]-2));
      const w=Math.min(img.width-x,Math.ceil(bounds[2]+2)-x), h=Math.min(img.height-y,Math.ceil(bounds[3]+2)-y);
      if (w<2 || h<2 || w*h>100000) continue;
      const angle = room.measurement_rotations_deg?.[side] ?? (['top','bottom'].includes(side) ? 0 : null);
      if (![0,-90,90,180,-180].includes(angle)) continue;
      const c=document.createElement('canvas'), vertical=Math.abs(angle)===90;
      c.width=vertical ? h : w; c.height=vertical ? w : h;
      const ctx=c.getContext('2d'); ctx.translate(c.width/2,c.height/2); ctx.rotate(angle*Math.PI/180);
      ctx.drawImage(img,x,y,w,h,-w/2,-h/2,w,h);
      samples.push({image:c.toDataURL('image/png'),label:String(value),group:f.training_group || f.floor_id});
    }
  }
  if (!samples.length) throw new Error('Пока нет подтверждённых цифр с координатами на скане.');
  saveFile('abris-corrections.json',JSON.stringify({version:1,samples}),'application/json');
  return samples.length;
}
function metadata() {
  const d = modal('Данные проекта');
  const form = make('form');
  d.append(form);
  const m = project.project_meta;
  const fields = {};
  for (const [key,label] of Object.entries({contract_number:'Номер договора',customer:'Заказчик',cadastral_number:'Кадастровый номер',address:'Адрес объекта'})) fields[key] = field(form,label,m[key]);
  const title = field(form,'Название текущего этажа',floor().title);
  const h = field(form,'Высота этажа, м',floor().height_m ?? m.height_m,'number');
  h.step = '0.01'; h.min = '0.2'; h.max = '100';
  const calibration = field(form,'Калибровка подложки, см/пиксель (если известна)',floor().cm_per_px,'number');
  calibration.step='any'; calibration.min='0.0001'; calibration.max='10000';
  const ok = make('button','Сохранить','ab-primary'); ok.type = 'submit'; form.append(ok);
  form.addEventListener('submit',e => {
    e.preventDefault();
    for (const [k,input] of Object.entries(fields)) m[k] = input.value.trim();
    floor().title = title.value.trim().slice(0,80) || floor().title;
    floor().height_m = h.value ? Number(h.value) : null;
    floor().cm_per_px = calibration.value ? Number(calibration.value) : null;
    changed(); render(); d.close(); notify('Данные проекта сохранены');
  });
  const more = make('details'); more.append(make('summary','Действия с проектом'));
  d.append(more);
  const result = make('p'); result.setAttribute('role','status');
  more.append(action('Скачать размеченные цифры для обучения',async () => {
    try { result.textContent='Сохранено фрагментов: ' + await exportTraining(); }
    catch(e) { result.textContent=e.message; }
  }),result);
  if (project.floors.length>1) {
    if (floorIndex>0) more.append(action('Переместить этаж выше',() => {
      const current=project.floors.splice(floorIndex,1)[0];
      project.floors.splice(--floorIndex,0,current); changed(); render(); d.close();
    }));
    more.append(action('Удалить текущий этаж',async () => {
      d.close();
      if (!await confirmChoice('Удалить этаж?', 'Из проекта будет удалён «' + floor().title + '» с его подложкой и помещениями.','Удалить этаж')) return;
      const removed = project.floors.splice(floorIndex,1)[0];
      project.dataset_corrections = (project.dataset_corrections || []).filter(c=>c.floor_id!==removed.floor_id);
      floorIndex=Math.min(floorIndex,project.floors.length-1); changed(); render();
    }));
  }
}
function confirmChoice(title, description, label) {
  return new Promise(resolve => {
    const d = modal(title); let answer = false;
    d.append(make('p',description));
    const row = make('div',null,'ab-actions');
    row.append(action('Отмена',() => d.close()), action(label,() => { answer=true; d.close(); },'ab-primary'));
    d.append(row); d.addEventListener('close',() => resolve(answer),{once:true});
  });
}
function importMode() {
  if (isExample) return Promise.resolve('replace');
  return new Promise(resolve => {
    const d = modal('Загрузить абрис');
    d.append(make('p','Добавить листы в открытый проект или начать новый? Текущий проект можно сохранить в JSON.'));
    let choice = null;
    const btn = (label,id,cls) => action(label,() => { choice=id; d.close(); },cls);
    const row = make('div',null,'ab-actions');
    row.append(btn('Новый проект','replace'),btn('Добавить листы','append','ab-primary'));
    d.append(row); d.addEventListener('close',() => resolve(choice),{once:true});
  });
}
async function readScan(file,signal) {
  if (file.size > 24*1024*1024) throw new Error('Один скан может занимать до 24 МБ.');
  const response = await fetch('/abris/api/recognize',{method:'POST',headers:{
    'Content-Type':'application/octet-stream','X-Abris-Filename':encodeURIComponent(file.name)},body:file,signal});
  if (response.status === 413) throw new Error('Скан слишком большой. Предел — 24 МБ.');
  if (response.status === 429 || response.status === 503) throw new Error('Сервис занят. Повторите загрузку чуть позже.');
  if (response.status === 504) throw new Error('Превышено время обработки. Загрузите страницы по отдельности.');
  let result;
  try { result = await response.json(); } catch { throw new Error('Сервис не ответил. Проверьте соединение и повторите загрузку.'); }
  if (!response.ok) throw new Error(result.error || 'Не удалось обработать скан.');
  return result;
}
async function loadFiles(files) {
  if (!files.length || loading) return;
  if (files.length > 12) { notify('За один раз можно загрузить до 12 файлов.',true); return; }
  loading = true;
  let progress;
  const controller = new AbortController();
  try {
    const mode = await importMode();
    if (!mode) return;
    const parts=[],warnings=[];
    progress = modal('Обработка абриса');
    const caption = make('p'); const bar = make('progress');
    progress.append(caption,bar,action('Отменить',() => progress.close()));
    progress.addEventListener('close',() => controller.abort(),{once:true});
    for (const [i,file] of files.entries()) {
      caption.textContent = files.length > 1 ? (i+1) + ' / ' + files.length + ' · ' + file.name : file.name;
      const json = /\.json$/i.test(file.name);
      if (json && file.size > 96*1024*1024) throw new Error('Файл проекта превышает 96 МБ.');
      const raw = json ? JSON.parse(await file.text()) : await readScan(file,controller.signal);
      if (controller.signal.aborted) return;
      if (Array.isArray(raw.warnings)) warnings.push(...raw.warnings.filter(s => typeof s === 'string'));
      const imported = await readProject(raw,file.name.replace(/\.[^.]+$/,''));
      if (!json) {
        const digest = await crypto.subtle.digest('SHA-256',await file.arrayBuffer());
        const group = Array.from(new Uint8Array(digest).slice(0,12),v=>v.toString(16).padStart(2,'0')).join('');
        imported.floors.forEach(f=>{ f.training_group=group; });
      }
      parts.push(imported);
    }
    if (controller.signal.aborted) return;
    const merged = mode === 'append' ? structuredClone(project) : parts.shift();
    const selected = mode === 'append' ? merged.floors.length : 0;
    for (const part of parts) {
      for (const f of part.floors) {
        const old = f.floor_id;
        if (merged.floors.some(existing => existing.floor_id === old)) {
          f.floor_id = crypto.randomUUID();
          for (const c of part.dataset_corrections || []) if (c.floor_id === old) c.floor_id = f.floor_id;
        }
        merged.floors.push(f);
      }
      merged.dataset_corrections ||= [];
      merged.dataset_corrections.push(...(part.dataset_corrections || []));
    }
    if (merged.floors.length > 100) throw new Error('В одном проекте можно открыть до 100 этажей.');
    project = merged; floorIndex = selected; isExample = false; solved = null; view = 'scan';
    render(); selectTool('select'); scheduleSave();
    const summary = warnings.length ? [...new Set(warnings)].join(' ') : 'Выберите помещение и сверьте промеры с подложкой.';
    notify(summary,true);
  } catch (err) {
    if (err.name !== 'AbortError') notify(err instanceof SyntaxError ? 'Файл не содержит корректный JSON проекта.' : err.message,true);
  } finally {
    if (progress?.open) progress.close();
    loading=false;
    $('import-file').value='';
  }
}
function generate() {
  solved = cad.solve(project);
  sheetZoom = 1; setView('plan');
  const count = issues().filter(i=>i.severity === 'error').length;
  notify(count ? 'Требуется проверка. Замечаний на этаже: ' + count : 'Чертёж построен · ' + floor().title,Boolean(count));
}
async function exportMenu() {
  solved = cad.solve(project);
  const d = modal('Экспорт чертежа');
  d.append(make('p',floor().title + ' · А4 · ' + project.project_meta.scale));
  const row = make('div',null,'ab-formats');
  d.append(row);
  const err = make('p',null,'ab-error');
  err.setAttribute('role','alert'); d.append(err);
  for (const [type,name,detail] of [['pdf','PDF','Печатный лист'],['jpeg','JPEG','А4 · 300 dpi'],['dxf','DXF','Редактируемые слои'],['dwg','DWG','Чертёж AutoCAD']]) {
    const b = action('',async () => {
      for (const btn of row.querySelectorAll('button')) btn.disabled = true;
      try {
        await cad.downloadPlan(type,solved,floorIndex,project.project_meta);
        d.close(); notify('Файл ' + name + ' подготовлен');
      } catch (e) { err.textContent = e.message || 'Не удалось создать файл.'; }
      finally { for (const btn of row.querySelectorAll('button')) btn.disabled=false; }
    },'ab-format');
    b.append(make('strong',name),make('span',detail)); row.append(b);
  }
  if (project.floors.length > 1) {
    const all = action('Скачать PDF со всеми этажами',async () => {
      all.disabled = true;
      try { await cad.downloadPlan('pdf',solved,null,project.project_meta); d.close(); notify('Комплект поэтажных планов подготовлен'); }
      catch(e) { err.textContent = e.message || 'Не удалось собрать комплект.'; }
      finally { all.disabled=false; }
    });
    d.append(all);
  }
}
function saveProject() {
  scheduleSave();
  const result = cad.solve(project);
  const document = {...project,schema_version:1,floors:project.floors.map((f,i) => {
    const finished = result.floors[i];
    const byId = new Map(finished.rooms.map(r=>[r.id,r]));
    return {...f,review_issues:finished.issues,rooms:f.rooms.map(raw=>{
      const r=byId.get(raw.id);
      if (!r) return raw;
      const geometryOK=!(finished.issues || []).some(issue=>issue.severity==='error' && (!issue.room_id || issue.room_id===raw.id));
      return {...raw,room_number:r.number,solved_dimensions_m:geometryOK ? r.solved_dimensions_m : null,
        solved_origin_m:geometryOK ? [r.x,r.y] : null,area_m2:geometryOK ? r.area_m2 : null};
    })};
  })};
  saveFile((project.name || 'Абрис') + '.abris.json',JSON.stringify(document,null,2),'application/json;charset=utf-8');
  notify('Проект с подложками и правками сохранён в JSON');
}
$('meta-btn').onclick = metadata;
$('generate').onclick = generate;
$('export-btn').onclick = exportMenu;
$('save-btn').onclick = saveProject;
$('project-name').onchange = () => { project.name = $('project-name').value.trim() || 'Новый проект'; changed(); render(); };
$('view-scan').onclick = () => setView('scan');
$('view-plan').onclick = generate;
function zoom(factor) {
  if (view === 'scan') editor.zoom(factor);
  else { sheetZoom = Math.max(0.4,Math.min(4,sheetZoom*factor)); drawSheet(); }
}
$('zoom-in').onclick = () => zoom(1.25);
$('zoom-out').onclick = () => zoom(0.8);
$('fit').onclick = () => { if (view === 'scan') editor.fit(); else {sheetZoom = 1; drawSheet();} };
$('opacity').oninput = () => editor.setOpacity(+$('opacity').value);
for (const b of document.querySelectorAll('[data-tool]')) b.onclick = () => selectTool(b.dataset.tool);
$('scale').onchange = () => { project.project_meta.scale = $('scale').value; changed(); if (view==='plan') drawSheet(); };
$('profile').onchange = () => { project.project_meta.target_profile = $('profile').value; changed(); if (view==='plan') drawSheet(); };
$('demo-btn').onclick = async () => {
  if (!isExample && !await confirmChoice('Открыть пример?', 'Демонстрационный дом заменит текущий проект в этом браузере. Для возврата сохраните свой проект в JSON.', 'Открыть пример')) return;
  project=createDemo(); floorIndex=0; solved=null; view='scan'; isExample=true; render(); selectTool('select'); scheduleSave();
  notify('Открыт демонстрационный дом');
};
$('add-floor').onclick = () => {
  if (project.floors.length >= 100) return notify('В проекте уже 100 этажей.',true);
  project.floors.push(emptyFloor(project.floors.length)); floorIndex=project.floors.length-1; view='scan';
  changed(); render(); selectTool('room'); notify('Протяните прямоугольник помещения на рабочем поле.');
};
$('import-file').multiple = true;
$('import-btn').onclick = () => $('import-file').click();
$('import-file').onchange = e => loadFiles([...e.target.files]);
const desk = $('desk');
desk.addEventListener('dragover',e => { if(e.dataTransfer.types.includes('Files')) { e.preventDefault(); desk.classList.add('drop-target'); } });
desk.addEventListener('dragleave',() => desk.classList.remove('drop-target'));
desk.addEventListener('drop',e => { e.preventDefault(); desk.classList.remove('drop-target'); loadFiles([...e.dataTransfer.files]); });
document.addEventListener('keydown',e => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') { e.preventDefault(); saveProject(); }
});
async function start() {
  try {
    const last = await draft();
    if (last?.project?.floors?.length) {
      project=last.project; floorIndex=Math.max(0,Math.min(last.floorIndex||0,project.floors.length-1)); isExample=last.isExample===true;
    }
  } catch { /* The workspace also works when browser storage is unavailable. */ }
  render();
  editor.setOpacity(+$('opacity').value);
  notify(isExample ? 'Демонстрационный проект · выберите комнату для проверки' : 'Проект восстановлен из этого браузера');
}
start().catch(() => notify('Не удалось открыть проект. Загрузите сохранённую копию JSON.',true));
