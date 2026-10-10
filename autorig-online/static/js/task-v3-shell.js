const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const RUN=/^[0-9a-f]{20}$/;
const els={viewer:document.querySelector('#viewer'),card:document.querySelector('#status-card'),title:document.querySelector('#title'),message:document.querySelector('#message'),stage:document.querySelector('#stage'),percent:document.querySelector('#percent'),fill:document.querySelector('#bar-fill'),progress:document.querySelector('[role="progressbar"]')};
let timer=0,loaded='';

function taskId(){return new URLSearchParams(location.search).get('id')||''}
function safeViewerUrl(raw){
  const url=new URL(String(raw||''),location.origin);
  if(url.origin!==location.origin||url.pathname!=='/api/mt/unity/test/index.html'||[...url.searchParams.keys()].some(k=>k!=='run')||!RUN.test(url.searchParams.get('run')||''))throw new Error('Некорректная привязка V3-вьювера');
  return url.pathname+url.search;
}
function setProgress(value){const n=Math.max(0,Math.min(100,Math.round(Number(value||0)*100)));els.fill.style.width=`${n}%`;els.percent.textContent=`${n}%`;els.progress.setAttribute('aria-valuenow',String(n))}
function showError(text){els.card.hidden=false;els.title.textContent='V3 недоступен';els.message.textContent=text;els.message.classList.add('error');els.stage.textContent='ошибка';setProgress(0)}
function render(data){
  if(!data||data.schema!=='autorig.task-v3-shell/1')throw new Error('Сервер вернул неизвестный V3-контракт');
  setProgress(data.progress);els.stage.textContent=data.stage||data.status||'обработка';els.message.textContent=data.message||'V3 обрабатывает модель';els.message.classList.toggle('error',data.status==='failed');
  if(data.status==='failed'){els.title.textContent='Обработка не завершена';els.card.hidden=false;return false}
  if(data.viewer_url){const src=safeViewerUrl(data.viewer_url);if(src!==loaded){loaded=src;els.viewer.src=src;els.viewer.hidden=false;els.viewer.addEventListener('load',()=>{els.card.hidden=true},{once:true})}els.title.textContent='Загружаем V3-вьювер';return data.status==='done'||data.status==='needs_review'}
  els.title.textContent=data.viewer_state==='awaiting_viewer_publication'?'Публикуем V3-вьювер':'Модель в очереди V3';els.card.hidden=false;return false
}
async function poll(){
  const id=taskId();if(!UUID.test(id)){showError('Некорректный идентификатор задачи');return}
  try{const response=await fetch(`/api/task/${encodeURIComponent(id)}/v3-shell`,{credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'}});if(!response.ok){const body=await response.json().catch(()=>({}));throw new Error(body.detail||`HTTP ${response.status}`)}const terminal=render(await response.json());if(!terminal)timer=setTimeout(poll,2500)}catch(error){showError(error instanceof Error?error.message:'Не удалось прочитать состояние V3');timer=setTimeout(poll,5000)}
}
addEventListener('pagehide',()=>clearTimeout(timer),{once:true});poll();
