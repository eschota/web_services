const RUN_RE=/^[0-9a-f]{20}$/;
const UUID_RE=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SAFE_KEY=/^[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}$/;
const SHA256=/^[0-9a-f]{64}$/;
const STATUS=new Set(['queued','pending','planned','partial','waiting','running','done','complete','completed','ready','success','failed','error','unavailable','skipped','cancelled']);
const IMAGE_EXT=new Set(['png','jpg','jpeg','webp']);
const VIDEO_EXT=new Set(['mp4','webm']);

function fail(message){throw new Error(message)}
function finite(value,min=0,max=Number.MAX_SAFE_INTEGER){return typeof value==='number'&&Number.isFinite(value)&&value>=min&&value<=max}
function shortText(value,max=400){return typeof value==='string'?value.slice(0,max):''}
function plain(value){return value&&typeof value==='object'&&!Array.isArray(value)}

export function validateRunId(value){if(typeof value!=='string'||!RUN_RE.test(value))fail('Motion Transfer run id is invalid');return value}

export function trustedMtFileUrl(value,runId,origin,expectedPath=null){
  if(typeof value!=='string'||value.length>2048)fail('Motion Transfer file URL is invalid');
  let url;try{url=new URL(value,origin)}catch{fail('Motion Transfer file URL is invalid')}
  if(url.origin!==origin||url.username||url.password||url.search||url.hash)fail('Motion Transfer file URL is not same-origin');
  const prefix=`/api/mt/files/${runId}/`;
  let decoded;try{decoded=decodeURIComponent(url.pathname)}catch{fail('Motion Transfer file URL encoding is invalid')}
  if(!decoded.startsWith(prefix)||decoded.includes('..')||decoded.includes('\\')||/[\u0000-\u001f\u007f]/.test(decoded))fail('Motion Transfer file URL path is invalid');
  if(expectedPath!=null&&decoded!==prefix+expectedPath)fail('Motion Transfer file URL does not match its declared path');
  return url.pathname;
}

function fileType(path){const ext=(path.split('.').pop()||'').toLowerCase();if(IMAGE_EXT.has(ext))return'image';if(VIDEO_EXT.has(ext))return'video';if(ext==='glb')return'glb';if(ext==='json')return'json';if(ext==='npz')return'npz';return'binary'}
function fileRole(category,path){
  const base=path.split('/').pop()||path,stem=base.replace(/\.[^.]+$/,'');
  if(category==='proj'){const match=stem.match(/^(front|back|left|right|top|sheet)_(.+)$/);return match?{family:'projection',view:match[1],pass:match[2]}:{family:'projection-meta'}}
  if(category==='labels'){const parts=path.split('/');return{family:'labels',group:parts[1]||'',role:stem}}
  if(category==='motion'){const match=stem.match(/^(.+?)(?:_(front|back|left|right|raw|60))?$/);return{family:'motion',clip:match?.[1]||stem,view:match?.[2]||'main'}}
  if(category==='track'){const parts=path.split('/');return{family:'track',clip:parts[1]||'',role:stem}}
  if(category==='orbits')return{family:'orbit',name:stem};
  if(/pose/i.test(path))return{family:'pose',name:stem};
  if(/vehicle/i.test(path))return{family:'vehicle',name:stem};
  if(base==='phases.json')return{family:'phases'};
  return{family:category,name:stem};
}

export function normalizeMotionTransferRun(raw,{runId,origin}){
  validateRunId(runId);if(!plain(raw)||raw.run_id!==runId)fail('Motion Transfer response identity mismatch');
  const request=plain(raw.request)?raw.request:{},taskId=typeof request.task_id==='string'&&UUID_RE.test(request.task_id)?request.task_id:null;
  const stages=[];if(raw.stages!=null&&!plain(raw.stages))fail('Motion Transfer stages must be an object');
  for(const [name,value] of Object.entries(raw.stages||{})){
    if(stages.length>=160||!SAFE_KEY.test(name)||!plain(value))fail('Motion Transfer stage is invalid');
    const status=shortText(value.status,24).toLowerCase();if(!STATUS.has(status))fail(`Motion Transfer stage status is invalid: ${name}`);
    const metrics={};for(const key of ['reprojection_px_mean','reprojection_px_p95','midplane_height_spread_px_mean','midplane_height_spread_px_p95','faces_seen','skeleton_segments'])if(value[key]!=null&&finite(value[key],0,1e9))metrics[key]=value[key];
    if(plain(value.view_iou)){metrics.view_iou={};for(const [key,score] of Object.entries(value.view_iou))if(SAFE_KEY.test(key)&&finite(score,0,1))metrics.view_iou[key]=score}
    stages.push({name,status,seconds:finite(value.seconds,0,86400)?value.seconds:null,error:shortText(value.error||value.message,500),metrics});
  }
  const files=[];if(raw.outputs!=null&&!plain(raw.outputs))fail('Motion Transfer outputs must be an object');
  for(const [category,items] of Object.entries(raw.outputs||{})){
    if(!SAFE_KEY.test(category)||!plain(items))fail('Motion Transfer output category is invalid');
    for(const [path,urlValue] of Object.entries(items)){
      if(files.length>=2000||!SAFE_KEY.test(path)||path.startsWith('/')||path.includes('..')||path.includes('\\'))fail('Motion Transfer output path is invalid');
      const url=trustedMtFileUrl(urlValue,runId,origin,path);files.push({category,path,url,type:fileType(path),...fileRole(category,path)});
    }
  }
  const status=shortText(raw.status,24).toLowerCase();if(!STATUS.has(status))fail('Motion Transfer run status is invalid');
  return{schema:'autorig.v3.mt-view/1',runId,kind:shortText(raw.kind,32),status,taskId,createdAt:finite(raw.created_at,0)?raw.created_at:null,finishedAt:finite(raw.finished_at,0)?raw.finished_at:null,seconds:finite(raw.seconds,0,86400)?raw.seconds:null,error:shortText(raw.error,500),forwardAxis:shortText(raw.forward_axis,16),request:{size:finite(request.size,1,8192)?request.size:null,frames:finite(request.frames,1,100000)?request.frames:null,mode:shortText(request.mode,32),maps:Array.isArray(request.maps)?request.maps.filter(x=>typeof x==='string').slice(0,32):[],motions:Array.isArray(request.motions)?request.motions.filter(x=>typeof x==='string').slice(0,32):[]},stages,files};
}

export function validateProjectionManifest(raw,{taskId,sourceSha}){
  if(!plain(raw)||raw.schema!=='autorig.motion-transfer.projections/1')fail('Projection manifest schema is invalid');
  if(taskId&&raw.task_id!==taskId)fail('Projection manifest task mismatch');
  if(typeof raw.glb_sha256!=='string'||!SHA256.test(raw.glb_sha256))fail('Projection source SHA-256 is invalid');
  if(sourceSha&&raw.glb_sha256!==sourceSha)fail('Projection source SHA-256 mismatch');
  const views=Array.isArray(raw.views)?raw.views.filter(x=>typeof x==='string'&&SAFE_KEY.test(x)).slice(0,16):[],passes=Array.isArray(raw.passes)?raw.passes.filter(x=>typeof x==='string'&&SAFE_KEY.test(x)).slice(0,32):[];
  return{taskId:raw.task_id,sourceSha:raw.glb_sha256,vertices:finite(raw.vertices,0,2e7)?raw.vertices:null,triangles:finite(raw.triangles,0,4e7)?raw.triangles:null,forwardAxis:shortText(raw.forward_axis,16),views,passes,seconds:finite(raw.seconds,0,86400)?raw.seconds:null};
}

function vec3(value){return Array.isArray(value)&&value.length===3&&value.every(x=>finite(x,-1e9,1e9))}
export function validateJoints3d(raw){
  if(!plain(raw)||raw.schema!=='autorig.motion-transfer.joints3d/1'||!finite(raw.fps,.01,240)||!Number.isInteger(raw.frames)||raw.frames<1||raw.frames>10000||raw.frame!=='glTF model frame (+Y up), same units as the model')fail('3D track header is invalid');
  if(!Array.isArray(raw.joints)||raw.joints.length<1||raw.joints.length>1000||raw.joints.some(x=>!vec3(x)))fail('3D track rest joints are invalid');
  if(raw.frames*raw.joints.length>1000000)fail('3D track aggregate point count is too large');
  if(!Array.isArray(raw.positions)||raw.positions.length!==raw.frames||raw.positions.some(frame=>!Array.isArray(frame)||frame.length!==raw.joints.length||frame.some(x=>!vec3(x))))fail('3D track positions are invalid');
  if(!Array.isArray(raw.valid)||raw.valid.length!==raw.frames||raw.valid.some(frame=>!Array.isArray(frame)||frame.length!==raw.joints.length||frame.some(x=>typeof x!=='boolean'))||!Array.isArray(raw.bones)||raw.bones.length>3000||raw.bones.some(edge=>!Array.isArray(edge)||edge.length!==2||edge.some(i=>!Number.isInteger(i)||i<0||i>=raw.joints.length)||edge[0]===edge[1]))fail('3D track validity or bones are invalid');
  return{fps:raw.fps,frames:raw.frames,frame:shortText(raw.frame,160),joints:raw.joints,positions:raw.positions,valid:raw.valid,bones:raw.bones,reprojectionMean:finite(raw.reprojection_px_mean,0,1e6)?raw.reprojection_px_mean:null,reprojectionP95:finite(raw.reprojection_px_p95,0,1e6)?raw.reprojection_px_p95:null};
}

export function normalizeLabelLegend(raw){
  if(!plain(raw)||raw.schema!=='autorig.motion-transfer.labels/1'||!Array.isArray(raw.legend)||raw.legend.length>256)fail('Label legend is invalid');
  const legend=raw.legend.map(item=>{if(!plain(item)||!Number.isInteger(item.index)||item.index<0||typeof item.name!=='string'||!Array.isArray(item.rgb)||item.rgb.length!==3||item.rgb.some(x=>!Number.isInteger(x)||x<0||x>255))fail('Label entry is invalid');return{index:item.index,name:item.name.slice(0,100),rgb:item.rgb}});
  const iou={};if(plain(raw.view_iou))for(const [view,value] of Object.entries(raw.view_iou))if(SAFE_KEY.test(view)&&finite(value,0,1))iou[view]=value;
  return{legend,faces:finite(raw.faces,0,1e9)?raw.faces:null,facesSeen:finite(raw.faces_seen,0,1e9)?raw.faces_seen:null,facesFilled:finite(raw.faces_filled,0,1e9)?raw.faces_filled:null,viewIou:iou,labelShare:plain(raw.label_share)?raw.label_share:{}};
}

function boundedValue(value,depth=0){
  if(depth>3)return'[depth limit]';
  if(value==null||typeof value==='boolean'||finite(value,-1e12,1e12))return value;
  if(typeof value==='string')return value.slice(0,2000);
  if(Array.isArray(value))return value.slice(0,64).map(item=>boundedValue(item,depth+1));
  if(plain(value)){const out={};for(const [key,item] of Object.entries(value).slice(0,64))if(SAFE_KEY.test(key))out[key]=boundedValue(item,depth+1);return out}
  return String(value).slice(0,200);
}
export function normalizePhases(raw,runId){
  if(!plain(raw)||raw.schema!=='autorig.mt.phases/1'||raw.run_id!==runId||!Array.isArray(raw.phases)||raw.phases.length>128)fail('Motion Transfer phases are invalid');
  return{status:shortText(raw.status,24),title:shortText(raw.title,160),taskId:UUID_RE.test(raw.task_id||'')?raw.task_id:null,phases:raw.phases.map(value=>{if(!plain(value))fail('Motion Transfer phase is invalid');return{id:shortText(value.id,64),title:shortText(value.title,160),status:shortText(value.status,24),seconds:finite(value.seconds,0,86400)?value.seconds:null,error:shortText(value.error,400),question:shortText(value.question,2000),answer:shortText(value.answer,2000),result:boundedValue(value.result),media:boundedValue(value.media),dialogue:boundedValue(value.dialogue),options:boundedValue(value.options)}})};
}

