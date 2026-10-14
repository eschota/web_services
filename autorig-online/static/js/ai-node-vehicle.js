/**
 * Vehicle image pipeline for /nodes.
 *
 * The server owns every result. This module only visualises fields returned by
 * /api/vehicle/runs and never fabricates an analysis, mask, mesh or stage.
 */
(function (global) {
  'use strict';

  const API = '/api/vehicle/runs';
  const POLL_MS = 1800;
  const TERMINAL = new Set(['done', 'failed', 'cancelled', 'canceled', 'blocked', 'needs_review']);
  const STOPPED = new Set(['failed', 'cancelled', 'canceled', 'blocked', 'needs_review']);
  const STAGES = [
    ['input', 'vehicle_stage_input', 'Input'],
    ['analyze', 'vehicle_stage_analysis', 'Vision · category · dimensions'],
    ['prepare', 'vehicle_stage_camera_prepare', 'Camera prep · 45° yaw / 45° pitch'],
    ['remove_wheels', 'vehicle_stage_remove_wheels', 'Qwen · clean vehicle body'],
    ['project_views', 'vehicle_stage_project_views', 'Four orthographic views'],
    ['depth_views', 'vehicle_stage_depth_views', 'Depth for every view'],
    ['fuse_voxels', 'vehicle_stage_fuse_voxels', 'Fuse voxels · symmetry'],
    ['cluster', 'vehicle_stage_segmentation', 'Objects · materials'],
    ['model', 'vehicle_stage_parts', 'Part choices · modelling'],
    ['rig', 'vehicle_stage_rig', 'Vehicle rig'],
    ['configure', 'vehicle_stage_physics', 'Wheel physics']
  ];

  let panel = null;
  let sourceUrl = '';
  let runId = '';
  let timer = 0;
  let run = null;
  let selectedPart = '';
  let activeTab = 'original';
  let tabTouched = false;
  let graphActions = null;

  function tr(key, fallback, vars) {
    if (global.I18n && typeof global.I18n.t === 'function') {
      const value = global.I18n.t(key, vars || {});
      if (value && value !== key) return value;
    }
    return fallback.replace(/\{(\w+)\}/g, (_, name) => String((vars || {})[name] ?? ''));
  }

  function stringOf(value) { return value == null ? '' : String(value); }
  function first(object, keys, fallback) {
    for (const key of keys) if (object && object[key] != null && object[key] !== '') return object[key];
    return fallback;
  }
  function listOf(object, keys) {
    const value = first(object, keys, []);
    return Array.isArray(value) ? value : [];
  }
  function urlOf(value) {
    const raw = stringOf(value).trim();
    if (!raw) return '';
    try {
      const parsed = new URL(raw, global.location.origin);
      return ['http:', 'https:', 'blob:'].includes(parsed.protocol) ? parsed.href : '';
    } catch (_) { return ''; }
  }

  function viewerHandoff(value) {
    const viewer=viewerFrameUrl(value);
    const wheelRig=urlOf(first(value && value.artifacts,['wheel_rig_url'],''));
    return viewer && wheelRig ? viewer : '';
  }

  function viewerFrameUrl(value) {
    const viewer=urlOf(first(value,['preview_viewer_url','staged_viewer_url','viewer_url'],''));
    if (!viewer) return '';
    const parsed=new URL(viewer);
    return parsed.origin === global.location.origin && parsed.pathname.startsWith('/api/mt/unity/') ? parsed.href : '';
  }

  function statusOf(value) {
    return stringOf(first(value, ['status_string', 'status'], 'queued')).toLowerCase();
  }

  function runUrl(id) { return API + '/' + encodeURIComponent(id); }
  function runIdFrom(value) {
    const raw = stringOf(value).trim();
    const match = /\/api\/vehicle\/runs\/([^/?#]+)/.exec(raw);
    return match ? decodeURIComponent(match[1]) : raw;
  }

  async function requestJson(url, options) {
    return fetch(url, Object.assign({credentials:'same-origin'}, options || {})).then(jsonResponse);
  }

  function runMessage(value) {
    const raw=first(value, ['error', 'review_reason', 'blocked_reason', 'reason'], '');
    if (!raw) return '';
    if (typeof raw === 'object') {
      const message=first(raw,['localized_message','message','message_string','error_string','reason'], '');
      if (message) return stringOf(message);
      const code=stringOf(first(raw,['code','code_string'],'')).trim();
      return code ? tr('vehicle_error_' + code, code.replace(/_/g,' ')) : tr('vehicle_error_unknown','Vehicle stage needs attention.');
    }
    return stringOf(raw);
  }

  function completionState(value, target) {
    if (!value) return {state:'wait', reason:''};
    const runStatus=statusOf(value);
    const stages = stageMap(value);
    const stage = stages.get(target);
    const artifacts=value.artifacts || {};
    const voxelReviewReady=target === 'voxelize' && runStatus === 'needs_review' && stage &&
      ['done','completed','ready'].includes(statusOf(stage)) && artifacts.voxel_url && artifacts.voxel_preview_url;
    if (STOPPED.has(runStatus) && !voxelReviewReady) return {state:'stop', reason:runMessage(value) || tr('vehicle_status_' + runStatus, runStatus.replace(/_/g,' '))};
    if (stage && STOPPED.has(statusOf(stage)) && !voxelReviewReady) return {state:'stop', reason:runMessage(stage) || runMessage(value) || tr('vehicle_stage_stopped','Stage needs attention.')};
    if (!stage || !['done', 'completed', 'ready'].includes(statusOf(stage))) return {state:'wait', reason:''};
    if (target === 'cluster' && !artifacts.overlay_url) return {state:'wait', reason:''};
    if (target === 'prepare' && !value.prepared_image_url) return {state:'wait', reason:''};
    if (['project_views','depth_views','fuse_voxels'].includes(target) && !multiviewReady(value,target)) return {state:'wait', reason:''};
    if (target === 'remove_wheels' && !wheelRemovalReady(value)) return {state:'wait', reason:''};
    if (target === 'voxelize' && !(artifacts.voxel_url && artifacts.voxel_preview_url)) return {state:'wait', reason:''};
    if (target === 'model' && !artifacts.model_url) return {state:'wait', reason:''};
    return {state:'complete', reason:''};
  }

  async function pollGraphRun(accepted, runner, report) {
    let value = accepted._vehicle_run || accepted;
    const id = runIdFrom(first(value, ['id'], accepted.task_id_string));
    for (let attempt = 0; attempt < 900; attempt += 1) {
      const task=first(value,['active_task_object','farm_task_object'],{}) || {};
      if (report) report({
        status: statusOf(value), stage: first(value, ['stage'], runner.target),
        progress_number: first(value, ['progress'], 0),
        node_string:first(task,['node_string','worker_string'],''),
        task_id_string:first(task,['task_id_string','id_string'],'')
      });
      const decision=completionState(value, runner.target);
      if (decision.state === 'stop') {
        run=value; runId=id; render(); open(); setError(decision.reason);
        throw new Error(decision.reason);
      }
      if (decision.state === 'complete') {
        run = value; runId = id; render();
        const receipt=runUrl(id);
        const outputs={vehicle_url:receipt};
        if (value.prepared_image_url) outputs.prepared_image_url=value.prepared_image_url;
        return {value:receipt,outputs};
      }
      await new Promise(resolve => global.setTimeout(resolve, POLL_MS));
      value = await requestJson(runUrl(id));
    }
    throw new Error(tr('vehicle_run_timeout', 'Vehicle stage did not finish in time.'));
  }

  function graphRunner(serviceId) {
    const specs = {
      vehicle_analyze: {target:'analyze', action:''},
      vehicle_prepare: {target:'prepare', action:'prepare'},
      vehicle_remove_wheels: {target:'remove_wheels', action:'remove_wheels'},
      vehicle_project_views: {target:'project_views', action:'project_views'},
      vehicle_depth_views: {target:'depth_views', action:'depth_views'},
      vehicle_fuse_voxels: {target:'fuse_voxels', action:'fuse_voxels'},
      vehicle_cluster: {target:'cluster', action:'cluster'},
      vehicle_voxel: {target:'voxelize', action:'voxelize'},
      vehicle_model: {target:'model', action:'model'}
    };
    const spec = specs[serviceId];
    if (!spec) return null;
    return {
      api: API, field:'vehicle_url', type:'vehicle', target:spec.target,
      submit: async body => {
        let value;
        if (!spec.action) {
          value = await requestJson(API, {
            method:'POST', headers:{'Content-Type':'application/json'},
            body:JSON.stringify(buildAnalyzeRequest(body))
          });
        } else {
          const id = runIdFrom(first(body, ['vehicle', 'vehicle_url'], ''));
          if (!id) throw new Error(tr('vehicle_error_missing_input', 'Connect a vehicle run from the previous stage.'));
          value = await requestJson(runUrl(id) + '/actions', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body:JSON.stringify(buildActionRequest(serviceId, body))
          });
        }
        const id = stringOf(first(value, ['id'], ''));
        if (!id) throw new Error(tr('vehicle_error_missing_run', 'The server did not return a run id.'));
        return {task_id_string:id, vehicle_url:runUrl(id), _vehicle_run:value};
      },
      finish: pollGraphRun
    };
  }

  function buildActionRequest(serviceId, body) {
    const actions={vehicle_prepare:'prepare',vehicle_remove_wheels:'remove_wheels',vehicle_project_views:'project_views',
      vehicle_depth_views:'depth_views',vehicle_fuse_voxels:'fuse_voxels',vehicle_cluster:'cluster',vehicle_voxel:'voxelize',vehicle_model:'model'};
    const options={};
    return {
      action:actions[serviceId], part_ids:parsePartIds(body.part_ids),
      provider:first(body,['provider'],'blender'), polygon_budget:Number(first(body,['polygon_budget'],10000)),
      mode:first(body,['mode'],'split'), options
    };
  }

  function buildAnalyzeRequest(body) {
    return {image_url:first(body,['image_url','image'],''),stage:'analyze',options:{
      working_side:first(body,['working_side'],'auto'),resolution:Number(first(body,['resolution'],192)),
      length_m:first(body,['length_m'],null)
    }};
  }

  const VIEW_IDS=Object.freeze(['top','front','left','back']);
  function multiviewObject(value) { return first(value,['multiview_object','multiview'],{}) || {}; }
  function multiviewViews(value) {
    const object=first(value,['views_object'],null);
    if (object && typeof object==='object' && !Array.isArray(object)) return Object.values(object);
    return listOf(multiviewObject(value),['views_array']);
  }
  function providerTaskFor(value,stage) {
    return listOf(multiviewObject(value),['provider_tasks_array']).find(task=>
      task && task.schema_string==='autorig.vehicle.provider-task/1' &&
      stringOf(task.pipeline_stage_string).toLowerCase()===String(stage).toLowerCase() &&
      !!stringOf(task.task_id_string)) || null;
  }
  function viewId(view) { return stringOf(first(view,['id','id_string','view_id_string'],'')).toLowerCase(); }
  function exactViews(value) {
    const views=multiviewViews(value);
    return views.length===4 && VIEW_IDS.every(id=>views.filter(view=>viewId(view)===id).length===1);
  }
  function wheelRemovalReady(value) {
    const receipt=first(value,['body_cleanup_object'],{}) || {};
    const counts=first(receipt,['semantic_counts_object'],{}) || {};
    const chain=(value && value.source_sha_chain) || {};
    const sourceHash=stringOf(first(receipt,['source_sha256_string'],'')).toLowerCase();
    const outputHash=stringOf(first(receipt,['output_sha256_string'],'')).toLowerCase();
    return receipt.schema_string==='autorig.vehicle.body-cleanup/1' && receipt.contract_version_string==='body-cleanup-v1' &&
      ['complete','completed','done'].includes(statusOf(receipt)) && receipt.wheels_removed_bool===true &&
      receipt.equipment_removed_bool===true && receipt.cargo_removed_bool===true &&
      Number(counts.wheels_int)===0 && Number(counts.equipment_int)===0 && Number(counts.cargo_int)===0 &&
      /^[a-f0-9]{64}$/.test(sourceHash) && sourceHash===stringOf(chain.prepared_sha256).toLowerCase() &&
      /^[a-f0-9]{64}$/.test(outputHash) && !!urlOf(first(receipt,['image_url','image_url_string'],'') || first(value && value.artifacts,['body_cleanup_url','wheel_free_reference_url'],''));
  }
  function cleanupCandidateUrl(value) {
    const receipt=first(value,['body_cleanup_object'],{}) || {};
    const chain=(value && value.source_sha_chain) || {};
    const sourceHash=stringOf(first(receipt,['source_sha256_string'],'')).toLowerCase();
    const outputHash=stringOf(first(receipt,['output_sha256_string'],'')).toLowerCase();
    if (receipt.schema_string!=='autorig.vehicle.body-cleanup/1' || receipt.contract_version_string!=='body-cleanup-v1' ||
        !/^[a-f0-9]{64}$/.test(sourceHash) || sourceHash!==stringOf(chain.prepared_sha256).toLowerCase() ||
        !/^[a-f0-9]{64}$/.test(outputHash)) return '';
    return urlOf(first(receipt,['image_url','image_url_string'],'') || first(value && value.artifacts,['body_cleanup_url'],''));
  }
  function multiviewReady(value,target) {
    const mv=multiviewObject(value);
    if (mv.schema_string!=='autorig.vehicle.multiview/1' || !exactViews(value)) return false;
    const views=multiviewViews(value);
    const refs=views.every(view=>{
      const reference=first(view,['reference_object'],{}) || {};
      const camera=first(view,['camera_object'],{}) || {};
      return reference.wheel_free_bool===true && String(first(camera,['projection','projection_string'],'')).toLowerCase()==='orthographic';
    });
    if (!refs) return false;
    if (target==='project_views') return true;
    const depths=views.every(view=>{
      const depth=first(view,['depth_object'],{}) || {};
      return /^[a-f0-9]{64}$/i.test(stringOf(first(depth,['sha256','sha256_string','depth_sha256_string'],'')));
    });
    if (!depths) return false;
    if (target==='depth_views') return true;
    const fused=first(value,['fusion_object'],null) || first(mv,['fused_voxels_object'],{}) || {};
    const symmetry=first(fused,['symmetry_object'],{}) || {};
    const resolution=Number(first(fused,['resolution_int'],0));
    const count=Number(first(fused,['voxel_count_int'],0));
    const wanted=Number(first(value && value.options,['resolution'],0));
    return String(first(mv,['stage','stage_string'],'')).toLowerCase()==='fuse_voxels' &&
      ['complete','completed','done'].includes(String(first(mv,['status','status_string'],'')).toLowerCase()) &&
      Number(symmetry.pair_symmetry_error_m_float)===0 && count>0 && count<=150000 &&
      (!wanted || resolution===wanted) && String(first(fused,['coordinate_space_string'],'model_local_gltf'))==='model_local_gltf';
  }

  function parsePartIds(value) {
    if (Array.isArray(value)) return value.map(stringOf).map(item=>item.trim()).filter(Boolean);
    const text=stringOf(value).trim();
    if (!text) return [];
    if (text.startsWith('[')) {
      try { const parsed=JSON.parse(text); if (Array.isArray(parsed)) return parsed.map(stringOf).map(item=>item.trim()).filter(Boolean); }
      catch (_) { /* semicolon fallback below */ }
    }
    return text.split(/[;,\n]+/).map(item=>item.trim()).filter(Boolean);
  }

  function vehicleTemplate() {
    return {
      name:tr('vehicle_template_title','Vehicle V3 · image to voxels'),
      nodes:[
        {id:'vehicle_source',kind:'input',entity_type:'image',value:'',x:40,y:180},
        {id:'vehicle_analyze',kind:'service',service:'vehicle_analyze',x:360,y:180,params:{working_side:'auto',resolution:192}},
        {id:'vehicle_prepare',kind:'service',service:'vehicle_prepare',x:700,y:180,params:{}},
        {id:'vehicle_remove_wheels',kind:'service',service:'vehicle_remove_wheels',x:1040,y:180,params:{}},
        {id:'vehicle_project_views',kind:'service',service:'vehicle_project_views',x:1380,y:180,params:{}},
        {id:'vehicle_depth_views',kind:'service',service:'vehicle_depth_views',x:1720,y:180,params:{}},
        {id:'vehicle_fuse_voxels',kind:'service',service:'vehicle_fuse_voxels',x:2060,y:180,params:{}},
        {id:'vehicle_cluster',kind:'service',service:'vehicle_cluster',x:2400,y:180,params:{mode:'split'}}
      ],
      links:[
        {from:'vehicle_source',output:'value',to:'vehicle_analyze',input:'image'},
        {from:'vehicle_analyze',output:'vehicle_url',to:'vehicle_prepare',input:'vehicle'},
        {from:'vehicle_prepare',output:'vehicle_url',to:'vehicle_remove_wheels',input:'vehicle'},
        {from:'vehicle_remove_wheels',output:'vehicle_url',to:'vehicle_project_views',input:'vehicle'},
        {from:'vehicle_project_views',output:'vehicle_url',to:'vehicle_depth_views',input:'vehicle'},
        {from:'vehicle_depth_views',output:'vehicle_url',to:'vehicle_fuse_voxels',input:'vehicle'},
        {from:'vehicle_fuse_voxels',output:'vehicle_url',to:'vehicle_cluster',input:'vehicle'}
      ]
    };
  }

  function installGraphTemplate(options) {
    if (options && options.serverTemplatePresent) return;
    if (!options || !options.picker || !options.loadGraph || options.picker.querySelector('[data-vehicle-template]')) return;
    const button=document.createElement('button'); button.type='button'; button.className='tmpl'; button.dataset.vehicleTemplate='yes';
    const title=document.createElement('b'); title.textContent=tr('vehicle_template_title','Vehicle V3 · image to voxels');
    const summary=document.createElement('i'); summary.textContent=tr('vehicle_template_summary','Analyse, cluster and voxel-bake any wheeled vehicle image. Choose parts for modelling after masks are ready.');
    button.append(title,summary); button.addEventListener('click',()=>{ options.loadGraph(vehicleTemplate()); if(options.close) options.close(); open(); });
    options.picker.appendChild(button);
  }
  function stageMap(value) {
    const map = new Map();
    listOf(value, ['stages_array', 'stages']).forEach(stage => {
      const key = stringOf(first(stage, ['stage', 'stage_string', 'id_string', 'id', 'name_string'], ''));
      if (key) map.set(key, stage);
    });
    return map;
  }

  function addStyle() {
    if (document.getElementById('vehicle-nodes-style')) return;
    const style = document.createElement('style');
    style.id = 'vehicle-nodes-style';
    style.textContent = `
      .veh-open{border-color:#5dd6b7!important;color:#b9ffe9!important}
      .veh-panel{position:fixed;inset:0;z-index:40;display:grid;grid-template-rows:auto auto minmax(0,1fr);background:#080b12;border:0;box-shadow:none;overflow:hidden;color:#eaf3ff}
      .veh-panel[hidden]{display:none}.veh-head,.veh-controls{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:10px 14px;border-bottom:1px solid #ffffff16;background:#101522e8}.veh-head h2{font-size:16px;margin:0}.veh-head .grow{flex:1}.veh-close{font-size:20px}.veh-controls input[type=file]{max-width:270px}.veh-controls label{font-size:12px;color:#b8c5d6}.veh-controls select,.veh-controls button{background:#171d2d;color:#edf6ff;border:1px solid #41546c;border-radius:7px;padding:7px}.veh-controls button.primary{background:#166b59;border-color:#36cfa7}.veh-main{position:relative;min-height:0;overflow:hidden}.veh-left{position:absolute;inset:0;padding:0}.veh-right{position:absolute;z-index:6;right:14px;top:14px;width:min(350px,calc(100% - 28px));max-height:calc(100% - 28px);padding:12px;overflow:auto;border:1px solid #ffffff22;border-radius:12px;background:#101522e8;backdrop-filter:blur(10px);box-shadow:0 12px 36px #0009}.veh-right.collapsed{display:none}.veh-flow{position:absolute;z-index:4;left:12px;right:380px;top:10px;display:flex;align-items:stretch;gap:10px;padding:8px;overflow-x:auto;background:#0b101bd9;border-radius:10px}.veh-node{flex:0 0 135px;min-height:78px;padding:9px;border-radius:10px;border:1px solid #33445d;background:#181d2c;position:relative}.veh-node:not(:last-child):after{content:'›';position:absolute;right:-9px;top:30px;color:#617b9f;font-size:18px}.veh-node b{display:block;font-size:12px}.veh-node small{display:block;margin-top:7px;color:#91a2b8;overflow-wrap:anywhere}.veh-node.running{border-color:#f0b84d;box-shadow:0 0 18px #f0b84d22}.veh-node.done{border-color:#42cca1}.veh-node.failed,.veh-node.blocked,.veh-node.needs_review{border-color:#ff687c}.veh-tabs{position:absolute;z-index:5;left:14px;bottom:14px;display:flex;gap:6px;padding:6px;background:#0b101bd9;border-radius:9px}.veh-tab[aria-selected=true]{border-color:#53d0ad;background:#18332f}.veh-tab:disabled{opacity:.42}.veh-preview{position:absolute;inset:0;display:grid;place-items:center;background:#090c13;overflow:hidden}.veh-preview iframe{width:100%;height:100%;border:0;background:#080b11}.veh-image-stack{position:relative;display:inline-block;line-height:0;max-width:100%;max-height:100%}.veh-preview img{display:block;max-width:100vw;max-height:100%;object-fit:contain}.veh-preview .veh-overlay,.veh-bbox{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}.veh-preview .veh-overlay{opacity:.58}.veh-bbox{pointer-events:auto}.veh-bbox rect{fill:#4ff0ba20;stroke:#55ffd0;stroke-width:3;vector-effect:non-scaling-stroke;cursor:pointer;pointer-events:all}.veh-bbox rect.selected{fill:#55ffd044;stroke:#fff}.veh-reference-review{position:absolute;z-index:5;left:14px;bottom:70px;max-width:min(520px,calc(100% - 390px));line-height:1.4;background:#101522e8!important;backdrop-filter:blur(10px)}.veh-empty{color:#75869c;text-align:center;padding:30px}.veh-facts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px;margin:10px 0}.veh-fact,.veh-part{background:#171d2b;border:1px solid #2e3b50;border-radius:8px;padding:8px}.veh-fact b{display:block;font-size:11px;color:#8192a9}.veh-part{width:100%;text-align:left;color:#e8f2ff;margin:5px 0;cursor:pointer}.veh-part.active{border-color:#60d7ba;background:#18332f}.veh-part span,.veh-part small,.veh-reference-review small{display:block}.veh-part small{color:#94a6bc;margin-top:3px}.veh-part-actions{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:7px}.veh-error{color:#ff91a0}.veh-note{font-size:12px;color:#92a4ba}.veh-progress{height:5px;background:#222b3a;border-radius:5px;overflow:hidden;margin-top:5px}.veh-progress i{display:block;height:100%;background:#53d0ad}.veh-unavailable{opacity:.48}
      @media(max-width:900px){.veh-flow{right:12px}.veh-right{top:112px;max-height:calc(100% - 128px)}}
    `;
    document.head.appendChild(style);
  }

  function buildPanel() {
    panel = document.createElement('section');
    panel.className = 'veh-panel';
    panel.hidden = true;
    panel.setAttribute('aria-label', tr('vehicle_panel_label', 'Vehicle image pipeline'));
    panel.innerHTML = `
      <div class="veh-head"><h2></h2><span class="veh-state veh-note"></span><span class="grow"></span><button type="button" class="veh-drawer-toggle"></button><button type="button" class="veh-close" aria-label="×">×</button></div>
      <div class="veh-controls">
        <input class="veh-file" type="file" accept="image/png,image/jpeg,image/webp">
        <label><span class="veh-mode-label"></span> <select class="veh-mode"><option value="body"></option><option value="split" selected></option></select></label>
        <button type="button" class="veh-start primary"></button>
        <span class="veh-api-note veh-note"></span>
      </div>
      <div class="veh-main"><div class="veh-left"><div class="veh-flow"></div><div class="veh-tabs" role="tablist"></div><div class="veh-preview"><div class="veh-empty"></div></div></div><aside class="veh-right"></aside></div>`;
    panel.querySelector('h2').textContent = tr('vehicle_panel_title', 'Vehicle V3 pipeline');
    panel.querySelector('.veh-mode-label').textContent = tr('vehicle_mode_label', 'Model');
    panel.querySelector('.veh-mode option[value=body]').textContent = tr('vehicle_mode_body', 'Body only');
    panel.querySelector('.veh-mode option[value=split]').textContent = tr('vehicle_mode_split', 'Body + recognised parts');
    panel.querySelector('.veh-start').textContent = tr('vehicle_start', 'Analyse image');
    panel.querySelector('.veh-drawer-toggle').textContent = tr('vehicle_details_toggle', 'Details');
    panel.querySelector('.veh-api-note').textContent = tr('vehicle_free_test_note', 'Free vehicle preview');
    panel.querySelector('.veh-empty').textContent = tr('vehicle_choose_image', 'Choose a vehicle image to start.');
    document.body.appendChild(panel);
    panel.querySelector('.veh-close').addEventListener('click', close);
    panel.querySelector('.veh-drawer-toggle').addEventListener('click',()=>panel.querySelector('.veh-right').classList.toggle('collapsed'));
    panel.querySelector('.veh-file').addEventListener('change', chooseFile);
    panel.querySelector('.veh-start').addEventListener('click', start);
    render();
  }

  function installButton() {
    const bar = document.querySelector('.bar');
    const anchor = document.getElementById('run');
    if (!bar || !anchor || document.getElementById('vehicle-pipeline-open')) return;
    const button = document.createElement('button');
    button.type = 'button';
    button.id = 'vehicle-pipeline-open';
    button.className = 'veh-open';
    button.textContent = tr('vehicle_open', 'Vehicle pipeline');
    button.addEventListener('click', open);
    bar.insertBefore(button, anchor);
  }

  function open() { panel.hidden = false; document.body.style.overflow = 'hidden'; }
  function close() { panel.hidden = true; document.body.style.overflow = ''; }

  async function openRun(value) {
    const id=runIdFrom(value);
    if (!id) return;
    open(); setError('');
    try { run=await requestJson(runUrl(id)); runId=id; render(); if(!TERMINAL.has(statusOf(run))) poll(); }
    catch(error) { setError(error.message || String(error)); }
  }

  function chooseFile(event) {
    const file = event.target.files && event.target.files[0];
    if (sourceUrl) URL.revokeObjectURL(sourceUrl);
    sourceUrl = file ? URL.createObjectURL(file) : '';
    runId = ''; run = null; selectedPart = ''; activeTab='original'; tabTouched=false;
    stopPolling(); render();
  }

  async function jsonResponse(response) {
    let data = null;
    try { data = await response.json(); } catch (_) { data = {}; }
    if (!response.ok) {
      const raw=first(data,['detail','error','error_string','message_string'],response.status + ' ' + response.statusText);
      const message=typeof raw === 'object' ? runMessage({error:raw}) : raw;
      throw new Error(stringOf(message));
    }
    return data || {};
  }

  async function start() {
    const input = panel.querySelector('.veh-file');
    const file = input.files && input.files[0];
    if (!file) { setError(tr('vehicle_error_image_required', 'Choose an image first.')); return; }
    if (!graphActions || !graphActions.startImage) { setError(tr('vehicle_graph_unavailable','The vehicle graph is not ready.')); return; }
    setBusy(true); setError('');
    try { await graphActions.startImage({file,mode:panel.querySelector('.veh-mode').value}); }
    catch (error) { setError(error.message || String(error)); }
    finally { setBusy(false); }
  }

  async function poll() {
    stopPolling();
    if (!runId) return;
    try {
      const data = await fetch(runUrl(runId), {credentials:'same-origin'}).then(jsonResponse);
      run = first(data, ['run_object', 'run'], data);
      render();
      if (!TERMINAL.has(statusOf(run))) timer = global.setTimeout(poll, POLL_MS);
    } catch (error) {
      setError(error.message || String(error));
      timer = global.setTimeout(poll, Math.min(8000, POLL_MS * 2));
    }
  }
  function stopPolling() { if (timer) global.clearTimeout(timer); timer = 0; }

  async function action(payload, button) {
    if (!runId) return null;
    if (button) button.disabled = true;
    setError('');
    try {
      const data = await fetch(runUrl(runId) + '/actions', {
        method:'POST', credentials:'same-origin', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)
      }).then(jsonResponse);
      run = first(data, ['run_object', 'run'], run);
      render(); poll();
      return run;
    } catch (error) { setError(error.message || String(error)); return null; }
    finally { if (button && document.contains(button)) button.disabled = false; }
  }

  function setBusy(value) { if (panel) panel.querySelector('.veh-start').disabled = !!value; }
  function setError(message) {
    if (!panel) return;
    const state = panel.querySelector('.veh-state');
    state.textContent = message || (run ? tr('vehicle_run_status', 'Run {id} · {status}', {id:runId, status:statusOf(run)}) : '');
    state.classList.toggle('veh-error', !!message);
  }

  function percentOf(stage) {
    const number = Number(first(stage, ['progress_float', 'progress_number', 'progress_int'], 0));
    return Math.max(0, Math.min(100, number <= 1 ? number * 100 : number));
  }

  function renderFlow() {
    const host = panel.querySelector('.veh-flow');
    host.replaceChildren();
    const stages = stageMap(run);
    STAGES.forEach(([id, key, fallback]) => {
      const stage = stages.get(id) || null;
      const state = stage ? statusOf(stage) : (id === 'input' && (run || sourceUrl) ? 'done' : 'queued');
      const node = document.createElement('div');
      node.className = 'veh-node ' + state;
      const title = document.createElement('b'); title.textContent = tr(key, fallback);
      const detail = document.createElement('small');
      const farmTask=(stage && first(stage,['task_object','farm_task_object','active_task_object'],null)) || providerTaskFor(run,id);
      detail.textContent = farmTask
        ? tr('vehicle_farm_task_status','Farm · {status}',{status:stringOf(first(farmTask,['status','status_string'],'queued'))}) +
          (first(farmTask,['node_string','worker_string'],'') ? ' · '+stringOf(first(farmTask,['node_string','worker_string'],'')) : '')
        : stage ? stringOf(first(stage, ['summary_string', 'message_string', 'status_string','status'], state)) : tr('vehicle_stage_waiting', 'Waiting');
      node.append(title, detail);
      if (stage && percentOf(stage)) {
        const progress = document.createElement('div'); progress.className = 'veh-progress';
        const fill = document.createElement('i'); fill.style.width = percentOf(stage) + '%'; progress.appendChild(fill); node.appendChild(progress);
      }
      host.appendChild(node);
    });
  }

  function selectedPartObject(parts) {
    return parts.find(part => stringOf(first(part, ['part_id_string', 'id_string', 'id'], '')) === selectedPart) || parts[0] || null;
  }

  function partCanModel(part) { return !!part && part.modeling_ready === true; }

  function boundaryEvidence(value, part) {
    const id=stringOf(first(part,['part_id_string','id_string','id'],'')).trim();
    const role=stringOf(first(part,['image_role'],'')).toLowerCase();
    const bbox=first(part,['source_bbox_px_array','bbox_px_array','bbox_xyxy'],null);
    const chain=(value && value.source_sha_chain) || {};
    const camera=(value && value.camera) || {};
    const sha=item=>/^[a-f0-9]{64}$/i.test(stringOf(item));
    const positive=Array.isArray(bbox) && bbox.length===4 && Number(bbox[2])>Number(bbox[0]) && Number(bbox[3])>Number(bbox[1]);
    const canonical=['prepared','canonical','prepared45'].includes(role);
    const semantic=[part.name,part.name_string,part.category,part.category_string,part.object_class,part.object_class_string].map(stringOf).join(' ').toLowerCase();
    const referenceObject=part.reference_only_bool === true || part.vehicle_part_bool === false ||
      /\b(person|people|human|driver|passenger|background|reference)\b/.test(semantic);
    const exactCamera=camera.normalization_status_string === 'accepted' && camera.projection_string === 'orthographic' &&
      Math.abs(Number(camera.yaw_deg_float)-45)<=.5 && Math.abs(Number(camera.pitch_deg_float)-45)<=.5;
    if (!id || !canonical || referenceObject || !positive || !part.mask_url || !exactCamera ||
        !sha(chain.original_sha256) || !sha(chain.prepared_sha256) || !sha(part.mask_sha256)) return null;
    return {part_id:id,original_sha256:chain.original_sha256,prepared_sha256:chain.prepared_sha256,mask_sha256:part.mask_sha256};
  }

  function providerSupported(value, provider) {
    return listOf(value,['supported_model_providers']).map(stringOf).includes(provider);
  }

  function referenceEvidence(value) {
    const chain=(value && value.source_sha_chain) || {};
    const sha=item=>/^[a-f0-9]{64}$/i.test(stringOf(item));
    if (!value || !value.prepared_image_url || !sha(chain.original_sha256) || !sha(chain.prepared_sha256)) return null;
    return {original_sha256:chain.original_sha256,prepared_sha256:chain.prepared_sha256};
  }

  function tabAvailability(parts) {
    const artifacts=(run && run.artifacts) || {};
    const frame=viewerFrameUrl(run);
    const available={
      original:true,
      prepared:!!urlOf(first(run,['prepared_image_url'],'')),
      clean:!!cleanupCandidateUrl(run),
      parts:parts.length > 0,
      voxels:!!(frame && artifacts.voxel_url && artifacts.voxel_preview_url),
      drive:!!(frame && artifacts.wheel_rig_url)
    };
    multiviewViews(run).forEach(view=>{ if (viewReferenceUrl(view)) available['view_' + viewId(view)]=true; });
    return available;
  }

  function viewReferenceUrl(view) {
    const reference=first(view,['reference_object'],{}) || {};
    return urlOf(first(reference,['image_url','image_url_string','url','url_string'],'') || first(reference,['path_url_string'],''));
  }

  function renderTabs(parts) {
    const host=panel.querySelector('.veh-tabs'); host.replaceChildren();
    const available=tabAvailability(parts);
    if (!available[activeTab]) activeTab='original';
    const tabs=[
      ['original','vehicle_tab_original','Original'],
      ['prepared','vehicle_tab_prepared','Reference 45°'],
      ['clean','vehicle_tab_clean_body','Clean body'],
      ['parts','vehicle_tab_parts','Parts'],
      ['voxels','vehicle_tab_voxels','Voxels'],
      ['drive','vehicle_tab_drive','Drive']
    ];
    const viewLabels={top:['vehicle_view_top','Top'],front:['vehicle_view_front','Front'],left:['vehicle_view_left','Left'],back:['vehicle_view_back','Back']};
    VIEW_IDS.forEach(id=>{ if(available['view_'+id]) tabs.splice(tabs.length-2,0,['view_'+id,...viewLabels[id]]); });
    tabs.forEach(([id,key,label])=>{
      const button=document.createElement('button'); button.type='button'; button.className='veh-tab';
      button.setAttribute('role','tab'); button.setAttribute('aria-selected',String(activeTab===id));
      button.disabled=!available[id]; button.textContent=tr(key,label);
      if (id === 'drive' && button.disabled && run && run.drive_blocker_string) {
        button.title=tr('vehicle_drive_blocked','Drive unavailable: {reason}',{reason:stringOf(run.drive_blocker_string)});
      }
      button.addEventListener('click',()=>{ activeTab=id; tabTouched=true; render(); }); host.appendChild(button);
    });
  }

  function renderPreview(parts) {
    const host = panel.querySelector('.veh-preview');
    host.replaceChildren();
    if (activeTab === 'voxels' || activeTab === 'drive') {
      const viewer=viewerFrameUrl(run);
      if (viewer) {
        const frame=document.createElement('iframe'); frame.src=viewer;
        frame.title=activeTab === 'drive' ? tr('vehicle_drive_frame_title','Vehicle driving preview') : tr('vehicle_voxel_frame_title','Vehicle voxel preview');
        frame.allow='fullscreen; gamepad'; frame.setAttribute('allowfullscreen','');
        host.appendChild(frame);
        if (activeTab==='voxels' && multiviewObject(run).schema_string!=='autorig.vehicle.multiview/1') {
          const note=document.createElement('div'); note.className='veh-reference-review veh-fact veh-error';
          note.textContent=tr('vehicle_single_view_diagnostic','Single-view diagnostic · not multiview'); host.appendChild(note);
        }
        return;
      }
      const empty=document.createElement('div'); empty.className='veh-empty';
      empty.textContent=tr('vehicle_viewer_waiting','The live 3D preview is not ready yet.'); host.appendChild(empty); return;
    }
    if (activeTab.startsWith('view_')) {
      const id=activeTab.slice(5);
      const view=multiviewViews(run).find(item=>viewId(item)===id);
      const source=viewReferenceUrl(view);
      if (source) {
        const stack=document.createElement('div'); stack.className='veh-image-stack';
        const image=document.createElement('img'); image.src=source; image.alt=tr('vehicle_wheel_free_source','Wheel-free reference'); stack.appendChild(image); host.appendChild(stack);
        const camera=first(view,['camera_object'],{}) || {};
        const note=document.createElement('div'); note.className='veh-reference-review veh-fact';
        const title=document.createElement('b'); title.textContent=tr('vehicle_view_camera','{view} · orthographic camera',{view:tr('vehicle_view_'+id,id)}); note.appendChild(title);
        const depth=first(view,['depth_object'],{}) || {};
        if (/^[a-f0-9]{64}$/i.test(stringOf(first(depth,['sha256','sha256_string','depth_sha256_string'],'')))) {
          const ready=document.createElement('small'); ready.textContent=tr('vehicle_view_depth_ready','Depth verified'); note.appendChild(ready);
        }
        const scale=first(camera,['orthographic_scale_m_float','orthographic_scale_m'],'');
        if (scale!=='') { const detail=document.createElement('small'); detail.textContent=String(scale)+' m'; note.appendChild(detail); }
        host.appendChild(note); return;
      }
    }
    const original = urlOf(first(run, ['original_url','source_url'], '')) || sourceUrl;
    const prepared = urlOf(first(run, ['prepared_image_url'], ''));
    const clean = cleanupCandidateUrl(run);
    const chosenPart=activeTab === 'parts' ? selectedPartObject(parts) : null;
    const imageRole=stringOf(first(chosenPart,['image_role'],'')).toLowerCase();
    const roleSource=imageRole === 'original' ? original : ['prepared','canonical','prepared45'].includes(imageRole) ? prepared :
      ['clean','body_cleanup','clean_body'].includes(imageRole) ? clean : '';
    const source=activeTab === 'prepared' ? prepared : activeTab === 'clean' ? clean :
      activeTab === 'parts' ? (roleSource || clean || prepared || original) : original;
    if (!source) {
      const empty = document.createElement('div'); empty.className = 'veh-empty'; empty.textContent = tr('vehicle_choose_image', 'Choose a vehicle image to start.'); host.appendChild(empty); return;
    }
    const stack=document.createElement('div'); stack.className='veh-image-stack';
    const image = document.createElement('img'); image.alt = tr('vehicle_source_alt', 'Vehicle source'); image.src = source; stack.appendChild(image); host.appendChild(stack);
    const part = chosenPart;
    const roleRegistered=!!(part && roleSource);
    const overlay = roleRegistered ? urlOf(first(part, ['mask_url_string', 'overlay_url_string', 'mask_url'], '')) : '';
    if (overlay) {
      const mask = document.createElement('img'); mask.className = 'veh-overlay'; mask.alt = tr('vehicle_mask_alt', 'Selected object mask'); mask.src = overlay;
      const validateMask=()=>{ if(image.naturalWidth && mask.naturalWidth && (mask.naturalWidth!==image.naturalWidth || mask.naturalHeight!==image.naturalHeight)) mask.hidden=true; };
      mask.addEventListener('load',validateMask); image.addEventListener('load',validateMask);
      stack.appendChild(mask);
    }
    const boxRole=imageRole || (activeTab === 'parts' && clean ? 'clean' : activeTab === 'parts' && prepared ? 'prepared' : activeTab);
    const boxParts=activeTab === 'parts' ? parts.filter(item=>{
      const role=stringOf(first(item,['image_role'],'')).toLowerCase();
      return role === boxRole || (boxRole === 'prepared' && ['canonical','prepared45'].includes(role)) ||
        (boxRole === 'clean' && ['body_cleanup','clean_body'].includes(role));
    }) : [];
    if (boxParts.length) {
      const mark=document.createElementNS('http://www.w3.org/2000/svg','svg'); mark.classList.add('veh-bbox');
      const drawBoxes=()=>{
        const width=image.naturalWidth || 1, height=image.naturalHeight || 1;
        mark.setAttribute('viewBox',`0 0 ${width} ${height}`);
        mark.replaceChildren();
        boxParts.forEach(item=>{
          const bbox=first(item,['source_bbox_px_array','bbox_px_array','bbox_xyxy'],null);
          if (!Array.isArray(bbox) || bbox.length!==4) return;
          const id=stringOf(first(item,['part_id_string','id_string','id'],''));
          const rect=document.createElementNS('http://www.w3.org/2000/svg','rect');
          rect.setAttribute('x',String(Number(bbox[0])||0)); rect.setAttribute('y',String(Number(bbox[1])||0));
          rect.setAttribute('width',String(Math.max(0,(Number(bbox[2])||0)-(Number(bbox[0])||0))));
          rect.setAttribute('height',String(Math.max(0,(Number(bbox[3])||0)-(Number(bbox[1])||0))));
          if (id === selectedPart) rect.classList.add('selected');
          rect.setAttribute('aria-label',stringOf(first(item,['name_string','name'],id)));
          rect.addEventListener('click',event=>{ event.stopPropagation(); selectedPart=id; render(); });
          mark.appendChild(rect);
        });
      };
      image.addEventListener('load',drawBoxes);
      if (image.complete) drawBoxes();
      stack.appendChild(mark);
    }
    if (activeTab === 'prepared' && run) renderReferenceReview(host);
  }

  function renderReferenceReview(host) {
    const camera=run.camera || {};
    const review=document.createElement('div'); review.className='veh-reference-review veh-fact';
    const title=document.createElement('b'); title.textContent=tr('vehicle_camera_estimate','Camera estimate'); review.appendChild(title);
    const estimate=document.createElement('span');
    estimate.textContent=[camera.projection_string,
      Number.isFinite(Number(camera.yaw_deg_float)) ? tr('vehicle_camera_yaw','yaw {value}°',{value:Number(camera.yaw_deg_float).toFixed(1)}) : '',
      Number.isFinite(Number(camera.pitch_deg_float)) ? tr('vehicle_camera_pitch','pitch {value}°',{value:Number(camera.pitch_deg_float).toFixed(1)}) : '',
      tr('vehicle_camera_attempt','attempt {value}/3',{value:Number(run.camera_attempt_int || 0)})].filter(Boolean).join(' · ');
    review.appendChild(estimate);
    const limitations=listOf(camera,['limitations_array','warnings_array']).concat(listOf(run.analysis || {},['limitations_array']));
    if (limitations.length) { const note=document.createElement('small'); note.textContent=tr('vehicle_reference_limitations','Limitations: {value}',{value:limitations.map(stringOf).join('; ')}); review.appendChild(note); }
    if (run.reference_accepted_bool) { const accepted=document.createElement('small'); accepted.textContent=tr('vehicle_reference_accepted','Prepared reference accepted'); review.appendChild(accepted); }
    if (statusOf(run)==='needs_review' && stringOf(run.stage)==='prepare') {
      const actions=document.createElement('div'); actions.className='veh-part-actions';
      if (run.prepare_retry_available_bool) {
        const retry=document.createElement('button'); retry.type='button'; retry.textContent=tr('vehicle_retry_prepare','Retry camera preparation');
        retry.addEventListener('click',()=>action({action:'retry',part_ids:[],provider:'blender',polygon_budget:10000,
          mode:'split',options:{},expected_revision:Number(run.revision)},retry)); actions.appendChild(retry);
      }
      const accept=document.createElement('button'); accept.type='button'; accept.className='primary';
      accept.textContent=tr('vehicle_accept_reference','Use this reference');
      const evidence=referenceEvidence(run);
      accept.disabled=!evidence;
      if (!evidence) accept.title=tr('vehicle_accept_reference_waiting','Reference acceptance is not available yet.');
      accept.addEventListener('click',async()=>{
        const updated=await action({
          action:'accept_reference',reference_evidence:evidence,expected_revision:Number(run.revision),
          part_ids:[],provider:'blender',polygon_budget:10000,mode:'split',options:{}
        },accept);
        if (updated && updated.reference_accepted_bool && graphActions && graphActions.acceptReference) {
          await graphActions.acceptReference({vehicleUrl:runUrl(runId),preparedImageUrl:updated.prepared_image_url || ''});
        }
      });
      actions.appendChild(accept); review.appendChild(actions);
    }
    host.appendChild(review);
  }

  function addFact(host, label, value) {
    if (value == null || value === '') return;
    const fact = document.createElement('div'); fact.className = 'veh-fact';
    const name = document.createElement('b'); name.textContent = label;
    const text = document.createElement('span'); text.textContent = stringOf(value);
    fact.append(name, text); host.appendChild(fact);
  }

  function renderPart(part, host) {
    const id = stringOf(first(part, ['part_id_string', 'id_string', 'id'], ''));
    const button = document.createElement('button'); button.type = 'button'; button.className = 'veh-part' + (id === selectedPart ? ' active' : '');
    const title = document.createElement('span'); title.textContent = stringOf(first(part, ['name_string', 'label_string', 'name'], tr('vehicle_part_unnamed', 'Recognised part')));
    const detail = document.createElement('small');
    const material = stringOf(first(part, ['material_string', 'material_name_string', 'material'], ''));
    const imageRole = stringOf(first(part, ['image_role'], ''));
    const confidence = Number(first(part, ['confidence_float', 'confidence_number', 'confidence'], NaN));
    detail.textContent = [material, imageRole ? tr('vehicle_part_reference_role','Reference: {role}',{role:imageRole}) : '', Number.isFinite(confidence) ? Math.round(confidence * (confidence <= 1 ? 100 : 1)) + '%' : ''].filter(Boolean).join(' · ');
    button.append(title, detail); button.addEventListener('click', () => { selectedPart = id; activeTab='parts'; render(); }); host.appendChild(button);
    if (id !== selectedPart) return;
    const controls = document.createElement('div'); controls.className = 'veh-part-actions';
    const provider = document.createElement('select'); provider.setAttribute('aria-label', tr('vehicle_provider', 'Provider'));
    [
      ['blender',tr('vehicle_provider_blender','Blender CLI')],
      ['hunyuan',tr('vehicle_provider_hunyuan','Hunyuan')],
      ['tripo',tr('vehicle_provider_tripo_deferred','Tripo · unavailable in this preview')]
    ].forEach(([value,label]) => {
      const option=document.createElement('option'); option.value=value; option.textContent=label;
      const contract=value === 'hunyuan' ? 'farm' : value;
      if(value==='tripo' || !providerSupported(run,contract)) option.disabled=true;
      provider.appendChild(option);
    });
    provider.value = stringOf(first(part, ['provider_string'], 'blender')) || 'blender';
    if (provider.selectedOptions[0] && provider.selectedOptions[0].disabled) provider.value='blender';
    const polygons = document.createElement('select'); polygons.setAttribute('aria-label', tr('vehicle_polygons', 'Polygon target'));
    [1000,5000,10000,25000,50000,100000].forEach(value => { const option=document.createElement('option'); option.value=String(value); option.textContent=value.toLocaleString(); polygons.appendChild(option); });
    polygons.value = stringOf(first(part, ['polygons_int', 'polygon_target_int'], 10000));
    const model = document.createElement('button'); model.type='button'; model.className='primary'; model.textContent=tr('vehicle_model_part', 'Model selected part');
    const selectedProvider=()=>provider.value === 'hunyuan' ? 'farm' : provider.value;
    const updateModelAvailability=()=>{ model.disabled=!partCanModel(part) || !providerSupported(run,selectedProvider()); };
    updateModelAvailability(); provider.addEventListener('change',updateModelAvailability);
    if (model.disabled) model.title=partCanModel(part)
      ? tr('vehicle_provider_unavailable','This modelling provider is unavailable.')
      : stringOf(first(part,['reason'],tr('vehicle_part_not_ready','This part needs review before modelling.')));
    model.addEventListener('click', async () => {
      model.disabled=true;
      try {
        const queued=graphActions && await graphActions.modelPart({
          vehicleUrl:runUrl(runId), partIds:[id], provider:selectedProvider(),
          polygonBudget:Number(polygons.value), mode:panel.querySelector('.veh-mode').value
        });
        if (!queued) setError(tr('vehicle_model_graph_required','Open this run from its graph before modelling a selected part.'));
      } catch(error) { setError(error.message || String(error)); }
      finally { if(document.contains(model)) updateModelAvailability(); }
    });
    controls.append(provider, polygons, model); host.appendChild(controls);
    const reason=stringOf(first(part,['reason','status_reason','review_reason'],'')).trim();
    if (reason) { const note=document.createElement('p'); note.className='veh-note'; note.textContent=tr('vehicle_part_status_reason','Status: {reason}',{reason}); host.appendChild(note); }
    const evidence=boundaryEvidence(run,part);
    if (evidence && !part.accepted_bool && !partCanModel(part)) {
      const accept=document.createElement('button'); accept.type='button'; accept.className='veh-part';
      accept.textContent=tr('vehicle_accept_boundaries','Use these boundaries');
      accept.addEventListener('click',async()=>{
        const updated=await action({
          action:'accept_parts',part_ids:[id],part_evidence:[evidence],expected_revision:Number(run.revision),
          provider:'blender',polygon_budget:Number(polygons.value),mode:panel.querySelector('.veh-mode').value,options:{}
        },accept);
        const accepted=updated && listOf(updated,['parts']).find(item=>stringOf(first(item,['part_id_string','id_string','id'],''))===id);
        if (accepted && partCanModel(accepted) && graphActions) await graphActions.modelPart({
          vehicleUrl:runUrl(runId),partIds:[id],provider:selectedProvider(),
          polygonBudget:Number(polygons.value),mode:panel.querySelector('.veh-mode').value
        });
      });
      host.appendChild(accept);
    } else if (part.accepted_bool) {
      const accepted=document.createElement('p'); accepted.className='veh-note'; accepted.textContent=tr('vehicle_boundaries_accepted','Boundaries accepted'); host.appendChild(accepted);
    }
  }

  function stageAction(host, actionName, label, options) {
    const button=document.createElement('button'); button.type='button'; button.textContent=label;
    button.addEventListener('click',()=>action({
      action:actionName, part_ids:[], provider:'blender', polygon_budget:10000,
      mode:panel.querySelector('.veh-mode').value, options:options || {},
      expected_revision:Number(first(run, ['revision'], 0)) || undefined
    },button));
    host.appendChild(button);
  }

  function renderDetails(parts) {
    const host = panel.querySelector('.veh-right'); host.replaceChildren();
    if (!run) { const note=document.createElement('p'); note.className='veh-note'; note.textContent=tr('vehicle_empty_analysis', 'Vision facts, detected parts, materials and generated assets will appear here.'); host.appendChild(note); return; }
    const analysis = first(run, ['analysis'], {});
    const heading = document.createElement('h3'); heading.textContent = tr('vehicle_analysis_title', 'Vehicle analysis'); host.appendChild(heading);
    const facts = document.createElement('div'); facts.className='veh-facts';
    addFact(facts, tr('vehicle_category', 'Category'), first(analysis, ['category_string', 'category'], ''));
    const dimensions=first(analysis, ['dimensions_label_string', 'dimensions_string', 'dimensions'], '');
    addFact(facts, tr('vehicle_dimensions', 'Dimensions'), typeof dimensions === 'object' ? JSON.stringify(dimensions) : dimensions);
    addFact(facts, tr('vehicle_wheels', 'Wheels'), first(analysis, ['wheel_count_int', 'wheels_int'], ''));
    addFact(facts, tr('vehicle_symmetry', 'Working side'), first(analysis, ['working_side_string', 'symmetry_side_string'], ''));
    host.appendChild(facts);
    const cleanup=first(run,['body_cleanup_object'],null);
    if (cleanup) {
      const title=document.createElement('h3'); title.textContent=tr('vehicle_body_cleanup','Clean vehicle body'); host.appendChild(title);
      const cleanFacts=document.createElement('div'); cleanFacts.className='veh-facts';
      addFact(cleanFacts,tr('vehicle_cleanup_wheels','Wheels removed'),cleanup.wheels_removed_bool ? '✓' : '—');
      addFact(cleanFacts,tr('vehicle_cleanup_equipment','Mounted equipment removed'),cleanup.equipment_removed_bool ? '✓' : '—');
      addFact(cleanFacts,tr('vehicle_cleanup_cargo','Bed/trunk cargo removed'),cleanup.cargo_removed_bool ? '✓' : '—');
      host.appendChild(cleanFacts);
      if (!wheelRemovalReady(run)) { const note=document.createElement('p'); note.className='veh-note veh-error'; note.textContent=tr('vehicle_cleanup_incomplete','Body cleanup is incomplete.'); host.appendChild(note); }
      if (statusOf(run)==='needs_review' && stringOf(run.stage)==='remove_wheels' && run.body_cleanup_retry_available_bool) {
        const retry=document.createElement('button'); retry.type='button'; retry.className='veh-part';
        retry.textContent=tr('vehicle_retry_body_cleanup','Retry body cleanup')+' · '+
          tr('vehicle_cleanup_attempt','cleanup attempt {value}/3',{value:Number(run.remove_attempt_int || 0)});
        retry.addEventListener('click',()=>action({action:'retry',part_ids:[],provider:'blender',polygon_budget:10000,
          mode:'split',options:{},expected_revision:Number(run.revision)},retry)); host.appendChild(retry);
      }
    }
    if (statusOf(run)==='needs_review' && stringOf(run.stage)==='project_views' && run.project_views_retry_available_bool) {
      const attempts=Object.entries(run.view_attempts_object || {}).map(([id,count])=>id+': '+count).join(' · ');
      const retry=document.createElement('button'); retry.type='button'; retry.className='veh-part';
      retry.textContent=tr('vehicle_retry_failed_views','Retry failed views');
      if (attempts) retry.title=tr('vehicle_view_attempts','View attempts: {value}',{value:attempts});
      retry.addEventListener('click',()=>action({action:'retry',part_ids:[],provider:'blender',polygon_budget:10000,
        mode:'split',options:{},expected_revision:Number(run.revision)},retry)); host.appendChild(retry);
    }
    const stageControls=document.createElement('div'); stageControls.className='veh-part-actions';
    if (run.artifacts && (run.artifacts.model_url || run.artifacts.voxel_url)) stageAction(stageControls,'rig',tr('vehicle_rig_action','Build vehicle rig'));
    if (stageControls.childNodes.length) host.appendChild(stageControls);
    const partsTitle=document.createElement('h3'); partsTitle.textContent=tr('vehicle_parts_title', 'Detected objects'); host.appendChild(partsTitle);
    if (!parts.length) { const empty=document.createElement('p'); empty.className='veh-note'; empty.textContent=tr('vehicle_no_parts_yet', 'No object masks have been returned yet.'); host.appendChild(empty); }
    else parts.forEach(part => renderPart(part, host));
  }

  function render() {
    if (!panel) return;
    const parts = listOf(run, ['parts']);
    if (!tabTouched && cleanupCandidateUrl(run)) activeTab='clean';
    else if (!tabTouched && run && run.prepared_image_url) activeTab='prepared';
    if (selectedPart && !parts.some(part => stringOf(first(part, ['part_id_string','id_string','id'],'')) === selectedPart)) selectedPart='';
    if (!selectedPart && parts.length) selectedPart=stringOf(first(parts[0], ['part_id_string','id_string','id'],''));
    renderFlow(); renderTabs(parts); renderPreview(parts); renderDetails(parts);
    const stopped=run && STOPPED.has(statusOf(run));
    setError(stopped ? (runMessage(run) || tr('vehicle_stage_stopped','Stage needs attention.')) : '');
  }

  function install() { addStyle(); buildPanel(); installButton(); }
  global.AIVehicleNodes = {install, installGraphTemplate, vehicleTemplate, runnerFor:graphRunner,
    bindGraph:value => { graphActions=value || null; },
    openRun, resultLinkText:()=>tr('vehicle_inspect_run','Inspect vehicle run'),
    __test:{completionState,buildActionRequest,buildAnalyzeRequest,viewerHandoff,viewerFrameUrl,partCanModel,parsePartIds,boundaryEvidence,providerSupported,referenceEvidence,providerTaskFor,cleanupCandidateUrl},
    renderRun:value => { run=value; runId=stringOf(first(value,['id'],'')); render(); }};
  if (document.readyState === 'loading') global.addEventListener('DOMContentLoaded', install);
  else install();
})(window);
