import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../ai-node-vehicle.js', import.meta.url), 'utf8');
const window = {location:{origin:'https://autorig.online'}, addEventListener(){}};
const context = vm.createContext({window, document:{readyState:'loading'}, URL, console});
vm.runInContext(source, context);
const api = window.AIVehicleNodes.__test;

test('analysis content cannot advance without an explicit completed stage', () => {
  assert.equal(api.completionState({status:'running',analysis:{category:'car'},stages:[]},'analyze').state,'wait');
  assert.equal(api.completionState({status:'running',analysis:{category:'car'},stages:[{stage:'analyze',status:'completed'}]},'analyze').state,'complete');
});

test('old parts cannot complete clustering and a registered overlay is required', () => {
  const base={status:'running',parts:[{id:'wheel'}],stages:[{stage:'cluster',status:'completed'}],artifacts:{}};
  assert.equal(api.completionState(base,'cluster').state,'wait');
  assert.equal(api.completionState({...base,artifacts:{overlay_url:'/asset/overlay.png'}},'cluster').state,'complete');
});

test('review and blocked runs stop rather than poll or advance', () => {
  assert.equal(api.completionState({status:'needs_review',review_reason:'wheel mask ambiguous'},'cluster').state,'stop');
  assert.equal(api.completionState({status:'blocked',error:{message:'source is not a vehicle'}},'analyze').state,'stop');
});

test('analysis fixes a supported resolution once and voxel sends no override', () => {
  const create=api.buildAnalyzeRequest({image_url:'/source.png',resolution:'96'});
  assert.equal(create.options.resolution,96);
  const request=api.buildActionRequest('vehicle_voxel',{resolution:'96'});
  assert.equal(request.action,'voxelize');
  assert.deepEqual({...request.options},{});
});

test('wheel editor handoff rejects the nodes self-link and requires a wheel rig', () => {
  assert.equal(api.viewerHandoff({viewer_url:'/nodes?vehicle_run=x',artifacts:{wheel_rig_url:'/wheels.json'}}),'');
  assert.equal(api.viewerHandoff({viewer_url:'/api/mt/unity/x/index.html',artifacts:{}}),'');
  assert.equal(api.viewerHandoff({viewer_url:'/api/mt/unity/x/index.html',artifacts:{wheel_rig_url:'/wheels.json'}}),'https://autorig.online/api/mt/unity/x/index.html');
});

test('viewer iframe only accepts same-origin Unity routes', () => {
  assert.equal(api.viewerFrameUrl({preview_viewer_url:'https://evil.example/api/mt/unity/x'}),'');
  assert.equal(api.viewerFrameUrl({preview_viewer_url:'/nodes?vehicle_run=x'}),'');
  assert.equal(api.viewerFrameUrl({preview_viewer_url:'/api/mt/unity/x/index.html'}),'https://autorig.online/api/mt/unity/x/index.html');
});

test('reviewable voxels can continue only with a completed stage and both artifacts', () => {
  const run={status:'needs_review',stages:[{stage:'voxelize',status:'completed'}],artifacts:{voxel_url:'/v.glb',voxel_preview_url:'/v.png'}};
  assert.equal(api.completionState(run,'voxelize').state,'complete');
  assert.equal(api.completionState({...run,artifacts:{voxel_url:'/v.glb'}},'voxelize').state,'stop');
});

test('part modelling stays disabled until the backend marks it ready', () => {
  assert.equal(api.partCanModel({modeling_ready:false}),false);
  assert.equal(api.partCanModel({}),false);
  assert.equal(api.partCanModel({modeling_ready:true}),true);
});

test('selected part IDs survive graph text serialization', () => {
  assert.deepEqual(Array.from(api.parsePartIds('wheel_fl; bumper')),['wheel_fl','bumper']);
  assert.deepEqual(Array.from(api.parsePartIds('["door_left"]')),['door_left']);
});

test('fallback graph executes wheel-free four-view fusion before parts', () => {
  const graph=window.AIVehicleNodes.vehicleTemplate();
  assert.deepEqual(Array.from(graph.links,link=>`${link.from}->${link.to}`),[
    'vehicle_source->vehicle_analyze','vehicle_analyze->vehicle_prepare',
    'vehicle_prepare->vehicle_remove_wheels','vehicle_remove_wheels->vehicle_project_views',
    'vehicle_project_views->vehicle_depth_views','vehicle_depth_views->vehicle_fuse_voxels',
    'vehicle_fuse_voxels->vehicle_cluster'
  ]);
});

test('prepare stage requires its explicit completion and prepared image', () => {
  const base={status:'running',stages:[{stage:'prepare',status:'completed'}]};
  assert.equal(api.completionState(base,'prepare').state,'wait');
  assert.equal(api.completionState({...base,prepared_image_url:'/prepared.png'},'prepare').state,'complete');
});

test('boundary acceptance is hash-bound and excludes original-scene people', () => {
  const hash='a'.repeat(64);
  const run={source_sha_chain:{original_sha256:hash,prepared_sha256:hash},camera:{
    normalization_status_string:'accepted',projection_string:'orthographic',yaw_deg_float:45,pitch_deg_float:45
  }};
  const vehicle={id:'door',name:'door',image_role:'prepared',source_bbox_px_array:[1,2,30,40],mask_url:'/mask.png',mask_sha256:hash};
  assert.equal(api.boundaryEvidence(run,vehicle).part_id,'door');
  assert.equal(api.boundaryEvidence(run,{...vehicle,id:'person',name:'person'}),null);
  assert.equal(api.boundaryEvidence(run,{...vehicle,image_role:'original'}),null);
  assert.equal(api.boundaryEvidence(run,{...vehicle,mask_sha256:'stale'}),null);
});

test('farm modelling stays unavailable unless the run explicitly supports it', () => {
  assert.equal(api.providerSupported({supported_model_providers:['blender']},'farm'),false);
  assert.equal(api.providerSupported({supported_model_providers:['blender','farm']},'farm'),true);
});

test('prepared reference acceptance is bound to original and prepared hashes', () => {
  const hash='b'.repeat(64);
  assert.deepEqual({...api.referenceEvidence({prepared_image_url:'/p.png',source_sha_chain:{original_sha256:hash,prepared_sha256:hash}})},
    {original_sha256:hash,prepared_sha256:hash});
  assert.equal(api.referenceEvidence({prepared_image_url:'/p.png',source_sha_chain:{original_sha256:hash}}),null);
});

test('multiview completion requires four wheel-free ortho views, depth hashes and zero symmetry error', () => {
  const hash='c'.repeat(64);
  const views=['top','front','left','back'].map(id=>({id,reference_object:{wheel_free_bool:true,image_url:'/v/'+id+'.png'},camera_object:{projection:'orthographic'},depth_object:{sha256:hash}}));
  const run={options:{resolution:192},multiview_object:{schema_string:'autorig.vehicle.multiview/1',stage:'fuse_voxels',status:'complete',views_array:views,fused_voxels_object:{resolution_int:192,voxel_count_int:1000,coordinate_space_string:'model_local_gltf',symmetry_object:{pair_symmetry_error_m_float:0}}}};
  assert.equal(api.completionState({...run,status:'running',stages:[{stage:'fuse_voxels',status:'completed'}]},'fuse_voxels').state,'complete');
  assert.equal(api.completionState({status:'running',stages:[{stage:'fuse_voxels',status:'completed'}],multiview_object:{schema_string:'r13',views_array:views}},'fuse_voxels').state,'wait');
  const bad=structuredClone(run); bad.multiview_object.views_array.pop();
  assert.equal(api.completionState({...bad,status:'running',stages:[{stage:'fuse_voxels',status:'completed'}]},'fuse_voxels').state,'wait');
});

test('new multiview services submit their own actions', () => {
  assert.equal(api.buildActionRequest('vehicle_remove_wheels',{}).action,'remove_wheels');
  assert.equal(api.buildActionRequest('vehicle_project_views',{}).action,'project_views');
  assert.equal(api.buildActionRequest('vehicle_depth_views',{}).action,'depth_views');
  assert.equal(api.buildActionRequest('vehicle_fuse_voxels',{}).action,'fuse_voxels');
});

test('body cleanup requires wheels, equipment and cargo removed with zero counts and hashes', () => {
  const source='e'.repeat(64), output='f'.repeat(64);
  const receipt={schema_string:'autorig.vehicle.body-cleanup/1',contract_version_string:'body-cleanup-v1',status_string:'complete',
    source_sha256_string:source,output_sha256_string:output,image_url:'/clean.png',wheels_removed_bool:true,equipment_removed_bool:true,cargo_removed_bool:true,
    semantic_counts_object:{wheels_int:0,equipment_int:0,cargo_int:0}};
  const run={status:'running',source_sha_chain:{prepared_sha256:source},body_cleanup_object:receipt,stages:[{stage:'remove_wheels',status:'completed'}]};
  assert.equal(api.completionState(run,'remove_wheels').state,'complete');
  assert.equal(api.completionState({...run,body_cleanup_object:{...receipt,equipment_removed_bool:false}},'remove_wheels').state,'wait');
  assert.equal(api.completionState({...run,body_cleanup_object:{...receipt,semantic_counts_object:{wheels_int:0,equipment_int:1,cargo_int:0}}},'remove_wheels').state,'wait');
});

test('hash-valid cleanup candidate becomes the primary reference even while needs review', () => {
  const source='a'.repeat(64), output='b'.repeat(64);
  const run={status:'needs_review',source_sha_chain:{prepared_sha256:source},body_cleanup_object:{
    schema_string:'autorig.vehicle.body-cleanup/1',contract_version_string:'body-cleanup-v1',status_string:'needs_review',
    source_sha256_string:source,output_sha256_string:output,image_url:'/clean-body.png'}};
  assert.equal(api.cleanupCandidateUrl(run),'https://autorig.online/clean-body.png');
  run.body_cleanup_object.source_sha256_string='c'.repeat(64);
  assert.equal(api.cleanupCandidateUrl(run),'');
});

test('API-projected views_object and fusion_object satisfy the strict fuse gate', () => {
  const hash='d'.repeat(64);
  const views=Object.fromEntries(['top','front','left','back'].map(id=>[id,{id_string:id,reference_object:{wheel_free_bool:true,image_url:'/v/'+id+'.png'},camera_object:{projection_string:'orthographic'},depth_object:{sha256_string:hash}}]));
  const run={status:'running',options:{resolution:192},stages:[{stage:'fuse_voxels',status:'completed'}],
    multiview_object:{schema_string:'autorig.vehicle.multiview/1',stage_string:'fuse_voxels',status_string:'complete'},views_object:views,
    fusion_object:{resolution_int:192,voxel_count_int:149999,coordinate_space_string:'model_local_gltf',symmetry_object:{pair_symmetry_error_m_float:0}}};
  assert.equal(api.completionState(run,'fuse_voxels').state,'complete');
  run.fusion_object.voxel_count_int=150001;
  assert.equal(api.completionState(run,'fuse_voxels').state,'wait');
});

test('farm task cards require an explicit pipeline stage receipt', () => {
  const legacy={multiview_object:{provider_tasks_array:[{service_string:'qwen',task_id_string:'t1'}]}};
  assert.equal(api.providerTaskFor(legacy,'remove_wheels'),null);
  const receipt={schema_string:'autorig.vehicle.provider-task/1',pipeline_stage_string:'remove_wheels',service_string:'qwen',task_id_string:'t1',status_string:'running',node_string:'f12',worker_string:'qwen'};
  assert.equal(api.providerTaskFor({multiview_object:{provider_tasks_array:[receipt]}},'remove_wheels').task_id_string,'t1');
  assert.equal(api.providerTaskFor({multiview_object:{provider_tasks_array:[receipt]}},'depth_views'),null);
});
