import assert from 'node:assert/strict';
import test from 'node:test';
import {assertSelfContainedGlb,nextPreviewMaterialMode,opaquePreviewOverrides,sha256Hex,validateBoneClip,validateSkeletonPayload,validateSkinAttributes,validateVoxelPayload} from '../viewer-v3-contract.js';

const sha='a'.repeat(64);
const manifest={task_id:'11111111-2222-3333-4444-555555555555',build:{source_sha256:'b'.repeat(64)}};
const voxelArtifact={name:'surface',stage:'c1_surface',stage_receipt_sha256:sha,points:1};
const voxel={schema:'autorig.v3.voxels/1',task_id:manifest.task_id,source_sha256:manifest.build.source_sha256,stage:'c1_surface',stage_receipt_sha256:sha,coordinate_space:'model_local_gltf',point_count:1,voxel_size:.1,positions:[0,0,0]};

test('accepts exact finite voxel payload',()=>assert.equal(validateVoxelPayload(voxel,voxelArtifact,manifest).count,1));
test('rejects numeric strings',()=>assert.throws(()=>validateVoxelPayload({...voxel,positions:['0',0,0]},voxelArtifact,manifest)));
test('rejects receipt drift',()=>assert.throws(()=>validateVoxelPayload({...voxel,stage_receipt_sha256:'c'.repeat(64)},voxelArtifact,manifest)));
test('requires transform for Blender world coordinates',()=>assert.throws(()=>validateVoxelPayload({...voxel,coordinate_space:'blender_world_z_up'},voxelArtifact,manifest)));

const graphArtifact={name:'graph',stage:'c4_graph',stage_receipt_sha256:sha,points:2};
const graph={schema:'autorig.v3.skeleton/1',task_id:manifest.task_id,source_sha256:manifest.build.source_sha256,stage:'c4_graph',stage_receipt_sha256:sha,coordinate_space:'model_local_gltf',joint_count:2,edge_count:1,joints:[[0,0,0],[0,1,0]],edges:[[0,1]]};
test('accepts exact graph counts and indices',()=>assert.equal(validateSkeletonPayload(graph,graphArtifact,manifest).edges.length,1));
test('rejects out of range graph edge',()=>assert.throws(()=>validateSkeletonPayload({...graph,edges:[[0,2]]},graphArtifact,manifest)));

function glb(json){
  const encoded=new TextEncoder().encode(JSON.stringify(json)),padded=(encoded.length+3)&~3,total=12+8+padded,buffer=new ArrayBuffer(total),view=new DataView(buffer),bytes=new Uint8Array(buffer);
  view.setUint32(0,0x46546c67,true);view.setUint32(4,2,true);view.setUint32(8,total,true);view.setUint32(12,padded,true);view.setUint32(16,0x4e4f534a,true);bytes.fill(0x20,20);bytes.set(encoded,20);return buffer;
}
test('accepts a self-contained GLB and hashes exact bytes',async()=>{const bytes=glb({asset:{version:'2.0'},buffers:[],images:[]});assert.equal(assertSelfContainedGlb(bytes).asset.version,'2.0');assert.match(await sha256Hex(bytes),/^[0-9a-f]{64}$/)});
test('rejects external GLB image URI',()=>assert.throws(()=>assertSelfContainedGlb(glb({asset:{version:'2.0'},images:[{uri:'https://evil.invalid/x.png'}]}))));

function attribute(rows){return {count:rows.length,itemSize:4,getX:i=>rows[i][0],getY:i=>rows[i][1],getZ:i=>rows[i][2],getW:i=>rows[i][3]}}
const position={count:1};
test('accepts finite normalized skin weights',async()=>assert.deepEqual(await validateSkinAttributes([{position,skinIndex:attribute([[0,1,0,0]]),skinWeight:attribute([[.75,.25,0,0]]),boneCount:2}]),{vertices:1}));
test('rejects zero and NaN skin weights',async()=>{
  await assert.rejects(validateSkinAttributes([{position,skinIndex:attribute([[0,0,0,0]]),skinWeight:attribute([[0,0,0,0]]),boneCount:1}]));
  await assert.rejects(validateSkinAttributes([{position,skinIndex:attribute([[0,0,0,0]]),skinWeight:attribute([[NaN,0,0,0]]),boneCount:1}]));
});
test('rejects out of range joint index',async()=>assert.rejects(validateSkinAttributes([{position,skinIndex:attribute([[2,0,0,0]]),skinWeight:attribute([[1,0,0,0]]),boneCount:2}])));

const clip={duration:1};
const boneTrack={nodeName:'leg',propertyName:'quaternion',times:new Float32Array([0,1]),values:new Float32Array([0,0,0,1,0,.2,0,.98]),valueSize:4};
test('accepts nonconstant track bound to a bone',()=>assert.ok(validateBoneClip(clip,[boneTrack],new Set(['leg'])).animatedBones.has('leg')));
test('rejects static camera/material tracks',()=>{
  assert.throws(()=>validateBoneClip(clip,[{...boneTrack,nodeName:'Camera',propertyName:'position'}],new Set(['leg'])));
  assert.throws(()=>validateBoneClip(clip,[{...boneTrack,values:new Float32Array([0,0,0,1,0,0,0,1])}],new Set(['leg'])));
});
test('opaque preview contract ignores alpha without changing texture properties',()=>{
  const source={map:{id:'same-map'},color:{id:'same-color'},roughness:.4,transparent:true,opacity:.12,alphaTest:.5,depthWrite:false,premultipliedAlpha:true};
  const clone={...source,...opaquePreviewOverrides()};
  assert.equal(clone.map,source.map);assert.equal(clone.color,source.color);assert.equal(clone.roughness,source.roughness);
  assert.deepEqual({transparent:clone.transparent,opacity:clone.opacity,alphaTest:clone.alphaTest,depthWrite:clone.depthWrite,premultipliedAlpha:clone.premultipliedAlpha},{transparent:false,opacity:1,alphaTest:0,depthWrite:true,premultipliedAlpha:false});
});
test('opaque and weight preview modes are mutually exclusive and reversible',()=>{
  let state={opaque:false,weights:false};state=nextPreviewMaterialMode(state,'opaque');assert.deepEqual(state,{opaque:true,weights:false});state=nextPreviewMaterialMode(state,'weights');assert.deepEqual(state,{opaque:false,weights:true});state=nextPreviewMaterialMode(state,'off');assert.deepEqual(state,{opaque:false,weights:false});
});
