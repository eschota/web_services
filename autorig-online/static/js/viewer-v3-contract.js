const SHA256=/^[0-9a-f]{64}$/;

function fail(message){throw new Error(message)}
function finiteNumber(value){return typeof value==='number'&&Number.isFinite(value)}

export function validateOverlayBase(payload,artifact,manifest,schema){
  if(!payload||typeof payload!=='object'||payload.schema!==schema)fail(`Overlay schema mismatch: ${artifact.name}`);
  if(payload.task_id!==manifest.task_id||payload.source_sha256!==manifest.build.source_sha256)fail(`Overlay provenance mismatch: ${artifact.name}`);
  if(!SHA256.test(String(artifact.stage_receipt_sha256||''))||payload.stage_receipt_sha256!==artifact.stage_receipt_sha256)fail(`Overlay stage receipt mismatch: ${artifact.name}`);
  if(payload.stage!==artifact.stage)fail(`Overlay stage mismatch: ${artifact.name}`);
  if(payload.coordinate_space==='model_local_gltf'){
    if(payload.matrix_to_model!=null)validateMatrix(payload.matrix_to_model);
  }else{
    validateMatrix(payload.matrix_to_model);
  }
}

export function validateMatrix(value){
  if(!Array.isArray(value)||value.length!==16||value.some(x=>!finiteNumber(x)))fail('Invalid artifact matrix_to_model');
  return value;
}

export function validateVoxelPayload(payload,artifact,manifest){
  validateOverlayBase(payload,artifact,manifest,'autorig.v3.voxels/1');
  const flat=payload.positions;
  if(!Array.isArray(flat)||flat.length%3!==0||flat.some(x=>!finiteNumber(x)))fail(`Invalid voxel positions: ${artifact.name}`);
  const count=flat.length/3;
  if(!Number.isInteger(payload.point_count)||payload.point_count!==count||artifact.points!==count||count>250000)fail(`Voxel point count mismatch: ${artifact.name}`);
  if(payload.classes!=null&&(!Array.isArray(payload.classes)||payload.classes.length!==count||payload.classes.some(x=>!Number.isInteger(x)||x<0||x>255)))fail(`Invalid voxel classes: ${artifact.name}`);
  if(!finiteNumber(payload.voxel_size)||payload.voxel_size<=0||payload.voxel_size>100000)fail(`Invalid voxel_size: ${artifact.name}`);
  return {count,positions:flat,voxelSize:payload.voxel_size};
}

export function validateSkeletonPayload(payload,artifact,manifest){
  validateOverlayBase(payload,artifact,manifest,'autorig.v3.skeleton/1');
  const joints=payload.joints,edges=payload.edges;
  if(!Array.isArray(joints)||!joints.length||joints.length>5000||joints.some(v=>!Array.isArray(v)||v.length!==3||v.some(x=>!finiteNumber(x))))fail(`Invalid skeleton joints: ${artifact.name}`);
  if(!Number.isInteger(payload.joint_count)||payload.joint_count!==joints.length||artifact.points!==joints.length)fail(`Skeleton joint count mismatch: ${artifact.name}`);
  if(!Array.isArray(edges)||!edges.length||edges.length>10000||!Number.isInteger(payload.edge_count)||payload.edge_count!==edges.length||edges.some(edge=>!Array.isArray(edge)||edge.length!==2||edge.some(i=>!Number.isInteger(i)||i<0||i>=joints.length)||edge[0]===edge[1]))fail(`Invalid skeleton edges: ${artifact.name}`);
  return {joints,edges};
}

export async function sha256Hex(buffer){
  const digest=await globalThis.crypto.subtle.digest('SHA-256',buffer);
  return Array.from(new Uint8Array(digest),byte=>byte.toString(16).padStart(2,'0')).join('');
}

export function assertSelfContainedGlb(buffer){
  if(!(buffer instanceof ArrayBuffer)||buffer.byteLength<20||buffer.byteLength>128*1024*1024)fail('GLB size is invalid');
  const view=new DataView(buffer);
  if(view.getUint32(0,true)!==0x46546c67||view.getUint32(4,true)!==2||view.getUint32(8,true)!==buffer.byteLength)fail('GLB header is invalid');
  let offset=12,jsonChunk=null;
  while(offset<buffer.byteLength){
    if(offset+8>buffer.byteLength)fail('GLB chunk header is truncated');
    const length=view.getUint32(offset,true),type=view.getUint32(offset+4,true);offset+=8;
    if(length>buffer.byteLength-offset)fail('GLB chunk is truncated');
    if(type===0x4e4f534a&&jsonChunk===null)jsonChunk=new Uint8Array(buffer,offset,length);
    offset+=length;
  }
  if(offset!==buffer.byteLength||!jsonChunk)fail('GLB JSON chunk is missing');
  let document;
  try{document=JSON.parse(new TextDecoder().decode(jsonChunk).replace(/\0+$/,''))}catch{fail('GLB JSON chunk is invalid')}
  for(const item of document.buffers||[])if(item.uri!=null)fail('External GLB buffers are not allowed');
  for(const item of document.images||[])if(item.uri!=null&&!/^data:image\/[a-z0-9.+-]+;base64,/i.test(item.uri))fail('External GLB images are not allowed');
  return document;
}

