import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {buildPlannerInput,parsePlannerProposal,readJsonBounded,summarizeValidatedDiff,validatePlannerContext,VIEWER_MT_PLANNER_API} from '../viewer-mt-planner.js';
import {buildPresentationDraft} from '../viewer-mt-contract.js';

const RUN='0123456789abcdefabcd',SHA='a'.repeat(64),POSE=`/api/mt/files/${RUN}/maps/pose_best.png`;

test('proposal parser accepts only bounded graph operations and the bound pose URL',()=>{
 const pose=POSE;
 const value=parsePlannerProposal(JSON.stringify({message:'Add hint',operations:[{op:'add_node',node:{id:'hint',kind:'input',entity_type:'text',value:pose,x:0,y:0}}]}),[pose]);
 assert.equal(value.operations.length,1);assert.equal(value.message,'Add hint');
 assert.throws(()=>parsePlannerProposal('{"message":"x","operations":[{"op":"execute"}]}'),/not allowed/);
 assert.throws(()=>parsePlannerProposal('{"message":"x","operations":[{"op":"set_input","id":"a","value":"https://evil.example/x"}]}'),/unapproved URL/);
 assert.throws(()=>parsePlannerProposal('{"message":"x","operations":[{"op":"set_input","id":"a","value":"javascript:alert(1)"}]}'),/unapproved URL/);
 assert.throws(()=>parsePlannerProposal('{"message":"x","operations":[{"op":"set_input","id":"a","value":"/api/other/file.png"}]}',[pose]),/unapproved URL/);
});

test('pure input builder compacts the actual backend schema response below the API budget',()=>{
 const backend=fileURLToPath(new URL('../../../backend',import.meta.url));
 const catalogue=fileURLToPath(new URL('../../../deploy/ai-models',import.meta.url));
 const script='import json;from fastapi import FastAPI;from fastapi.testclient import TestClient;import ai_graph_edits;app=FastAPI();app.include_router(ai_graph_edits.router);print(json.dumps(TestClient(app).get("/api/ai/graph-edits/schema").json(),separators=(",",":")))';
 const output=execFileSync('python',['-c',script],{cwd:backend,env:{...process.env,AUTORIG_AI_MODEL_DIR:catalogue},encoding:'utf8'}).trim().split(/\r?\n/).at(-1);
 const schema=JSON.parse(output);assert.equal(schema.operations_array.length,9);assert.ok(schema.services_array.length>=20);assert.ok(schema.models_array.length>=7);
 const graph=buildPresentationDraft(schema,POSE);
 const encoded=buildPlannerInput('Create a 3 m diorama hint, then final HQ video.',graph,schema,{runId:RUN,sourceSha256:SHA,poseBestUrl:POSE},{id:'bonsai2-27b',title:'Bonsai 2',context_tokens:4096});
 const payload=JSON.parse(encoded);assert.ok(encoded.length<6400,encoded.length);assert.deepEqual(payload.graph_edit_schema.operations_array.map(item=>item.op),schema.operations_array.map(item=>item.op));
 assert.deepEqual(payload.graph_edit_schema.services_array.map(item=>item.id).sort(),['qwen_image','text','video']);assert.equal(payload.planner_model.id,'bonsai2-27b');assert.equal(payload.graph_edit_schema.models_array,undefined);
 assert.equal(payload.graph.nodes[1].params.prompt,graph.nodes[1].params.prompt);assert.equal(payload.graph.nodes[2].params.frame_count,97);
});

test('validated run identity and exact pose_best path are inseparable',()=>{
 assert.equal(validatePlannerContext({validated:true,runId:RUN,sourceSha256:SHA,poseBestUrl:POSE}).poseBestUrl,POSE);
 for(const bad of [{validated:true,runId:'run',sourceSha256:SHA,poseBestUrl:POSE},{validated:true,runId:RUN,sourceSha256:'a',poseBestUrl:POSE},{validated:true,runId:RUN,sourceSha256:SHA,poseBestUrl:`/api/mt/files/${RUN}/maps/not_pose.png`}])assert.throws(()=>validatePlannerContext(bad),/invalid/);
});

test('bounded reader rejects content-length before consuming a response',async()=>{
 let read=false;const response={status:200,ok:true,headers:{get:()=>String(600000)},text:async()=>{read=true;return '{}';}};
 await assert.rejects(()=>readJsonBounded(response),/exceeded/);assert.equal(read,false);
});

test('validated diff is human-readable and does not execute actions',()=>{
 const before={name:'A',nodes:[{id:'a',params:{x:1}}],links:[]};const after={name:'B',nodes:[{id:'a',params:{x:2}},{id:'b'}],links:[{from:'a',to:'b'}]};
 assert.deepEqual(summarizeValidatedDiff(before,after,{renamed_bool:true}),['Add 1: b','Update 1: a','Add 1 link','Rename graph to “B”']);
 assert.equal(VIEWER_MT_PLANNER_API.sideEffects,'local draft only');
});

test('module has fixed same-origin API routes and no graph CRUD or execution fetches',()=>{
 const source=readFileSync(new URL('../viewer-mt-planner.js',import.meta.url),'utf8');
 for(const route of ['/api/ai/models','/api/ai/graph-edits/schema','/api/ai/graph-edits/validate','/api/text2text','/api/ai/status/'])assert.ok(source.includes(route),route);
 assert.doesNotMatch(source,/fetchImpl\([^E][^,]*user|\/api\/graphs|\/execute|Authorization|Bearer/i);
 assert.match(source,/Apply to local draft/);assert.match(source,/Save, render, delete and execute are unavailable/);
});

test('planner requires explicit user click and never auto-submits on bootstrap',()=>{
 const source=readFileSync(new URL('../viewer-mt-planner.js',import.meta.url),'utf8');
 assert.match(source,/propose\.addEventListener\('click',ask\)/);
 assert.doesNotMatch(source,/await ask\(\)/);
 assert.match(source,/validated!==true/);assert.match(source,/pose_best/);assert.match(source,/abortableDelay/);
});

test('planner bridges and removes the parent abort signal',()=>{
 const source=readFileSync(new URL('../viewer-mt-planner.js',import.meta.url),'utf8');
 assert.match(source,/externalSignal=options\.signal/);
 assert.match(source,/addEventListener\?\.\('abort',externalAbort/);
 assert.match(source,/removeEventListener\?\.\('abort',externalAbort/);
});
