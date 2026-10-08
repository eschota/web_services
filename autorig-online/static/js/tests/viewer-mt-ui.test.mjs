import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';

const ui=readFileSync(new URL('../viewer-mt-ui.js',import.meta.url),'utf8');
const base=readFileSync(new URL('../viewer-v3.js',import.meta.url),'utf8');
const css=readFileSync(new URL('../../css/viewer-mt.css',import.meta.url),'utf8');

test('MT bootstrap is lazy and receives the exact loaded V3 model digest',()=>{assert.match(base,/has\('mt'\).*viewer-mt-ui\.js/s);assert.match(base,/loadedModelSha/);assert.match(ui,/await getViewerContext\(\)/);assert.doesNotMatch(ui,/fetchBytesBounded\(`\/api\/task\//)});
test('run API and auxiliary JSON use bounded abortable reads',()=>{assert.match(ui,/new AbortController\(\)/);assert.match(ui,/fetchJsonBounded\(`\/api\/mt\/runs\//);assert.match(ui,/controller\.abort\(\)/);assert.doesNotMatch(ui,/response\.json\(\)/)});
test('stable production QA selectors cover all review surfaces',()=>{for(const id of ['mt-panel','mt-summary','mt-projections','mt-selected-projection','mt-labels','mt-label-selector','mt-load-label-glb','mt-motion','mt-motion-master','mt-tracks','mt-load-track','mt-track-play','mt-track-timeline','mt-outputs','mt-load-source-glb','mt-load-phases','mt-file-count'])assert.ok(ui.includes(id),id)});
test('invalid joints use compact draw ranges and 60fps derivative is separate',()=>{assert.match(ui,/buildTrackFrameGeometry\(data,index\)/);assert.match(ui,/setDrawRange\(0,frame\.points\.length\/3\)/);assert.doesNotMatch(ui,/\[NaN,NaN,NaN\]/);assert.match(ui,/fitBounds\(frame\.bounds\)/);assert.match(ui,/dataset\.motionView!==['"]60['"]/);assert.match(ui,/60 fps derivative · independent playback/)});
test('track finite mask is never presented as anatomy or confidence',()=>{assert.match(ui,/valid означает только конечные числа/);assert.match(ui,/это не skin и не готовая анимация/);assert.match(ui,/finite joints/);assert.doesNotMatch(ui,/\bvalid joints|invalid \/ low-confidence/)});
test('raw self edges remain evidence but compact geometry skips them only while drawing',()=>{assert.match(ui,/zero-length edges \$\{frame\.skippedDegenerateEdges\}/);assert.match(ui,/invalid-endpoint edges \$\{frame\.skippedInvalidEdges\}/)});
test('missing anatomy and skin are explicit and panel has bounded responsive layout',()=>{assert.match(ui,/fitted anatomical bones · anatomical weights · skin binding · validated deformation/);assert.match(css,/\.mt-panel\{/);assert.match(css,/@media\(max-width:900px\)/)});
test('presentation planner is mounted only after source validation and never auto-runs inference',()=>{assert.match(ui,/plannerPanel\(run,projection,disposers\)/);assert.match(ui,/pose_best\.png/);assert.match(ui,/fetchJsonBounded\('\/api\/ai\/graph-edits\/schema'/);assert.match(ui,/validated:true,runId:run\.runId,sourceSha256:projection\.sourceSha/);assert.match(ui,/Presentation-only branch/);assert.doesNotMatch(ui,/\/api\/text2text/)});
test('planner async bootstrap is cancelled and destroyed with the parent MT panel',()=>{assert.match(ui,/let cancelled=false,planner=null/);assert.match(ui,/disposers\.push\(\(\)=>\{cancelled=true;planner\?\.destroy\(\)\}\)/);assert.match(ui,/if\(cancelled\|\|signal\?\.aborted\)\{created\.destroy\(\);return\}/);assert.match(ui,/bootstrapViewerMtPlanner\(\{mount,signal/)});
test('examples do not hardcode changing run statuses and refresh is explicit GET reload',()=>{assert.doesNotMatch(ui,/label:'[^']*· (?:done|running)'/);assert.match(ui,/refresh\.id='mt-refresh'/);assert.match(ui,/location\.reload\(\)/)});

