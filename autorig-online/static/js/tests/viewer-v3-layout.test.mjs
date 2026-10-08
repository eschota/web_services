import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';

const html=readFileSync(new URL('../../viewer-v3.html',import.meta.url),'utf8');
const css=readFileSync(new URL('../../css/viewer-v3.css',import.meta.url),'utf8');
const js=readFileSync(new URL('../viewer-v3.js',import.meta.url),'utf8');

test('hidden panels cannot be forced visible by grid styles',()=>assert.match(css,/\[hidden\]\{display:none!important\}/));
test('desktop task mode is viewport bounded with independent scroll regions',()=>{
  assert.match(css,/body\.has-task main\{height:calc\(100vh - 62px\)/);
  assert.match(css,/body\.has-task \.stage-list\{[^}]*overflow:auto/);
  assert.match(css,/body\.has-task \.metrics-panel\{[^}]*overflow:auto/);
  assert.match(css,/body\.has-task \.viewport-wrap\{min-height:0/);
});
test('mobile task mode is not height locked',()=>assert.match(css,/@media\(max-width:1050px\)\{body\.has-task\{overflow:auto\}/));
test('only the source model is enabled and visible by default',()=>{
  assert.match(html,/data-layer="model" checked/);
  for(const layer of ['surface','solid','thin','skeleton','bones','weights'])assert.match(html,new RegExp(`data-layer="${layer}" disabled`));
});
test('missing layers are synchronized from exact manifest artifacts',()=>{
  assert.match(js,/function syncLayerAvailability\(\)/);
  assert.match(js,/has\('fitted_bones','r1_bones'\)/);
  assert.match(js,/document\.body\.classList\.add\('has-task'\)/);
});
test('opaque preview is explicit, optional and not a data layer',()=>{
  assert.match(html,/id="opaque-preview" disabled/);
  assert.doesNotMatch(html,/data-layer="opaque/);
  assert.match(html,/Игнорировать прозрачность/);
});
test('current cache busters are present',()=>{
  assert.match(html,/viewer-v3\.css\?v=3/);
  assert.match(html,/viewer-v3\.js\?v=7/);
  assert.match(js,/viewer-v3-contract\.js\?v=3/);
});
