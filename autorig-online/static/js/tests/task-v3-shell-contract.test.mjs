import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';

const root=path.resolve(import.meta.dirname,'..','..');
const js=fs.readFileSync(path.join(root,'js','task-v3-shell.js'),'utf8');
const html=fs.readFileSync(path.join(root,'task-v3.html'),'utf8');

test('task V3 shell derives identity only from task id and server binding',()=>{
  assert.match(js,/\/api\/task\/\$\{encodeURIComponent\(id\)\}\/v3-shell/);
  assert.doesNotMatch(js,/URLSearchParams\(location\.search\)\.get\(['"]run['"]\)/);
  assert.doesNotMatch(js,/\/api\/mt\/kit/);
  assert.match(js,/url\.pathname!==['"]\/api\/mt\/unity\/test\/index\.html['"]/);
});

test('task V3 page contains only the current viewer shell and honest progress',()=>{
  assert.match(html,/id="viewer"/);
  assert.match(html,/role="progressbar"/);
  assert.match(html,/task-v3-shell\.js/);
  assert.doesNotMatch(html,/task-play-mode|task-split-viewer|three\.module|bundle\.zip/);
});
