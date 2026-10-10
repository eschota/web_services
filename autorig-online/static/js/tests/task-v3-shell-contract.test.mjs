import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';

const root=path.resolve(import.meta.dirname,'..','..');
const js=fs.readFileSync(path.join(root,'js','task-v3-shell.js'),'utf8');
const html=fs.readFileSync(path.join(root,'task-v3.html'),'utf8');

test('task V3 shell derives identity only from the task id and the server state',()=>{
  assert.match(js,/\/api\/task\/\$\{encodeURIComponent\(taskId\)\}\/v3-view/);
  assert.doesNotMatch(js,/params\.get\(['"]run['"]\)/);
  assert.doesNotMatch(js,/URLSearchParams\(location\.search\)\.get\(['"]run['"]\)/);
  assert.doesNotMatch(js,/\/api\/mt\/kit/);
  assert.match(js,/const UNITY_PAGE = '\/api\/mt\/unity\/test\/index\.html'/);
  assert.match(js,/viewer\.page !== UNITY_PAGE/);
  assert.match(js,/api !== `\/api\/task-viewer\/\$\{taskId\}`/);
});

test('task V3 page is one viewer with honest progress and the SEO layout',()=>{
  assert.match(html,/id="tv3-viewer"/);
  assert.match(html,/role="progressbar"/);
  assert.match(html,/task-v3-shell\.js/);
  assert.match(html,/<!-- TASK_SEO_PLACEHOLDER -->/);
  assert.match(html,/<meta name="robots" content="noindex, nofollow">/);
  assert.match(html,/<title>Task Progress \| AutoRig\.online<\/title>/);
  assert.match(html,/<h2 data-i18n="task_title" class="task-status-header-title">AutoRig task<\/h2>/);
  assert.match(html,/<div id="site-header"><\/div>/);
  assert.match(html,/<div id="site-footer"><\/div>/);
  assert.doesNotMatch(html,/task-play-mode|task-split-viewer|three\.module|bundle\.zip/);
});
