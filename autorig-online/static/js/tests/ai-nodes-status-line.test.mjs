import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.resolve(here, '..', 'ai-nodes.js'), 'utf8');

function compactStatus() {
  const start = source.indexOf('  function compactStatus(');
  const end = source.indexOf('  function syncStatusLine(', start);
  assert.ok(start > 0 && end > start);
  return vm.runInNewContext(source.slice(start, end) + '; compactStatus');
}

test('the tracker caption folds into one state line with the machine name', () => {
  const compact = compactStatus();
  assert.equal(compact('Raptor · ~1m 29s estimated', 'rendering'), 'rendering · Raptor · ~1m 29s');
  assert.equal(compact('f12 · rendering · 20m 2s', 'rendering'), 'rendering · f12 · 20m 2s');
  assert.equal(compact('f12 · 12s', ''), 'rendering · f12 · 12s');
  assert.equal(compact('Queued · worker-4090 · 2nd in line · ~3m', 'queued'), 'queued · worker-4090 · 2nd in line · ~3m');
  assert.equal(compact('Queued · 4s', 'queued'), 'queued · 4s');
  assert.equal(compact('', 'rendering'), '');
});

test('result previews carry no Open link, only an Avatar keeps its profile link', () => {
  assert.doesNotMatch(source, /: 'open';/);
  assert.match(source, /link\.textContent = (type === 'avatar' \? 'open profile' : )?'';/);
});

test('the page hides the applied-settings note and the tracker caption on nodes', () => {
  const page = fs.readFileSync(path.resolve(here, '..', '..', 'nodes.html'), 'utf8');
  assert.match(page, /\.drawflow \.drawflow-node \.nrec \{ display: none !important; \}/);
  assert.match(page, /\.drawflow \.drawflow-node \.nprog \.task-eta \{ display: none; \}/);
});
