import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, '..', 'ai-node-compare.js'), 'utf8');
const sandbox = { window: {}, URL, Date };
vm.runInNewContext(source, sandbox, { filename: 'ai-node-compare.js' });
const api = sandbox.window.AINodeCompare;

function finished(value, overrides = {}) {
  return {
    status: 'done', type: 'image', value,
    input_reference_url: 'https://example.test/reference.png', created_at: 1700000000,
    ...overrides,
  };
}

test('history uses strict numeric Unix timestamps and keeps at most five unique URLs', () => {
  const previous = finished('https://example.test/current.png', {
    history: Array.from({ length: 7 }, (_, index) => ({
      type: 'image', value: `https://example.test/history-${index}.png`,
      input_reference_url: '', created_at: 1699999000 - index,
    })),
  });
  const next = api.enhanceRecord(finished('https://example.test/new.png'), previous);
  assert.equal(next.history.length, 5);
  assert.equal(next.history[0].value, previous.value);
  assert.ok(next.history.every((entry) => Number.isFinite(entry.created_at)));
  assert.equal(new Set(next.history.map((entry) => entry.value)).size, 5);
});

test('running and failed states carry history and the pinned input reference', () => {
  const previous = finished('https://example.test/old.png');
  for (const status of ['running', 'failed']) {
    const next = api.enhanceRecord({ status, type: 'image', value: '' }, previous);
    assert.equal(next.history[0].value, previous.value);
    assert.equal(next.input_reference_url, previous.input_reference_url);
  }
});

test('stale visual output is retained once and current output is never duplicated into history', () => {
  const stale = finished('https://example.test/stale.png', { status: 'stale' });
  const next = api.enhanceRecord(finished('https://example.test/new.png'), stale);
  assert.equal(next.history.filter((entry) => entry.value === stale.value).length, 1);
  const same = api.enhanceRecord(finished(stale.value), stale);
  assert.equal(same.history.some((entry) => entry.value === stale.value), false);
});

test('blank, relative and credential-bearing URLs cannot become comparison A', () => {
  for (const value of ['', '   ', '/relative.png', 'data:image/png;base64,AA', 'https://u:p@example.test/x.png']) {
    assert.equal(api.isHttpUrl(value), '');
  }
  assert.equal(api.isHttpUrl('https://example.test/a.png'), 'https://example.test/a.png');
});
