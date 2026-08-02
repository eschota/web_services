import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

function sourceBlock(startMarker, endMarker) {
    const start = source.indexOf(startMarker);
    const end = source.indexOf(endMarker, start + startMarker.length);
    assert.ok(start >= 0, `missing start marker: ${startMarker}`);
    assert.ok(end > start, `missing end marker after: ${startMarker}`);
    return source.slice(start, end);
}

test('initial task hydration runs once even when the page booted in a hidden tab', () => {
    assert.match(source, /await this\.fetchTask\(\{ allowHidden: true \}\);/);

    const fetchTask = sourceBlock('async fetchTask(options = {})', 'cardInfo: null');
    assert.match(fetchTask, /const allowHidden = options\?\.allowHidden === true;/);
    assert.match(fetchTask, /document\.hidden && !allowHidden/);
    assert.match(fetchTask, /await this\.applyTaskState\(taskState, \{ refreshFinalLog: true \}\)/);
});

test('viewer task refresh renders terminal state instead of only poisoning the shared cache', () => {
    const latestTask = sourceBlock('async function getLatestTaskState(options = {})', 'function taskHasReadyAnimations');
    assert.match(latestTask, /await window\.TaskUI\.applyTaskState\(t, \{ refreshFinalLog: false \}\)/);
    assert.doesNotMatch(latestTask, /window\.TaskUI\.task\s*=\s*t/);

    const applyState = sourceBlock('async applyTaskState(taskState, options = {})', 'async fetchTask(options = {})');
    assert.match(applyState, /this\.task = taskState;/);
    assert.match(applyState, /await this\.updateUI\(\);/);
    assert.match(applyState, /taskState\.status === 'done' \|\| taskState\.status === 'error'/);
    assert.match(applyState, /this\.stopPolling\(\);/);
    assert.doesNotMatch(applyState, /setInterval\(/);
});
