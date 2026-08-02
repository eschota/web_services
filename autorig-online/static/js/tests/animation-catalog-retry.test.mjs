import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

delete globalThis.AnimationCatalogRetryPolicy;
await import('../animation-catalog-retry.js');

const policy = globalThis.AnimationCatalogRetryPolicy;

test('done catalog with entries but no ready files uses a bounded backoff', () => {
    const pendingCatalog = {
        animations: [
            { id: 'walking', available: false, ready: false },
            { id: 'running', available: false, ready: false },
        ],
    };

    assert.equal(policy.shouldRetry('done', pendingCatalog), true);
    assert.equal(policy.shouldRetry('processing', pendingCatalog), false);
    assert.equal(policy.shouldRetry('done', { animations: [] }), false);
    assert.equal(
        policy.shouldRetry('done', {
            animations: [{ id: 'walking', available: true, ready: true }],
        }),
        false,
    );

    assert.deepEqual(policy.RETRY_DELAYS_MS, [1500, 4000, 9000, 20000, 40000]);
    policy.RETRY_DELAYS_MS.forEach((delay, attempt) => {
        assert.equal(policy.delayForAttempt(attempt), delay);
    });
    assert.equal(policy.delayForAttempt(policy.RETRY_DELAYS_MS.length), null);
    assert.equal(policy.delayForAttempt(-1), null);
});

test('task page refreshes stale zero-ready catalogs and ignores stale responses', async () => {
    const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

    assert.match(taskHtml, /animation-catalog-retry\.js\?v=1/);
    assert.match(taskHtml, /scheduleAnimationCatalogRetry\(stateHash, catalog/);
    assert.match(taskHtml, /delayForAttempt\(this\.animationCatalogRetryAttempt\)/);
    assert.match(taskHtml, /void this\.loadAnimationCatalog\(true\)/);
    assert.match(taskHtml, /fetch\(`\/api\/task\/\$\{this\.taskId\}\/animations\/catalog\$\{query\}`, \{ cache: 'no-store' \}\)/);
    assert.match(taskHtml, /requestSequence !== this\.animationCatalogRequestSequence/);
});

test('successful GLB and FBX animation loads force a catalog refresh', async () => {
    const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');
    const refreshCalls = [
        ...taskHtml.matchAll(/refreshAnimationCatalogAfterViewerAssetLoad\?\.\(([^\n]+)\)/g),
    ].map((match) => match[1]);

    assert.ok(refreshCalls.some((call) => call.includes('${label} GLB')));
    assert.ok(refreshCalls.some((call) => call.includes('${label} FBX')));
    assert.ok(refreshCalls.some((call) => call.includes('background Animations GLB')));
    assert.match(
        taskHtml,
        /async refreshAnimationCatalogAfterViewerAssetLoad[\s\S]*?await this\.loadAnimationCatalog\(true\)/,
    );
});
