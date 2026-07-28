import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

function extractFunction(startMarker, endMarker) {
    const start = taskHtml.indexOf(startMarker);
    const end = taskHtml.indexOf(endMarker, start + startMarker.length);
    assert.notEqual(start, -1, `missing ${startMarker}`);
    assert.notEqual(end, -1, `missing ${endMarker}`);
    return taskHtml.slice(start, end).trim();
}

const selectedManifestSource = extractFunction(
    'function selectedAnimationManifestUrl()',
    'async function loadAnimationPlaylistManifest()',
);
const loadManifestSource = extractFunction(
    'async function loadAnimationPlaylistManifest()',
    'function configureAnimationPlaylistForCurrentModel(',
);

function selectedManifestUrl(taskUi, taskId = 'task/id') {
    const factory = new Function(
        'window',
        'taskId',
        `return (${selectedManifestSource});`,
    );
    return factory({ TaskUI: taskUi }, taskId)();
}

test('animal viewer uses only the selected variant explicit animation manifest URL', () => {
    const animalTaskUi = {
        isAnimalTask: () => true,
        animalAnimationCatalogParams: () => ({ animal_type: 'Cat', orientation: 'Front' }),
        animalVariantsData: {
            variants: [
                {
                    animal_type: 'cat',
                    orientation: 'front',
                    animation_manifest_url: '/worker/cat-front/animation-manifest.json',
                },
                {
                    animal_type: 'cat',
                    orientation: 'side',
                    animation_manifest_url: '/worker/cat-side/animation-manifest.json',
                },
            ],
        },
    };

    assert.equal(
        selectedManifestUrl(animalTaskUi),
        '/worker/cat-front/animation-manifest.json',
    );

    animalTaskUi.animalVariantsData.variants[0] = {
        animal_type: 'cat',
        orientation: 'front',
    };
    assert.equal(selectedManifestUrl(animalTaskUi), null);
    assert.doesNotMatch(selectedManifestSource, /animation-manifest\?\$\{/);
    assert.doesNotMatch(selectedManifestSource, /URLSearchParams/);
});

test('non-animal viewer keeps the task animation manifest endpoint', () => {
    assert.equal(
        selectedManifestUrl({ isAnimalTask: () => false }, 'abc/123'),
        '/api/task/abc%2F123/animation-manifest',
    );
});

test('null animal manifest URL returns before cache lookup or fetch', async () => {
    let fetchCalls = 0;
    const cache = new Map();
    const factory = new Function(
        'selectedAnimationManifestUrl',
        'animationManifestCache',
        'fetch',
        'console',
        `return (${loadManifestSource});`,
    );
    const loadManifest = factory(
        () => null,
        cache,
        async () => {
            fetchCalls += 1;
            throw new Error('fetch must not be called');
        },
        console,
    );

    assert.equal(await loadManifest(), null);
    assert.equal(fetchCalls, 0);
    assert.equal(cache.size, 0);
});
