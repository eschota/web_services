import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../animation-preview-policy.js', import.meta.url), 'utf8');
const policy = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

test('loads an animal variant only after an explicit unmatched selection', () => {
    assert.equal(policy.shouldLoadAnimalVariantPreview({
        previewUrl: '/api/task/id/animations/preview/dog_run',
        isAnimalTask: true,
    }), true);
    assert.equal(policy.shouldLoadAnimalVariantPreview({
        previewUrl: '/api/task/id/animations/preview/dog_run',
        isAnimalTask: true,
        automatic: true,
    }), false);
    assert.equal(policy.shouldLoadAnimalVariantPreview({
        previewUrl: '/api/task/id/animations/preview/dog_run',
        isAnimalTask: true,
        embeddedMatched: true,
    }), false);
});

test('does not use the animal variant path for humanoid clips', () => {
    assert.equal(policy.shouldLoadAnimalVariantPreview({
        previewUrl: '/api/task/id/animations/preview/walk',
        isAnimalTask: false,
    }), false);
});
