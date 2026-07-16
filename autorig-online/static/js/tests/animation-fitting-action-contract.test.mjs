import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
    ANIMATION_FITTING_ACTION_CONTRACTS,
    resolveAnimationFittingAction,
} from '../animation-fitting-action-contract.js';

test('browser action contracts exactly match the canonical 30-action prompt spec', async () => {
    const path = new URL('../../../backend/animation_fitting/specs/action_prompts.v1.json', import.meta.url);
    const spec = JSON.parse(await readFile(path, 'utf8'));
    const expected = Object.fromEntries(spec.actions_array.map((row) => [
        row.action_id_string,
        {
            generationMode: row.generation_mode_string,
            frameCount: row.frame_count_int,
        },
    ]));
    assert.equal(Object.keys(expected).length, 30);
    assert.deepEqual(ANIMATION_FITTING_ACTION_CONTRACTS, expected);
});

test('legacy four-button ids resolve to exact canonical action contracts', () => {
    assert.deepEqual(
        resolveAnimationFittingAction('idle'),
        {
            actionId: 'idle_neutral', generationMode: 'loop', frameCount: 97,
            isLoop: true, isOneShot: false, requestedId: 'idle', aliasApplied: true,
        },
    );
    assert.equal(resolveAnimationFittingAction('walk').actionId, 'walk_forward');
    assert.equal(resolveAnimationFittingAction('run').actionId, 'run');
    assert.equal(resolveAnimationFittingAction('die').actionId, 'death');
});

test('fall stays one-shot and caller contract mismatches fail closed', () => {
    assert.equal(resolveAnimationFittingAction('fall').isOneShot, true);
    assert.throws(
        () => resolveAnimationFittingAction('fall', { loop: true }),
        /requires one_shot/,
    );
    assert.throws(
        () => resolveAnimationFittingAction('death', { frameCount: 49 }),
        /requires 65 frames/,
    );
    assert.throws(() => resolveAnimationFittingAction('unknown'), /Unknown animation-fitting action/);
});
