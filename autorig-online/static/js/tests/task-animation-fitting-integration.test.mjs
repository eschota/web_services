import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';


const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

function extractFunctionBody(source, assignmentMarker) {
    const markerIndex = source.indexOf(assignmentMarker);
    assert.ok(markerIndex >= 0, `missing function assignment: ${assignmentMarker}`);

    const signatureTail = source.slice(markerIndex + assignmentMarker.length);
    const signatureEnd = signatureTail.search(/\)\s*\{/);
    assert.ok(signatureEnd >= 0, `missing function signature terminator after: ${assignmentMarker}`);
    const openBrace = markerIndex + assignmentMarker.length
        + signatureEnd + signatureTail.slice(signatureEnd).indexOf('{');
    assert.ok(openBrace >= 0, `missing opening brace after: ${assignmentMarker}`);

    let depth = 1;
    let quote = '';
    let escaped = false;
    let lineComment = false;
    let blockComment = false;

    for (let index = openBrace + 1; index < source.length; index += 1) {
        const character = source[index];
        const next = source[index + 1];

        if (lineComment) {
            if (character === '\n' || character === '\r') lineComment = false;
            continue;
        }
        if (blockComment) {
            if (character === '*' && next === '/') {
                blockComment = false;
                index += 1;
            }
            continue;
        }
        if (quote) {
            if (escaped) escaped = false;
            else if (character === '\\') escaped = true;
            else if (character === quote) quote = '';
            continue;
        }
        if (character === '/' && next === '/') {
            lineComment = true;
            index += 1;
            continue;
        }
        if (character === '/' && next === '*') {
            blockComment = true;
            index += 1;
            continue;
        }
        if (character === "'" || character === '"' || character === '`') {
            quote = character;
            continue;
        }
        if (character === '{') depth += 1;
        if (character === '}') {
            depth -= 1;
            if (depth === 0) return source.slice(openBrace + 1, index);
        }
    }

    assert.fail(`unterminated function body: ${assignmentMarker}`);
}

const dispatcherBody = extractFunctionBody(
    taskHtml,
    'window.startAnimalAnimationFittingFromVideo = async function startAnimalAnimationFittingFromVideo',
);
const legacyFittingBody = extractFunctionBody(
    taskHtml,
    'async function startLegacyRgbAnimationFittingFromVideo',
);
const semanticFittingBody = extractFunctionBody(
    taskHtml,
    'async function startSemanticBrowserAnimationFittingFromVideo',
);

test('task viewer imports the complete browser-first fitting pipeline from real modules', () => {
    assert.match(
        taskHtml,
        /import\s*\{[\s\S]*?buildThreeAnimationClip[\s\S]*?decodeVideoFramesExact[\s\S]*?mapHorseSemanticPalette[\s\S]*?\}\s*from '\/static\/js\/task-animation-fitting-panel\.js\?v=1'/,
    );
    assert.match(
        taskHtml,
        /import\s*\{[\s\S]*?assessHorseWalkGait[\s\S]*?buildSemanticObservations[\s\S]*?\}\s*from '\/static\/js\/animation-fitting-semantic-tracker\.js\?v=1'/,
    );
    assert.match(
        taskHtml,
        /import\s*\{\s*fitBrowserAnimation\s*\}\s*from '\/static\/js\/animation-fitting-browser-core\.js\?v=1'/,
    );
    assert.match(
        taskHtml,
        /import\s*\{[\s\S]*?buildHorse2BrowserFittingSkeleton[\s\S]*?\}\s*from '\/static\/js\/animation-fitting-three-adapter\.js\?v=1'/,
    );
    assert.match(
        taskHtml,
        /import\s*\{\s*resolveAnimationFittingAction\s*\}\s*from '\/static\/js\/animation-fitting-action-contract\.js\?v=1'/,
    );
});

test('semantic entrypoint executes decode, tracking, constrained fitting, and shared-model apply in order', () => {
    const expectedCalls = [
        'buildHorse2BrowserFittingSkeleton',
        'resolveAnimationFittingAction',
        'decodeVideoFramesExact',
        'mapHorseSemanticPalette',
        'buildSemanticObservations',
        'assessHorseWalkGait',
        'fitBrowserAnimation',
        'buildThreeAnimationClip',
        'applyAnimationClipsToCurrentModel',
    ];
    let previousIndex = -1;

    for (const functionName of expectedCalls) {
        const callIndex = semanticFittingBody.search(new RegExp(`\\b${functionName}\\s*\\(`));
        assert.ok(callIndex >= 0, `entrypoint must call ${functionName}`);
        assert.ok(
            callIndex > previousIndex,
            `${functionName} must run after ${expectedCalls[expectedCalls.indexOf(functionName) - 1] || 'entrypoint start'}`,
        );
        previousIndex = callIndex;
    }

    assert.match(semanticFittingBody, /const\s+requestedFrameCount\s*=\s*actionContract\.frameCount/);
    assert.match(semanticFittingBody, /const\s+oneShot\s*=\s*actionContract\.isOneShot/);
    assert.match(semanticFittingBody, /const\s+decodedVideo\s*=\s*await\s+decodeVideoFramesExact\(opts\.videoUrl,\s*\{[\s\S]*?frameCount:\s*requestedFrameCount[\s\S]*?fps:\s*30/);
    assert.match(semanticFittingBody, /const\s+frames\s*=\s*decodedVideo\?\.frames/);
    assert.match(semanticFittingBody, /const\s+nextClips\s*=\s*\[[\s\S]*?clip[\s\S]*?\];[\s\S]*?applyAnimationClipsToCurrentModel\(nextClips,\s*\[clip\.name\]\)/);
});

test('semantic entrypoint has no legacy server proxy and creates no independent mixer', () => {
    assert.doesNotMatch(semanticFittingBody, /\bfetch\s*\(/);
    assert.doesNotMatch(semanticFittingBody, /\b(?:XMLHttpRequest|WebSocket|EventSource)\b/);
    assert.doesNotMatch(semanticFittingBody, /\/api\/(?:animation[-_]?fitting|fit[-_]?animation|fitted[-_]?animation)/i);
    assert.doesNotMatch(semanticFittingBody, /\b(?:request|run|start)(?:Server|Remote|Proxy)AnimationFitting\s*\(/i);
    assert.doesNotMatch(semanticFittingBody, /new\s+(?:THREE\.)?AnimationMixer\s*\(/);
});

test('legacy RGB entrypoint preserves production motion-proxy behavior without semantic modules', () => {
    const legacyCalls = [
        'classifyAnimalSkeletonRoles',
        'loadFittingVideo',
        'extractVideoMotionProxy',
        'buildFittedClipFromMotionProxy',
        'applyAnimationClipsToCurrentModel',
    ];
    let previousIndex = -1;
    for (const functionName of legacyCalls) {
        const callIndex = legacyFittingBody.search(new RegExp(`\\b${functionName}\\s*\\(`));
        assert.ok(callIndex > previousIndex, `${functionName} must stay in production legacy order`);
        previousIndex = callIndex;
    }
    for (const semanticIdentifier of [
        'buildHorse2BrowserFittingSkeleton',
        'resolveAnimationFittingAction',
        'decodeVideoFramesExact',
        'mapHorseSemanticPalette',
        'buildSemanticObservations',
        'assessHorseWalkGait',
        'fitBrowserAnimation',
        'buildThreeAnimationClip',
    ]) {
        assert.doesNotMatch(legacyFittingBody, new RegExp(`\\b${semanticIdentifier}\\b`));
    }
    assert.match(legacyFittingBody, /const\s+oneShot\s*=\s*\/die\|death\|fall\/i/);
    assert.doesNotMatch(legacyFittingBody, /new\s+(?:THREE\.)?AnimationMixer\s*\(/);
});

test('version dispatcher keeps unversioned 41-frame callers legacy and requires explicit semantic v1', async () => {
    const AsyncFunction = Object.getPrototypeOf(async function empty() {}).constructor;
    const dispatch = new AsyncFunction(
        'opts',
        'startLegacyRgbAnimationFittingFromVideo',
        'startSemanticBrowserAnimationFittingFromVideo',
        dispatcherBody,
    );
    const calls = [];
    const legacy = async (opts) => {
        calls.push({ route: 'legacy', opts });
        return 'legacy-result';
    };
    const semantic = async (opts) => {
        calls.push({ route: 'semantic', opts });
        return 'semantic-result';
    };

    const oldCaller = { videoUrl: '/legacy-41.mp4', frame_count_int: 41, variantName: 'walk' };
    assert.equal(await dispatch(oldCaller, legacy, semantic), 'legacy-result');
    assert.equal(calls.at(-1).route, 'legacy');
    assert.equal(calls.at(-1).opts, oldCaller);

    const explicitLegacy = { ...oldCaller, pipeline_version_string: 'legacy-rgb-renderfin-v1' };
    assert.equal(await dispatch(explicitLegacy, legacy, semantic), 'legacy-result');
    assert.equal(calls.at(-1).route, 'legacy');

    const explicitSemantic = {
        videoUrl: '/semantic-49.mp4',
        frame_count_int: 49,
        action_id_string: 'walk_forward',
        pipeline_version_string: 'semantic-comfy-browser-v1',
    };
    assert.equal(await dispatch(explicitSemantic, legacy, semantic), 'semantic-result');
    assert.equal(calls.at(-1).route, 'semantic');
    assert.equal(calls.at(-1).opts, explicitSemantic);

    const callsBeforeUnknown = calls.length;
    await assert.rejects(
        dispatch({ ...oldCaller, pipeline_version_string: 'future-unknown-v9' }, legacy, semantic),
        /Unsupported animation-fitting pipeline: future-unknown-v9/,
    );
    assert.equal(calls.length, callsBeforeUnknown);
});

test('legacy and semantic implementations remain isolated behind the dispatcher', () => {
    assert.match(dispatcherBody, /!pipelineVersion\s*\|\|\s*pipelineVersion\s*===\s*'legacy-rgb-renderfin-v1'/);
    assert.match(dispatcherBody, /pipelineVersion\s*===\s*'semantic-comfy-browser-v1'/);
    assert.match(dispatcherBody, /throw\s+new\s+Error\(`Unsupported animation-fitting pipeline:/);
    assert.doesNotMatch(dispatcherBody, /\b(?:decodeVideoFramesExact|extractVideoMotionProxy|fitBrowserAnimation)\s*\(/);
    assert.doesNotMatch(semanticFittingBody, /\b(?:loadFittingVideo|extractVideoMotionProxy|buildFittedClipFromMotionProxy)\s*\(/);
    assert.match(
        semanticFittingBody,
        /const\s+clipName\s*=\s*`fitted_\$\{normalizeClipKey\(actionId\s*\|\|\s*variantName\s*\|\|\s*'reference'\)/,
    );
});
