import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';


const HERE = path.dirname(fileURLToPath(import.meta.url));
const STATIC = path.resolve(HERE, '..', '..');
const taskHtml = fs.readFileSync(path.join(STATIC, 'task.html'), 'utf8');
const panelSource = fs.readFileSync(path.join(STATIC, 'js', 'task-bone-correction-panel.js'), 'utf8');


test('task viewer wires corrections into the one shared mixer and playlist', () => {
    assert.match(taskHtml, /import \{ TaskBoneCorrectionPanel \} from '\/static\/js\/task-bone-correction-panel\.js\?v=1'/);
    assert.match(taskHtml, /boneCorrectionPanel\?\.setActiveClip\?\.\(entry\)/);
    assert.match(taskHtml, /configureBoneCorrectionForCurrentModel\(configuredModel\)/);
    assert.doesNotMatch(panelSource, /new\s+THREE\.AnimationMixer|setInterval\s*\(/);
});


test('frame order restores raw pose, updates mixer, applies correction, then renders', () => {
    const loopStart = taskHtml.indexOf('const correctionRuntimeEnabled =');
    const prepare = taskHtml.indexOf('boneCorrectionPanel?.beforeMixerUpdate?.()', loopStart);
    const mixer = taskHtml.indexOf('mixer.update(dt)', prepare);
    const apply = taskHtml.indexOf('boneCorrectionPanel?.afterMixerUpdate?.()', mixer);
    const render = taskHtml.indexOf('renderMainViewerFrame()', apply);
    assert.ok(loopStart > 0 && prepare > loopStart && mixer > prepare && apply > mixer && render > apply);
    assert.match(taskHtml, /currentModelType === 'animations' && !playModeController\?\.active/);
});


test('Blueprint Left switches semantic surface to shared orthographic animation view', () => {
    assert.match(taskHtml, /viewId === 'left'\s*\? new THREE\.OrthographicCamera/);
    assert.match(taskHtml, /card\.dataset\.blueprintMode !== 'animation'/);
    assert.match(panelSource, /data-blueprint-mode='animation'/);
    assert.match(panelSource, /Rig Points/);
    assert.match(panelSource, /Animation Correction/);
});


test('panel exposes numeric global and per-clip editing plus draft and publish APIs', () => {
    assert.match(panelSource, /data-scope="global"/);
    assert.match(panelSource, /data-scope="clip"/);
    assert.match(panelSource, /data-rotation="0"/);
    assert.match(panelSource, /data-position="0"/);
    assert.match(panelSource, /data-motion-range/);
    assert.match(panelSource, /method: 'PUT'/);
    assert.match(panelSource, /animation-corrections\/publish/);
    assert.match(panelSource, /animation-corrections\/export\/retry/);
    assert.match(panelSource, /beginBatch\('ik-drag'\)/);
    assert.match(panelSource, /_solveCcd\(/);
});
