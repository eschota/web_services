import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    V14_RUNTIME_PINS_SCHEMA,
    V14_TOOL_SOURCE_PINS_SCHEMA,
} from '../author_v14_browser_fitting_spec.mjs';
import {
    authorV14InputPins,
    buildV14InputPinPayloads,
    parseInputPinArgs,
} from '../author_v14_browser_fitting_input_pins.mjs';
import { V14_PIPELINE_TOOL_SOURCE_PATHS } from '../run_v14_browser_fitting_pipeline.mjs';

const digest = (buffer) => crypto.createHash('sha256').update(buffer).digest('hex');

function write(filename, value) {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8');
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, buffer);
    return path.resolve(filename);
}

function fixture() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-input-pins-'));
    const bin = path.join(root, 'bin');
    const config = {
        python: write(path.join(bin, 'python.exe'), 'python-runtime'),
        node: write(path.join(bin, 'node.exe'), 'node-runtime'),
        chrome: write(path.join(bin, 'chrome.exe'), 'chrome-runtime'),
        ffmpeg: write(path.join(bin, 'ffmpeg.exe'), 'ffmpeg-runtime'),
        ffprobe: write(path.join(bin, 'ffprobe.exe'), 'ffprobe-runtime'),
        threeModule: write(path.join(root, 'three.module.js'), "const REVISION = '160';\nexport { REVISION };\n"),
        trackingRuntimeRoot: path.join(root, 'tracking-runtime'),
        trackingRuntimeLock: '',
        outputDirectory: path.join(root, 'pins-v1'),
    };
    fs.mkdirSync(config.trackingRuntimeRoot);
    config.trackingRuntimeLock = write(path.join(config.trackingRuntimeRoot, 'runtime-lock.json'), JSON.stringify({
        schema: 'autorig-tracking-runtime-lock.v1',
        repos: { tapnet: { commit: 'a'.repeat(40) } },
        checkpoints: { sam2: { sha256: 'b'.repeat(64), bytes: 1 } },
        python: { version: '3.10' },
    }));
    return { root, config };
}

test('CLI is explicit, side-effect free on help, and requires every runtime path', () => {
    assert.deepEqual(parseInputPinArgs(['--help']), { help: true });
    assert.throws(() => parseInputPinArgs([]), /--python is required/);
    assert.throws(() => parseInputPinArgs(['--python', 'a', '--python', 'b']), /duplicate option --python/);
});

test('authors exactly two deterministic immutable manifests and reads every input file once', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const reads = new Map();
    const afterFileRead = ({ path: filename }) => reads.set(filename, (reads.get(filename) || 0) + 1);
    const first = authorV14InputPins(f.config, { afterFileRead });
    assert.equal(first.inputReadCount, 35);
    assert.equal(reads.size, 35);
    assert.ok([...reads.values()].every((count) => count === 1));
    assert.deepEqual(fs.readdirSync(f.config.outputDirectory).sort(), ['runtime-pins.json', 'tool-source-pins.json']);
    const runtimeBuffer = fs.readFileSync(first.runtimePins.path);
    const toolsBuffer = fs.readFileSync(first.toolSourcePins.path);
    assert.equal(runtimeBuffer.length, first.runtimePins.bytes);
    assert.equal(digest(runtimeBuffer), first.runtimePins.sha256);
    assert.equal(toolsBuffer.length, first.toolSourcePins.bytes);
    assert.equal(digest(toolsBuffer), first.toolSourcePins.sha256);
    const runtime = JSON.parse(runtimeBuffer);
    const tools = JSON.parse(toolsBuffer);
    assert.equal(runtime.schema, V14_RUNTIME_PINS_SCHEMA);
    assert.equal(runtime.threeModule.revision, '160');
    assert.equal(runtime.trackingRuntimeRoot, path.resolve(f.config.trackingRuntimeRoot));
    assert.equal(tools.schema, V14_TOOL_SOURCE_PINS_SCHEMA);
    assert.deepEqual(Object.keys(tools.sources).sort(), Object.keys(V14_PIPELINE_TOOL_SOURCE_PATHS).sort());
    assert.equal(Object.keys(tools.sources).length, 28);

    const secondConfig = { ...f.config, outputDirectory: path.join(f.root, 'pins-v1-copy') };
    const second = authorV14InputPins(secondConfig);
    assert.equal(second.runtimePins.sha256, first.runtimePins.sha256);
    assert.equal(second.toolSourcePins.sha256, first.toolSourcePins.sha256);
    assert.throws(() => authorV14InputPins(f.config), /output directory already exists/);
    assert.deepEqual(fs.readdirSync(f.config.outputDirectory).sort(), ['runtime-pins.json', 'tool-source-pins.json']);
});

test('missing input fails before any output or staging directory is published', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    fs.unlinkSync(f.config.node);
    assert.throws(() => authorV14InputPins(f.config), /runtime executable node is unavailable/);
    assert.equal(fs.existsSync(f.config.outputDirectory), false);
    assert.equal(fs.readdirSync(f.root).some((name) => name.includes('.pins-v1.staging-')), false);
});

test('duplicate paths are rejected before any file is read', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const reads = [];
    const config = { ...f.config, node: f.config.python };
    assert.throws(
        () => buildV14InputPinPayloads(config, { afterFileRead: (row) => reads.push(row) }),
        /duplicate input path/,
    );
    assert.equal(reads.length, 0);
    assert.equal(fs.existsSync(f.config.outputDirectory), false);
});

test('in-read tampering is detected from the one snapshot and leaves no output', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    let changed = false;
    assert.throws(() => authorV14InputPins(f.config, {
        afterFileRead: ({ field, path: filename }) => {
            if (!changed && field === 'Three r160 module') {
                changed = true;
                fs.appendFileSync(filename, 'tamper');
            }
        },
    }), /Three r160 module changed while its single immutable snapshot was read/);
    assert.equal(fs.existsSync(f.config.outputDirectory), false);
});

test('wrong Three revision is rejected without evaluating the module', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    fs.writeFileSync(f.config.threeModule, "const REVISION = '159';\n");
    assert.throws(() => authorV14InputPins(f.config), /Three module must declare exactly/);
    assert.equal(fs.existsSync(f.config.outputDirectory), false);
});

test('tracking root and lock must remain a semantic runtime contract', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    fs.writeFileSync(f.config.trackingRuntimeLock, '{"schema":"wrong"}\n');
    assert.throws(() => authorV14InputPins(f.config), /autorig-tracking-runtime-lock\.v1 contract/);
    assert.equal(fs.existsSync(f.config.outputDirectory), false);
    const insideConfig = { ...f.config, outputDirectory: path.join(f.config.trackingRuntimeRoot, 'pins') };
    fs.writeFileSync(f.config.trackingRuntimeLock, JSON.stringify({
        schema: 'autorig-tracking-runtime-lock.v1', repos: { a: {} }, checkpoints: { b: {} }, python: {},
    }));
    assert.throws(() => authorV14InputPins(insideConfig), /cannot be created inside trackingRuntimeRoot/);
});

test('output-directory race never overwrites or deletes the winner', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const sentinel = path.join(f.config.outputDirectory, 'race-winner.txt');
    assert.throws(() => authorV14InputPins(f.config, {
        beforeRename: ({ outputDirectory }) => {
            fs.mkdirSync(outputDirectory);
            fs.writeFileSync(sentinel, 'winner');
        },
    }));
    assert.equal(fs.readFileSync(sentinel, 'utf8'), 'winner');
    assert.deepEqual(fs.readdirSync(f.config.outputDirectory), ['race-winner.txt']);
    assert.equal(fs.readdirSync(f.root).some((name) => name.includes('.pins-v1.staging-')), false);
});
