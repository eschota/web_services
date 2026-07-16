#!/usr/bin/env node
/**
 * Author the two immutable local-input pin manifests consumed by the V14
 * browser fitting spec author.  This program only snapshots local files and
 * atomically publishes JSON; it never executes subprocesses or stages.
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import {
    V14_RUNTIME_PINS_SCHEMA,
    V14_TOOL_SOURCE_PINS_SCHEMA,
} from './author_v14_browser_fitting_spec.mjs';
import { V14_PIPELINE_TOOL_SOURCE_PATHS } from './run_v14_browser_fitting_pipeline.mjs';

const EXECUTABLE_NAMES = Object.freeze(['python', 'node', 'chrome', 'ffmpeg', 'ffprobe']);
const OUTPUT_FILENAMES = Object.freeze(['runtime-pins.json', 'tool-source-pins.json']);

function nonEmptyString(value, field) {
    if (typeof value !== 'string' || !value.trim()) throw new Error(`${field} must be a non-empty string`);
    return value.trim();
}

function hash(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function normalizedPathKey(filename) {
    const resolved = path.resolve(filename);
    return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function isInside(parent, child) {
    const relative = path.relative(path.resolve(parent), path.resolve(child));
    return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function readSnapshot(filenameValue, field, dependencies, readIndex) {
    const filename = path.resolve(filenameValue);
    let before;
    let buffer;
    let after;
    try {
        before = fs.statSync(filename);
        buffer = fs.readFileSync(filename);
        dependencies.afterFileRead?.({ path: filename, field, readIndex });
        after = fs.statSync(filename);
    } catch (error) {
        throw new Error(`${field} is unavailable at ${filename}: ${error.message}`);
    }
    if (!before.isFile() || !after.isFile() || buffer.length < 1) throw new Error(`${field} must be a non-empty file`);
    if (before.size !== buffer.length || after.size !== buffer.length
        || before.dev !== after.dev || before.ino !== after.ino || before.mtimeMs !== after.mtimeMs) {
        throw new Error(`${field} changed while its single immutable snapshot was read`);
    }
    return { path: filename, bytes: buffer.length, sha256: hash(buffer), buffer };
}

function descriptor(snapshot) {
    return { path: snapshot.path, bytes: snapshot.bytes, sha256: snapshot.sha256 };
}

function jsonPayload(value) {
    return Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function assertUniquePaths(rows) {
    const seen = new Map();
    for (const row of rows) {
        const key = normalizedPathKey(row.path);
        if (seen.has(key)) throw new Error(`duplicate input path for ${seen.get(key)} and ${row.field}: ${path.resolve(row.path)}`);
        seen.set(key, row.field);
    }
}

function validateThreeRevision(snapshot) {
    const source = snapshot.buffer.toString('utf8');
    const matches = [...source.matchAll(/\bconst\s+REVISION\s*=\s*['"]([^'"]+)['"]\s*;/g)];
    if (matches.length !== 1 || matches[0][1] !== '160') {
        throw new Error('Three module must declare exactly const REVISION = \'160\'');
    }
}

function validateTrackingRuntimeLock(snapshot) {
    let value;
    try { value = JSON.parse(snapshot.buffer.toString('utf8')); } catch (error) {
        throw new Error(`tracking runtime lock is invalid JSON: ${error.message}`);
    }
    if (!value || typeof value !== 'object' || Array.isArray(value)
        || value.schema !== 'autorig-tracking-runtime-lock.v1'
        || !value.repos || typeof value.repos !== 'object' || Array.isArray(value.repos)
        || !Object.keys(value.repos).length
        || !value.checkpoints || typeof value.checkpoints !== 'object' || Array.isArray(value.checkpoints)
        || !Object.keys(value.checkpoints).length
        || !value.python || typeof value.python !== 'object' || Array.isArray(value.python)) {
        throw new Error('tracking runtime lock must be the non-empty autorig-tracking-runtime-lock.v1 contract');
    }
}

function validateOutputTarget(outputDirectoryValue, inputRows) {
    const outputDirectory = path.resolve(nonEmptyString(outputDirectoryValue, 'outputDirectory'));
    const parent = path.dirname(outputDirectory);
    if (!fs.existsSync(parent) || !fs.statSync(parent).isDirectory()) {
        throw new Error(`output directory parent must already exist: ${parent}`);
    }
    if (fs.existsSync(outputDirectory)) throw new Error(`output directory already exists: ${outputDirectory}`);
    const outputKey = normalizedPathKey(outputDirectory);
    for (const row of inputRows) {
        if (normalizedPathKey(row.path) === outputKey) throw new Error('output directory duplicates an input path');
    }
    return outputDirectory;
}

export function buildV14InputPinPayloads(configValue, dependencies = {}) {
    const config = configValue && typeof configValue === 'object' && !Array.isArray(configValue)
        ? configValue : (() => { throw new Error('config must be an object'); })();
    const toolNames = Object.keys(V14_PIPELINE_TOOL_SOURCE_PATHS).sort();
    if (toolNames.length !== 28) throw new Error('runner must export the exact 28-file V14 tool-source closure');
    const executableRows = EXECUTABLE_NAMES.map((name) => ({
        field: `runtime executable ${name}`,
        path: path.resolve(nonEmptyString(config[name], name)),
        kind: 'executable',
        name,
    }));
    const threeRow = {
        field: 'Three r160 module',
        path: path.resolve(nonEmptyString(config.threeModule, 'threeModule')),
        kind: 'three',
    };
    const lockRow = {
        field: 'tracking runtime lock',
        path: path.resolve(nonEmptyString(config.trackingRuntimeLock, 'trackingRuntimeLock')),
        kind: 'trackingLock',
    };
    const toolRows = toolNames.map((name) => ({
        field: `tool source ${name}`,
        path: path.resolve(V14_PIPELINE_TOOL_SOURCE_PATHS[name]),
        kind: 'tool',
        name,
    }));
    const fileRows = [...executableRows, threeRow, lockRow, ...toolRows];
    assertUniquePaths(fileRows);
    const outputDirectory = validateOutputTarget(config.outputDirectory, fileRows);
    const trackingRuntimeRoot = path.resolve(nonEmptyString(config.trackingRuntimeRoot, 'trackingRuntimeRoot'));
    if (!fs.existsSync(trackingRuntimeRoot) || !fs.statSync(trackingRuntimeRoot).isDirectory()) {
        throw new Error(`trackingRuntimeRoot must be an existing directory: ${trackingRuntimeRoot}`);
    }
    if (normalizedPathKey(trackingRuntimeRoot) === normalizedPathKey(outputDirectory)) {
        throw new Error('output directory cannot replace trackingRuntimeRoot');
    }
    if (isInside(trackingRuntimeRoot, outputDirectory)) {
        throw new Error('output directory cannot be created inside trackingRuntimeRoot');
    }

    const snapshots = new Map();
    fileRows.forEach((row, index) => {
        snapshots.set(row, readSnapshot(row.path, row.field, dependencies, index));
    });
    const threeSnapshot = snapshots.get(threeRow);
    validateThreeRevision(threeSnapshot);
    validateTrackingRuntimeLock(snapshots.get(lockRow));
    const runtimePins = {
        schema: V14_RUNTIME_PINS_SCHEMA,
        executables: Object.fromEntries(executableRows.map((row) => [row.name, descriptor(snapshots.get(row))])),
        threeModule: { ...descriptor(threeSnapshot), revision: '160' },
        trackingRuntimeRoot,
        trackingRuntimeLock: descriptor(snapshots.get(lockRow)),
    };
    const toolSourcePins = {
        schema: V14_TOOL_SOURCE_PINS_SCHEMA,
        sources: Object.fromEntries(toolRows.map((row) => [row.name, descriptor(snapshots.get(row))])),
    };
    return {
        outputDirectory,
        runtimePayload: jsonPayload(runtimePins),
        toolSourcePayload: jsonPayload(toolSourcePins),
        runtimePins,
        toolSourcePins,
        inputReadCount: fileRows.length,
    };
}

function writeExclusive(filename, payload) {
    const handle = fs.openSync(filename, 'wx');
    try {
        fs.writeFileSync(handle, payload);
        fs.fsyncSync(handle);
    } finally {
        fs.closeSync(handle);
    }
}

function removeVerifiedStagingDirectory(staging, parent, base) {
    const resolved = path.resolve(staging);
    const expectedParent = path.resolve(parent);
    const expectedPrefix = `.${base}.staging-`;
    if (normalizedPathKey(path.dirname(resolved)) !== normalizedPathKey(expectedParent)
        || !path.basename(resolved).startsWith(expectedPrefix)) {
        throw new Error(`refusing to remove unverified staging directory: ${resolved}`);
    }
    if (fs.existsSync(resolved)) fs.rmSync(resolved, { recursive: true, force: true });
}

function publishDirectory(built, dependencies) {
    const outputDirectory = built.outputDirectory;
    const parent = path.dirname(outputDirectory);
    const base = path.basename(outputDirectory);
    const staging = fs.mkdtempSync(path.join(parent, `.${base}.staging-`));
    try {
        writeExclusive(path.join(staging, OUTPUT_FILENAMES[0]), built.runtimePayload);
        writeExclusive(path.join(staging, OUTPUT_FILENAMES[1]), built.toolSourcePayload);
        const inventory = fs.readdirSync(staging, { withFileTypes: true });
        const names = inventory.map((entry) => entry.name).sort();
        if (inventory.some((entry) => !entry.isFile())
            || JSON.stringify(names) !== JSON.stringify([...OUTPUT_FILENAMES].sort())) {
            throw new Error('staged input-pin directory contains unexpected entries');
        }
        dependencies.beforeRename?.({ staging, outputDirectory });
        fs.renameSync(staging, outputDirectory);
    } catch (error) {
        try { removeVerifiedStagingDirectory(staging, parent, base); } catch { /* keep original */ }
        throw error;
    }
    return {
        outputDirectory,
        runtimePins: {
            path: path.join(outputDirectory, OUTPUT_FILENAMES[0]),
            bytes: built.runtimePayload.length,
            sha256: hash(built.runtimePayload),
        },
        toolSourcePins: {
            path: path.join(outputDirectory, OUTPUT_FILENAMES[1]),
            bytes: built.toolSourcePayload.length,
            sha256: hash(built.toolSourcePayload),
        },
        inputReadCount: built.inputReadCount,
    };
}

export function authorV14InputPins(config, dependencies = {}) {
    const built = buildV14InputPinPayloads(config, dependencies);
    return publishDirectory(built, dependencies);
}

export function parseInputPinArgs(argv) {
    const allowed = new Set([
        '--python', '--node', '--chrome', '--ffmpeg', '--ffprobe', '--three-module',
        '--tracking-runtime-root', '--tracking-runtime-lock', '--output-dir',
    ]);
    const values = {};
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        if (flag === '--help' || flag === '-h') { help = true; continue; }
        if (!allowed.has(flag)) throw new Error(`unknown option ${flag}`);
        if (values[flag] != null) throw new Error(`duplicate option ${flag}`);
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) throw new Error(`${flag} requires a value`);
        values[flag] = argv[++index];
    }
    if (help) return { help: true };
    for (const flag of allowed) if (values[flag] == null) throw new Error(`${flag} is required`);
    return {
        python: values['--python'], node: values['--node'], chrome: values['--chrome'],
        ffmpeg: values['--ffmpeg'], ffprobe: values['--ffprobe'], threeModule: values['--three-module'],
        trackingRuntimeRoot: values['--tracking-runtime-root'],
        trackingRuntimeLock: values['--tracking-runtime-lock'],
        outputDirectory: values['--output-dir'],
    };
}

function helpText() {
    return `Usage:
  node author_v14_browser_fitting_input_pins.mjs \\
    --python FILE --node FILE --chrome FILE --ffmpeg FILE --ffprobe FILE \\
    --three-module FILE --tracking-runtime-root DIR \\
    --tracking-runtime-lock FILE --output-dir NEW_DIRECTORY

Creates exactly runtime-pins.json (${V14_RUNTIME_PINS_SCHEMA}) and
tool-source-pins.json (${V14_TOOL_SOURCE_PINS_SCHEMA}) for the exact 28-file
closure exported by the V14 runner. Every input file is read exactly once.
No subprocess, GPU stage, Blender, database, or network operation is used.`;
}

export function runInputPinCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseInputPinArgs(argv);
        if (config.help) { streams.stdout.write(`${helpText()}\n`); return 0; }
        const result = authorV14InputPins(config);
        streams.stdout.write(`${JSON.stringify({ status: 'AUTHORED', ...result })}\n`);
        return 0;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) process.exitCode = runInputPinCli();
