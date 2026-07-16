#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import { validateContactRefitInputs } from './browser_contact_refit.mjs';

const SCHEMA = 'autorig-browser-contact-refit-input.v1';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

function string(value, field) {
    if (typeof value !== 'string' || !value.trim()) {
        throw new Error(`${field} must be a non-empty string`);
    }
    return value.trim();
}

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function sha256(value, field) {
    const result = string(value, field);
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function sha256Buffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function snapshotJson(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, field));
    let before;
    let after;
    let buffer;
    try {
        before = fs.statSync(filename);
        buffer = fs.readFileSync(filename);
        after = fs.statSync(filename);
    } catch (error) {
        throw new Error(`${field} is unavailable at ${filename}: ${error.message}`);
    }
    if (!before.isFile() || !after.isFile() || buffer.length <= 0) {
        throw new Error(`${field} must be a non-empty file`);
    }
    if (before.size !== buffer.length || after.size !== buffer.length
        || before.dev !== after.dev || before.ino !== after.ino
        || before.mtimeMs !== after.mtimeMs) {
        throw new Error(`${field} changed while its immutable bytes were read`);
    }
    let json;
    try {
        json = object(JSON.parse(buffer.toString('utf8')), field);
    } catch (error) {
        if (error.message.startsWith(`${field} must be`)) throw error;
        throw new Error(`${field} is not valid JSON: ${error.message}`);
    }
    return {
        path: filename,
        bytes: buffer.length,
        sha256: sha256Buffer(buffer),
        json,
    };
}

function fileRow(snapshot) {
    return {
        path: snapshot.path,
        bytes: snapshot.bytes,
        sha256: snapshot.sha256,
    };
}

function writeExclusive(filename, buffer) {
    const handle = fs.openSync(filename, 'wx');
    try {
        fs.writeFileSync(handle, buffer);
        fs.fsyncSync(handle);
    } finally {
        fs.closeSync(handle);
    }
}

export function authorContactRefitInputManifest(configuration) {
    const config = object(configuration, 'configuration');
    const bundleDirectory = path.resolve(string(config.bundleDirectory, 'configuration.bundleDirectory'));
    if (!fs.existsSync(bundleDirectory) || !fs.statSync(bundleDirectory).isDirectory()) {
        throw new Error(`configuration.bundleDirectory is not a directory: ${bundleDirectory}`);
    }
    const outputPath = path.resolve(string(config.outputPath, 'configuration.outputPath'));
    const outputParent = path.dirname(outputPath);
    if (!fs.existsSync(outputParent) || !fs.statSync(outputParent).isDirectory()) {
        throw new Error(`output parent does not exist: ${outputParent}`);
    }
    if (fs.existsSync(outputPath)) throw new Error(`output manifest already exists: ${outputPath}`);

    const files = {
        observations: snapshotJson(config.observationsPath, 'observations'),
        bridgeReport: snapshotJson(config.bridgeReportPath, 'bridgeReport'),
        initialFitSummary: snapshotJson(config.initialFitSummaryPath, 'initialFitSummary'),
        contactDiagnostic: snapshotJson(config.contactDiagnosticPath, 'contactDiagnostic'),
    };
    const initialInputs = object(files.initialFitSummary.json.inputs, 'initialFitSummary.inputs');
    const manifest = {
        schema: SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        inputs: {
            bundleDirectory,
            observations: fileRow(files.observations),
            bridgeReport: fileRow(files.bridgeReport),
            initialFitSummary: fileRow(files.initialFitSummary),
            contactDiagnostic: fileRow(files.contactDiagnostic),
        },
        pins: {
            observationsSha256: files.observations.sha256,
            bridgeReportSha256: files.bridgeReport.sha256,
            initialFitSummarySha256: files.initialFitSummary.sha256,
            diagnosticSha256: files.contactDiagnostic.sha256,
            sourceVideoSha256: sha256(initialInputs.sourceVideoSha256, 'initialFitSummary.inputs.sourceVideoSha256'),
            fittingBundleSha256: sha256(initialInputs.fittingBundleSha256, 'initialFitSummary.inputs.fittingBundleSha256'),
            immutableManifestSha256: sha256(initialInputs.immutableManifestSha256, 'initialFitSummary.inputs.immutableManifestSha256'),
            sourceModelSha256: sha256(initialInputs.sourceModelSha256, 'initialFitSummary.inputs.sourceModelSha256'),
            sourceSkeletonSha256: sha256(initialInputs.skeletonSha256, 'initialFitSummary.inputs.skeletonSha256'),
        },
    };
    const payload = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
    const integrity = { path: outputPath, bytes: payload.length, sha256: sha256Buffer(payload) };
    const stagingPath = `${outputPath}.staging-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    try {
        writeExclusive(stagingPath, payload);
        validateContactRefitInputs({
            inputManifestPath: stagingPath,
            expectedManifestSha256: integrity.sha256,
        });
        // link() is an atomic create-if-absent operation. Unlike rename(), it
        // cannot replace an output that appears after the initial existence
        // check, so a concurrent author never overwrites immutable evidence.
        fs.linkSync(stagingPath, outputPath);
        try {
            fs.unlinkSync(stagingPath);
        } catch {
            // The immutable output has already been atomically published. A
            // stale staging hardlink is cleanup-only and must not turn a valid
            // publication into a false command failure.
        }
    } catch (error) {
        try {
            if (fs.existsSync(stagingPath)) fs.unlinkSync(stagingPath);
        } catch {
            // Preserve the original fail-closed error.
        }
        throw error;
    }
    return { ...integrity, manifest };
}

export function parseAuthorContactRefitArgs(argv) {
    const config = {};
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        const take = () => {
            if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) {
                throw new Error(`${flag} requires a value`);
            }
            index += 1;
            return argv[index];
        };
        if (flag === '--help') help = true;
        else if (flag === '--bundle-dir') config.bundleDirectory = take();
        else if (flag === '--observations') config.observationsPath = take();
        else if (flag === '--bridge-report') config.bridgeReportPath = take();
        else if (flag === '--initial-fit-summary') config.initialFitSummaryPath = take();
        else if (flag === '--contact-diagnostic') config.contactDiagnosticPath = take();
        else if (flag === '--output') config.outputPath = take();
        else throw new Error(`unknown option ${flag}`);
    }
    if (help) return { help: true };
    for (const field of [
        'bundleDirectory', 'observationsPath', 'bridgeReportPath',
        'initialFitSummaryPath', 'contactDiagnosticPath', 'outputPath',
    ]) {
        if (!config[field]) throw new Error(`missing required option ${field}`);
    }
    return config;
}

function helpText() {
    return `Usage:
  node author_browser_contact_refit_manifest.mjs --bundle-dir DIR \\
    --observations FILE --bridge-report FILE --initial-fit-summary FILE \\
    --contact-diagnostic FILE --output NEW_FILE

Authors one deterministic, externally pinnable contact-refit input manifest.
The manifest is atomically published only after the complete browser-only
observation/bridge/initial-fit/hoof-diagnostic chain validates. Blender is not
loaded or used. Existing output files are never overwritten.`;
}

export function runAuthorContactRefitCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseAuthorContactRefitArgs(argv);
        if (config.help) {
            streams.stdout.write(`${helpText()}\n`);
            return 0;
        }
        const result = authorContactRefitInputManifest(config);
        streams.stdout.write(`${JSON.stringify({
            status: 'PASS_CONTACT_REFIT_INPUT_MANIFEST',
            path: result.path,
            bytes: result.bytes,
            sha256: result.sha256,
            browserOnly: true,
            blenderUsed: false,
        })}\n`);
        return 0;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) process.exitCode = runAuthorContactRefitCli();
