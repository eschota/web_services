#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import {
    HORSE_TROT_CONTACT_REFIT_INPUT_SCHEMA,
    validateTrotContactRefitInputs,
} from './browser_trot_contact_refit.mjs';

const SHA256_PATTERN = /^[0-9a-f]{64}$/;

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${field} must be an object`);
    return value;
}

function string(value, field) {
    if (typeof value !== 'string' || !value) throw new Error(`${field} must be a non-empty string`);
    return value;
}

function sha256(value, field) {
    const result = string(value, field);
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function hash(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function jsonSnapshot(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, field));
    const before = fs.lstatSync(filename);
    if (!before.isFile() || before.isSymbolicLink() || before.size <= 0) {
        throw new Error(`${field} must be a non-empty regular file, not a symlink`);
    }
    const buffer = fs.readFileSync(filename);
    const after = fs.lstatSync(filename);
    if (!after.isFile() || after.isSymbolicLink() || before.dev !== after.dev || before.ino !== after.ino
        || before.size !== buffer.length || after.size !== buffer.length || before.mtimeMs !== after.mtimeMs) {
        throw new Error(`${field} changed while its immutable bytes were read`);
    }
    let json;
    try {
        json = object(JSON.parse(buffer.toString('utf8')), field);
    } catch (error) {
        if (error.message.startsWith(`${field} must be`)) throw error;
        throw new Error(`${field} is not valid JSON: ${error.message}`);
    }
    return { path: filename, bytes: buffer.length, sha256: hash(buffer), json };
}

function row(snapshot) {
    return { path: snapshot.path, bytes: snapshot.bytes, sha256: snapshot.sha256 };
}

export function authorTrotContactRefitInputManifest(configuration) {
    const config = object(configuration, 'configuration');
    const bundleDirectory = path.resolve(string(config.bundleDirectory, 'configuration.bundleDirectory'));
    if (!fs.existsSync(bundleDirectory) || !fs.statSync(bundleDirectory).isDirectory()) {
        throw new Error(`configuration.bundleDirectory is not a directory: ${bundleDirectory}`);
    }
    const outputPath = path.resolve(string(config.outputPath, 'configuration.outputPath'));
    if (!fs.existsSync(path.dirname(outputPath)) || !fs.statSync(path.dirname(outputPath)).isDirectory()) {
        throw new Error(`output parent does not exist: ${path.dirname(outputPath)}`);
    }
    if (fs.existsSync(outputPath)) throw new Error(`output manifest already exists: ${outputPath}`);
    const files = {
        observations: jsonSnapshot(config.observationsPath, 'observations'),
        bridgeReport: jsonSnapshot(config.bridgeReportPath, 'bridgeReport'),
        initialFitSummary: jsonSnapshot(config.initialFitSummaryPath, 'initialFitSummary'),
        trotDiagnostic: jsonSnapshot(config.trotDiagnosticPath, 'trotDiagnostic'),
    };
    const initialInputs = object(files.initialFitSummary.json.inputs, 'initialFitSummary.inputs');
    const manifest = {
        schema: HORSE_TROT_CONTACT_REFIT_INPUT_SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        gait: 'diagonal_pair_trot',
        humanReviewRequired: true,
        inputs: {
            bundleDirectory,
            observations: row(files.observations),
            bridgeReport: row(files.bridgeReport),
            initialFitSummary: row(files.initialFitSummary),
            trotDiagnostic: row(files.trotDiagnostic),
        },
        pins: {
            observationsSha256: files.observations.sha256,
            bridgeReportSha256: files.bridgeReport.sha256,
            initialFitSummarySha256: files.initialFitSummary.sha256,
            diagnosticSha256: files.trotDiagnostic.sha256,
            sourceVideoSha256: sha256(initialInputs.sourceVideoSha256, 'initialFitSummary.inputs.sourceVideoSha256'),
            fittingBundleSha256: sha256(initialInputs.fittingBundleSha256, 'initialFitSummary.inputs.fittingBundleSha256'),
            immutableManifestSha256: sha256(initialInputs.immutableManifestSha256, 'initialFitSummary.inputs.immutableManifestSha256'),
            sourceModelSha256: sha256(initialInputs.sourceModelSha256, 'initialFitSummary.inputs.sourceModelSha256'),
            sourceSkeletonSha256: sha256(initialInputs.skeletonSha256, 'initialFitSummary.inputs.skeletonSha256'),
        },
    };
    const payload = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
    const integrity = { path: outputPath, bytes: payload.length, sha256: hash(payload) };
    const staging = `${outputPath}.staging-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    try {
        const handle = fs.openSync(staging, 'wx');
        try {
            fs.writeFileSync(handle, payload);
            fs.fsyncSync(handle);
        } finally {
            fs.closeSync(handle);
        }
        validateTrotContactRefitInputs({
            inputManifestPath: staging,
            expectedManifestSha256: integrity.sha256,
        });
        fs.linkSync(staging, outputPath);
        fs.unlinkSync(staging);
    } catch (error) {
        try { if (fs.existsSync(staging)) fs.unlinkSync(staging); } catch { /* preserve original error */ }
        throw error;
    }
    return { ...integrity, manifest };
}

export function parseAuthorTrotRefitArgs(argv) {
    const config = {};
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        const take = () => {
            if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) throw new Error(`${flag} requires a value`);
            index += 1;
            return argv[index];
        };
        if (flag === '--help') help = true;
        else if (flag === '--bundle-dir') config.bundleDirectory = take();
        else if (flag === '--observations') config.observationsPath = take();
        else if (flag === '--bridge-report') config.bridgeReportPath = take();
        else if (flag === '--initial-fit-summary') config.initialFitSummaryPath = take();
        else if (flag === '--trot-diagnostic') config.trotDiagnosticPath = take();
        else if (flag === '--output') config.outputPath = take();
        else throw new Error(`unknown option ${flag}`);
    }
    if (help) return { help: true };
    for (const field of [
        'bundleDirectory', 'observationsPath', 'bridgeReportPath',
        'initialFitSummaryPath', 'trotDiagnosticPath', 'outputPath',
    ]) if (!config[field]) throw new Error(`missing required option ${field}`);
    return config;
}

function helpText() {
    return `Usage:
  node author_browser_trot_contact_refit_manifest.mjs --bundle-dir DIR \\
    --observations FILE --bridge-report FILE --initial-fit-summary FILE \\
    --trot-diagnostic FILE --output NEW_FILE

Authors one immutable browser-only diagonal-TROT refit manifest and publishes it
only after all source/bundle/bridge/diagnostic pins and recomputed gait QA pass.
Blender is not used; an existing output is never overwritten.`;
}

export function runAuthorTrotRefitCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseAuthorTrotRefitArgs(argv);
        if (config.help) {
            streams.stdout.write(`${helpText()}\n`);
            return 0;
        }
        const result = authorTrotContactRefitInputManifest(config);
        streams.stdout.write(`${JSON.stringify({
            status: 'PASS_TROT_CONTACT_REFIT_INPUT_MANIFEST',
            path: result.path,
            bytes: result.bytes,
            sha256: result.sha256,
            humanReviewRequired: true,
        })}\n`);
        return 0;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invoked = process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;
if (invoked) process.exitCode = runAuthorTrotRefitCli();
