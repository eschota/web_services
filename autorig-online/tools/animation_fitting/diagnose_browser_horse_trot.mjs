#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { assessHorseTrotGait } from '../../static/js/animation-fitting-semantic-tracker.js';
import {
    prepareBridgeObservations,
    validateBridgeAndRawPins,
} from './diagnose_browser_hoof_contacts.mjs';

const REPORT_SCHEMA = 'autorig-browser-horse-trot-contact-diagnostic.v1';

function parseArguments(argv) {
    const result = {};
    for (let index = 0; index < argv.length; index += 1) {
        const token = argv[index];
        if (!token.startsWith('--')) throw new Error(`unexpected positional argument ${token}`);
        const name = token.slice(2);
        const value = argv[index + 1];
        if (!value || value.startsWith('--')) throw new Error(`--${name} requires a value`);
        result[name] = value;
        index += 1;
    }
    ['observations', 'bridge-report', 'output', 'candidate-id', 'source-reference']
        .forEach((name) => {
            if (!result[name]) throw new Error(`--${name} is required`);
        });
    return result;
}

function snapshotJson(filename, label) {
    const resolved = path.resolve(filename);
    const buffer = fs.readFileSync(resolved);
    let json;
    try {
        json = JSON.parse(buffer.toString('utf8'));
    } catch (error) {
        throw new Error(`${label} is not valid JSON: ${error.message}`);
    }
    return {
        json,
        path: resolved,
        bytes: buffer.length,
        sha256: crypto.createHash('sha256').update(buffer).digest('hex'),
    };
}

function snapshotFile(filename) {
    const resolved = path.resolve(filename);
    const buffer = fs.readFileSync(resolved);
    return {
        path: resolved,
        bytes: buffer.length,
        sha256: crypto.createHash('sha256').update(buffer).digest('hex'),
    };
}

export function buildTrotDiagnosticReport({
    observations,
    integrity,
    candidateId,
    sourceReference,
    relationshipNote,
    runtime = null,
    createdAt = new Date().toISOString(),
} = {}) {
    const qa = assessHorseTrotGait(observations, { loopEndpointDuplicated: true });
    return {
        schema: REPORT_SCHEMA,
        createdAt,
        status: qa.status,
        candidate: {
            id: candidateId,
            sourceReference,
            relationshipNote: relationshipNote || null,
        },
        inputs: {
            observations: integrity.observations,
            bridgeReport: integrity.bridgeReport,
            sourceVideo: integrity.sourceVideo,
            bundleManifest: integrity.bundleManifest,
            immutableManifest: integrity.immutableManifest,
            sourceSkeletonSha256: integrity.sourceSkeletonSha256,
            sourceModelSha256: integrity.sourceModelSha256,
            trackerBackend: observations.provenance?.tracker?.backend || null,
            segmenterBackend: observations.provenance?.segmenter?.backend || null,
        },
        profile: qa.profile,
        runtime,
        qa,
        decision: {
            eligibleForContactConstrainedRefit: qa.accepted === true,
            approvedForAnimationLibrary: false,
            humanFixedCameraReviewRequired: true,
            interpretation: qa.accepted
                ? 'Tracked hoof motion satisfies the dedicated diagonal-pair TROT contact profile.'
                : 'Tracked hoof motion does not satisfy the dedicated diagonal-pair TROT contact profile.',
        },
    };
}

export function main(argv = process.argv.slice(2)) {
    const args = parseArguments(argv);
    const observationSnapshot = snapshotJson(args.observations, 'observations');
    const bridgeSnapshot = snapshotJson(args['bridge-report'], 'bridge report');
    const integrity = validateBridgeAndRawPins({
        raw: observationSnapshot.json,
        report: bridgeSnapshot.json,
        observationPath: observationSnapshot.path,
        bridgeReportPath: bridgeSnapshot.path,
    });
    if (integrity.observations.sha256 !== observationSnapshot.sha256
        || integrity.bridgeReport.sha256 !== bridgeSnapshot.sha256) {
        throw new Error('TROT diagnostic input changed after its immutable snapshot was parsed');
    }
    const observations = prepareBridgeObservations(
        observationSnapshot.json,
        bridgeSnapshot.json,
    );
    const report = buildTrotDiagnosticReport({
        observations,
        integrity,
        candidateId: args['candidate-id'],
        sourceReference: args['source-reference'],
        relationshipNote: args['relationship-note'],
        runtime: {
            browserOnly: true,
            blenderUsed: false,
            implementation: snapshotFile(path.resolve(
                path.dirname(fileURLToPath(import.meta.url)),
                '../../static/js/animation-fitting-semantic-tracker.js',
            )),
            cli: snapshotFile(fileURLToPath(import.meta.url)),
        },
    });
    const output = path.resolve(args.output);
    fs.mkdirSync(path.dirname(output), { recursive: true });
    fs.writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
    process.stdout.write(`${JSON.stringify({
        status: report.status,
        output,
        profile: report.profile.id,
        failures: report.qa.failures,
        diagonalPairs: Object.fromEntries(Object.entries(report.qa.pairs).map(([id, pair]) => [id, {
            feet: pair.feet,
            swingDice: pair.swingDice,
            contactDice: pair.contactDice,
            zeroLagLiftCorrelation: pair.zeroLagLiftCorrelation,
            bestLagFrames: pair.bestLagFrames,
            accepted: pair.accepted,
        }])),
    }, null, 2)}\n`);
    return report.status === 'PASS' ? 0 : 2;
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) {
    try {
        process.exitCode = main();
    } catch (error) {
        process.stderr.write(`${error?.stack || error}\n`);
        process.exitCode = 1;
    }
}

export const HORSE_TROT_CONTACT_DIAGNOSTIC_SCHEMA = REPORT_SCHEMA;
