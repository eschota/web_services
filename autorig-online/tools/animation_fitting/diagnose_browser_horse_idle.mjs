#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { assessHorsePlantedIdle } from '../../static/js/animation-fitting-semantic-tracker.js';
import {
    prepareBridgeObservations,
    validateBridgeAndRawPins,
} from './diagnose_browser_hoof_contacts.mjs';

const REPORT_SCHEMA = 'autorig-browser-horse-planted-idle-diagnostic.v1';
const CONTACT_PROVENANCE_SCHEMA = 'autorig-horse-planted-idle-contact-observations.v1';

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
    ['observations', 'bridge-report', 'output', 'contact-observations-output', 'candidate-id', 'source-reference']
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

function writeNewJson(filename, value) {
    const resolved = path.resolve(filename);
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    fs.writeFileSync(resolved, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    return snapshotFile(resolved);
}

export function buildIdleDiagnosticReport({
    observations,
    integrity,
    candidateId,
    sourceReference,
    relationshipNote,
    runtime = null,
    createdAt = new Date().toISOString(),
} = {}) {
    const qa = assessHorsePlantedIdle(observations);
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
            eligibleForContactConstrainedFit: qa.accepted === true,
            approvedForAnimationLibrary: false,
            humanFixedCameraReviewRequired: true,
            targetMeshDeformationQaRequired: true,
            interpretation: qa.accepted
                ? 'All four tracked hooves satisfy the planted-idle contract and the selected body tracks satisfy source loop C0/C1 gates.'
                : 'The source motion does not satisfy the strict planted-idle contact and loop contract.',
        },
    };
}

function buildContactObservations({ raw, prepared, report, reportSnapshot, integrity }) {
    if (report.status !== 'PASS') {
        throw new Error('contact observations may be authored only from a PASS planted-idle report');
    }
    const mappings = prepared.provenance?.browser_rgb_bridge?.mappings || [];
    const sourceTrackById = new Map((raw.tracks || []).map((track) => [track.id, track]));
    const contacts = Object.entries(report.qa.feet).map(([foot, footQa]) => {
        if (footQa.accepted !== true || footQa.contactFrames.length !== raw.frame_count) {
            throw new Error(`PASS planted-idle report does not contain full-frame contact for ${foot}`);
        }
        const mapping = mappings.find((row) => row.semanticAnchorId === `${foot}.hoof`);
        const sourceTrack = mapping ? sourceTrackById.get(mapping.sourceTrackId) : null;
        if (!mapping || !sourceTrack || sourceTrack.anchor_id !== mapping.sourceAnchorId) {
            throw new Error(`cannot resolve pinned source hoof track for ${foot}`);
        }
        const y = sourceTrack.points
            .filter((point) => point.visible)
            .map((point) => Number(point.y))
            .sort((first, second) => first - second);
        if (!y.length) throw new Error(`source hoof track ${foot} has no visible ground samples`);
        const middle = (y.length - 1) / 2;
        const groundHeight = Number.isInteger(middle)
            ? y[middle]
            : (y[Math.floor(middle)] + y[Math.ceil(middle)]) / 2;
        return {
            anchor_id: mapping.sourceAnchorId,
            frames: [...footQa.contactFrames],
            ground_height: groundHeight,
            weight: 1,
        };
    });
    return {
        ...structuredClone(raw),
        contacts,
        provenance: {
            ...structuredClone(raw.provenance),
            horse_planted_idle_contacts: {
                schema: CONTACT_PROVENANCE_SCHEMA,
                profile: report.profile.id,
                diagnostic: reportSnapshot,
                observationsSha256: integrity.observations.sha256,
                bridgeReportSha256: integrity.bridgeReport.sha256,
                sourceVideoSha256: integrity.sourceVideo.sha256,
                contacts: contacts.map((contact) => ({
                    anchorId: contact.anchor_id,
                    frameCount: contact.frames.length,
                    firstFrame: contact.frames[0],
                    lastFrame: contact.frames.at(-1),
                })),
                approvedForAnimationLibrary: false,
            },
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
        throw new Error('idle diagnostic input changed after its immutable snapshot was parsed');
    }
    const observations = prepareBridgeObservations(observationSnapshot.json, bridgeSnapshot.json);
    const report = buildIdleDiagnosticReport({
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
    const reportSnapshot = writeNewJson(args.output, report);
    let contactSnapshot = null;
    if (report.status === 'PASS') {
        const contactObservations = buildContactObservations({
            raw: observationSnapshot.json,
            prepared: observations,
            report,
            reportSnapshot,
            integrity,
        });
        contactSnapshot = writeNewJson(args['contact-observations-output'], contactObservations);
    }
    process.stdout.write(`${JSON.stringify({
        status: report.status,
        output: reportSnapshot,
        contactObservations: contactSnapshot,
        profile: report.profile.id,
        failures: report.qa.failures,
        feet: Object.fromEntries(Object.entries(report.qa.feet).map(([foot, detail]) => [foot, {
            horizontalRangePx: detail.horizontalRangePx,
            verticalRangePx: detail.verticalRangePx,
            maximumDisplacementPx: detail.maximumDisplacementPx,
            endpointDisplacementPx: detail.endpointDisplacementPx,
            p95SpeedPxPerFrame: detail.p95SpeedPxPerFrame,
            velocitySeamPxPerFrame: detail.velocitySeamPxPerFrame,
            accepted: detail.accepted,
        }])),
        body: report.qa.body,
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

export const HORSE_PLANTED_IDLE_DIAGNOSTIC_SCHEMA = REPORT_SCHEMA;
export const HORSE_PLANTED_IDLE_CONTACT_PROVENANCE_SCHEMA = CONTACT_PROVENANCE_SCHEMA;
