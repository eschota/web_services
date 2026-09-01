#!/usr/bin/env node

import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';
import zlib from 'node:zlib';

import {
    deriveSam2GroundEvidence,
    diagnoseHoofContacts,
    HOOF_CONTACT_INFERENCE_CONTRACT,
} from '../../static/js/animation-fitting-hoof-contact-inference.js';

const BRIDGE_REPORT_SCHEMA = 'autorig-browser-fit-canary-bridge-report.v1';
const DIAGNOSTIC_SCHEMA = 'autorig-browser-hoof-contact-diagnostic.v1';
const BUNDLE_SCHEMA = 'autorig-actionless-fitting-bundle.v1';
const IMMUTABLE_MANIFEST_SCHEMA = 'autorig-fitting-immutable-copy.v1';
const MASK_MANIFEST_SCHEMA = 'autorig-sam2-mask-manifest.v1';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

function parseArguments(argv) {
    const result = {};
    for (let index = 0; index < argv.length; index += 1) {
        const token = argv[index];
        if (!token.startsWith('--')) throw new Error(`unexpected argument ${token}`);
        const key = token.slice(2);
        const value = argv[index + 1];
        if (!value || value.startsWith('--')) throw new Error(`${token} requires a value`);
        result[key] = value;
        index += 1;
    }
    for (const required of ['observations', 'bridge-report', 'output']) {
        if (!result[required]) throw new Error(`--${required} is required`);
    }
    return result;
}

function requireObject(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function requireString(value, field) {
    if (typeof value !== 'string' || !value) throw new Error(`${field} must be a non-empty string`);
    return value;
}

function requireSha256(value, field) {
    const result = requireString(value, field);
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function sha256Buffer(value) {
    return crypto.createHash('sha256').update(value).digest('hex');
}

function fileSnapshot(filename, field) {
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
    return {
        path: path.resolve(filename),
        bytes: buffer.length,
        sha256: sha256Buffer(buffer),
        buffer,
    };
}

function fileIntegrity(filename, field) {
    const snapshot = fileSnapshot(filename, field);
    return { path: snapshot.path, bytes: snapshot.bytes, sha256: snapshot.sha256 };
}

function jsonFileSnapshot(filename, field) {
    const snapshot = fileSnapshot(filename, field);
    let json;
    try {
        json = JSON.parse(snapshot.buffer.toString('utf8'));
    } catch (error) {
        throw new Error(`${field} is not valid JSON: ${error.message}`);
    }
    return {
        path: snapshot.path,
        bytes: snapshot.bytes,
        sha256: snapshot.sha256,
        json,
    };
}

function samePath(first, second) {
    const normalize = (value) => {
        const resolved = path.normalize(path.resolve(value));
        return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
    };
    return normalize(first) === normalize(second);
}

function resolveDeclaredPath(ownerPath, declaredPath) {
    const value = requireString(declaredPath, 'declared path');
    return path.isAbsolute(value) ? path.normalize(value) : path.resolve(path.dirname(ownerPath), value);
}

function resolveBundleFile(bundleDirectory, filename, field) {
    const root = path.resolve(bundleDirectory);
    const resolved = path.resolve(root, requireString(filename, field));
    const relative = path.relative(root, resolved);
    if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
        throw new Error(`${field} must resolve inside the immutable bundle`);
    }
    return resolved;
}

export function validateBridgeAndRawPins({ raw, report, observationPath, bridgeReportPath }) {
    requireObject(raw, 'raw observations');
    if (raw.schema !== HOOF_CONTACT_INFERENCE_CONTRACT.observations) {
        throw new Error(`raw observations.schema must be ${HOOF_CONTACT_INFERENCE_CONTRACT.observations}`);
    }
    const provenance = requireObject(raw.provenance, 'raw observations.provenance');
    const sourceVideoSha256 = requireSha256(
        provenance.source_video_sha256,
        'raw observations.provenance.source_video_sha256',
    );
    if (provenance.tracker?.backend !== HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend) {
        throw new Error(`raw tracker backend must be ${HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend}`);
    }
    if (provenance.segmenter?.backend !== HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend) {
        throw new Error(`raw segmenter backend must be ${HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend}`);
    }
    const rawBundleSha256 = requireSha256(provenance.bundle_sha256, 'raw bundle SHA-256');
    const rawManifestSha256 = requireSha256(
        provenance.immutable_manifest_sha256,
        'raw immutable-manifest SHA-256',
    );

    requireObject(report, 'bridge report');
    if (report.schema !== BRIDGE_REPORT_SCHEMA) {
        throw new Error(`bridge report.schema must be ${BRIDGE_REPORT_SCHEMA}`);
    }
    const inputs = requireObject(report.inputs, 'bridge report.inputs');
    const reportBundleSha256 = requireSha256(
        inputs.fittingBundleSha256,
        'bridge report fitting-bundle SHA-256',
    );
    const reportManifestSha256 = requireSha256(
        inputs.immutableManifestSha256,
        'bridge report immutable-manifest SHA-256',
    );
    const sourceSkeletonSha256 = requireSha256(
        inputs.skeletonSha256,
        'bridge report source-skeleton SHA-256',
    );
    const sourceModelSha256 = requireSha256(
        inputs.sourceModelSha256,
        'bridge report source-model SHA-256',
    );
    const mappingSourceVideoSha256 = requireSha256(
        inputs.sourceVideoSha256,
        'bridge report source-video SHA-256',
    );
    if (sourceVideoSha256 !== mappingSourceVideoSha256) {
        throw new Error('bridge report source-video SHA-256 does not match raw observations');
    }
    if (rawBundleSha256 !== reportBundleSha256) {
        throw new Error('bridge report bundle SHA-256 does not match raw observations');
    }
    if (rawManifestSha256 !== reportManifestSha256) {
        throw new Error('bridge report immutable-manifest SHA-256 does not match raw observations');
    }
    const declaredObservationPath = resolveDeclaredPath(bridgeReportPath, inputs.observationsPath);
    if (!samePath(declaredObservationPath, observationPath)) {
        throw new Error('bridge report observations path does not match the supplied raw observations');
    }
    const bundleDirectory = resolveDeclaredPath(bridgeReportPath, inputs.bundleDirectory);
    const rawBundleDirectory = resolveDeclaredPath(observationPath, provenance.bundle);
    if (!samePath(bundleDirectory, rawBundleDirectory)) {
        throw new Error('bridge report bundle directory does not match raw observations');
    }
    const bundleManifestPath = path.join(bundleDirectory, 'fitting_bundle.json');
    const immutableManifestPath = path.join(bundleDirectory, 'immutable_manifest.json');
    const bundleManifestIntegrity = jsonFileSnapshot(bundleManifestPath, 'immutable fitting-bundle manifest');
    const immutableManifestIntegrity = jsonFileSnapshot(immutableManifestPath, 'immutable-copy manifest');
    if (bundleManifestIntegrity.sha256 !== reportBundleSha256) {
        throw new Error('fitting_bundle.json SHA-256 does not match bridge/raw pins');
    }
    if (immutableManifestIntegrity.sha256 !== reportManifestSha256) {
        throw new Error('immutable_manifest.json SHA-256 does not match bridge/raw pins');
    }

    const bundleManifest = bundleManifestIntegrity.json;
    const immutableManifest = immutableManifestIntegrity.json;
    if (bundleManifest.schema !== BUNDLE_SCHEMA) {
        throw new Error(`fitting_bundle.json schema must be ${BUNDLE_SCHEMA}`);
    }
    if (immutableManifest.schema !== IMMUTABLE_MANIFEST_SCHEMA) {
        throw new Error(`immutable_manifest.json schema must be ${IMMUTABLE_MANIFEST_SCHEMA}`);
    }
    if (requireSha256(bundleManifest.source?.sha256, 'bundle source-model SHA-256') !== sourceModelSha256
        || requireSha256(immutableManifest.source_model?.sha256, 'immutable source-model SHA-256') !== sourceModelSha256) {
        throw new Error('source-model SHA-256 is inconsistent across bridge and bundle manifests');
    }
    if (requireSha256(immutableManifest.bundle_manifest?.sha256, 'immutable bundle-manifest SHA-256')
        !== reportBundleSha256) {
        throw new Error('immutable manifest does not pin fitting_bundle.json');
    }
    const skeletonRow = requireObject(bundleManifest.artifacts?.skeleton, 'bundle skeleton artifact');
    if (requireSha256(skeletonRow.sha256, 'bundle skeleton SHA-256') !== sourceSkeletonSha256) {
        throw new Error('source-skeleton SHA-256 is inconsistent across bridge and bundle manifest');
    }
    const skeletonIntegrity = fileIntegrity(
        resolveBundleFile(bundleDirectory, skeletonRow.filename, 'bundle skeleton filename'),
        'bundle skeleton',
    );
    if (skeletonIntegrity.sha256 !== sourceSkeletonSha256) {
        throw new Error('bundle skeleton bytes do not match the bridge pin');
    }

    if (!Array.isArray(immutableManifest.files) || !immutableManifest.files.length) {
        throw new Error('immutable manifest files must not be empty');
    }
    if (immutableManifest.bundle_file_count !== immutableManifest.files.length
        || inputs.bundleFileCount !== immutableManifest.files.length) {
        throw new Error('immutable bundle file count does not match the bridge report');
    }
    const immutableFilenames = new Set();
    const immutableFiles = immutableManifest.files.map((row, index) => {
        const item = requireObject(row, `immutable manifest files[${index}]`);
        const filename = requireString(item.filename, `immutable manifest files[${index}].filename`);
        if (immutableFilenames.has(filename)) throw new Error(`immutable manifest repeats file ${filename}`);
        immutableFilenames.add(filename);
        const expectedSha256 = requireSha256(item.sha256, `immutable manifest files[${index}].sha256`);
        const integrity = fileIntegrity(
            resolveBundleFile(bundleDirectory, filename, `immutable manifest files[${index}].filename`),
            `immutable bundle file ${filename}`,
        );
        if (integrity.sha256 !== expectedSha256 || integrity.bytes !== item.bytes) {
            throw new Error(`immutable bundle file ${filename} does not match its byte/hash pin`);
        }
        return { filename, bytes: integrity.bytes, sha256: integrity.sha256 };
    });
    const immutableByFilename = new Map(immutableFiles.map((item) => [item.filename, item]));
    const immutableTotalBytes = immutableFiles.reduce((sum, item) => sum + item.bytes, 0);
    if (immutableManifest.bundle_total_bytes !== immutableTotalBytes
        || inputs.bundleTotalBytes !== immutableTotalBytes) {
        throw new Error('immutable bundle total bytes do not match the file pins');
    }
    Object.entries(requireObject(bundleManifest.artifacts, 'bundle artifacts')).forEach(([name, rowValue]) => {
        const row = requireObject(rowValue, `bundle artifacts.${name}`);
        const filename = requireString(row.filename, `bundle artifacts.${name}.filename`);
        const immutable = immutableByFilename.get(filename);
        if (!immutable
            || immutable.sha256 !== requireSha256(row.sha256, `bundle artifacts.${name}.sha256`)
            || immutable.bytes !== row.bytes) {
            throw new Error(`bundle artifact ${name} does not match its immutable file pin`);
        }
    });

    const observationsIntegrity = fileIntegrity(observationPath, 'observations JSON');
    if (observationsIntegrity.sha256
        !== requireSha256(inputs.observationsSha256, 'bridge report observations SHA-256')) {
        throw new Error('observations JSON bytes do not match the bridge report pin');
    }
    const bridgeReportIntegrity = fileIntegrity(bridgeReportPath, 'bridge report JSON');
    let sourceVideoIntegrity = null;
    if (provenance.source_video != null) {
        const sourceVideoPath = resolveDeclaredPath(observationPath, provenance.source_video);
        sourceVideoIntegrity = fileIntegrity(sourceVideoPath, 'source video');
        if (sourceVideoIntegrity.sha256 !== sourceVideoSha256) {
            throw new Error('source-video bytes do not match observations provenance SHA-256');
        }
    }
    return {
        sourceVideoSha256,
        mappingSourceVideoSha256,
        bundleSha256: reportBundleSha256,
        immutableManifestSha256: reportManifestSha256,
        sourceSkeletonSha256,
        sourceModelSha256,
        observations: observationsIntegrity,
        bridgeReport: bridgeReportIntegrity,
        sourceVideo: sourceVideoIntegrity,
        bundleManifest: {
            path: bundleManifestIntegrity.path,
            bytes: bundleManifestIntegrity.bytes,
            sha256: bundleManifestIntegrity.sha256,
        },
        immutableManifest: {
            path: immutableManifestIntegrity.path,
            bytes: immutableManifestIntegrity.bytes,
            sha256: immutableManifestIntegrity.sha256,
        },
        immutableFiles,
    };
}

function paeth(left, above, upperLeft) {
    const prediction = left + above - upperLeft;
    const leftDistance = Math.abs(prediction - left);
    const aboveDistance = Math.abs(prediction - above);
    const upperLeftDistance = Math.abs(prediction - upperLeft);
    if (leftDistance <= aboveDistance && leftDistance <= upperLeftDistance) return left;
    if (aboveDistance <= upperLeftDistance) return above;
    return upperLeft;
}

function decodeGrayscalePng(input, filename, frame) {
    const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
    if (input.length < signature.length || !input.subarray(0, 8).equals(signature)) {
        throw new Error(`${filename} is not a PNG`);
    }
    let offset = 8;
    let width = null;
    let height = null;
    const compressed = [];
    while (offset < input.length) {
        if (offset + 12 > input.length) throw new Error(`${filename} has a truncated PNG chunk`);
        const length = input.readUInt32BE(offset);
        const type = input.toString('ascii', offset + 4, offset + 8);
        const dataStart = offset + 8;
        const dataEnd = dataStart + length;
        if (dataEnd + 4 > input.length) throw new Error(`${filename} has a truncated ${type} chunk`);
        const data = input.subarray(dataStart, dataEnd);
        if (type === 'IHDR') {
            width = data.readUInt32BE(0);
            height = data.readUInt32BE(4);
            const bitDepth = data[8];
            const colorType = data[9];
            const compression = data[10];
            const filter = data[11];
            const interlace = data[12];
            if (bitDepth !== 8 || colorType !== 0 || compression !== 0 || filter !== 0 || interlace !== 0) {
                throw new Error(`${filename} must be a non-interlaced 8-bit grayscale SAM2 mask`);
            }
        } else if (type === 'IDAT') {
            compressed.push(data);
        } else if (type === 'IEND') {
            break;
        }
        offset = dataEnd + 4;
    }
    if (!width || !height || !compressed.length) throw new Error(`${filename} is missing PNG image data`);
    const encoded = zlib.inflateSync(Buffer.concat(compressed));
    const stride = width;
    if (encoded.length !== height * (stride + 1)) {
        throw new Error(`${filename} decoded byte count does not match grayscale dimensions`);
    }
    const output = new Uint8Array(width * height);
    for (let y = 0; y < height; y += 1) {
        const filter = encoded[y * (stride + 1)];
        const source = y * (stride + 1) + 1;
        const target = y * stride;
        for (let x = 0; x < stride; x += 1) {
            const raw = encoded[source + x];
            const left = x ? output[target + x - 1] : 0;
            const above = y ? output[target - stride + x] : 0;
            const upperLeft = x && y ? output[target - stride + x - 1] : 0;
            let value;
            if (filter === 0) value = raw;
            else if (filter === 1) value = raw + left;
            else if (filter === 2) value = raw + above;
            else if (filter === 3) value = raw + Math.floor((left + above) / 2);
            else if (filter === 4) value = raw + paeth(left, above, upperLeft);
            else throw new Error(`${filename} uses unsupported PNG filter ${filter}`);
            output[target + x] = value & 0xff;
        }
    }
    return { frame, width, height, channels: 1, data: output };
}

function isDeclaredHeadEarBranchDuplicate(first, second) {
    const semantics = new Set([first.semanticAnchorId, second.semanticAnchorId]);
    return semantics.size === 2
        && semantics.has('body_neck_head.terminal')
        && semantics.has('head_left_ear.proximal')
        && first.collection === 'auxiliaryChains'
        && second.collection === 'auxiliaryChains'
        && first.sourceBone === 'head.x'
        && second.sourceBone === 'head.x'
        && first.sourceTrackId === second.sourceTrackId
        && first.sourceAnchorId === 'head.x:5'
        && second.sourceAnchorId === 'head.x:5';
}

export function prepareBridgeObservations(raw, report, options = {}) {
    const mappings = report?.mappings;
    if (!Array.isArray(mappings)) throw new Error('bridge report has no mappings');
    const includeAllSelectedMappings = options.includeAllSelectedMappings === true;
    const allowDeclaredBranchDuplicate = options.allowDeclaredBranchDuplicate === true;
    const mappingBySemantic = new Map();
    const mappedSourceTrackIds = new Map();
    const mappedSourceAnchorIds = new Map();
    mappings.forEach((mappingValue, index) => {
        const mapping = requireObject(mappingValue, `bridge mapping[${index}]`);
        const semanticAnchorId = requireString(mapping.semanticAnchorId, `bridge mapping[${index}].semanticAnchorId`);
        const sourceTrackId = requireString(mapping.sourceTrackId, `bridge mapping[${index}].sourceTrackId`);
        const sourceAnchorId = requireString(mapping.sourceAnchorId, `bridge mapping[${index}].sourceAnchorId`);
        const sourceBone = requireString(mapping.sourceBone, `bridge mapping[${index}].sourceBone`);
        if (mappingBySemantic.has(semanticAnchorId)) throw new Error(`duplicate bridge semantic mapping ${semanticAnchorId}`);
        const previousTrackMapping = mappedSourceTrackIds.get(sourceTrackId);
        const previousAnchorMapping = mappedSourceAnchorIds.get(sourceAnchorId);
        const declaredBranchDuplicate = allowDeclaredBranchDuplicate
            && previousTrackMapping
            && previousTrackMapping === previousAnchorMapping
            && isDeclaredHeadEarBranchDuplicate(previousTrackMapping, mapping);
        if (previousTrackMapping && !declaredBranchDuplicate) {
            throw new Error(`duplicate bridge source track ${sourceTrackId}`);
        }
        if (previousAnchorMapping && !declaredBranchDuplicate) {
            throw new Error(`duplicate bridge source anchor ${sourceAnchorId}`);
        }
        const separator = sourceAnchorId.lastIndexOf(':');
        if (separator <= 0 || !/^\d+$/.test(sourceAnchorId.slice(separator + 1))
            || sourceAnchorId.slice(0, separator) !== sourceBone) {
            throw new Error(`bridge source anchor ${sourceAnchorId} does not match sourceBone ${sourceBone}`);
        }
        mappingBySemantic.set(semanticAnchorId, mapping);
        if (!previousTrackMapping) mappedSourceTrackIds.set(sourceTrackId, mapping);
        if (!previousAnchorMapping) mappedSourceAnchorIds.set(sourceAnchorId, mapping);
    });
    if (!Array.isArray(raw.tracks)) throw new Error('raw observations.tracks must be an array');
    const sourceById = new Map();
    const sourceAnchorIds = new Set();
    raw.tracks.forEach((trackValue, index) => {
        const track = requireObject(trackValue, `raw observations.tracks[${index}]`);
        const id = requireString(track.id, `raw observations.tracks[${index}].id`);
        const anchorId = requireString(track.anchor_id, `raw observations.tracks[${index}].anchor_id`);
        if (sourceById.has(id)) throw new Error(`duplicate raw source track ${id}`);
        if (sourceAnchorIds.has(anchorId)) throw new Error(`duplicate raw source anchor ${anchorId}`);
        sourceById.set(id, track);
        sourceAnchorIds.add(anchorId);
    });
    const semanticIds = includeAllSelectedMappings
        ? mappings.map((mapping) => mapping.semanticAnchorId)
        : HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.flatMap((foot) => [
            `${foot}.proximal`, `${foot}.joint`, `${foot}.hoof`,
        ]);
    const tracks = semanticIds.map((semanticId) => {
        const mapping = mappingBySemantic.get(semanticId);
        if (!mapping || mapping.limb !== semanticId.split('.')[0]) {
            throw new Error(`bridge report does not preserve exact mapping for ${semanticId}`);
        }
        const source = sourceById.get(mapping.sourceTrackId);
        if (!source || source.anchor_id !== mapping.sourceAnchorId) {
            throw new Error(`bridge source pin for ${semanticId} does not match raw TAPNext observations`);
        }
        return {
            ...structuredClone(source),
            anchor_id: semanticId,
        };
    });
    return {
        ...structuredClone(raw),
        tracks,
        contacts: [],
        provenance: {
            ...structuredClone(raw.provenance),
            browser_rgb_bridge: {
                source: includeAllSelectedMappings ? 'all_selected_mappings' : 'hoof_contact_mappings',
                mappings: semanticIds.map((semanticId) => structuredClone(mappingBySemantic.get(semanticId))),
            },
        },
    };
}

function resolveMaskPath(observationPath, masksDirectory, silhouette) {
    if (masksDirectory) return path.join(masksDirectory, path.basename(silhouette.path));
    return path.resolve(path.dirname(observationPath), silhouette.path);
}

export function loadMaskFrames({ raw, observationPath, masksDirectory = null }) {
    if (!Array.isArray(raw.silhouettes) || raw.silhouettes.length !== raw.frame_count) {
        throw new Error('raw observations do not contain one SAM2 silhouette per frame');
    }
    const files = [];
    const masks = raw.silhouettes.map((silhouetteValue, frame) => {
        const silhouette = requireObject(silhouetteValue, `silhouette ${frame}`);
        if (silhouette.frame !== frame || typeof silhouette.path !== 'string' || !silhouette.path) {
            throw new Error(`silhouette ${frame} is not chronological`);
        }
        const filename = resolveMaskPath(observationPath, masksDirectory, silhouette);
        const snapshot = fileSnapshot(filename, `SAM2 mask frame ${frame}`);
        if (silhouette.sha256 != null
            && snapshot.sha256 !== requireSha256(silhouette.sha256, `silhouette ${frame}.sha256`)) {
            throw new Error(`SAM2 mask frame ${frame} does not match its declared SHA-256`);
        }
        if (silhouette.bytes != null && snapshot.bytes !== silhouette.bytes) {
            throw new Error(`SAM2 mask frame ${frame} does not match its declared byte count`);
        }
        files.push({
            frame,
            declaredPath: silhouette.path,
            path: snapshot.path,
            bytes: snapshot.bytes,
            sha256: snapshot.sha256,
        });
        return decodeGrayscalePng(snapshot.buffer, snapshot.path, frame);
    });
    const hashPayload = {
        schema: MASK_MANIFEST_SCHEMA,
        frameCount: raw.frame_count,
        files: files.map(({ frame, declaredPath, bytes, sha256 }) => ({
            frame, declaredPath, bytes, sha256,
        })),
    };
    return {
        masks,
        manifest: {
            ...hashPayload,
            sha256: sha256Buffer(Buffer.from(JSON.stringify(hashPayload))),
            files,
        },
    };
}

export function main(argv = process.argv.slice(2)) {
    const args = parseArguments(argv);
    const minimumSupportFeet = args['minimum-support-feet'] == null
        ? null
        : Number(args['minimum-support-feet']);
    if (minimumSupportFeet != null
        && (!Number.isInteger(minimumSupportFeet) || minimumSupportFeet < 1 || minimumSupportFeet > 4)) {
        throw new Error('--minimum-support-feet must be an integer inside [1, 4]');
    }
    const contactOptions = minimumSupportFeet == null ? {} : { minimumSupportFeet };
    const observationPath = path.resolve(args.observations);
    const bridgeReportPath = path.resolve(args['bridge-report']);
    const outputPath = path.resolve(args.output);
    const groundOutputPath = args['ground-output'] ? path.resolve(args['ground-output']) : null;
    const rawSnapshot = jsonFileSnapshot(observationPath, 'observations JSON');
    const bridgeSnapshot = jsonFileSnapshot(bridgeReportPath, 'bridge report JSON');
    const raw = rawSnapshot.json;
    const bridgeReport = bridgeSnapshot.json;
    const integrity = validateBridgeAndRawPins({
        raw,
        report: bridgeReport,
        observationPath,
        bridgeReportPath,
    });
    if (integrity.observations.sha256 !== rawSnapshot.sha256
        || integrity.bridgeReport.sha256 !== bridgeSnapshot.sha256) {
        throw new Error('diagnostic JSON input changed after its immutable snapshot was parsed');
    }
    const observations = prepareBridgeObservations(raw, bridgeReport);
    const loadedMasks = loadMaskFrames({
        raw,
        observationPath,
        masksDirectory: args['masks-dir'] ? path.resolve(args['masks-dir']) : null,
    });
    const groundEvidence = deriveSam2GroundEvidence({
        observations,
        masks: loadedMasks.masks,
        options: contactOptions,
    });
    groundEvidence.provenance = {
        ...groundEvidence.provenance,
        trackerBackend: HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend,
        observationsSha256: integrity.observations.sha256,
        bridgeReportSha256: integrity.bridgeReport.sha256,
        bundleSha256: integrity.bundleSha256,
        immutableManifestSha256: integrity.immutableManifestSha256,
        maskManifestSha256: loadedMasks.manifest.sha256,
    };
    const schedule = diagnoseHoofContacts({ observations, groundEvidence, options: contactOptions });
    const report = {
        schema: DIAGNOSTIC_SCHEMA,
        status: schedule.status,
        inputs: {
            observations: integrity.observations,
            bridgeReport: integrity.bridgeReport,
            sourceVideo: integrity.sourceVideo ?? { sha256: integrity.sourceVideoSha256 },
            bundleManifest: integrity.bundleManifest,
            immutableManifest: integrity.immutableManifest,
            immutableBundleFiles: integrity.immutableFiles,
            sourceSkeletonSha256: integrity.sourceSkeletonSha256,
            sourceModelSha256: integrity.sourceModelSha256,
            maskManifest: loadedMasks.manifest,
            trackerBackend: observations.provenance.tracker?.backend,
            segmenterBackend: observations.provenance.segmenter?.backend,
            frames: observations.frame_count,
            fps: observations.fps,
            loop: schedule.loop,
            resolution: [observations.width, observations.height],
            minimumSupportFeet: schedule.qa.thresholds.minimumSupportFeet,
        },
        bridge: {
            semanticTracks: observations.tracks.length,
            hoofTracks: HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.map((foot) => {
                const semanticId = `${foot}.hoof`;
                const mapping = observations.provenance.browser_rgb_bridge.mappings
                    .find((row) => row.semanticAnchorId === semanticId);
                return {
                    foot,
                    semanticId,
                    sourceTrackId: mapping.sourceTrackId,
                    sourceAnchorId: mapping.sourceAnchorId,
                    sourceBone: mapping.sourceBone,
                };
            }),
        },
        schedule,
    };
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
    if (groundOutputPath) {
        fs.mkdirSync(path.dirname(groundOutputPath), { recursive: true });
        fs.writeFileSync(groundOutputPath, `${JSON.stringify(groundEvidence, null, 2)}\n`);
    }
    process.stdout.write(`${JSON.stringify({
        status: report.status,
        output: outputPath,
        failures: schedule.qa.failures,
        order: schedule.inferredTouchdownOrder,
        integrity: {
            observationsSha256: integrity.observations.sha256,
            bridgeReportSha256: integrity.bridgeReport.sha256,
            sourceVideoSha256: integrity.sourceVideoSha256,
            maskManifestSha256: loadedMasks.manifest.sha256,
        },
        feet: Object.fromEntries(HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.map((foot) => [foot, {
            touchdown: schedule.feet[foot].touchdownFrame,
            liftoff: schedule.feet[foot].liftoffFrame,
            dutyFactor: schedule.feet[foot].dutyFactor,
            slide: schedule.feet[foot].slide,
            candidateIntervals: schedule.feet[foot].candidateIntervals,
            failures: schedule.feet[foot].failures,
        }])),
    }, null, 2)}\n`);
    return schedule.status === 'PASS' ? 0 : 2;
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
