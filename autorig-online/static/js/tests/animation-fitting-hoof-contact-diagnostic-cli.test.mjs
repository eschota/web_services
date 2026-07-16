import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import zlib from 'node:zlib';

import {
    loadMaskFrames,
    prepareBridgeObservations,
    validateBridgeAndRawPins,
} from '../../../tools/animation_fitting/diagnose_browser_hoof_contacts.mjs';

const sha256 = (value) => crypto.createHash('sha256').update(value).digest('hex');

function write(filename, value) {
    const data = Buffer.isBuffer(value) ? value : Buffer.from(value);
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, data);
    return { filename: path.basename(filename), bytes: data.length, sha256: sha256(data) };
}

function json(filename, value) {
    return write(filename, `${JSON.stringify(value, null, 2)}\n`);
}

function fixture(root) {
    const bundleDirectory = path.join(root, 'bundle');
    const skeleton = write(path.join(bundleDirectory, 'skeleton.json'), '{"bones":[]}\n');
    const sourceModelSha256 = 'a'.repeat(64);
    const bundle = {
        schema: 'autorig-actionless-fitting-bundle.v1',
        source: { sha256: sourceModelSha256 },
        artifacts: { skeleton },
    };
    const bundleFile = json(path.join(bundleDirectory, 'fitting_bundle.json'), bundle);
    const immutableFiles = [bundleFile, skeleton];
    const immutable = {
        schema: 'autorig-fitting-immutable-copy.v1',
        bundle_file_count: immutableFiles.length,
        bundle_total_bytes: immutableFiles.reduce((sum, row) => sum + row.bytes, 0),
        source_model: { sha256: sourceModelSha256 },
        bundle_manifest: { filename: bundleFile.filename, sha256: bundleFile.sha256 },
        files: immutableFiles,
    };
    const immutableFile = json(path.join(bundleDirectory, 'immutable_manifest.json'), immutable);
    const videoPath = path.join(root, 'source.mp4');
    const video = write(videoPath, 'synthetic video bytes');
    const observationPath = path.join(root, 'observations.json');
    const bridgeReportPath = path.join(root, 'bridge-report.json');
    const raw = {
        schema: 'autorig-fitting-observations.v1',
        frame_count: 8,
        tracks: [],
        provenance: {
            source_video: videoPath,
            source_video_sha256: video.sha256,
            bundle: bundleDirectory,
            bundle_sha256: bundleFile.sha256,
            immutable_manifest_sha256: immutableFile.sha256,
            tracker: { backend: 'google-deepmind-tapnextpp-online' },
            segmenter: { backend: 'facebookresearch-sam2.1-video' },
        },
    };
    const observationFile = json(observationPath, raw);
    const report = {
        schema: 'autorig-browser-fit-canary-bridge-report.v1',
        inputs: {
            bundleDirectory,
            observationsPath: observationPath,
            fittingBundleSha256: bundleFile.sha256,
            immutableManifestSha256: immutableFile.sha256,
            sourceVideoSha256: video.sha256,
            skeletonSha256: skeleton.sha256,
            sourceModelSha256,
            observationsSha256: observationFile.sha256,
            bundleFileCount: immutableFiles.length,
            bundleTotalBytes: immutable.bundle_total_bytes,
        },
        mappings: [],
    };
    json(bridgeReportPath, report);
    return { raw, report, observationPath, bridgeReportPath, videoPath, bundleDirectory };
}

function png(width, height, pixels) {
    const chunk = (type, data) => {
        const size = Buffer.alloc(4);
        size.writeUInt32BE(data.length);
        return Buffer.concat([size, Buffer.from(type), data, Buffer.alloc(4)]);
    };
    const header = Buffer.alloc(13);
    header.writeUInt32BE(width, 0);
    header.writeUInt32BE(height, 4);
    header.set([8, 0, 0, 0, 0], 8);
    const rows = [];
    for (let y = 0; y < height; y += 1) {
        rows.push(Buffer.from([0, ...pixels.slice(y * width, (y + 1) * width)]));
    }
    return Buffer.concat([
        Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
        chunk('IHDR', header),
        chunk('IDAT', zlib.deflateSync(Buffer.concat(rows))),
        chunk('IEND', Buffer.alloc(0)),
    ]);
}

test('diagnostic validates every bridge/raw/immutable/source-video pin and fails closed on tampering', (t) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'autorig-hoof-pins-'));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const value = fixture(root);
    const integrity = validateBridgeAndRawPins(value);
    assert.equal(integrity.immutableFiles.length, 2);
    assert.equal(integrity.sourceVideo.sha256, value.raw.provenance.source_video_sha256);

    const mismatchedVideoPin = structuredClone(value.report);
    mismatchedVideoPin.inputs.sourceVideoSha256 = 'b'.repeat(64);
    assert.throws(
        () => validateBridgeAndRawPins({ ...value, report: mismatchedVideoPin }),
        /source-video SHA-256 does not match/,
    );

    const wrongObservationPath = structuredClone(value.report);
    wrongObservationPath.inputs.observationsPath = path.join(root, 'other.json');
    assert.throws(
        () => validateBridgeAndRawPins({ ...value, report: wrongObservationPath }),
        /observations path does not match/,
    );

    fs.appendFileSync(value.videoPath, 'tampered');
    assert.throws(() => validateBridgeAndRawPins(value), /source-video bytes do not match/);
    fs.writeFileSync(value.videoPath, 'synthetic video bytes');

    fs.appendFileSync(path.join(value.bundleDirectory, 'skeleton.json'), 'tampered');
    assert.throws(
        () => validateBridgeAndRawPins(value),
        /(skeleton bytes|immutable bundle file skeleton\.json)/,
    );
});

test('bridge mappings preserve one-to-one semantic, source-track and source-anchor identity', () => {
    const feet = ['hind_left', 'fore_left', 'hind_right', 'fore_right'];
    const semanticIds = feet.flatMap((foot) => ['proximal', 'joint', 'hoof'].map((part) => `${foot}.${part}`));
    const raw = {
        tracks: semanticIds.map((semanticAnchorId, index) => ({
            id: `track-${index}`,
            anchor_id: `bone-${index}:${index}`,
            points: [],
        })),
        provenance: {},
    };
    const report = {
        mappings: semanticIds.map((semanticAnchorId, index) => ({
            limb: semanticAnchorId.split('.')[0],
            semanticAnchorId,
            sourceTrackId: `track-${index}`,
            sourceAnchorId: `bone-${index}:${index}`,
            sourceBone: `bone-${index}`,
        })),
    };
    assert.equal(prepareBridgeObservations(raw, report).tracks.length, semanticIds.length);
    const duplicate = structuredClone(report);
    duplicate.mappings[1].sourceTrackId = 'track-0';
    assert.throws(() => prepareBridgeObservations(raw, duplicate), /duplicate bridge source track/);
    const wrongBone = structuredClone(report);
    wrongBone.mappings[0].sourceBone = 'different-bone';
    assert.throws(() => prepareBridgeObservations(raw, wrongBone), /does not match sourceBone/);
});

test('SAM2 mask manifest binds chronological declared paths, bytes and hashes', (t) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'autorig-hoof-masks-'));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const maskDirectory = path.join(root, 'masks');
    const first = write(path.join(maskDirectory, 'frame-0.png'), png(2, 2, [0, 255, 255, 0]));
    write(path.join(maskDirectory, 'frame-1.png'), png(2, 2, [255, 0, 0, 255]));
    const raw = {
        frame_count: 2,
        silhouettes: [
            { frame: 0, path: 'masks/frame-0.png', bytes: first.bytes, sha256: first.sha256 },
            { frame: 1, path: 'masks/frame-1.png' },
        ],
    };
    const result = loadMaskFrames({ raw, observationPath: path.join(root, 'observations.json') });
    assert.equal(result.masks.length, 2);
    assert.equal(result.manifest.files.length, 2);
    assert.match(result.manifest.sha256, /^[0-9a-f]{64}$/);
    assert.equal(result.manifest.files[0].sha256, first.sha256);
    const tamperedPin = structuredClone(raw);
    tamperedPin.silhouettes[0].sha256 = 'c'.repeat(64);
    assert.throws(
        () => loadMaskFrames({ raw: tamperedPin, observationPath: path.join(root, 'observations.json') }),
        /does not match its declared SHA-256/,
    );

    const firstPath = path.join(maskDirectory, 'frame-0.png');
    const originalReadFileSync = fs.readFileSync;
    let mutated = false;
    fs.readFileSync = function patchedReadFileSync(filename, ...args) {
        const bytes = originalReadFileSync.call(fs, filename, ...args);
        if (!mutated && typeof filename === 'string' && path.resolve(filename) === path.resolve(firstPath)) {
            mutated = true;
            fs.appendFileSync(firstPath, Buffer.from([0]));
        }
        return bytes;
    };
    try {
        assert.throws(
            () => loadMaskFrames({ raw, observationPath: path.join(root, 'observations.json') }),
            /changed while its immutable bytes were read/,
        );
        assert.equal(mutated, true);
    } finally {
        fs.readFileSync = originalReadFileSync;
    }
});
