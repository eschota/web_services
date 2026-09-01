#!/usr/bin/env node
/** Semantic terminal-pose policy over the canonical Horse_2 browser QA.
 *
 * `browser_horse_visual_phase_qa.mjs` intentionally started with a death/fall
 * one-shot gate.  Applying its mandatory centroid-drop gate to attacks, turns,
 * hits, jump take-off, or get-up would reject valid taxonomy actions.  This
 * wrapper runs that unchanged fixed-camera/Three/deformation measurement, then
 * applies the authoritative taxonomy end-pose policy without weakening mesh,
 * camera, ground-penetration, or human-review gates.
 */
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
    HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS,
    HORSE_VISUAL_PHASE_QA_SCHEMA,
    HORSE_VISUAL_PHASE_REQUIRED_PHASES,
    HORSE_VISUAL_PHASE_THRESHOLDS,
    isHorseRootMotionBone,
    parseHorseVisualPhaseQaArgs,
    runHorseVisualPhaseQa,
} from './browser_horse_visual_phase_qa.mjs';

export const SEMANTIC_VISUAL_PHASE_QA_SCHEMA =
    'autorig.browser-animation-semantic-visual-phase-qa.v2';

export const SEMANTIC_NON_ROOT_LOCAL_MOTION_THRESHOLDS = Object.freeze({
    minimumPositionDeltaWorld: 1e-3,
    minimumQuaternionAngleRad: 1e-2,
});

const TAXONOMY_PATH = fileURLToPath(new URL(
    '../../backend/animal_animation_taxonomy.v1.json',
    import.meta.url,
));
const SHA256_RE = /^[0-9a-f]{64}$/;
const ACTION_PROMPTS_PATH = fileURLToPath(new URL(
    '../../backend/animation_fitting/specs/action_prompts.v1.json',
    import.meta.url,
));

function fail(message) {
    throw new Error(message);
}

function sha256Buffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function samePath(left, right) {
    const normalize = (value) => {
        const resolved = path.resolve(value);
        return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
    };
    return normalize(left) === normalize(right);
}

function statIdentity(stat) {
    return {
        dev: String(stat.dev), ino: String(stat.ino), nlink: Number(stat.nlink), size: Number(stat.size),
        mtimeNs: String(stat.mtimeNs ?? BigInt(Math.trunc(Number(stat.mtimeMs) * 1e6))),
        ctimeNs: String(stat.ctimeNs ?? BigInt(Math.trunc(Number(stat.ctimeMs) * 1e6))),
    };
}

function sameIdentity(left, right) {
    return Object.keys(left).every((key) => left[key] === right[key]);
}

function sameDirectoryObject(left, right) {
    return left.dev === right.dev && left.ino === right.ino;
}

function unlinked(filenameValue, field) {
    const filename = path.resolve(filenameValue);
    let lstat;
    let realpath;
    try {
        lstat = fs.lstatSync(filename, { bigint: true });
        realpath = fs.realpathSync.native(filename);
    } catch (error) {
        fail(`${field} cannot be resolved safely: ${error.message}`);
    }
    if (lstat.isSymbolicLink() || !samePath(filename, realpath)) {
        fail(`${field} must not use a symlink, junction, or reparse alias`);
    }
    return { filename, realpath: path.resolve(realpath), lstat };
}

function directory(filenameValue, field) {
    const value = unlinked(filenameValue, field);
    if (!value.lstat.isDirectory()) fail(`${field} must be a directory`);
    const identity = statIdentity(value.lstat);
    const after = unlinked(value.filename, field);
    if (!after.lstat.isDirectory() || !sameIdentity(identity, statIdentity(after.lstat))) {
        fail(`${field} changed while inspected`);
    }
    return { ...value, identity };
}

function snapshot(filenameValue, field) {
    const value = unlinked(filenameValue, field);
    if (!value.lstat.isFile()) fail(`${field} must be a file: ${value.filename}`);
    const beforePath = statIdentity(value.lstat);
    if (beforePath.nlink !== 1) fail(`${field} must not be hard-linked`);
    const flags = fs.constants.O_RDONLY | (process.platform === 'win32' ? 0 : fs.constants.O_NOFOLLOW);
    let descriptor;
    try {
        descriptor = fs.openSync(value.filename, flags);
        const before = statIdentity(fs.fstatSync(descriptor, { bigint: true }));
        if (!sameIdentity(beforePath, before)) fail(`${field} changed before open`);
        const buffer = fs.readFileSync(descriptor);
        const after = statIdentity(fs.fstatSync(descriptor, { bigint: true }));
        const afterPath = unlinked(value.filename, field);
        if (!sameIdentity(before, after) || !sameIdentity(after, statIdentity(afterPath.lstat))
            || buffer.length !== after.size || after.nlink !== 1
            || !samePath(value.realpath, afterPath.realpath)) fail(`${field} changed while read`);
        return {
            path: value.filename,
            realpath: value.realpath,
            bytes: buffer.length,
            sha256: sha256Buffer(buffer),
            buffer,
        };
    } finally {
        if (descriptor !== undefined) fs.closeSync(descriptor);
    }
}

function jsonSnapshot(filename, field) {
    const pin = snapshot(filename, field);
    try {
        return { ...pin, json: JSON.parse(pin.buffer.toString('utf8')) };
    } catch (error) {
        fail(`${field} is invalid JSON: ${error.message}`);
    }
}

function parseArtifactJson(snapshotValue, field) {
    let payload = snapshotValue.buffer;
    if (snapshotValue.path.toLowerCase().endsWith('.gz')) {
        try { payload = zlib.gunzipSync(payload); } catch (error) {
            fail(`${field} is not valid gzip: ${error.message}`);
        }
    }
    try { return JSON.parse(payload.toString('utf8')); } catch (error) {
        fail(`${field} is not valid JSON: ${error.message}`);
    }
}

const PNG_SIGNATURE = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);

function crc32(buffer) {
    let crc = 0xffffffff;
    for (const byte of buffer) {
        crc ^= byte;
        for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
    }
    return (crc ^ 0xffffffff) >>> 0;
}

function validatePng(snapshotValue, expectedWidth, expectedHeight, field) {
    const input = snapshotValue.buffer;
    if (input.length < 33 || !input.subarray(0, 8).equals(PNG_SIGNATURE)) fail(`${field} is not PNG`);
    let offset = 8;
    let ihdr = null;
    let sawEnd = false;
    const compressed = [];
    while (offset < input.length) {
        if (offset + 12 > input.length) fail(`${field} has a truncated PNG chunk`);
        const length = input.readUInt32BE(offset);
        const type = input.toString('ascii', offset + 4, offset + 8);
        const dataStart = offset + 8;
        const dataEnd = dataStart + length;
        if (dataEnd + 4 > input.length) fail(`${field} PNG chunk ${type} exceeds file bounds`);
        const expectedCrc = input.readUInt32BE(dataEnd);
        const actualCrc = crc32(input.subarray(offset + 4, dataEnd));
        if (actualCrc !== expectedCrc) fail(`${field} PNG chunk ${type} has invalid CRC`);
        const data = input.subarray(dataStart, dataEnd);
        if (type === 'IHDR') {
            if (ihdr || length !== 13 || offset !== 8) fail(`${field} has invalid IHDR`);
            ihdr = {
                width: data.readUInt32BE(0),
                height: data.readUInt32BE(4),
                bitDepth: data[8],
                colorType: data[9],
                compression: data[10],
                filter: data[11],
                interlace: data[12],
            };
        } else if (type === 'IDAT') compressed.push(data);
        else if (type === 'IEND') {
            if (length !== 0) fail(`${field} has invalid IEND`);
            sawEnd = true;
        }
        offset = dataEnd + 4;
        if (sawEnd && offset !== input.length) fail(`${field} has bytes after IEND`);
    }
    if (!ihdr || !sawEnd || !compressed.length
        || ihdr.width !== expectedWidth || ihdr.height !== expectedHeight
        || ihdr.bitDepth !== 8 || ![2, 6].includes(ihdr.colorType)
        || ihdr.compression !== 0 || ihdr.filter !== 0 || ihdr.interlace !== 0) {
        fail(`${field} is not an exact ${expectedWidth}x${expectedHeight} 8-bit RGB/RGBA PNG`);
    }
    const channels = ihdr.colorType === 6 ? 4 : 3;
    const stride = 1 + expectedWidth * channels;
    const expectedDecodedBytes = stride * expectedHeight;
    const compressedBytes = compressed.reduce((total, chunk) => total + chunk.length, 0);
    if (compressedBytes > expectedDecodedBytes + 64 * 1024) {
        fail(`${field} PNG compressed payload exceeds its exact decoded-size bound`);
    }
    let raw;
    try {
        raw = zlib.inflateSync(Buffer.concat(compressed, compressedBytes), {
            maxOutputLength: expectedDecodedBytes,
        });
    } catch (error) {
        fail(`${field} PNG IDAT cannot be decoded: ${error.message}`);
    }
    if (raw.length !== stride * expectedHeight) fail(`${field} PNG scanline inventory is invalid`);
    for (let row = 0; row < expectedHeight; row += 1) {
        if (raw[row * stride] > 4) fail(`${field} PNG row ${row} has an invalid filter`);
    }
}

function mp4Boxes(buffer, start = 0, end = buffer.length, field = 'MP4') {
    const boxes = [];
    let offset = start;
    while (offset < end) {
        if (offset + 8 > end) fail(`${field} has a truncated box header`);
        let size = buffer.readUInt32BE(offset);
        const type = buffer.toString('ascii', offset + 4, offset + 8);
        let header = 8;
        if (size === 1) {
            if (offset + 16 > end) fail(`${field} has a truncated extended box`);
            const extended = buffer.readBigUInt64BE(offset + 8);
            if (extended > BigInt(Number.MAX_SAFE_INTEGER)) fail(`${field} box ${type} is too large`);
            size = Number(extended);
            header = 16;
        } else if (size === 0) size = end - offset;
        if (size < header || offset + size > end || !/^[\x20-\x7e]{4}$/.test(type)) {
            fail(`${field} box ${type} is invalid`);
        }
        boxes.push({ type, start: offset, dataStart: offset + header, end: offset + size });
        offset += size;
    }
    if (offset !== end) fail(`${field} box inventory does not cover its parent`);
    return boxes;
}

function mp4Child(buffer, box, type, field, required = true) {
    const matches = mp4Boxes(buffer, box.dataStart, box.end, field).filter((item) => item.type === type);
    if (required && matches.length !== 1) fail(`${field} must contain exactly one ${type}`);
    if (matches.length > 1) fail(`${field} repeats ${type}`);
    return matches[0] || null;
}

class BitReader {
    constructor(buffer, field = 'H.264 bitstream') {
        this.buffer = buffer;
        this.field = field;
        this.bit = 0;
    }
    readBit() {
        if (this.bit >= this.buffer.length * 8) fail(`${this.field} is truncated`);
        const result = (this.buffer[Math.floor(this.bit / 8)] >> (7 - (this.bit % 8))) & 1;
        this.bit += 1;
        return result;
    }
    readBits(count) {
        let value = 0;
        for (let index = 0; index < count; index += 1) value = value * 2 + this.readBit();
        return value;
    }
    ue() {
        let leading = 0;
        while (this.readBit() === 0) {
            leading += 1;
            if (leading > 31) fail(`${this.field} Exp-Golomb value is invalid`);
        }
        return (2 ** leading) - 1 + (leading ? this.readBits(leading) : 0);
    }
}

function h264PixelFormat(avcC, field) {
    if (avcC.length < 8 || avcC[0] !== 1) fail(`${field} has invalid avcC`);
    const spsCount = avcC[5] & 0x1f;
    if (!spsCount) fail(`${field} has no H.264 SPS`);
    let cursor = 6;
    let source = null;
    for (let index = 0; index < spsCount; index += 1) {
        if (cursor + 2 > avcC.length) fail(`${field} H.264 SPS length is truncated`);
        const length = avcC.readUInt16BE(cursor);
        cursor += 2;
        if (length < 4 || cursor + length > avcC.length) fail(`${field} H.264 SPS is truncated`);
        const candidate = avcC.subarray(cursor, cursor + length);
        if ((candidate[0] & 0x1f) !== 7) fail(`${field} avcC contains a non-SPS sequence parameter set`);
        source ??= candidate;
        cursor += length;
    }
    if (cursor >= avcC.length) fail(`${field} has no H.264 PPS inventory`);
    const ppsCount = avcC[cursor];
    cursor += 1;
    if (!ppsCount) fail(`${field} has no H.264 PPS`);
    for (let index = 0; index < ppsCount; index += 1) {
        if (cursor + 2 > avcC.length) fail(`${field} H.264 PPS length is truncated`);
        const length = avcC.readUInt16BE(cursor);
        cursor += 2;
        if (length < 2 || cursor + length > avcC.length
            || (avcC[cursor] & 0x1f) !== 8) fail(`${field} H.264 PPS is invalid`);
        cursor += length;
    }
    const rbsp = [];
    for (let index = 1; index < source.length; index += 1) {
        if (index >= 3 && source[index] === 3 && source[index - 1] === 0 && source[index - 2] === 0) continue;
        rbsp.push(source[index]);
    }
    const bits = new BitReader(Buffer.from(rbsp), `${field} H.264 SPS`);
    const profile = bits.readBits(8);
    bits.readBits(8);
    bits.readBits(8);
    bits.ue();
    let chroma = 1;
    let bitDepthLuma = 8;
    let bitDepthChroma = 8;
    if ([100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135].includes(profile)) {
        chroma = bits.ue();
        if (chroma === 3) bits.readBit();
        bitDepthLuma = 8 + bits.ue();
        bitDepthChroma = 8 + bits.ue();
    }
    if (chroma !== 1 || bitDepthLuma !== 8 || bitDepthChroma !== 8) {
        fail(`${field} is not H.264 8-bit 4:2:0`);
    }
    return 'yuv420p';
}

function validateH264Slice(nal, field) {
    const nalType = nal[0] & 0x1f;
    if (![1, 5].includes(nalType) || nal.length < 2) {
        fail(`${field} is not a complete H.264 coded slice`);
    }
    const rbsp = [];
    for (let index = 1; index < nal.length; index += 1) {
        if (index >= 3 && nal[index] === 3 && nal[index - 1] === 0 && nal[index - 2] === 0) continue;
        rbsp.push(nal[index]);
    }
    const bits = new BitReader(Buffer.from(rbsp), `${field} H.264 slice header`);
    bits.ue();
    const sliceType = bits.ue();
    const pictureParameterSetId = bits.ue();
    if (sliceType > 9 || pictureParameterSetId > 255) {
        fail(`${field} H.264 slice header is invalid`);
    }
}

function inspectMp4(snapshotValue, field, expectedFrameCount) {
    if (!Number.isInteger(expectedFrameCount) || ![33, 49, 65, 97].includes(expectedFrameCount)) {
        fail(`${field} expected frame count is outside the canonical action contract`);
    }
    const input = snapshotValue.buffer;
    const root = mp4Boxes(input, 0, input.length, field);
    if (!root.some((box) => box.type === 'ftyp') || !root.some((box) => box.type === 'mdat')) {
        fail(`${field} lacks ftyp/mdat`);
    }
    const moovRows = root.filter((box) => box.type === 'moov');
    if (moovRows.length !== 1) fail(`${field} must contain one moov`);
    const moov = moovRows[0];
    const mediaRanges = root.filter((box) => box.type === 'mdat')
        .map((box) => ({ start: box.dataStart, end: box.end }));
    const tracks = mp4Boxes(input, moov.dataStart, moov.end, `${field}.moov`)
        .filter((box) => box.type === 'trak').map((trak, trackIndex) => {
            const mdia = mp4Child(input, trak, 'mdia', `${field}.trak[${trackIndex}]`);
            const hdlr = mp4Child(input, mdia, 'hdlr', `${field}.trak[${trackIndex}].mdia`);
            if (hdlr.dataStart + 12 > hdlr.end) fail(`${field} hdlr is truncated`);
            const handler = input.toString('ascii', hdlr.dataStart + 8, hdlr.dataStart + 12);
            const mdhd = mp4Child(input, mdia, 'mdhd', `${field}.trak[${trackIndex}].mdia`);
            const version = input[mdhd.dataStart];
            const timescaleOffset = mdhd.dataStart + (version === 1 ? 20 : 12);
            const durationOffset = timescaleOffset + 4;
            if (durationOffset + (version === 1 ? 8 : 4) > mdhd.end) fail(`${field} mdhd is truncated`);
            const timescale = input.readUInt32BE(timescaleOffset);
            const duration = version === 1
                ? Number(input.readBigUInt64BE(durationOffset)) : input.readUInt32BE(durationOffset);
            if (!(timescale > 0) || !(duration > 0)) fail(`${field} mdhd timing is invalid`);
            const result = { handler, durationSeconds: duration / timescale };
            if (handler !== 'vide') return result;
            const minf = mp4Child(input, mdia, 'minf', `${field}.trak[${trackIndex}].mdia`);
            const stbl = mp4Child(input, minf, 'stbl', `${field}.trak[${trackIndex}].minf`);
            const stsd = mp4Child(input, stbl, 'stsd', `${field}.trak[${trackIndex}].stbl`);
            if (stsd.dataStart + 8 > stsd.end || input.readUInt32BE(stsd.dataStart + 4) !== 1) {
                fail(`${field} video stsd must contain one sample entry`);
            }
            const entries = mp4Boxes(input, stsd.dataStart + 8, stsd.end, `${field}.stsd`);
            if (entries.length !== 1 || !['avc1', 'avc3'].includes(entries[0].type)) fail(`${field} video is not H.264`);
            const entry = entries[0];
            if (entry.dataStart + 78 > entry.end) fail(`${field} avc1 entry is truncated`);
            const width = input.readUInt16BE(entry.dataStart + 24);
            const height = input.readUInt16BE(entry.dataStart + 26);
            const codecChildren = mp4Boxes(input, entry.dataStart + 78, entry.end, `${field}.avc1`);
            const avcC = codecChildren.find((box) => box.type === 'avcC');
            if (!avcC) fail(`${field} video has no avcC`);
            const pixelFormat = h264PixelFormat(input.subarray(avcC.dataStart, avcC.end), field);
            const stsz = mp4Child(input, stbl, 'stsz', `${field}.trak[${trackIndex}].stbl`);
            if (stsz.dataStart + 12 > stsz.end) fail(`${field} stsz is truncated`);
            const fixedSampleSize = input.readUInt32BE(stsz.dataStart + 4);
            const sampleCount = input.readUInt32BE(stsz.dataStart + 8);
            if (sampleCount !== expectedFrameCount) fail(`${field} video coded sample count changed`);
            const sampleSizes = fixedSampleSize
                ? Array(sampleCount).fill(fixedSampleSize)
                : Array.from({ length: sampleCount }, (_, index) => {
                    const offset = stsz.dataStart + 12 + index * 4;
                    if (offset + 4 > stsz.end) fail(`${field} stsz sample table is truncated`);
                    return input.readUInt32BE(offset);
                });
            if (sampleSizes.some((size) => size <= 0)) fail(`${field} contains an empty coded sample`);
            const stts = mp4Child(input, stbl, 'stts', `${field}.trak[${trackIndex}].stbl`);
            if (stts.dataStart + 8 > stts.end) fail(`${field} stts is truncated`);
            const entryCount = input.readUInt32BE(stts.dataStart + 4);
            if (!entryCount || entryCount > expectedFrameCount) fail(`${field} stts entry count is invalid`);
            let sampleTimelineCount = 0;
            let mediaTicks = 0;
            for (let index = 0; index < entryCount; index += 1) {
                const row = stts.dataStart + 8 + index * 8;
                if (row + 8 > stts.end) fail(`${field} stts entry is truncated`);
                const count = input.readUInt32BE(row);
                const delta = input.readUInt32BE(row + 4);
                sampleTimelineCount += count;
                mediaTicks += count * delta;
            }
            if (sampleTimelineCount !== sampleCount || mediaTicks <= 0
                || Math.abs(mediaTicks / timescale - duration / timescale) > 1e-9) {
                fail(`${field} sample timeline is inconsistent`);
            }
            const stsc = mp4Child(input, stbl, 'stsc', `${field}.trak[${trackIndex}].stbl`);
            if (stsc.dataStart + 8 > stsc.end) fail(`${field} stsc is truncated`);
            const stscCount = input.readUInt32BE(stsc.dataStart + 4);
            if (!stscCount || stscCount > expectedFrameCount) fail(`${field} stsc entry count is invalid`);
            const chunkRuns = Array.from({ length: stscCount }, (_, index) => {
                const offset = stsc.dataStart + 8 + index * 12;
                if (offset + 12 > stsc.end) fail(`${field} stsc entry is truncated`);
                return {
                    firstChunk: input.readUInt32BE(offset),
                    samplesPerChunk: input.readUInt32BE(offset + 4),
                    descriptionIndex: input.readUInt32BE(offset + 8),
                };
            });
            if (!chunkRuns.length || chunkRuns[0].firstChunk !== 1
                || chunkRuns.some((row, index) => !row.samplesPerChunk
                    || row.samplesPerChunk > expectedFrameCount || row.descriptionIndex !== 1
                    || (index > 0 && row.firstChunk <= chunkRuns[index - 1].firstChunk))) {
                fail(`${field} sample-to-chunk table is invalid`);
            }
            const stco = mp4Child(input, stbl, 'stco', `${field}.trak[${trackIndex}].stbl`, false);
            const co64 = mp4Child(input, stbl, 'co64', `${field}.trak[${trackIndex}].stbl`, false);
            if (Boolean(stco) === Boolean(co64)) fail(`${field} must contain exactly one chunk-offset table`);
            const offsetsBox = stco || co64;
            if (offsetsBox.dataStart + 8 > offsetsBox.end) fail(`${field} chunk-offset table is truncated`);
            const chunkCount = input.readUInt32BE(offsetsBox.dataStart + 4);
            if (!chunkCount || chunkCount > expectedFrameCount) fail(`${field} chunk count is invalid`);
            const chunkOffsets = Array.from({ length: chunkCount }, (_, index) => {
                const offset = offsetsBox.dataStart + 8 + index * (stco ? 4 : 8);
                if (offset + (stco ? 4 : 8) > offsetsBox.end) fail(`${field} chunk-offset entry is truncated`);
                const value = stco ? input.readUInt32BE(offset) : input.readBigUInt64BE(offset);
                if (typeof value === 'bigint' && value > BigInt(Number.MAX_SAFE_INTEGER)) {
                    fail(`${field} chunk offset exceeds safe file addressing`);
                }
                return Number(value);
            });
            if (!chunkOffsets.length || new Set(chunkOffsets).size !== chunkOffsets.length) {
                fail(`${field} chunk-offset inventory is invalid`);
            }
            const lengthSize = (input[avcC.dataStart + 4] & 3) + 1;
            let sampleIndex = 0;
            let sawVcl = false;
            chunkOffsets.forEach((chunkOffset, zeroBasedChunk) => {
                const chunkNumber = zeroBasedChunk + 1;
                const run = [...chunkRuns].reverse().find((row) => row.firstChunk <= chunkNumber);
                let cursor = chunkOffset;
                for (let item = 0; item < run.samplesPerChunk; item += 1) {
                    if (sampleIndex >= sampleSizes.length) fail(`${field} chunk table declares too many samples`);
                    const sampleEnd = cursor + sampleSizes[sampleIndex];
                    const mediaRange = mediaRanges.find((range) => cursor >= range.start && sampleEnd <= range.end);
                    if (!mediaRange) fail(`${field} coded sample is outside mdat payload`);
                    let nalOffset = cursor;
                    let nalCount = 0;
                    let sampleVcl = false;
                    while (nalOffset < sampleEnd) {
                        if (nalOffset + lengthSize > sampleEnd) fail(`${field} H.264 NAL length is truncated`);
                        let nalLength = 0;
                        for (let byte = 0; byte < lengthSize; byte += 1) {
                            nalLength = nalLength * 256 + input[nalOffset + byte];
                        }
                        nalOffset += lengthSize;
                        if (!nalLength || nalOffset + nalLength > sampleEnd) fail(`${field} H.264 NAL payload is invalid`);
                        const nal = input.subarray(nalOffset, nalOffset + nalLength);
                        const nalType = nal[0] & 0x1f;
                        if ([1, 5].includes(nalType)) {
                            validateH264Slice(nal, `${field} sample ${sampleIndex}`);
                            sampleVcl = true;
                        }
                        nalCount += 1;
                        nalOffset += nalLength;
                    }
                    if (!nalCount || !sampleVcl) fail(`${field} coded sample has no H.264 VCL NAL unit`);
                    sawVcl ||= sampleVcl;
                    cursor = sampleEnd;
                    sampleIndex += 1;
                }
            });
            if (sampleIndex !== sampleCount || !sawVcl) fail(`${field} chunk/sample inventory is incomplete`);
            return {
                ...result,
                codec: 'h264',
                pixelFormat,
                width,
                height,
                frameCount: sampleCount,
                fps: sampleCount / (mediaTicks / timescale),
            };
        });
    const video = tracks.filter((track) => track.handler === 'vide');
    const audio = tracks.filter((track) => track.handler === 'soun');
    if (video.length !== 1) fail(`${field} must contain one video track`);
    return { ...video[0], audioStreamCount: audio.length };
}

function validateMp4Decode(snapshotValue, expectedFfmpeg) {
    const descriptor = object(expectedFfmpeg, 'expectedFfmpeg');
    const executable = snapshot(descriptor.path, 'pinned ffmpeg decoder');
    if (!samePath(executable.realpath, descriptor.realpath)
        || executable.bytes !== descriptor.bytes || executable.sha256 !== descriptor.sha256
        || !/^ffmpeg(?:\.exe)?$/i.test(path.basename(executable.realpath))) {
        fail('ffmpeg decoder does not match its immutable runtime pin');
    }
    const decoded = spawnSync(executable.path, [
        '-hide_banner', '-loglevel', 'error', '-xerror', '-nostdin',
        '-i', snapshotValue.path, '-map', '0:v:0', '-f', 'null', '-',
    ], {
        encoding: 'utf8', windowsHide: true, maxBuffer: 8 * 1024 * 1024,
        timeout: 15_000, killSignal: 'SIGKILL',
    });
    if (decoded.error || decoded.status !== 0) {
        fail(`fixed-camera MP4 fails pinned ffmpeg decode: ${decoded.error?.message || decoded.stderr || decoded.status}`);
    }
}

function pin(snapshotValue) {
    return {
        path: snapshotValue.path,
        realpath: snapshotValue.realpath,
        bytes: snapshotValue.bytes,
        sha256: snapshotValue.sha256,
    };
}

function writeNewJson(filename, value) {
    const buffer = Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
    const descriptor = fs.openSync(filename, 'wx');
    try {
        fs.writeFileSync(descriptor, buffer);
        fs.fsyncSync(descriptor);
    } finally {
        fs.closeSync(descriptor);
    }
    const written = snapshot(filename, 'semantic visual QA report');
    return pin(written);
}

function requireExactKeys(value, keys, field) {
    const actual = Object.keys(object(value, field)).sort();
    const expected = [...keys].sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        fail(`${field} must contain exactly: ${expected.join(', ')}`);
    }
}

export function resolveSemanticTerminalPolicy(semanticId) {
    const taxonomySnapshot = jsonSnapshot(TAXONOMY_PATH, 'animal animation taxonomy');
    const promptSnapshot = jsonSnapshot(ACTION_PROMPTS_PATH, 'animation fitting action prompts');
    const taxonomy = taxonomySnapshot.json;
    if (taxonomy.schema !== 'animal-animation-taxonomy.v1'
        || taxonomy.output_fps !== 30
        || !Array.isArray(taxonomy.clips) || taxonomy.clips.length !== 30
        || taxonomy.clips.some((row, index) => row?.order !== index + 1
            || typeof row.id !== 'string' || ![33, 49, 65, 97].includes(row.frame_profile))) {
        fail('animal animation taxonomy contract changed');
    }
    const prompts = promptSnapshot.json;
    if (prompts.schema !== 'autorig.animation-fitting-prompts.v1'
        || prompts.taxonomy_schema !== taxonomy.schema || prompts.output_fps_int !== 30
        || prompts.frame_rule_string !== '8n+1' || !Array.isArray(prompts.actions_array)
        || prompts.actions_array.length !== 30) {
        fail('animation fitting action prompt contract changed');
    }
    const taxonomyIds = taxonomy.clips.map((row) => row.id);
    const promptIds = prompts.actions_array.map((row) => row?.action_id_string);
    if (new Set(taxonomyIds).size !== 30 || new Set(promptIds).size !== 30
        || JSON.stringify(taxonomyIds) !== JSON.stringify(promptIds)) {
        fail('taxonomy/action prompt inventories are not the same exact ordered 30 actions');
    }
    taxonomy.clips.forEach((taxonomyRow, index) => {
        const promptRow = prompts.actions_array[index];
        const generationMode = taxonomyRow.loop === true ? 'loop' : 'one_shot';
        if (promptRow.generation_mode_string !== generationMode
            || promptRow.frame_count_int !== taxonomyRow.frame_profile
            || typeof promptRow.motion_prompt_string !== 'string'
            || !promptRow.motion_prompt_string.trim()) {
            fail(`taxonomy/action prompt timing contract changed for ${taxonomyRow.id}`);
        }
    });
    const rows = taxonomy.clips.filter((row) => row?.id === semanticId);
    if (rows.length !== 1) fail(`semanticId ${semanticId} is not unique in taxonomy`);
    const action = rows[0];
    const prompt = prompts.actions_array[taxonomy.clips.indexOf(action)];
    const generationMode = action.loop === true ? 'loop' : 'one_shot';
    let terminalPolicy;
    if (generationMode === 'loop') terminalPolicy = 'loop_closure';
    else if (action.end_pose_id === 'death_end') terminalPolicy = 'settled_grounded_death';
    else if (action.end_pose_id === 'airborne') terminalPolicy = 'airborne_transition';
    else if (['default_pose', 'locomotion_contact'].includes(action.end_pose_id)) {
        terminalPolicy = 'settled_grounded_action';
    } else fail(`${semanticId} has unsupported end_pose_id ${action.end_pose_id}`);
    if (semanticId === 'fall' && (generationMode !== 'one_shot' || action.frame_profile !== 49
        || action.start_pose_id !== 'airborne' || action.end_pose_id !== 'death_end'
        || !/Do not loop, reverse, recover, or return/.test(prompt.motion_prompt_string))) {
        fail('fall must remain the exact one-shot airborne-to-death terminal contract');
    }
    return {
        semanticId,
        generationMode,
        frameCount: action.frame_profile,
        startPoseId: action.start_pose_id,
        endPoseId: action.end_pose_id,
        terminalPolicy,
        taxonomy: pin(taxonomySnapshot),
        actionPrompts: pin(promptSnapshot),
    };
}

export function evaluateSemanticVisualQa({ policy, deformationReport, finalPoseReport = null }) {
    if (!policy || !['loop', 'one_shot'].includes(policy.generationMode)) fail('semantic policy is invalid');
    if (deformationReport?.schema !== 'autorig.browser-horse-target-deformation-qa.v1') {
        fail('target deformation report schema is invalid');
    }
    const deformationPassed = deformationReport.passed === true;
    let terminalGates;
    if (policy.terminalPolicy === 'loop_closure') {
        if (finalPoseReport != null) fail('loop QA must not carry one-shot final-pose evidence');
        terminalGates = { loopDeformationAndClosure: deformationPassed };
    } else {
        if (finalPoseReport?.schema !== 'autorig.browser-horse-one-shot-final-pose-qa.v1') {
            fail('one-shot semantic QA requires measured final-pose evidence');
        }
        const measured = finalPoseReport.gates || {};
        if (policy.terminalPolicy === 'settled_grounded_death') {
            terminalGates = {
                finalP99Motion: measured.finalP99Motion === true,
                finalMedianMotion: measured.finalMedianMotion === true,
                centroidDrop: measured.centroidDrop === true,
                groundContact: measured.groundContact === true,
                groundPenetration: measured.groundPenetration === true,
                cameraStatic: measured.cameraStatic === true,
            };
        } else if (policy.terminalPolicy === 'settled_grounded_action') {
            terminalGates = {
                finalP99Motion: measured.finalP99Motion === true,
                finalMedianMotion: measured.finalMedianMotion === true,
                groundContact: measured.groundContact === true,
                groundPenetration: measured.groundPenetration === true,
                cameraStatic: measured.cameraStatic === true,
            };
        } else if (policy.terminalPolicy === 'airborne_transition') {
            terminalGates = {
                airborne: measured.groundContact === false,
                groundPenetration: measured.groundPenetration === true,
                cameraStatic: measured.cameraStatic === true,
            };
        } else fail(`unsupported semantic terminal policy ${policy.terminalPolicy}`);
    }
    const terminalPassed = Object.values(terminalGates).every(Boolean);
    return {
        deformationPassed,
        terminalPolicy: policy.terminalPolicy,
        terminalGates,
        terminalPassed,
        machinePassed: deformationPassed && terminalPassed,
    };
}

function quaternionAngleFromFirst(first, current, field) {
    const firstLength = Math.hypot(...first);
    const currentLength = Math.hypot(...current);
    if (!(firstLength > 1e-12) || !(currentLength > 1e-12)) {
        fail(`${field} contains a zero-length quaternion`);
    }
    const dot = Math.abs(first.reduce(
        (total, value, index) => total + value * current[index],
        0,
    ) / (firstLength * currentLength));
    return 2 * Math.acos(Math.min(1, Math.max(-1, dot)));
}

/**
 * Measures animation in bone-local space while excluding every armature/root
 * motion bone.  This prevents one-shot model travel (or floating-point noise
 * copied into a limb track) from being accepted as skeletal animation.
 */
export function assessMeaningfulNonRootLocalMotion({
    tracks,
    frameCount,
    rootBoneNames,
    thresholds = SEMANTIC_NON_ROOT_LOCAL_MOTION_THRESHOLDS,
}) {
    if (!Array.isArray(tracks) || !tracks.length) fail('local motion tracks must be a non-empty array');
    if (!Number.isSafeInteger(frameCount) || frameCount < 2) fail('local motion frameCount must be >= 2');
    const roots = rootBoneNames instanceof Set ? rootBoneNames : new Set(rootBoneNames || []);
    const minimumPositionDeltaWorld = finite(
        thresholds?.minimumPositionDeltaWorld,
        'minimum local position delta',
    );
    const minimumQuaternionAngleRad = finite(
        thresholds?.minimumQuaternionAngleRad,
        'minimum local quaternion angle',
    );
    if (!(minimumPositionDeltaWorld > 0) || !(minimumQuaternionAngleRad > 0)) {
        fail('local motion thresholds must be positive');
    }
    const movingBoneNames = new Set();
    let maximumPositionDeltaWorld = 0;
    let maximumQuaternionAngleRad = 0;
    for (const [trackIndex, track] of tracks.entries()) {
        const match = typeof track?.name === 'string'
            && track.name.match(/^(.*)\.(quaternion|position)$/);
        if (!match || roots.has(match[1])) continue;
        const size = match[2] === 'quaternion' ? 4 : 3;
        if (!Array.isArray(track.times) || track.times.length !== frameCount
            || !Array.isArray(track.values) || track.values.length !== frameCount * size) {
            fail(`local motion track ${trackIndex} has an invalid timeline/value inventory`);
        }
        const first = track.values.slice(0, size)
            .map((value, item) => finite(value, `${track.name}[0,${item}]`));
        for (let frame = 1; frame < frameCount; frame += 1) {
            const current = track.values.slice(frame * size, (frame + 1) * size)
                .map((value, item) => finite(value, `${track.name}[${frame},${item}]`));
            if (match[2] === 'quaternion') {
                const angle = quaternionAngleFromFirst(first, current, `${track.name}[${frame}]`);
                maximumQuaternionAngleRad = Math.max(maximumQuaternionAngleRad, angle);
                if (angle >= minimumQuaternionAngleRad) movingBoneNames.add(match[1]);
            } else {
                const delta = Math.hypot(...current.map((value, item) => value - first[item]));
                maximumPositionDeltaWorld = Math.max(maximumPositionDeltaWorld, delta);
                if (delta >= minimumPositionDeltaWorld) movingBoneNames.add(match[1]);
            }
        }
    }
    return {
        movingBoneNames: [...movingBoneNames].sort(),
        maximumPositionDeltaWorld,
        maximumQuaternionAngleRad,
        thresholds: { minimumPositionDeltaWorld, minimumQuaternionAngleRad },
        passed: movingBoneNames.size > 0,
    };
}

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) fail(`${field} must be an object`);
    return value;
}

function finite(value, field) {
    const result = Number(value);
    if (!Number.isFinite(result)) fail(`${field} must be finite`);
    return result;
}

function integer(value, field, minimum = 0) {
    const result = Number(value);
    if (!Number.isSafeInteger(result) || result < minimum) fail(`${field} must be an integer >= ${minimum}`);
    return result;
}

function nearlyEqual(leftValue, rightValue, field, tolerance = 1e-9) {
    const left = finite(leftValue, `${field}.left`);
    const right = finite(rightValue, `${field}.right`);
    if (Math.abs(left - right) > tolerance * Math.max(1, Math.abs(left), Math.abs(right))) {
        fail(`${field} values disagree`);
    }
}

function descriptorMatches(actual, expected, field) {
    object(actual, field);
    if (!samePath(actual.path || '', expected.path)
        || (actual.realpath != null && !samePath(actual.realpath, expected.realpath))
        || actual.bytes !== expected.bytes || actual.sha256 !== expected.sha256) {
        fail(`${field} does not pin the exact artifact`);
    }
}

function isInside(parent, child) {
    const relative = path.relative(path.resolve(parent), path.resolve(child));
    return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function baseArtifactInventory(baseDirectory, frameCount, oneShot) {
    const baseRoot = directory(baseDirectory, 'base Horse evidence directory');
    const expectedRoot = [
        'camera-settings.json',
        'deformation-report.json',
        'fixed-camera-preview.mp4',
        'frames',
        ...(oneShot ? ['final-pose-stability-report.json'] : []),
        'visual-phase-qa.json',
    ].sort();
    const rootEntries = fs.readdirSync(baseDirectory, { withFileTypes: true })
        .sort((left, right) => left.name.localeCompare(right.name));
    if (JSON.stringify(rootEntries.map((entry) => entry.name)) !== JSON.stringify(expectedRoot)) {
        fail('base Horse evidence has missing or unexpected root inventory');
    }
    rootEntries.forEach((entry) => {
        const resolved = unlinked(path.join(baseDirectory, entry.name), `base Horse evidence ${entry.name}`);
        if (entry.name === 'frames'
            ? !entry.isDirectory() || !resolved.lstat.isDirectory()
            : !entry.isFile() || !resolved.lstat.isFile()) {
            fail(`base Horse evidence entry type changed: ${entry.name}`);
        }
    });
    const framesDirectory = path.join(baseDirectory, 'frames');
    const framesRoot = directory(framesDirectory, 'base Horse frames directory');
    const expectedFrames = Array.from(
        { length: frameCount },
        (_, index) => `frame_${String(index).padStart(4, '0')}.png`,
    );
    const frameEntries = fs.readdirSync(framesDirectory, { withFileTypes: true })
        .sort((left, right) => left.name.localeCompare(right.name));
    if (frameEntries.some((entry) => !entry.isFile())) {
        fail('base Horse frame inventory contains a non-file entry');
    }
    const actualFrames = frameEntries.map((entry) => entry.name);
    if (JSON.stringify(actualFrames) !== JSON.stringify(expectedFrames)) {
        fail('base Horse evidence does not contain the exact all-frame PNG inventory');
    }
    const relativeFiles = [
        ...expectedRoot.filter((name) => name !== 'frames'),
        ...expectedFrames.map((name) => `frames/${name}`),
    ].sort();
    const inventory = relativeFiles.map((relative) => ({
        relative,
        snapshot: snapshot(path.join(baseDirectory, ...relative.split('/')), `base artifact ${relative}`),
    }));
    inventory.filter((row) => row.relative.endsWith('.png')).forEach((row) => {
        validatePng(row.snapshot, 768, 448, `base frame ${row.relative}`);
    });
    const afterBase = directory(baseDirectory, 'base Horse evidence directory');
    const afterFrames = directory(framesDirectory, 'base Horse frames directory');
    const afterRootNames = fs.readdirSync(baseDirectory).sort();
    const afterFrameNames = fs.readdirSync(framesDirectory).sort();
    if (!sameIdentity(baseRoot.identity, afterBase.identity)
        || !sameIdentity(framesRoot.identity, afterFrames.identity)
        || JSON.stringify(afterRootNames) !== JSON.stringify(expectedRoot)
        || JSON.stringify(afterFrameNames) !== JSON.stringify(expectedFrames)) {
        fail('base Horse evidence inventory changed while inspected');
    }
    return inventory;
}

function validateDeformation(deformation, config, evidenceInputs) {
    requireExactKeys(deformation, [
        'schema', 'measuredEveryFrame', 'frameCount', 'vertexCount', 'edgeCount',
        'edgeSampleCount', 'collapsedEdgeSampleCount', 'coincidentRestGroupCount',
        'coincidentRestSampleCount', 'maximumEdgeStretch', 'p99EdgeStretch',
        'zeroWeightVertices', 'maximumCoincidentRestSeparationM', 'thresholds',
        'rootMotionLocked', 'rootMotionLockRequired', 'cameraStatic', 'gates',
        'passed', 'frames', 'inputs',
    ], 'target deformation report');
    requireExactKeys(deformation.inputs, [
        'fittingBundleSha256', 'threeClipSha256', 'skinWeightsSha256', 'topologySha256',
    ], 'target deformation report.inputs');
    if (deformation.schema !== 'autorig.browser-horse-target-deformation-qa.v1'
        || deformation.measuredEveryFrame !== true
        || deformation.frameCount !== evidenceInputs.frameCount
        || deformation.inputs?.fittingBundleSha256 !== config.expectedFittingBundleSha256
        || deformation.inputs?.threeClipSha256 !== config.expectedThreeClipSha256
        || deformation.inputs?.skinWeightsSha256 !== evidenceInputs.skinWeights.sha256
        || deformation.inputs?.topologySha256 !== evidenceInputs.surfaceTopology.sha256
        || JSON.stringify(deformation.thresholds) !== JSON.stringify(HORSE_VISUAL_PHASE_THRESHOLDS)
        || deformation.rootMotionLockRequired !== evidenceInputs.loop
        || !Array.isArray(deformation.frames) || deformation.frames.length !== evidenceInputs.frameCount) {
        fail('target deformation report lost exact inputs, thresholds, chronology, or static camera');
    }
    const frameMaximums = [];
    const frameP99Values = [];
    const frameCoincident = [];
    let collapsedEdgeSampleCount = 0;
    let allRootMotionLocked = true;
    deformation.frames.forEach((frame, index) => {
        requireExactKeys(frame, [
            'frameIndex', 'timeSeconds', 'maximumEdgeStretch', 'p99EdgeStretch',
            'collapsedEdgeSampleCount', 'maximumCoincidentRestSeparationM',
            'rootMotionLocked', 'cameraStatic',
        ], `target deformation report.frames[${index}]`);
        const maximum = finite(frame.maximumEdgeStretch, `frame ${index} maximumEdgeStretch`);
        const p99 = finite(frame.p99EdgeStretch, `frame ${index} p99EdgeStretch`);
        const coincident = finite(
            frame.maximumCoincidentRestSeparationM,
            `frame ${index} maximumCoincidentRestSeparationM`,
        );
        if (frame.frameIndex !== index || Math.abs(finite(frame.timeSeconds, `frame ${index} timeSeconds`)
                - index / 30) > 2e-6
            || maximum < 1 || p99 < 1 || p99 > maximum || coincident < 0
            || frame.cameraStatic !== true || typeof frame.rootMotionLocked !== 'boolean'
            || (evidenceInputs.loop && frame.rootMotionLocked !== true)) {
            fail('target deformation report lost exact frame chronology/measurements/static camera');
        }
        collapsedEdgeSampleCount += integer(
            frame.collapsedEdgeSampleCount,
            `frame ${index} collapsedEdgeSampleCount`,
            0,
        );
        allRootMotionLocked &&= frame.rootMotionLocked;
        frameMaximums.push(maximum);
        frameP99Values.push(p99);
        frameCoincident.push(coincident);
    });
    const vertexCount = integer(deformation.vertexCount, 'deformation vertexCount', 1);
    const edgeCount = integer(deformation.edgeCount, 'deformation edgeCount', 1);
    const topologyEdges = new Set();
    for (const [faceIndex, face] of evidenceInputs.surfaceTopologyJson.faces.entries()) {
        if (!Array.isArray(face?.vertex_ids) || face.vertex_ids.length < 3) fail(`topology face ${faceIndex} is invalid`);
        face.vertex_ids.forEach((first, index) => {
            const second = face.vertex_ids[(index + 1) % face.vertex_ids.length];
            if (!Number.isInteger(first) || !Number.isInteger(second)
                || first < 0 || second < 0 || first >= evidenceInputs.restVertices.length
                || second >= evidenceInputs.restVertices.length || first === second) {
                fail(`topology face ${faceIndex} has an invalid edge`);
            }
            const key = first < second ? `${first}:${second}` : `${second}:${first}`;
            const delta = evidenceInputs.restVertices[first].map(
                (value, axis) => value - evidenceInputs.restVertices[second][axis],
            );
            if (Math.hypot(...delta) > 1e-9) topologyEdges.add(key);
        });
    }
    const coincidentBins = new Map();
    evidenceInputs.restVertices.forEach((position, index) => {
        const key = position.map((value) => Math.round(value / 1e-6)).join(':');
        const rows = coincidentBins.get(key) || [];
        rows.push(index);
        coincidentBins.set(key, rows);
    });
    const coincidentGroupCount = [...coincidentBins.values()].filter((rows) => rows.length > 1).length;
    if (integer(deformation.edgeSampleCount, 'deformation edgeSampleCount', 1)
            !== edgeCount * evidenceInputs.frameCount
        || integer(deformation.collapsedEdgeSampleCount, 'deformation collapsedEdgeSampleCount', 0)
            !== collapsedEdgeSampleCount
        || vertexCount !== evidenceInputs.restVertices.length
        || edgeCount !== topologyEdges.size
        || integer(deformation.coincidentRestGroupCount, 'deformation coincidentRestGroupCount', 0)
            !== coincidentGroupCount
        || integer(deformation.coincidentRestSampleCount, 'deformation coincidentRestSampleCount', 1)
            !== evidenceInputs.frameCount
        || integer(deformation.zeroWeightVertices, 'deformation zeroWeightVertices', 0)
            !== evidenceInputs.zeroWeightVertices
        || deformation.rootMotionLocked !== allRootMotionLocked
        || deformation.cameraStatic !== true || vertexCount < 1) {
        fail('target deformation aggregate inventory does not recompute from per-frame evidence');
    }
    nearlyEqual(deformation.maximumEdgeStretch, Math.max(...frameMaximums), 'maximum edge stretch aggregate');
    nearlyEqual(
        deformation.maximumCoincidentRestSeparationM,
        Math.max(...frameCoincident),
        'coincident separation aggregate',
    );
    const expectedGates = {
        maximumEdgeStretch: finite(deformation.maximumEdgeStretch, 'maximumEdgeStretch')
            <= HORSE_VISUAL_PHASE_THRESHOLDS.maximumEdgeStretch,
        // The report does not carry the raw per-edge sample distribution, so
        // fail closed: both the producer aggregate and every per-frame P99 must
        // satisfy the threshold.  This prevents a forged low top-level P99.
        p99EdgeStretch: finite(deformation.p99EdgeStretch, 'p99EdgeStretch')
            <= HORSE_VISUAL_PHASE_THRESHOLDS.p99EdgeStretch
            && frameP99Values.every((value) => value <= HORSE_VISUAL_PHASE_THRESHOLDS.p99EdgeStretch),
        zeroWeightVertices: deformation.zeroWeightVertices === HORSE_VISUAL_PHASE_THRESHOLDS.zeroWeightVertices,
        coincidentRestSeparation: finite(
            deformation.maximumCoincidentRestSeparationM,
            'maximumCoincidentRestSeparationM',
        ) <= HORSE_VISUAL_PHASE_THRESHOLDS.coincidentRestSeparationM,
        rootMotionLocked: evidenceInputs.loop ? deformation.rootMotionLocked === true : true,
        cameraStatic: deformation.cameraStatic === true,
    };
    if (JSON.stringify(deformation.gates) !== JSON.stringify(expectedGates)
        || deformation.passed !== Object.values(expectedGates).every(Boolean)) {
        fail('target deformation gates do not recompute from exact measurements');
    }
}

function validateFinalPose(finalPose, config, evidenceInputs) {
    requireExactKeys(finalPose, [
        'schema', 'temporalMode', 'frameCount', 'finalWindowFrames', 'modelHeightM',
        'modelDiagonalM', 'groundHeightM', 'initialCentroidZM', 'finalCentroidZM',
        'centroidDropM', 'finalMinimumZM', 'maximumP99AdjacentDisplacementM',
        'maximumMedianAdjacentDisplacementM', 'transitions', 'thresholds',
        'resolvedThresholds', 'gates', 'passed', 'inputs',
    ], 'one-shot final-pose report');
    requireExactKeys(finalPose.inputs, [
        'fittingBundleSha256', 'threeClipSha256', 'skinWeightsSha256',
    ], 'one-shot final-pose report.inputs');
    if (finalPose.schema !== 'autorig.browser-horse-one-shot-final-pose-qa.v1'
        || finalPose.temporalMode !== 'one_shot'
        || finalPose.frameCount !== evidenceInputs.frameCount
        || finalPose.inputs?.fittingBundleSha256 !== config.expectedFittingBundleSha256
        || finalPose.inputs?.threeClipSha256 !== config.expectedThreeClipSha256
        || finalPose.inputs?.skinWeightsSha256 !== evidenceInputs.skinWeights.sha256
        || JSON.stringify(finalPose.thresholds) !== JSON.stringify(HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS)
        || !Array.isArray(finalPose.finalWindowFrames)
        || JSON.stringify(finalPose.finalWindowFrames)
            !== JSON.stringify(Array.from({ length: HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.windowFrames },
                (_, index) => evidenceInputs.frameCount - HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.windowFrames + index))) {
        fail('one-shot final-pose report lost exact inputs, thresholds, or final window');
    }
    const resolved = object(finalPose.resolvedThresholds, 'finalPose.resolvedThresholds');
    requireExactKeys(resolved, [
        'maximumP99AdjacentDisplacementM', 'maximumMedianAdjacentDisplacementM',
        'minimumCentroidDropM', 'groundContactToleranceM', 'maximumGroundPenetrationM',
    ], 'finalPose.resolvedThresholds');
    const modelHeight = finite(finalPose.modelHeightM, 'finalPose.modelHeightM');
    const modelDiagonal = finite(finalPose.modelDiagonalM, 'finalPose.modelDiagonalM');
    const restMinimum = [0, 1, 2].map((axis) => Math.min(...evidenceInputs.restVertices.map((row) => row[axis])));
    const restMaximum = [0, 1, 2].map((axis) => Math.max(...evidenceInputs.restVertices.map((row) => row[axis])));
    const expectedModelHeight = restMaximum[2] - restMinimum[2];
    const expectedModelDiagonal = Math.hypot(...restMaximum.map((value, axis) => value - restMinimum[axis]));
    const expectedGroundHeight = finite(
        evidenceInputs.fittingBundle?.ground_plane?.height,
        'canonical fitting-bundle ground height',
    );
    if (!(modelHeight > 0) || !(modelDiagonal > 0)
        || Math.abs(modelHeight - expectedModelHeight) > 1e-9
        || Math.abs(modelDiagonal - expectedModelDiagonal) > 1e-9
        || Math.abs(finite(finalPose.groundHeightM, 'finalPose.groundHeightM') - expectedGroundHeight) > 1e-9) {
        fail('one-shot final-pose bounds/ground are not derived from the immutable Horse_2 bundle');
    }
    nearlyEqual(
        resolved.maximumP99AdjacentDisplacementM,
        HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.maximumP99AdjacentDisplacementModelDiagonal * modelDiagonal,
        'resolved maximum P99 displacement',
    );
    nearlyEqual(
        resolved.maximumMedianAdjacentDisplacementM,
        HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.maximumMedianAdjacentDisplacementModelDiagonal * modelDiagonal,
        'resolved maximum median displacement',
    );
    nearlyEqual(
        resolved.minimumCentroidDropM,
        HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.minimumCentroidDropModelHeight * modelHeight,
        'resolved minimum centroid drop',
    );
    nearlyEqual(
        resolved.groundContactToleranceM,
        HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.groundContactToleranceModelHeight * modelHeight,
        'resolved ground contact tolerance',
    );
    nearlyEqual(
        resolved.maximumGroundPenetrationM,
        HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.maximumGroundPenetrationModelHeight * modelHeight,
        'resolved maximum ground penetration',
    );
    nearlyEqual(
        finalPose.centroidDropM,
        finite(finalPose.initialCentroidZM, 'initial centroid Z')
            - finite(finalPose.finalCentroidZM, 'final centroid Z'),
        'centroid drop',
    );
    if (!Array.isArray(finalPose.transitions)
        || finalPose.transitions.length !== HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS.windowFrames - 1) {
        fail('one-shot final-pose transition inventory changed');
    }
    const transitionP99 = [];
    const transitionMedians = [];
    finalPose.transitions.forEach((transition, index) => {
        requireExactKeys(transition, [
            'fromFrame', 'toFrame', 'medianDisplacementM', 'p99DisplacementM', 'maximumDisplacementM',
        ], `finalPose.transitions[${index}]`);
        const expectedFrom = finalPose.finalWindowFrames[index];
        const expectedTo = finalPose.finalWindowFrames[index + 1];
        const median = finite(transition.medianDisplacementM, `transition ${index} median`);
        const p99 = finite(transition.p99DisplacementM, `transition ${index} p99`);
        const maximum = finite(transition.maximumDisplacementM, `transition ${index} maximum`);
        if (transition.fromFrame !== expectedFrom || transition.toFrame !== expectedTo
            || median < 0 || p99 < median || maximum < p99) {
            fail('one-shot final-pose transition chronology/measurements changed');
        }
        transitionP99.push(p99);
        transitionMedians.push(median);
    });
    nearlyEqual(
        finalPose.maximumP99AdjacentDisplacementM,
        Math.max(...transitionP99),
        'maximum adjacent P99 displacement',
    );
    nearlyEqual(
        finalPose.maximumMedianAdjacentDisplacementM,
        Math.max(...transitionMedians),
        'maximum adjacent median displacement',
    );
    const expectedGates = {
        finalP99Motion: finite(finalPose.maximumP99AdjacentDisplacementM, 'final P99 motion')
            <= finite(resolved.maximumP99AdjacentDisplacementM, 'resolved final P99 threshold'),
        finalMedianMotion: finite(finalPose.maximumMedianAdjacentDisplacementM, 'final median motion')
            <= finite(resolved.maximumMedianAdjacentDisplacementM, 'resolved final median threshold'),
        centroidDrop: finite(finalPose.centroidDropM, 'centroid drop')
            >= finite(resolved.minimumCentroidDropM, 'minimum centroid drop'),
        groundContact: finite(finalPose.finalMinimumZM, 'final minimum Z')
            <= finite(finalPose.groundHeightM, 'ground height')
                + finite(resolved.groundContactToleranceM, 'ground contact tolerance'),
        groundPenetration: finalPose.finalMinimumZM
            >= finalPose.groundHeightM - finite(resolved.maximumGroundPenetrationM, 'ground penetration threshold'),
        cameraStatic: finalPose.gates?.cameraStatic === true,
    };
    if (JSON.stringify(finalPose.gates) !== JSON.stringify(expectedGates)
        || finalPose.passed !== Object.values(expectedGates).every(Boolean)) {
        fail('one-shot final-pose gates do not recompute from exact measurements');
    }
}

function validateBaseEvidence({ config, policy, baseDirectory, baseResult }) {
    const oneShot = policy.generationMode === 'one_shot';
    const inventory = baseArtifactInventory(baseDirectory, policy.frameCount, oneShot);
    const byRelative = new Map(inventory.map((row) => [row.relative, row.snapshot]));
    const expectedResultPaths = {
        evidencePath: 'visual-phase-qa.json',
        deformationPath: 'deformation-report.json',
        videoPath: 'fixed-camera-preview.mp4',
        finalPosePath: oneShot ? 'final-pose-stability-report.json' : null,
    };
    for (const [name, relative] of Object.entries(expectedResultPaths)) {
        if (relative == null ? baseResult[name] != null
            : !samePath(baseResult[name] || '', path.join(baseDirectory, relative))) {
            fail(`base Horse runner returned an unexpected ${name}`);
        }
    }
    const baseEvidence = jsonSnapshot(path.join(baseDirectory, 'visual-phase-qa.json'), 'base Horse visual evidence');
    const deformation = jsonSnapshot(path.join(baseDirectory, 'deformation-report.json'), 'target deformation report');
    const finalPose = oneShot
        ? jsonSnapshot(path.join(baseDirectory, 'final-pose-stability-report.json'), 'one-shot final-pose report')
        : null;
    const video = byRelative.get('fixed-camera-preview.mp4');
    const videoMetadata = inspectMp4(video, 'base fixed-camera preview', policy.frameCount);
    if (config.expectedFfmpeg != null) validateMp4Decode(video, config.expectedFfmpeg);
    const camera = jsonSnapshot(path.join(baseDirectory, 'camera-settings.json'), 'fixed camera settings');
    const clip = snapshot(config.threeClipPath, 'fitted Three clip');
    const immutable = jsonSnapshot(path.join(config.bundleDirectory, 'immutable_manifest.json'), 'canonical immutable manifest');
    const fitting = jsonSnapshot(path.join(config.bundleDirectory, 'fitting_bundle.json'), 'canonical fitting bundle');
    const actionPrompts = jsonSnapshot(ACTION_PROMPTS_PATH, 'action prompts contract');
    const threeModule = snapshot(config.threeModule, 'Three module');
    if (clip.sha256 !== config.expectedThreeClipSha256
        || immutable.sha256 !== config.expectedImmutableManifestSha256
        || fitting.sha256 !== config.expectedFittingBundleSha256
        || actionPrompts.sha256 !== policy.actionPrompts.sha256
        || threeModule.sha256 !== config.expectedThreeModuleSha256
        || String(config.expectedThreeRevision) !== '160') {
        fail('semantic wrapper immutable CLI pins changed after base QA');
    }
    const envelope = baseEvidence.json;
    const gate = object(envelope.visual_phase_gate, 'base visual phase gate');
    const local = object(envelope.local_evidence, 'base local evidence');
    const inputs = object(local.immutable_inputs, 'base immutable inputs');
    requireExactKeys(envelope, ['schema', 'visual_phase_gate', 'local_evidence'], 'base Horse visual evidence');
    requireExactKeys(gate, [
        'schema', 'version', 'rig_type', 'semantic_id', 'fitted_clip_sha256', 'decision',
        'camera', 'coincident_rest_vertex_separation', 'required_phases', 'frames', 'reviewer',
    ], 'base visual phase gate');
    requireExactKeys(local, [
        'source_rig_type', 'temporal_mode', 'browser_only', 'blender_used',
        'animation_evaluation', 'immutable_inputs', 'camera_settings',
        'browser_reconstruction_qa', 'target_mesh_deformation_qa', 'phase_frames',
        'one_shot_final_pose_qa', 'video', 'renderer', 'human_review', 'approvals',
    ], 'base local evidence');
    requireExactKeys(inputs, [
        'source_model', 'immutable_manifest', 'fitting_bundle', 'skeleton', 'skin_weights',
        'surface_topology', 'three_clip', 'action_prompts_contract',
    ], 'base immutable inputs');
    if (envelope.schema !== 'autorig.browser-horse-visual-phase-evidence-envelope.v1'
        || gate.schema !== HORSE_VISUAL_PHASE_QA_SCHEMA || gate.version !== 1
        || gate.rig_type !== 'horse' || gate.semantic_id !== config.semanticId
        || gate.fitted_clip_sha256 !== clip.sha256 || gate.decision !== null
        || local.source_rig_type !== 'HORSE_2' || local.temporal_mode !== policy.generationMode
        || local.browser_only !== true || local.blender_used !== false
        || local.animation_evaluation !== 'Three.AnimationMixer'
        || local.human_review?.decision !== null || local.human_review?.required !== true
        || local.approvals?.approved_for_animation_library !== false
        || local.approvals?.release_ready !== false) {
        fail('base Horse visual evidence lost its exact fail-closed browser contract');
    }
    descriptorMatches(inputs.immutable_manifest, immutable, 'base immutable manifest');
    descriptorMatches(inputs.fitting_bundle, fitting, 'base fitting bundle');
    descriptorMatches(inputs.three_clip, clip, 'base Three clip');
    descriptorMatches(inputs.action_prompts_contract, actionPrompts, 'base action prompts contract');
    requireExactKeys(inputs.source_model, ['filename', 'sha256'], 'base source model');
    if (inputs.source_model.filename !== fitting.json.source?.filename
        || inputs.source_model.sha256 !== fitting.json.source?.sha256) {
        fail('base source model does not match canonical fitting bundle');
    }
    if (fitting.json.counts?.vertices !== 344 || fitting.json.counts?.faces !== 258
        || fitting.json.counts?.armatures !== 1 || fitting.json.counts?.meshes !== 1
        || !Array.isArray(immutable.json?.files)
        || immutable.json.bundle_file_count !== immutable.json.files.length
        || immutable.json.bundle_total_bytes !== immutable.json.files
            .reduce((total, row) => total + integer(row.bytes, 'immutable file bytes', 1), 0)) {
        fail('base canonical Horse_2 bundle inventory/count contract changed');
    }
    const immutableByFilename = new Map(immutable.json.files.map((row, index) => {
        if (typeof row?.filename !== 'string' || !row.filename || row.filename.includes('..')) {
            fail(`immutable file ${index} has an unsafe filename`);
        }
        if (immutable.json.files.findIndex((item) => item.filename === row.filename) !== index) {
            fail(`immutable manifest repeats ${row.filename}`);
        }
        return [row.filename, row];
    }));
    const artifactInput = (name, field, expectedFilename) => {
        const descriptor = object(inputs[name], `base ${field}`);
        const fittingDescriptor = object(fitting.json.artifacts?.[name], `fitting bundle ${field}`);
        if (fittingDescriptor.filename !== expectedFilename
            || !samePath(descriptor.path || '', path.join(config.bundleDirectory, expectedFilename))
            || !isInside(config.bundleDirectory, descriptor.path || '')) {
            fail(`base ${field} is not the exact fitting-bundle artifact`);
        }
        const value = snapshot(descriptor.path, `base ${field}`);
        descriptorMatches(descriptor, value, `base ${field}`);
        const immutableRow = immutableByFilename.get(expectedFilename);
        if (!immutableRow || fittingDescriptor.bytes !== value.bytes || fittingDescriptor.sha256 !== value.sha256
            || immutableRow.bytes !== value.bytes || immutableRow.sha256 !== value.sha256) {
            fail(`base ${field} is not pinned by both fitting and immutable manifests`);
        }
        return value;
    };
    const skeleton = artifactInput('skeleton', 'skeleton', 'skeleton.json');
    const skinWeights = artifactInput('skin_weights', 'skin weights', 'skin_weights.json.gz');
    const surfaceTopology = artifactInput('surface_topology', 'surface topology', 'surface_topology.json.gz');
    const skeletonJson = parseArtifactJson(skeleton, 'base skeleton');
    const skinWeightsJson = parseArtifactJson(skinWeights, 'base skin weights');
    const surfaceTopologyJson = parseArtifactJson(surfaceTopology, 'base surface topology');
    const clipJson = parseArtifactJson(clip, 'base Three clip');
    if (!Array.isArray(skeletonJson.armatures) || skeletonJson.armatures.length !== 1
        || !Array.isArray(skeletonJson.armatures[0]?.bones) || skeletonJson.armatures[0].bones.length !== 304
        || !Array.isArray(skinWeightsJson.vertices) || skinWeightsJson.vertices.length !== 344
        || !Array.isArray(surfaceTopologyJson.faces) || surfaceTopologyJson.faces.length !== 258) {
        fail('base measured artifacts are not the canonical 304-bone/344-vertex/258-face Horse_2');
    }
    const boneNames = new Set();
    const rootBoneNames = new Set();
    skeletonJson.armatures[0].bones.forEach((bone, index) => {
        if (typeof bone?.name !== 'string' || !bone.name || boneNames.has(bone.name)) {
            fail(`base skeleton bone ${index} is missing or duplicated`);
        }
        boneNames.add(bone.name);
        if (isHorseRootMotionBone(bone)) rootBoneNames.add(bone.name);
    });
    skeletonJson.armatures[0].bones.forEach((bone) => {
        if (bone.parent != null && !boneNames.has(bone.parent)) fail(`base skeleton parent is missing for ${bone.name}`);
    });
    const restVertices = skinWeightsJson.vertices.map((vertex, index) => {
        if (!Array.isArray(vertex?.world) || vertex.world.length !== 3) fail(`skin vertex ${index} world position is invalid`);
        return vertex.world.map((value, axis) => finite(value, `skin vertex ${index} world[${axis}]`));
    });
    const zeroWeightVertices = skinWeightsJson.vertices.filter((vertex) => !Array.isArray(vertex.weights)
        || !vertex.weights.some((row) => Number.isFinite(Number(row?.weight)) && Number(row.weight) > 0)).length;
    const evidenceInputs = {
        frameCount: policy.frameCount,
        loop: policy.generationMode === 'loop',
        skeleton,
        skinWeights,
        surfaceTopology,
        fittingBundle: fitting.json,
        skeletonJson,
        skinWeightsJson,
        surfaceTopologyJson,
        clipJson,
        boneNames,
        rootBoneNames,
        restVertices,
        zeroWeightVertices,
    };
    validateDeformation(deformation.json, config, evidenceInputs);
    if (oneShot) validateFinalPose(finalPose.json, config, evidenceInputs);
    const expectedBaseMachinePassed = deformation.json.passed === true
        && (!oneShot || finalPose.json.passed === true);
    const indices = [0, Math.floor((policy.frameCount - 1) / 2), Math.floor((policy.frameCount - 1) * 0.75)];
    if (JSON.stringify(gate.required_phases) !== JSON.stringify(HORSE_VISUAL_PHASE_REQUIRED_PHASES)
        || !Array.isArray(gate.frames) || gate.frames.length !== 3
        || !Array.isArray(local.phase_frames) || local.phase_frames.length !== 3) {
        fail('base Horse visual evidence phase inventory changed');
    }
    HORSE_VISUAL_PHASE_REQUIRED_PHASES.forEach((phase, index) => {
        const relative = `frames/frame_${String(indices[index]).padStart(4, '0')}.png`;
        const frame = byRelative.get(relative);
        const gateRow = gate.frames[index];
        const localRow = local.phase_frames[index];
        if (gateRow?.phase !== phase || gateRow?.frame_index !== indices[index]
            || gateRow?.sha256 !== frame.sha256 || gateRow?.evidence_url !== null
            || localRow?.phase !== phase || localRow?.frame_index !== indices[index]) {
            fail('base Horse visual phase pin does not match exact rendered frame');
        }
        descriptorMatches(localRow, frame, `base visual phase ${phase}`);
    });
    descriptorMatches(local.video, video, 'base fixed-camera video');
    descriptorMatches(local.camera_settings, camera, 'base camera settings');
    descriptorMatches(local.target_mesh_deformation_qa?.report, deformation, 'base deformation report');
    const reconstruction = object(local.browser_reconstruction_qa, 'base browser reconstruction QA');
    const reconstructionThresholds = {
        maximum_bone_head_error_world: 1e-5,
        maximum_rest_vertex_error_world: 1e-5,
        minimum_animated_bone_head_displacement_world: 1e-6,
    };
    const maximumHeadError = finite(
        reconstruction.maximum_bone_head_error_world,
        'base maximum bone head reconstruction error',
    );
    const maximumRestError = finite(
        reconstruction.maximum_rest_vertex_error_world,
        'base maximum rest vertex reconstruction error',
    );
    const maximumAnimatedDisplacement = finite(
        reconstruction.maximum_animated_bone_head_displacement_world,
        'base maximum animated bone displacement',
    );
    if (!Array.isArray(clipJson.tracks) || !clipJson.tracks.length) fail('base Three clip has no tracks');
    const clipTypesByBone = new Map();
    clipJson.tracks.forEach((track, index) => {
        const match = typeof track?.name === 'string' && track.name.match(/^(.*)\.(quaternion|position)$/);
        if (!match || !evidenceInputs.boneNames.has(match[1])
            || !Array.isArray(track.times) || track.times.length !== policy.frameCount) {
            fail(`base Three clip track ${index} is not bound to the canonical skeleton/timeline`);
        }
        const size = match[2] === 'quaternion' ? 4 : 3;
        if (!Array.isArray(track.values) || track.values.length !== policy.frameCount * size) {
            fail(`base Three clip track ${track.name} has an invalid value inventory`);
        }
        const types = clipTypesByBone.get(match[1]) || new Set();
        types.add(match[2]);
        clipTypesByBone.set(match[1], types);
    });
    if ([...clipTypesByBone.values()].some((types) => !types.has('quaternion') || !types.has('position'))) {
        fail('base Three clip lacks paired position/quaternion tracks for a bound bone');
    }
    const meaningfulLocalMotion = assessMeaningfulNonRootLocalMotion({
        tracks: clipJson.tracks,
        frameCount: policy.frameCount,
        rootBoneNames: evidenceInputs.rootBoneNames,
    });
    const declaredAnimatedBones = reconstruction.animated_non_root_bones;
    if (JSON.stringify(reconstruction.thresholds) !== JSON.stringify(reconstructionThresholds)
        || maximumHeadError > reconstructionThresholds.maximum_bone_head_error_world
        || maximumRestError > reconstructionThresholds.maximum_rest_vertex_error_world
        || maximumAnimatedDisplacement
            < reconstructionThresholds.minimum_animated_bone_head_displacement_world
        || !Array.isArray(declaredAnimatedBones)
        || !declaredAnimatedBones.length
        || new Set(declaredAnimatedBones).size !== declaredAnimatedBones.length
        || declaredAnimatedBones.some((name) => typeof name !== 'string' || !name
            || !evidenceInputs.boneNames.has(name) || evidenceInputs.rootBoneNames.has(name))
        || meaningfulLocalMotion.passed !== true) {
        fail('base browser reconstruction evidence is not an exact animated PASS');
    }
    const expectedDeformationThresholds = {
        maximum_edge_stretch: HORSE_VISUAL_PHASE_THRESHOLDS.maximumEdgeStretch,
        p99_edge_stretch: HORSE_VISUAL_PHASE_THRESHOLDS.p99EdgeStretch,
        zero_weight_vertices: HORSE_VISUAL_PHASE_THRESHOLDS.zeroWeightVertices,
    };
    const expectedFailReason = expectedBaseMachinePassed
        ? 'human_visual_phase_decision_and_public_urls_unset'
        : (deformation.json.passed !== true
            ? 'machine_target_deformation_qa_failed'
            : 'machine_one_shot_final_pose_qa_failed');
    requireExactKeys(gate.camera, ['static', 'projection', 'view', 'root_motion_locked', 'settings_sha256'], 'base gate camera');
    requireExactKeys(gate.coincident_rest_vertex_separation, [
        'measured', 'pass', 'threshold_m', 'max_separation_m', 'sample_count',
        'group_count', 'report_url', 'report_sha256',
    ], 'base coincident-rest evidence');
    requireExactKeys(gate.reviewer, ['id', 'reviewed_at'], 'base gate reviewer');
    if (local.target_mesh_deformation_qa?.measured_every_frame !== true
        || local.target_mesh_deformation_qa?.passed !== expectedBaseMachinePassed
        || local.target_mesh_deformation_qa?.maximum_edge_stretch !== deformation.json.maximumEdgeStretch
        || local.target_mesh_deformation_qa?.p99_edge_stretch !== deformation.json.p99EdgeStretch
        || local.target_mesh_deformation_qa?.zero_weight_vertices !== deformation.json.zeroWeightVertices
        || JSON.stringify(local.target_mesh_deformation_qa?.thresholds)
            !== JSON.stringify(expectedDeformationThresholds)
        || gate.coincident_rest_vertex_separation.measured !== true
        || gate.coincident_rest_vertex_separation?.report_sha256 !== deformation.sha256
        || gate.coincident_rest_vertex_separation?.pass !== deformation.json.gates.coincidentRestSeparation
        || gate.coincident_rest_vertex_separation.threshold_m
            !== HORSE_VISUAL_PHASE_THRESHOLDS.coincidentRestSeparationM
        || gate.coincident_rest_vertex_separation.max_separation_m
            !== deformation.json.maximumCoincidentRestSeparationM
        || gate.coincident_rest_vertex_separation.sample_count !== deformation.json.coincidentRestSampleCount
        || gate.coincident_rest_vertex_separation.group_count !== deformation.json.coincidentRestGroupCount
        || gate.coincident_rest_vertex_separation.report_url !== null
        || gate.camera?.static !== true || gate.camera?.projection !== 'perspective'
        || gate.camera?.view !== 'canonical_fitting_bundle'
        || gate.camera?.root_motion_locked !== evidenceInputs.loop
        || gate.camera?.settings_sha256 !== camera.sha256
        || gate.reviewer.id !== null || gate.reviewer.reviewed_at !== null
        || camera.json.schema !== 'autorig.browser-horse-fixed-camera.v1'
        || camera.json.temporalMode !== policy.generationMode
        || camera.json.rootMotionPolicy !== (evidenceInputs.loop
            ? 'suppress_armature_root_tracks_and_lock_model_transform'
            : 'allow_one_shot_root_tracks_keep_camera_static')
        || JSON.stringify(camera.json.camera) !== JSON.stringify(fitting.json.camera)
        || JSON.stringify(camera.json.resolution) !== JSON.stringify([768, 448])
        || JSON.stringify(camera.json.renderer) !== JSON.stringify(local.renderer?.runtime?.renderer)
        || local.renderer?.browser !== 'headless_chrome_cdp'
        || String(local.renderer?.three_revision) !== '160'
        || local.renderer?.three_module?.sha256 !== threeModule.sha256
        || local.renderer?.runtime?.animationEvaluation !== 'Three.AnimationMixer'
        || local.renderer?.runtime?.zeroWeightVertices !== deformation.json.zeroWeightVertices
        || local.renderer?.runtime?.maximumHeadReconstructionErrorWorld !== maximumHeadError
        || local.renderer?.runtime?.maximumRestVertexErrorWorld !== maximumRestError
        || local.renderer?.runtime?.maximumAnimatedBoneHeadDisplacementWorld !== maximumAnimatedDisplacement
        || local.renderer?.runtime?.renderer?.webgl2 !== true
        || local.renderer?.runtime?.renderer?.outputColorSpace !== 'SRGBColorSpace'
        || local.renderer?.runtime?.renderer?.toneMapping !== 'ACESFilmicToneMapping'
        || local.renderer?.runtime?.renderer?.toneMappingExposure !== 1.1
        || local.renderer?.runtime?.renderer?.shadowsEnabled !== false
        || local.video?.container !== 'mp4' || local.video?.codec !== 'h264'
        || local.video?.pixel_format !== 'yuv420p' || local.video?.width !== 768
        || local.video?.height !== 448 || finite(local.video?.fps, 'base video fps') !== 30
        || local.video?.frame_count !== policy.frameCount || local.video?.audio_stream_count !== 0
        || !(finite(local.video?.duration_seconds, 'base video duration') > 0)
        || local.video?.fixed_camera !== true || local.video?.root_motion_locked !== evidenceInputs.loop
        || local.video?.root_motion_policy !== (evidenceInputs.loop ? 'suppress' : 'allow_one_shot')
        || videoMetadata.codec !== local.video.codec
        || videoMetadata.pixelFormat !== local.video.pixel_format
        || videoMetadata.width !== local.video.width || videoMetadata.height !== local.video.height
        || Math.abs(videoMetadata.fps - local.video.fps) > 1e-6
        || videoMetadata.frameCount !== local.video.frame_count
        || videoMetadata.audioStreamCount !== local.video.audio_stream_count
        || Math.abs(videoMetadata.durationSeconds - local.video.duration_seconds) > (1 / 30 + 1e-6)
        || local.human_review?.decision !== null || local.human_review?.reviewer_id !== null
        || local.human_review?.reviewed_at !== null || local.human_review?.required !== true
        || local.approvals?.machine_qa_passed !== expectedBaseMachinePassed
        || local.approvals?.ready_for_human_review !== expectedBaseMachinePassed
        || local.approvals?.approved_for_animation_library !== false
        || local.approvals?.release_ready !== false
        || local.approvals?.fail_closed_reason !== expectedFailReason) {
        fail('base Horse nested deformation/camera/runtime evidence is inconsistent');
    }
    if (oneShot) {
        descriptorMatches(local.one_shot_final_pose_qa?.report, finalPose, 'base final-pose report');
        if (local.one_shot_final_pose_qa?.passed !== finalPose.json.passed
            || JSON.stringify(local.one_shot_final_pose_qa?.gates) !== JSON.stringify(finalPose.json.gates)) {
            fail('base Horse nested final-pose evidence is inconsistent');
        }
    } else if (local.one_shot_final_pose_qa !== null) fail('loop base evidence carries one-shot evidence');
    return { inventory, baseEvidence, deformation, finalPose, video, camera, clip, immutable, fitting, actionPrompts, threeModule };
}

export async function runSemanticVisualPhaseQa(config, dependencies = {}) {
    const outputDirectory = path.resolve(config.outputDirectory);
    const parent = path.dirname(outputDirectory);
    const parentDirectory = directory(parent, 'semantic QA output parent');
    if (!samePath(outputDirectory, path.join(parentDirectory.realpath, path.basename(outputDirectory)))) {
        fail('semantic QA output directory uses a path alias');
    }
    if (fs.existsSync(outputDirectory)) fail(`outputDirectory must not exist: ${outputDirectory}`);
    const policy = resolveSemanticTerminalPolicy(config.semanticId);
    if ((config.loop !== false ? 'loop' : 'one_shot') !== policy.generationMode) {
        fail('CLI temporal mode does not match semantic taxonomy action');
    }
    fs.mkdirSync(outputDirectory);
    const createdOutput = directory(outputDirectory, 'semantic QA created output directory');
    const parentAfterCreate = directory(parent, 'semantic QA output parent');
    if (!sameDirectoryObject(parentDirectory.identity, parentAfterCreate.identity)) {
        fail('semantic QA output parent changed during creation');
    }
    try {
        const baseDirectory = path.join(outputDirectory, 'horse-base-evidence');
        const runner = dependencies.runHorseVisualPhaseQa || runHorseVisualPhaseQa;
        const baseResult = await runner({ ...config, outputDirectory: baseDirectory });
        const validated = validateBaseEvidence({ config, policy, baseDirectory, baseResult });
        const evaluation = evaluateSemanticVisualQa({
            policy,
            deformationReport: validated.deformation.json,
            finalPoseReport: validated.finalPose?.json || null,
        });
        const report = {
            schema: SEMANTIC_VISUAL_PHASE_QA_SCHEMA,
            status: evaluation.machinePassed
                ? 'PASS_MACHINE_QA_AWAITING_HUMAN'
                : 'FAIL_MACHINE_QA',
            semanticId: config.semanticId,
            generationMode: policy.generationMode,
            frameCount: policy.frameCount,
            terminalPoseContract: {
                startPoseId: policy.startPoseId,
                endPoseId: policy.endPoseId,
                policy: policy.terminalPolicy,
                gates: evaluation.terminalGates,
                passed: evaluation.terminalPassed,
            },
            immutableInputs: {
                taxonomy: policy.taxonomy,
                actionPrompts: pin(validated.actionPrompts),
                baseHorseVisualEvidence: pin(validated.baseEvidence),
                targetDeformationReport: pin(validated.deformation),
                oneShotFinalPoseReport: validated.finalPose ? pin(validated.finalPose) : null,
                fixedCameraPreview: pin(validated.video),
                fixedCameraSettings: pin(validated.camera),
                fittedThreeClip: pin(validated.clip),
                canonicalImmutableManifest: pin(validated.immutable),
                canonicalFittingBundle: pin(validated.fitting),
                threeModule: pin(validated.threeModule),
            },
            artifactInventory: validated.inventory.map(({ relative, snapshot: artifact }) => ({
                relativePath: `horse-base-evidence/${relative}`,
                artifact: pin(artifact),
            })),
            runtime: {
                browserOnly: true,
                blenderUsed: false,
                fittingMixerUsed: false,
                qaAnimationMixerUsed: true,
                fixedCamera: true,
                measuredEveryFrame: validated.deformation.json.measuredEveryFrame === true,
            },
            machineQa: {
                deformationPassed: evaluation.deformationPassed,
                terminalPassed: evaluation.terminalPassed,
                passed: evaluation.machinePassed,
            },
            humanReview: { required: true, decision: null, reviewerId: null, reviewedAt: null },
            approvals: {
                readyForHumanReview: evaluation.machinePassed,
                approvedForAnimationLibrary: false,
                releaseReady: false,
            },
        };
        const reportPin = writeNewJson(path.join(outputDirectory, 'visual-phase-qa.json'), report);
        const finalRootInventory = fs.readdirSync(outputDirectory).sort();
        if (JSON.stringify(finalRootInventory) !== JSON.stringify(['horse-base-evidence', 'visual-phase-qa.json'])) {
            fail('semantic visual QA output inventory changed during publication');
        }
        const finalParent = directory(parent, 'semantic QA final output parent');
        const finalOutput = directory(outputDirectory, 'semantic QA final output directory');
        if (!sameDirectoryObject(parentDirectory.identity, finalParent.identity)
            || !sameDirectoryObject(createdOutput.identity, finalOutput.identity)
            || !isInside(parentDirectory.realpath, finalOutput.realpath)) {
            fail('semantic visual QA output path changed during publication');
        }
        return { report, reportPin, baseResult };
    } catch (error) {
        try {
            const currentParent = directory(parent, 'semantic QA cleanup parent');
            const currentOutput = directory(outputDirectory, 'semantic QA cleanup output');
            if (sameDirectoryObject(parentDirectory.identity, currentParent.identity)
                && sameDirectoryObject(createdOutput.identity, currentOutput.identity)
                && isInside(parentDirectory.realpath, currentOutput.realpath)) {
                fs.rmSync(currentOutput.path, { recursive: true, force: true });
            }
        } catch { /* fail closed: never recursively remove a replaced/aliased path */ }
        throw error;
    }
}

export function inspectPublishedSemanticVisualQa({
    outputDirectory: outputValue,
    semanticId,
    expectedThreeClip,
    expectedImmutableManifest,
    expectedFittingBundle,
    expectedThreeModule,
    expectedFfmpeg,
}) {
    object(expectedFfmpeg, 'expectedFfmpeg');
    const outputDirectory = path.resolve(outputValue);
    const publishedRoot = directory(outputDirectory, 'published semantic visual QA directory');
    if (JSON.stringify(fs.readdirSync(outputDirectory).sort())
        !== JSON.stringify(['horse-base-evidence', 'visual-phase-qa.json'])) {
        fail('published semantic visual QA has unexpected root inventory');
    }
    const reportSnapshot = jsonSnapshot(path.join(outputDirectory, 'visual-phase-qa.json'), 'semantic visual QA report');
    const report = reportSnapshot.json;
    const policy = resolveSemanticTerminalPolicy(semanticId);
    requireExactKeys(report, [
        'schema', 'status', 'semanticId', 'generationMode', 'frameCount',
        'terminalPoseContract', 'immutableInputs', 'artifactInventory', 'runtime',
        'machineQa', 'humanReview', 'approvals',
    ], 'published semantic visual QA report');
    requireExactKeys(report.terminalPoseContract, [
        'startPoseId', 'endPoseId', 'policy', 'gates', 'passed',
    ], 'published semantic terminal contract');
    requireExactKeys(report.immutableInputs, [
        'taxonomy', 'actionPrompts', 'baseHorseVisualEvidence', 'targetDeformationReport',
        'oneShotFinalPoseReport', 'fixedCameraPreview', 'fixedCameraSettings',
        'fittedThreeClip', 'canonicalImmutableManifest', 'canonicalFittingBundle', 'threeModule',
    ], 'published semantic immutable inputs');
    requireExactKeys(report.runtime, [
        'browserOnly', 'blenderUsed', 'fittingMixerUsed', 'qaAnimationMixerUsed',
        'fixedCamera', 'measuredEveryFrame',
    ], 'published semantic runtime');
    requireExactKeys(report.machineQa, [
        'deformationPassed', 'terminalPassed', 'passed',
    ], 'published semantic machine QA');
    requireExactKeys(report.humanReview, [
        'required', 'decision', 'reviewerId', 'reviewedAt',
    ], 'published semantic human review');
    requireExactKeys(report.approvals, [
        'readyForHumanReview', 'approvedForAnimationLibrary', 'releaseReady',
    ], 'published semantic approvals');
    if (report.schema !== SEMANTIC_VISUAL_PHASE_QA_SCHEMA
        || report.semanticId !== semanticId || report.generationMode !== policy.generationMode
        || report.frameCount !== policy.frameCount
        || report.terminalPoseContract?.startPoseId !== policy.startPoseId
        || report.terminalPoseContract?.endPoseId !== policy.endPoseId
        || report.terminalPoseContract?.policy !== policy.terminalPolicy
        || report.humanReview?.required !== true || report.humanReview?.decision !== null
        || report.approvals?.approvedForAnimationLibrary !== false
        || report.approvals?.releaseReady !== false) {
        fail('published semantic visual QA header/approval contract changed');
    }
    const expected = {
        fittedThreeClip: expectedThreeClip,
        canonicalImmutableManifest: expectedImmutableManifest,
        canonicalFittingBundle: expectedFittingBundle,
        threeModule: expectedThreeModule,
    };
    for (const [name, snapshotValue] of Object.entries(expected)) {
        descriptorMatches(report.immutableInputs?.[name], snapshotValue, `semantic report ${name}`);
    }
    const taxonomy = snapshot(TAXONOMY_PATH, 'current taxonomy');
    const prompts = snapshot(ACTION_PROMPTS_PATH, 'current action prompts');
    descriptorMatches(report.immutableInputs?.taxonomy, taxonomy, 'semantic report taxonomy');
    descriptorMatches(report.immutableInputs?.actionPrompts, prompts, 'semantic report action prompts');
    descriptorMatches(policy.taxonomy, taxonomy, 'semantic policy taxonomy');
    descriptorMatches(policy.actionPrompts, prompts, 'semantic policy action prompts');
    if (!Array.isArray(report.artifactInventory) || !report.artifactInventory.length) {
        fail('semantic report artifactInventory is missing');
    }
    const baseDirectory = path.join(outputDirectory, 'horse-base-evidence');
    const actualInventory = baseArtifactInventory(baseDirectory, policy.frameCount, policy.generationMode === 'one_shot');
    const expectedRows = actualInventory.map(({ relative, snapshot: artifact }) => ({
        relativePath: `horse-base-evidence/${relative}`,
        artifact: pin(artifact),
    }));
    if (JSON.stringify(report.artifactInventory) !== JSON.stringify(expectedRows)) {
        fail('semantic report does not pin the exact nested base evidence inventory');
    }
    const baseResult = {
        evidencePath: path.join(baseDirectory, 'visual-phase-qa.json'),
        deformationPath: path.join(baseDirectory, 'deformation-report.json'),
        finalPosePath: policy.generationMode === 'one_shot'
            ? path.join(baseDirectory, 'final-pose-stability-report.json') : null,
        videoPath: path.join(baseDirectory, 'fixed-camera-preview.mp4'),
    };
    const validated = validateBaseEvidence({
        config: {
            semanticId,
            bundleDirectory: path.dirname(expectedImmutableManifest.path),
            expectedImmutableManifestSha256: expectedImmutableManifest.sha256,
            expectedFittingBundleSha256: expectedFittingBundle.sha256,
            expectedThreeClipSha256: expectedThreeClip.sha256,
            threeClipPath: expectedThreeClip.path,
            threeModule: expectedThreeModule.path,
            expectedThreeModuleSha256: expectedThreeModule.sha256,
            expectedThreeRevision: '160',
            expectedFfmpeg,
        },
        policy,
        baseDirectory,
        baseResult,
    });
    descriptorMatches(report.immutableInputs?.baseHorseVisualEvidence, validated.baseEvidence, 'semantic report base evidence');
    descriptorMatches(report.immutableInputs?.targetDeformationReport, validated.deformation, 'semantic report deformation');
    descriptorMatches(report.immutableInputs?.fixedCameraPreview, validated.video, 'semantic report video');
    descriptorMatches(report.immutableInputs?.fixedCameraSettings, validated.camera, 'semantic report camera');
    if (validated.finalPose) {
        descriptorMatches(report.immutableInputs?.oneShotFinalPoseReport, validated.finalPose, 'semantic report final pose');
    } else if (report.immutableInputs?.oneShotFinalPoseReport !== null) {
        fail('semantic loop report carries one-shot final pose');
    }
    const evaluation = evaluateSemanticVisualQa({
        policy,
        deformationReport: validated.deformation.json,
        finalPoseReport: validated.finalPose?.json || null,
    });
    if (report.status !== (evaluation.machinePassed ? 'PASS_MACHINE_QA_AWAITING_HUMAN' : 'FAIL_MACHINE_QA')
        || JSON.stringify(report.terminalPoseContract?.gates) !== JSON.stringify(evaluation.terminalGates)
        || report.terminalPoseContract?.passed !== evaluation.terminalPassed
        || report.machineQa?.deformationPassed !== evaluation.deformationPassed
        || report.machineQa?.terminalPassed !== evaluation.terminalPassed
        || report.machineQa?.passed !== evaluation.machinePassed
        || report.approvals?.readyForHumanReview !== evaluation.machinePassed
        || report.runtime?.browserOnly !== true || report.runtime?.blenderUsed !== false
        || report.runtime?.fittingMixerUsed !== false || report.runtime?.qaAnimationMixerUsed !== true
        || report.runtime?.fixedCamera !== true || report.runtime?.measuredEveryFrame !== true) {
        fail('semantic visual QA machine decision does not recompute from nested evidence');
    }
    const afterPublishedRoot = directory(outputDirectory, 'published semantic visual QA directory');
    if (!sameIdentity(publishedRoot.identity, afterPublishedRoot.identity)
        || JSON.stringify(fs.readdirSync(outputDirectory).sort())
            !== JSON.stringify(['horse-base-evidence', 'visual-phase-qa.json'])) {
        fail('published semantic visual QA changed while inspected');
    }
    return { reportSnapshot, report, policy, validated, evaluation };
}

function helpText() {
    return `Usage:
  node browser_animation_visual_phase_qa.mjs <same pinned options as browser_horse_visual_phase_qa.mjs>

Runs the canonical Horse_2 browser fixed-camera/deformation QA, then applies the
taxonomy end-pose policy: death requires settled/drop/ground; ordinary one-shot
actions require settled/ground without a death drop; airborne transitions require
camera stability and no ground penetration. Human approval always remains null.`;
}

export async function runSemanticVisualPhaseQaCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseHorseVisualPhaseQaArgs(argv);
        if (config.help) { streams.stdout.write(`${helpText()}\n`); return 0; }
        const result = await runSemanticVisualPhaseQa(config);
        streams.stdout.write(`${JSON.stringify({
            status: result.report.status,
            report: result.reportPin,
            approvedForAnimationLibrary: false,
        })}\n`);
        return result.report.machineQa.passed ? 0 : 3;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invoked = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invoked === import.meta.url) process.exitCode = await runSemanticVisualPhaseQaCli();
