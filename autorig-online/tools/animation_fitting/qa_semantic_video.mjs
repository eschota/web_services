#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { buildSemanticObservations, assessHorseWalkGait } from '../../static/js/animation-fitting-semantic-tracker.js';
import { HORSE_2_SEMANTIC_PROFILE } from '../../static/js/animation-fitting-three-adapter.js';

function parseArguments(argv) {
    const values = {};
    for (let index = 0; index < argv.length; index += 1) {
        const token = argv[index];
        if (!token.startsWith('--')) throw new Error(`Unexpected argument: ${token}`);
        const key = token.slice(2);
        const value = argv[index + 1];
        if (!value || value.startsWith('--')) throw new Error(`Missing value for --${key}`);
        values[key] = value;
        index += 1;
    }
    if (!values.video) throw new Error('--video is required');
    if (!values.output) throw new Error('--output is required');
    return values;
}

function run(binary, args, options = {}) {
    const result = spawnSync(binary, args, {
        encoding: options.encoding ?? null,
        maxBuffer: 256 * 1024 * 1024,
        windowsHide: true,
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
        const stderr = Buffer.isBuffer(result.stderr)
            ? result.stderr.toString('utf8')
            : String(result.stderr || '');
        throw new Error(`${binary} failed (${result.status}): ${stderr.trim()}`);
    }
    return result.stdout;
}

function parseRate(value) {
    const [numerator, denominator = '1'] = String(value).split('/').map(Number);
    const fps = numerator / denominator;
    if (!Number.isFinite(fps) || fps <= 0) throw new Error(`Invalid frame rate: ${value}`);
    return fps;
}

function main() {
    const args = parseArguments(process.argv.slice(2));
    const videoPath = resolve(args.video);
    const outputPath = resolve(args.output);
    const ffmpeg = args.ffmpeg || 'ffmpeg';
    const ffprobe = args.ffprobe || ffmpeg.replace(/ffmpeg(?:\.exe)?$/i, 'ffprobe.exe');
    const probe = JSON.parse(run(ffprobe, [
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height,nb_frames,r_frame_rate',
        '-of', 'json',
        videoPath,
    ], { encoding: 'utf8' }));
    const stream = probe.streams?.[0];
    const width = Number(stream?.width);
    const height = Number(stream?.height);
    const fps = parseRate(stream?.r_frame_rate);
    if (!Number.isInteger(width) || width <= 0 || !Number.isInteger(height) || height <= 0) {
        throw new Error('ffprobe did not return a valid video size');
    }

    const raw = run(ffmpeg, [
        '-v', 'error',
        '-i', videoPath,
        '-f', 'rawvideo',
        '-pix_fmt', 'rgba',
        'pipe:1',
    ]);
    const bytesPerFrame = width * height * 4;
    if (!Buffer.isBuffer(raw) || raw.length === 0 || raw.length % bytesPerFrame !== 0) {
        throw new Error(`Decoded byte count ${raw?.length || 0} is not divisible by ${bytesPerFrame}`);
    }
    const frameCount = raw.length / bytesPerFrame;
    const frames = Array.from({ length: frameCount }, (_, frame) => ({
        width,
        height,
        data: new Uint8Array(raw.buffer, raw.byteOffset + frame * bytesPerFrame, bytesPerFrame),
    }));
    const observations = buildSemanticObservations(
        frames,
        HORSE_2_SEMANTIC_PROFILE.palette_linear,
        { fps },
    );
    const gait = assessHorseWalkGait(observations);
    const result = {
        schema: 'autorig.semantic-video-qa.v1',
        video_path: videoPath,
        width,
        height,
        fps,
        frame_count: frameCount,
        expected_frame_count: Number(stream.nb_frames) || null,
        gait,
        observations,
    };
    mkdirSync(dirname(outputPath), { recursive: true });
    writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
    process.stdout.write(`${JSON.stringify({
        output: outputPath,
        accepted: gait.accepted,
        frame_count: frameCount,
        phase_gaps: gait.phaseGaps,
        simultaneous_swing_frames: gait.simultaneousSwingFrames.length,
    }, null, 2)}\n`);
}

try {
    main();
} catch (error) {
    process.stderr.write(`${error?.stack || error}\n`);
    process.exitCode = 1;
}
