import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const TEST_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
const AUTORIG_ONLINE = path.resolve(TEST_DIRECTORY, '..', '..', '..');
const CLI = path.join(AUTORIG_ONLINE, 'tools', 'animation_fitting', 'author_browser_horse_swing_guides.mjs');
const DEFAULT_CHROME = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const DEFAULT_THREE = 'R:\\ComfyUI-data\\autorig-fitting\\runtimes\\three-r160\\three.module.js';

test('native CDP synthetic smoke renders a pinned Three r160 WebGL2 PNG', async (context) => {
    const chrome = process.env.AUTORIG_CHROME || DEFAULT_CHROME;
    const three = process.env.AUTORIG_THREE_R160 || DEFAULT_THREE;
    if (!fs.statSync(chrome, { throwIfNoEntry: false })?.isFile()) {
        context.skip(`Chrome is unavailable: ${chrome}`);
        return;
    }
    if (!fs.statSync(three, { throwIfNoEntry: false })?.isFile()) {
        context.skip(`Three r160 is unavailable: ${three}`);
        return;
    }
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), 'autorig-browser-guide-test-'));
    const output = path.join(parent, 'smoke');
    try {
        const { stdout, stderr } = await execFileAsync(process.execPath, [
            CLI,
            '--synthetic-smoke',
            '--chrome', chrome,
            '--three', three,
            '--output', output,
        ], { timeout: 30_000, windowsHide: true });
        assert.equal(stderr, '');
        assert.match(stdout, /"status": "PASS"/);
        const report = JSON.parse(fs.readFileSync(path.join(output, 'report.json'), 'utf8'));
        assert.equal(report.schema, 'autorig-browser-webgl-synthetic-smoke.v1');
        assert.equal(report.status, 'PASS');
        assert.equal(report.browserOnly, true);
        assert.equal(report.blenderUsed, false);
        assert.equal(report.three.revision, '160');
        assert.equal(report.webgl.isWebGL2, true);
        assert.match(report.webgl.version, /^WebGL 2\.0/);
        assert.equal(report.output.width, 64);
        assert.equal(report.output.height, 64);
        assert.equal(report.output.sha256.length, 64);
        assert.ok(report.output.bytes > 0);
    } finally {
        fs.rmSync(parent, { recursive: true, force: true });
    }
});
