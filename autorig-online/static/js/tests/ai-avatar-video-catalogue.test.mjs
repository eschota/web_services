import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const services = fs.readFileSync(path.join(here, '..', '..', '..', 'backend', 'ai_services.py'), 'utf8');

test('Avatar video declares the promoted typed Wan-Animate-2 contract', () => {
  const start = services.indexOf('"id": "avatar_video"');
  const end = services.indexOf('\n    },', start) + 7;
  const block = services.slice(start, end);
  assert.ok(start > 0);
  assert.match(block, /"title": "Avatar video · Wan-Animate-2"/);
  assert.match(block, /"api": "\/api\/ai\/avatar-video", "status": "live", "slow": True/);
  for (const field of ['"field": "avatar"', '"field": "avatar_secondary"',
    '"field": "control_video_url"', '"field": "image"', '"field": "prompt"']) {
    assert.match(block, new RegExp(field));
  }
  assert.match(block, /"type": VIDEO, "field": "video_url_string"/);
});

test('Avatar video exposes bounded production controls and no fake sampling knobs', () => {
  const start = services.indexOf('"avatar_video": [');
  const end = services.indexOf('\n    ],', start) + 7;
  const block = services.slice(start, end);
  for (const name of ['width', 'height', 'frame_count', 'control_strength', 'seed']) {
    assert.match(block, new RegExp('"name": "' + name + '"'));
  }
  assert.match(block, /524288 pixel area/);
  assert.doesNotMatch(block, /"name": "(?:steps|cfg|scheduler|sampler|checkpoint|lora)"/);
});
