import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, readdir, stat } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { resolveGameProjectRoot } from '../tools/game-project-root.mjs';

const clientRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const gameProjectRoot = await resolveGameProjectRoot();
const unityRoot = resolve(gameProjectRoot, 'Assets', 'SandFlowOnline', 'Voice');

test('Unity target requires explicit canonical Game root and rejects AssetStore', async () => {
  await assert.rejects(resolveGameProjectRoot({}), /SANDFLOW_GAME_PROJECT/);
  await assert.rejects(
    resolveGameProjectRoot({ SANDFLOW_GAME_PROJECT: 'R:\\AssetStore\\ASStore26\\SandFlow\\project' }),
    /read-only/,
  );
  assert.equal(gameProjectRoot, resolve(process.env.SANDFLOW_GAME_PROJECT));
});

test('C# externs and jslib exports remain exactly aligned', async () => {
  const nativeSource = await readFile(resolve(unityRoot, 'Runtime', 'SandFlowWebVoiceNative.cs'), 'utf8');
  const jslib = await readFile(resolve(unityRoot, 'Plugins', 'WebGL', 'SandFlowVoice.jslib'), 'utf8');
  const externs = [...nativeSource.matchAll(/extern (?:int|void) (SFVoice_[A-Za-z0-9_]+)\(/g)].map((match) => match[1]).sort();
  const exports = [...jslib.matchAll(/^  (SFVoice_[A-Za-z0-9_]+): function/gm)].map((match) => match[1]).sort();
  assert.deepEqual(exports, externs);
  assert.equal(new Function(jslib) instanceof Function, true, 'jslib must be valid JavaScript syntax');
});

test('generated Unity bundle matches its deterministic provenance manifest', async () => {
  const bundle = await readFile(resolve(unityRoot, 'StreamingAssets', 'SandFlowVoice', 'sandflow-voice.bundle.js'));
  const manifest = JSON.parse(await readFile(resolve(unityRoot, 'StreamingAssets', 'SandFlowVoice', 'bundle-manifest.json'), 'utf8'));
  const lock = await readFile(resolve(clientRoot, 'package-lock.json'));
  const sha = (value) => createHash('sha256').update(value).digest('hex');
  assert.equal(manifest.livekitClient, '2.21.0');
  assert.equal(manifest.generator, 'esbuild@0.28.2');
  assert.equal(manifest.bundleBytes, bundle.byteLength);
  assert.equal(manifest.bundleSha256, sha(bundle));
  assert.equal(manifest.packageLockSha256, sha(lock));
});

test('every Unity voice file and folder has a committed meta sidecar', async () => {
  async function walk(directory) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      if (entry.name.endsWith('.meta')) continue;
      const path = resolve(directory, entry.name);
      await stat(`${path}.meta`);
      if (entry.isDirectory()) await walk(path);
    }
  }
  await stat(`${unityRoot}.meta`);
  await walk(unityRoot);
});
