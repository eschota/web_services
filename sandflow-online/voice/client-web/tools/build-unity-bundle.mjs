import { build } from 'esbuild';
import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { resolveGameProjectRoot } from './game-project-root.mjs';

const toolDirectory = dirname(fileURLToPath(import.meta.url));
const clientRoot = resolve(toolDirectory, '..');
const gameProjectRoot = await resolveGameProjectRoot();
const outputFile = resolve(
  gameProjectRoot, 'Assets',
  'SandFlowOnline', 'Voice', 'StreamingAssets', 'SandFlowVoice',
  'sandflow-voice.bundle.js',
);

await mkdir(dirname(outputFile), { recursive: true });
await build({
  entryPoints: [resolve(clientRoot, 'src', 'unityBridge.ts')],
  outfile: outputFile,
  bundle: true,
  format: 'iife',
  globalName: 'SandFlowVoiceWeb',
  platform: 'browser',
  target: ['chrome120', 'firefox121', 'edge120'],
  minify: true,
  sourcemap: false,
  legalComments: 'eof',
  banner: { js: '/* SandFlow voice bundle: livekit-client 2.21.0; generated, do not edit. */' },
});

const bundle = await readFile(outputFile);
const lock = await readFile(resolve(clientRoot, 'package-lock.json'));
const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const manifest = {
  schemaVersion: 1,
  generator: 'esbuild@0.28.2',
  livekitClient: '2.21.0',
  bundleFile: 'sandflow-voice.bundle.js',
  bundleBytes: bundle.byteLength,
  bundleSha256: sha256(bundle),
  packageLockSha256: sha256(lock),
};
await writeFile(resolve(dirname(outputFile), 'bundle-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);

console.log(outputFile);
