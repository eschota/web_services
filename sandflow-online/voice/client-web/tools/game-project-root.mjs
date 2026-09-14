import { readFile, stat } from 'node:fs/promises';
import { basename, dirname, isAbsolute, resolve } from 'node:path';

const RequiredUnityVersion = 'm_EditorVersion: 6000.3.21f1';

export async function resolveGameProjectRoot(environment = process.env) {
  const configured = environment.SANDFLOW_GAME_PROJECT;
  if (!configured || !isAbsolute(configured)) {
    throw new Error('SANDFLOW_GAME_PROJECT must be an explicit absolute path to the canonical Unity Game project.');
  }

  const projectRoot = resolve(configured);
  if (/(^|[\\/])(AssetStore|ASStore26)([\\/]|$)/i.test(projectRoot)) {
    throw new Error('AssetStore/ASStore26 is read-only and cannot be a SandFlow game bundle target.');
  }
  if (basename(projectRoot).toLowerCase() !== 'game') {
    throw new Error('SANDFLOW_GAME_PROJECT must identify the canonical Game directory.');
  }

  const versionFile = resolve(projectRoot, 'ProjectSettings', 'ProjectVersion.txt');
  const version = await readFile(versionFile, 'utf8');
  if (!version.split(/\r?\n/).includes(RequiredUnityVersion)) {
    throw new Error(`Unity project must use exactly ${RequiredUnityVersion}.`);
  }

  const onlineAssets = resolve(projectRoot, 'Assets', 'SandFlowOnline');
  if (!(await stat(onlineAssets)).isDirectory()) {
    throw new Error('Canonical Assets/SandFlowOnline directory is missing.');
  }

  const markerCandidates = [resolve(projectRoot, 'AGENTS.md'), resolve(dirname(projectRoot), 'AGENTS.md')];
  let markerAccepted = false;
  for (const marker of markerCandidates) {
    try {
      const text = await readFile(marker, 'utf8');
      if (
        text.includes('# SandFlow game development') &&
        text.includes('The Unity game lives in `Game/`') &&
        text.includes('Asset Store source') &&
        text.includes('read-only')
      ) {
        markerAccepted = true;
        break;
      }
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
  }
  if (!markerAccepted) {
    throw new Error('Canonical SandFlow game AGENTS marker was not found at the project root or its parent.');
  }

  return projectRoot;
}
