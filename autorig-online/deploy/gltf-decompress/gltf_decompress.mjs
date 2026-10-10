// Decompress a glTF/GLB that uses EXT_meshopt_compression or KHR_draco_mesh_compression into a plain GLB.
// The V3 loaders (mt/glb.py, mt/fastrig.py) read raw accessors only; this runs before them on intake.
// Usage: node gltf_decompress.mjs <in.glb> <out.glb>   (prints a JSON receipt; exit 0 = written)
import { NodeIO } from '@gltf-transform/core';
import { ALL_EXTENSIONS } from '@gltf-transform/extensions';
import { MeshoptDecoder } from 'meshoptimizer';
import draco3d from 'draco3dgltf';

const [input, output] = process.argv.slice(2);
if (!input || !output) {
  console.error('usage: gltf_decompress.mjs <in.glb> <out.glb>');
  process.exit(2);
}
const COMPRESSION = new Set(['EXT_meshopt_compression', 'KHR_draco_mesh_compression', 'KHR_meshopt_compression']);

await MeshoptDecoder.ready;
const io = new NodeIO()
  .registerExtensions(ALL_EXTENSIONS)
  .registerDependencies({
    'meshopt.decoder': MeshoptDecoder,
    'draco3d.decoder': await draco3d.createDecoderModule(),
  });

const doc = await io.read(input);
const removed = [];
for (const ext of doc.getRoot().listExtensionsUsed()) {
  if (COMPRESSION.has(ext.extensionName)) {
    removed.push(ext.extensionName);
    ext.dispose();
  }
}
await io.write(output, doc);
console.log(JSON.stringify({ input, output, removed }));
