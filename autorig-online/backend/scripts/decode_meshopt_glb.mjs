#!/usr/bin/env node
import fs from 'node:fs';
import crypto from 'node:crypto';
import { MeshoptDecoder } from '../vendor/meshoptimizer/meshopt_decoder.mjs';

const [inputPath, outputPath] = process.argv.slice(2);
if (!inputPath || !outputPath) throw new Error('usage: decode_meshopt_glb.mjs INPUT OUTPUT');

const MAX_INPUT = 512 * 1024 * 1024;
const MAX_JSON = 16 * 1024 * 1024;
const MAX_DECODED = 512 * 1024 * 1024;
const source = fs.readFileSync(inputPath);
if (source.length < 20 || source.length > MAX_INPUT) throw new Error('input size is outside policy');
if (source.toString('ascii', 0, 4) !== 'glTF') throw new Error('input is not GLB');
if (source.readUInt32LE(4) !== 2 || source.readUInt32LE(8) !== source.length) {
  throw new Error('invalid GLB header');
}

let cursor = 12;
let jsonChunk = null;
let binChunk = null;
while (cursor < source.length) {
  if (cursor + 8 > source.length) throw new Error('truncated GLB chunk header');
  const length = source.readUInt32LE(cursor);
  const type = source.readUInt32LE(cursor + 4);
  cursor += 8;
  if (length < 0 || cursor + length > source.length) throw new Error('truncated GLB chunk');
  const data = source.subarray(cursor, cursor + length);
  cursor += length;
  if (type === 0x4e4f534a && jsonChunk === null) jsonChunk = data;
  else if (type === 0x004e4942 && binChunk === null) binChunk = data;
  else throw new Error('unsupported extra or duplicate GLB chunk');
}
if (!jsonChunk || !binChunk || jsonChunk.length > MAX_JSON) throw new Error('GLB JSON/BIN chunk missing');

const document = JSON.parse(jsonChunk.toString('utf8').replace(/[\u0000 ]+$/g, ''));
if (!Array.isArray(document.buffers) || document.buffers.length < 1 || document.buffers[0]?.uri) {
  throw new Error('embedded GLB buffer is missing');
}
for (const [index, buffer] of document.buffers.entries()) {
  if (index === 0) continue;
  if (buffer?.uri || buffer?.extensions?.EXT_meshopt_compression?.fallback !== true) {
    throw new Error('unsupported non-meshopt auxiliary GLB buffer');
  }
}
await MeshoptDecoder.ready;

const originalBin = new Uint8Array(binChunk.buffer, binChunk.byteOffset, binChunk.byteLength);
const parts = [Buffer.from(originalBin)];
let outputLength = originalBin.byteLength;
let decodedTotal = 0;
let decodedViews = 0;

for (const [index, view] of (document.bufferViews || []).entries()) {
  const extension = view?.extensions?.EXT_meshopt_compression;
  if (!extension) continue;
  const sourceOffset = Number(extension.byteOffset || 0);
  const sourceLength = Number(extension.byteLength);
  const count = Number(extension.count);
  const stride = Number(extension.byteStride);
  const mode = String(extension.mode || '');
  const filter = String(extension.filter || 'NONE');
  if (
    extension.buffer !== 0 || !Number.isInteger(sourceOffset) || !Number.isInteger(sourceLength) ||
    !Number.isInteger(count) || !Number.isInteger(stride) || sourceOffset < 0 || sourceLength <= 0 ||
    count <= 0 || stride <= 0 || sourceOffset + sourceLength > originalBin.byteLength ||
    !['ATTRIBUTES', 'TRIANGLES', 'INDICES'].includes(mode)
  ) throw new Error(`invalid meshopt extension at bufferView ${index}`);
  const decodedLength = count * stride;
  if (!Number.isSafeInteger(decodedLength) || decodedLength <= 0 || decodedTotal + decodedLength > MAX_DECODED) {
    throw new Error('decoded meshopt data exceeds policy');
  }
  const decoded = new Uint8Array(decodedLength);
  const encoded = originalBin.subarray(sourceOffset, sourceOffset + sourceLength);
  MeshoptDecoder.decodeGltfBuffer(decoded, count, stride, encoded, mode, filter);

  const padding = (4 - (outputLength % 4)) % 4;
  if (padding) {
    parts.push(Buffer.alloc(padding));
    outputLength += padding;
  }
  view.buffer = 0;
  view.byteOffset = outputLength;
  view.byteLength = decoded.byteLength;
  if (mode === 'ATTRIBUTES') view.byteStride = stride;
  else delete view.byteStride;
  delete view.extensions.EXT_meshopt_compression;
  if (Object.keys(view.extensions).length === 0) delete view.extensions;
  parts.push(Buffer.from(decoded));
  outputLength += decoded.byteLength;
  decodedTotal += decoded.byteLength;
  decodedViews += 1;
}
if (decodedViews === 0) throw new Error('input does not use EXT_meshopt_compression');

for (const key of ['extensionsRequired', 'extensionsUsed']) {
  if (!Array.isArray(document[key])) continue;
  document[key] = document[key].filter((name) => name !== 'EXT_meshopt_compression');
  if (document[key].length === 0) delete document[key];
}
const bin = Buffer.concat(parts);
for (const [index, view] of (document.bufferViews || []).entries()) {
  if (view?.buffer !== 0) throw new Error(`bufferView ${index} still references a virtual buffer`);
}
document.buffers = [{ byteLength: bin.length }];
let json = Buffer.from(JSON.stringify(document), 'utf8');
const jsonPadding = (4 - (json.length % 4)) % 4;
if (jsonPadding) json = Buffer.concat([json, Buffer.alloc(jsonPadding, 0x20)]);
const binPadding = (4 - (bin.length % 4)) % 4;
const paddedBin = binPadding ? Buffer.concat([bin, Buffer.alloc(binPadding)]) : bin;
const totalLength = 12 + 8 + json.length + 8 + paddedBin.length;
const header = Buffer.alloc(12);
header.write('glTF', 0, 'ascii');
header.writeUInt32LE(2, 4);
header.writeUInt32LE(totalLength, 8);
const jsonHeader = Buffer.alloc(8);
jsonHeader.writeUInt32LE(json.length, 0);
jsonHeader.writeUInt32LE(0x4e4f534a, 4);
const binHeader = Buffer.alloc(8);
binHeader.writeUInt32LE(paddedBin.length, 0);
binHeader.writeUInt32LE(0x004e4942, 4);
const result = Buffer.concat([header, jsonHeader, json, binHeader, paddedBin]);
fs.writeFileSync(outputPath, result, { flag: 'wx' });
console.log(JSON.stringify({
  schema: 'autorig.meshopt_decode.v1',
  decoder_version: 'meshoptimizer-1.2.0',
  input_sha256: crypto.createHash('sha256').update(source).digest('hex'),
  output_sha256: crypto.createHash('sha256').update(result).digest('hex'),
  input_bytes: source.length,
  output_bytes: result.length,
  decoded_views: decodedViews,
  decoded_bytes: decodedTotal,
}));
