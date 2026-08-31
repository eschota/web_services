#!/usr/bin/env node
import fs from 'node:fs';
import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { MeshoptDecoder } from '../vendor/meshoptimizer/meshopt_decoder.mjs';

const [inputPath, outputPath] = process.argv.slice(2);
if (!inputPath || !outputPath) throw new Error('usage: decode_meshopt_glb.mjs INPUT OUTPUT');

const MAX_INPUT = 512 * 1024 * 1024;
const MAX_JSON = 16 * 1024 * 1024;
const MAX_DECODED = 512 * 1024 * 1024;
const MAX_OUTPUT = 1024 * 1024 * 1024;
const KTX_EXE = process.env.KTX_EXE || '/srv/autorig/tools/KTX-Software-4.4.2-Linux-x86_64/bin/ktx';
const KTX2_MAGIC = Buffer.from([0xab, 0x4b, 0x54, 0x58, 0x20, 0x32, 0x30, 0xbb, 0x0d, 0x0a, 0x1a, 0x0a]);
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
let decodedTextures = 0;

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

const basisSources = new Set();
for (const [textureIndex, texture] of (document.textures || []).entries()) {
  const extension = texture?.extensions?.KHR_texture_basisu;
  if (!extension) continue;
  const sourceIndex = Number(extension.source);
  if (!Number.isInteger(sourceIndex) || sourceIndex < 0 || sourceIndex >= (document.images || []).length) {
    throw new Error(`invalid KHR_texture_basisu source at texture ${textureIndex}`);
  }
  basisSources.add(sourceIndex);
}
for (const [imageIndex, image] of (document.images || []).entries()) {
  if (String(image?.mimeType || '').toLowerCase() === 'image/ktx2') basisSources.add(imageIndex);
}

let textureDecodedBudget = 0;
let textureTempRoot = null;
try {
  if (basisSources.size > 0) {
    if (!fs.existsSync(KTX_EXE)) throw new Error('KTX decoder runtime is unavailable');
    textureTempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'autorig-ktx2-'));
  }
  for (const imageIndex of basisSources) {
    const image = document.images?.[imageIndex];
    const viewIndex = Number(image?.bufferView);
    const view = document.bufferViews?.[viewIndex];
    const viewOffset = Number(view?.byteOffset || 0);
    const viewLength = Number(view?.byteLength);
    if (
      !Number.isInteger(viewIndex) || !view || view.buffer !== 0 ||
      !Number.isInteger(viewOffset) || !Number.isInteger(viewLength) ||
      viewOffset < 0 || viewLength < 68 || viewOffset + viewLength > originalBin.byteLength
    ) throw new Error(`invalid KTX2 image bufferView ${viewIndex}`);
    const ktx2 = Buffer.from(originalBin.subarray(viewOffset, viewOffset + viewLength));
    if (!ktx2.subarray(0, 12).equals(KTX2_MAGIC)) throw new Error(`image ${imageIndex} is not KTX2`);
    const width = ktx2.readUInt32LE(20);
    const height = ktx2.readUInt32LE(24);
    const depth = ktx2.readUInt32LE(28);
    const layers = ktx2.readUInt32LE(32);
    const faces = ktx2.readUInt32LE(36);
    const pixels = width * height;
    if (
      width <= 0 || height <= 0 || width > 16384 || height > 16384 ||
      depth > 1 || layers > 1 || faces !== 1 || !Number.isSafeInteger(pixels) ||
      pixels > 64 * 1024 * 1024
    ) throw new Error(`KTX2 image ${imageIndex} dimensions are outside policy`);
    textureDecodedBudget += pixels * 4;
    if (textureDecodedBudget > MAX_DECODED) throw new Error('decoded KTX2 textures exceed policy');

    const inputKtx = path.join(textureTempRoot, `image-${imageIndex}.ktx2`);
    const outputPng = path.join(textureTempRoot, `image-${imageIndex}.png`);
    fs.writeFileSync(inputKtx, ktx2, { flag: 'wx' });
    const converted = spawnSync(
      KTX_EXE,
      ['extract', '--transcode', 'rgba8', inputKtx, outputPng],
      { encoding: 'utf8', timeout: 120000, windowsHide: true },
    );
    if (converted.error || converted.status !== 0 || !fs.existsSync(outputPng)) {
      const detail = String(converted.stderr || converted.stdout || converted.error || 'KTX extract failed');
      throw new Error(`KTX2 decode failed: ${detail.slice(-1000)}`);
    }
    const png = fs.readFileSync(outputPng);
    if (png.length < 8 || png.toString('hex', 0, 8) !== '89504e470d0a1a0a') {
      throw new Error(`KTX2 decode did not produce PNG for image ${imageIndex}`);
    }
    const padding = (4 - (outputLength % 4)) % 4;
    if (padding) {
      parts.push(Buffer.alloc(padding));
      outputLength += padding;
    }
    if (outputLength + png.length > MAX_OUTPUT) throw new Error('normalized GLB exceeds output policy');
    const pngViewIndex = document.bufferViews.length;
    document.bufferViews.push({ buffer: 0, byteOffset: outputLength, byteLength: png.length });
    image.bufferView = pngViewIndex;
    image.mimeType = 'image/png';
    delete image.uri;
    parts.push(png);
    outputLength += png.length;
    decodedTextures += 1;
  }
} finally {
  if (textureTempRoot) fs.rmSync(textureTempRoot, { recursive: true, force: true });
}

for (const texture of (document.textures || [])) {
  const extension = texture?.extensions?.KHR_texture_basisu;
  if (!extension) continue;
  texture.source = Number(extension.source);
  delete texture.extensions.KHR_texture_basisu;
  if (Object.keys(texture.extensions).length === 0) delete texture.extensions;
}

if (decodedViews === 0 && decodedTextures === 0) {
  throw new Error('input does not use a supported normalization extension');
}

for (const key of ['extensionsRequired', 'extensionsUsed']) {
  if (!Array.isArray(document[key])) continue;
  document[key] = document[key].filter(
    (name) => name !== 'EXT_meshopt_compression' && name !== 'KHR_texture_basisu',
  );
  if (document[key].length === 0) delete document[key];
}
const bin = Buffer.concat(parts);
if (bin.length > MAX_OUTPUT) throw new Error('normalized GLB exceeds output policy');
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
  decoded_textures: decodedTextures,
  texture_decoder_version: decodedTextures ? 'KTX-Software-4.4.2' : null,
}));
