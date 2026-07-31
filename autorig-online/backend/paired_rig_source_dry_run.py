"""Pure, no-mutation validation for paired animal rig-source uploads.

The HTTP endpoint is intentionally wired elsewhere.  Everything in this module
operates on in-memory bytes and returns JSON-safe evidence so capability probes
cannot create tasks, charge credits, or persist uploads by accident.
"""
from __future__ import annotations

import base64
from collections import Counter, defaultdict
import hashlib
import json
import math
import re
import struct
from typing import Any, Dict, List, Mapping, Sequence, Tuple


DRY_RUN_CAPABILITY_SCHEMA = "autorig.paired-rig-source-dry-run-capability.v1"
DRY_RUN_VALIDATION_SCHEMA = "autorig.paired-rig-source-dry-run-validation.v1"
DRY_RUN_PATH = "/api/task/paired-rig-source/dry-run"
POSITION_TOLERANCE_M = 1e-6
REQUIRED_EXPECTED_CLIPS = ("Idle", "Walk", "Run")
TRANSFER_MODE = "position-and-triangle-topology"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
EXPECTED_PIN_FORM_FIELDS = {
    "connected_source_sha256": "expected_connected_source_sha256",
    "connected_source_bytes": "expected_connected_source_bytes",
    "appearance_target_sha256": "expected_appearance_target_sha256",
    "appearance_target_bytes": "expected_appearance_target_bytes",
}
_JSON_CHUNK_TYPE = 0x4E4F534A
_BIN_CHUNK_TYPE = 0x004E4942
_COMPONENT_FORMATS = {
    5120: ("b", 1),
    5121: ("B", 1),
    5122: ("h", 2),
    5123: ("H", 2),
    5125: ("I", 4),
    5126: ("f", 4),
}
_TYPE_COMPONENTS = {
    "SCALAR": 1,
    "VEC2": 2,
    "VEC3": 3,
    "VEC4": 4,
    "MAT2": 4,
    "MAT3": 9,
    "MAT4": 16,
}


class PairedRigSourceDryRunError(ValueError):
    """Fail-closed validation error with a stable machine-readable code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _fail(code: str, message: str) -> None:
    raise PairedRigSourceDryRunError(code, message)


def preservation_declarations() -> Dict[str, bool]:
    """Return the immutable appearance-target rules shared by capability/results."""
    return {
        "appearanceTargetGeometryUntouched": True,
        "appearanceTargetFaceTopologyUntouched": True,
        "appearanceTargetUvsUntouched": True,
        "appearanceTargetNormalsUntouched": True,
        "appearanceTargetMaterialsUntouched": True,
        "appearanceTargetImagesUntouched": True,
        "topologyWeldForbidden": True,
        "targetVertexWeightAveragingForbidden": True,
        "sourceWeightsCopiedToAllUvDuplicates": True,
    }


def build_paired_rig_source_dry_run_capability() -> Dict[str, Any]:
    """Describe the only documented no-mutation paired-upload probe."""
    return {
        "schema": DRY_RUN_CAPABILITY_SCHEMA,
        "pairedAnimalDryRun": {
            "enabled": True,
            "method": "POST",
            "path": DRY_RUN_PATH,
            "formFields": {
                "dryRun": "dry_run",
                "connectedRigSource": "rig_source_file",
                "texturedTarget": "appearance_target_file",
                "transferMode": "rig_source_transfer",
                "expectedConnectedRigSourceSha256": "expected_connected_source_sha256",
                "expectedConnectedRigSourceBytes": "expected_connected_source_bytes",
                "expectedTexturedTargetSha256": "expected_appearance_target_sha256",
                "expectedTexturedTargetBytes": "expected_appearance_target_bytes",
            },
            "transferModeValue": TRANSFER_MODE,
            "dryRunGuarantees": {
                "createsTask": False,
                "chargesCredits": False,
                "persistsUpload": False,
            },
        },
        "available": True,
        "method": "POST",
        "path": DRY_RUN_PATH,
        "contentType": "multipart/form-data",
        "createsTask": False,
        "chargesCredits": False,
        "persistsUpload": False,
        "fieldMap": {
            "dry_run": {
                "location": "form",
                "required": True,
                "type": "boolean",
                "constant": True,
            },
            "rig_source_file": {
                "location": "multipart",
                "required": True,
                "role": "connected_pretexture_mesh",
                "formats": ["obj"],
            },
            "appearance_target_file": {
                "location": "multipart",
                "required": True,
                "role": "textured_pbr_uv_split_mesh",
                "formats": ["glb"],
            },
            "rig_source_transfer": {
                "location": "form",
                "required": True,
                "type": "string",
                "constant": TRANSFER_MODE,
            },
            "expected_connected_source_sha256": {
                "location": "form",
                "required": True,
                "type": "lowercase_sha256",
            },
            "expected_connected_source_bytes": {
                "location": "form",
                "required": True,
                "type": "positive_integer",
            },
            "expected_appearance_target_sha256": {
                "location": "form",
                "required": True,
                "type": "lowercase_sha256",
            },
            "expected_appearance_target_bytes": {
                "location": "form",
                "required": True,
                "type": "positive_integer",
            },
        },
        "validation": {
            "positionToleranceM": POSITION_TOLERANCE_M,
            "requiresPinnedSha256AndBytes": True,
            "requiresTriangularSource": True,
            "sourceComponentPolicy": "one_or_more_watertight_components",
            "requiresFullSourcePositionCoverage": True,
            "requiresFullTargetPositionCoverage": True,
            "requiresFaceTopologyMultisetIdentity": True,
            "requiresCompleteUvDuplicateCorrespondence": True,
            "requiresTexturedPbrTarget": True,
        },
        "requiredExpectedClips": list(REQUIRED_EXPECTED_CLIPS),
        "preservationDeclarations": preservation_declarations(),
    }


def _verified_artifact(
    data: bytes,
    *,
    expected_sha256: str,
    expected_bytes: int,
    field: str,
) -> Dict[str, Any]:
    if not isinstance(data, bytes):
        _fail("invalid_bytes", f"{field} must be bytes")
    if not isinstance(expected_sha256, str) or not _SHA256_RE.fullmatch(
        expected_sha256
    ):
        _fail("invalid_expected_sha256", f"{field} expected SHA-256 must be lowercase 64-hex")
    if (
        isinstance(expected_bytes, bool)
        or not isinstance(expected_bytes, int)
        or expected_bytes <= 0
    ):
        _fail("invalid_expected_bytes", f"{field} expected bytes must be a positive integer")
    actual_bytes = len(data)
    actual_sha256 = hashlib.sha256(data).hexdigest()
    if actual_bytes != expected_bytes:
        _fail(
            "artifact_bytes_mismatch",
            f"{field} bytes mismatch: expected {expected_bytes}, got {actual_bytes}",
        )
    if actual_sha256 != expected_sha256:
        _fail(
            "artifact_sha256_mismatch",
            f"{field} SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}",
        )
    return {"sha256": actual_sha256, "bytes": actual_bytes}


def normalize_expected_pair_pins(value: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Validate caller-supplied immutable pins used by dry-run and task creation."""
    if not isinstance(value, Mapping):
        _fail("invalid_expected_pins", "expected paired artifact pins are required")

    def sha256(field: str) -> str:
        result = str(value.get(field) or "").strip()
        if not _SHA256_RE.fullmatch(result):
            _fail("invalid_expected_sha256", f"{field} must be lowercase 64-hex")
        return result

    def byte_count(field: str) -> int:
        raw = value.get(field)
        if isinstance(raw, bool):
            _fail("invalid_expected_bytes", f"{field} must be a positive integer")
        try:
            result = int(str(raw).strip())
        except (TypeError, ValueError):
            _fail("invalid_expected_bytes", f"{field} must be a positive integer")
        if result <= 0 or str(result) != str(raw).strip():
            _fail("invalid_expected_bytes", f"{field} must be a canonical positive integer")
        return result

    return {
        "connected_source": {
            "sha256": sha256("expected_connected_source_sha256"),
            "bytes": byte_count("expected_connected_source_bytes"),
        },
        "appearance_target": {
            "sha256": sha256("expected_appearance_target_sha256"),
            "bytes": byte_count("expected_appearance_target_bytes"),
        },
    }


def _parse_obj(data: bytes) -> Tuple[List[Tuple[float, float, float]], List[Tuple[int, int, int]]]:
    try:
        text = data.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as exc:
        _fail("invalid_obj_encoding", f"connected source OBJ is not UTF-8: {exc}")

    vertices: List[Tuple[float, float, float]] = []
    faces: List[Tuple[int, int, int]] = []
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.partition("#")[0].strip()
        if not line:
            continue
        parts = line.split()
        if parts[0] == "v":
            if len(parts) < 4:
                _fail("invalid_obj_vertex", f"OBJ line {line_number} has an incomplete vertex")
            try:
                vertex = tuple(float(value) for value in parts[1:4])
            except ValueError:
                _fail("invalid_obj_vertex", f"OBJ line {line_number} has a non-numeric vertex")
            if not all(math.isfinite(value) for value in vertex):
                _fail("invalid_obj_vertex", f"OBJ line {line_number} has a non-finite vertex")
            vertices.append(vertex)  # type: ignore[arg-type]
        elif parts[0] == "f":
            if len(parts) != 4:
                _fail("non_triangular_obj", f"OBJ line {line_number} is not a triangle")
            face: List[int] = []
            for token in parts[1:]:
                raw_index = token.split("/", 1)[0]
                try:
                    index = int(raw_index)
                except ValueError:
                    _fail("invalid_obj_face", f"OBJ line {line_number} has an invalid face index")
                if index == 0:
                    _fail("invalid_obj_face", f"OBJ line {line_number} uses forbidden index 0")
                resolved = index - 1 if index > 0 else len(vertices) + index
                if resolved < 0 or resolved >= len(vertices):
                    _fail("invalid_obj_face", f"OBJ line {line_number} references a missing vertex")
                face.append(resolved)
            if len(set(face)) != 3:
                _fail("degenerate_obj_face", f"OBJ line {line_number} is degenerate")
            faces.append((face[0], face[1], face[2]))
    if not vertices:
        _fail("empty_obj", "connected source OBJ has no vertices")
    if not faces:
        _fail("empty_obj", "connected source OBJ has no triangular faces")
    return vertices, faces


class _UnionFind:
    def __init__(self, size: int):
        self.parent = list(range(size))

    def find(self, item: int) -> int:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def _source_component_evidence(
    vertices: Sequence[Tuple[float, float, float]],
    faces: Sequence[Tuple[int, int, int]],
) -> List[Dict[str, int]]:
    used_vertices = {index for face in faces for index in face}
    if len(used_vertices) != len(vertices):
        _fail("unused_source_vertex", "connected source OBJ has vertices outside its face topology")

    union_find = _UnionFind(len(vertices))
    for first, second, third in faces:
        union_find.union(first, second)
        union_find.union(second, third)
    component_faces: Dict[int, List[Tuple[int, int, int]]] = defaultdict(list)
    for face in faces:
        component_faces[union_find.find(face[0])].append(face)

    evidence: List[Dict[str, int]] = []
    ordered_components = sorted(component_faces.values(), key=lambda values: min(min(face) for face in values))
    for component_index, component in enumerate(ordered_components):
        edge_counts: Counter[Tuple[int, int]] = Counter()
        directed_counts: Counter[Tuple[int, int]] = Counter()
        component_vertices = {index for face in component for index in face}
        for first, second, third in component:
            for start, end in ((first, second), (second, third), (third, first)):
                edge_counts[tuple(sorted((start, end)))] += 1
                directed_counts[(start, end)] += 1
        bad_edges = [edge for edge, count in edge_counts.items() if count != 2]
        bad_orientation = [
            edge
            for edge in edge_counts
            if directed_counts[(edge[0], edge[1])] != 1
            or directed_counts[(edge[1], edge[0])] != 1
        ]
        if bad_edges or bad_orientation:
            _fail(
                "source_component_not_watertight",
                f"source component {component_index} is not a closed oriented two-manifold",
            )
        evidence.append(
            {
                "index": component_index,
                "vertexCount": len(component_vertices),
                "triangleCount": len(component),
                "edgeCount": len(edge_counts),
                "eulerCharacteristic": len(component_vertices) - len(edge_counts) + len(component),
            }
        )
    return evidence


def _parse_glb(data: bytes) -> Tuple[Dict[str, Any], List[bytes]]:
    if len(data) < 20:
        _fail("invalid_glb", "appearance target GLB is truncated")
    magic, version, declared_length = struct.unpack_from("<4sII", data, 0)
    if magic != b"glTF" or version != 2 or declared_length != len(data):
        _fail("invalid_glb", "appearance target is not an exact-length GLB v2")
    offset = 12
    json_chunk = None
    bin_chunks: List[bytes] = []
    while offset < len(data):
        if offset + 8 > len(data):
            _fail("invalid_glb", "appearance target has a truncated chunk header")
        chunk_length, chunk_type = struct.unpack_from("<II", data, offset)
        offset += 8
        chunk_end = offset + chunk_length
        if chunk_end > len(data):
            _fail("invalid_glb", "appearance target has a truncated chunk")
        chunk = data[offset:chunk_end]
        offset = chunk_end
        if chunk_type == _JSON_CHUNK_TYPE:
            if json_chunk is not None:
                _fail("invalid_glb", "appearance target has multiple JSON chunks")
            json_chunk = chunk
        elif chunk_type == _BIN_CHUNK_TYPE:
            bin_chunks.append(chunk)
    if json_chunk is None:
        _fail("invalid_glb", "appearance target has no JSON chunk")
    try:
        root = json.loads(json_chunk.rstrip(b"\x00 \t\r\n").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("invalid_glb_json", f"appearance target JSON is invalid: {exc}")
    if not isinstance(root, dict):
        _fail("invalid_glb_json", "appearance target JSON root must be an object")
    return root, bin_chunks


def _decode_data_uri(uri: str, field: str) -> bytes:
    if not uri.startswith("data:") or "," not in uri:
        _fail("external_glb_resource", f"{field} must be embedded in the in-memory GLB")
    header, encoded = uri.split(",", 1)
    try:
        if header.endswith(";base64"):
            return base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        _fail("invalid_glb_resource", f"{field} has invalid base64: {exc}")
    _fail("invalid_glb_resource", f"{field} data URI must use base64")


def _resolve_buffers(root: Mapping[str, Any], bin_chunks: Sequence[bytes]) -> List[bytes]:
    raw_buffers = root.get("buffers")
    if not isinstance(raw_buffers, list) or not raw_buffers:
        _fail("invalid_glb_buffers", "appearance target has no buffers")
    resolved: List[bytes] = []
    bin_index = 0
    for index, buffer in enumerate(raw_buffers):
        if not isinstance(buffer, dict):
            _fail("invalid_glb_buffers", f"buffer {index} is not an object")
        uri = buffer.get("uri")
        if uri is None:
            if bin_index >= len(bin_chunks):
                _fail("invalid_glb_buffers", f"buffer {index} has no BIN chunk")
            content = bin_chunks[bin_index]
            bin_index += 1
        elif isinstance(uri, str):
            content = _decode_data_uri(uri, f"buffer {index}")
        else:
            _fail("invalid_glb_buffers", f"buffer {index}.uri is invalid")
        byte_length = buffer.get("byteLength")
        if isinstance(byte_length, bool) or not isinstance(byte_length, int) or byte_length < 0:
            _fail("invalid_glb_buffers", f"buffer {index}.byteLength is invalid")
        if len(content) < byte_length:
            _fail("invalid_glb_buffers", f"buffer {index} is shorter than declared")
        resolved.append(content[:byte_length])
    return resolved


def _normalized_integer(value: int, component_type: int) -> float:
    if component_type == 5120:
        return max(float(value) / 127.0, -1.0)
    if component_type == 5121:
        return float(value) / 255.0
    if component_type == 5122:
        return max(float(value) / 32767.0, -1.0)
    if component_type == 5123:
        return float(value) / 65535.0
    if component_type == 5125:
        return float(value) / 4294967295.0
    return float(value)


def _read_accessor(
    root: Mapping[str, Any],
    buffers: Sequence[bytes],
    accessor_index: Any,
    *,
    field: str,
) -> Tuple[List[Tuple[float, ...]], int, str]:
    accessors = root.get("accessors")
    views = root.get("bufferViews")
    if not isinstance(accessor_index, int) or isinstance(accessor_index, bool):
        _fail("invalid_glb_accessor", f"{field} accessor index is invalid")
    if not isinstance(accessors, list) or not 0 <= accessor_index < len(accessors):
        _fail("invalid_glb_accessor", f"{field} accessor does not exist")
    accessor = accessors[accessor_index]
    if not isinstance(accessor, dict) or "sparse" in accessor:
        _fail("unsupported_glb_accessor", f"{field} accessor must be dense")
    view_index = accessor.get("bufferView")
    if not isinstance(views, list) or not isinstance(view_index, int) or not 0 <= view_index < len(views):
        _fail("invalid_glb_accessor", f"{field} accessor has no valid bufferView")
    view = views[view_index]
    if not isinstance(view, dict):
        _fail("invalid_glb_accessor", f"{field} bufferView is invalid")
    buffer_index = view.get("buffer", 0)
    if not isinstance(buffer_index, int) or not 0 <= buffer_index < len(buffers):
        _fail("invalid_glb_accessor", f"{field} buffer index is invalid")
    component_type = accessor.get("componentType")
    accessor_type = accessor.get("type")
    if component_type not in _COMPONENT_FORMATS or accessor_type not in _TYPE_COMPONENTS:
        _fail("unsupported_glb_accessor", f"{field} accessor format is unsupported")
    count = accessor.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        _fail("invalid_glb_accessor", f"{field} accessor count is invalid")
    format_code, component_size = _COMPONENT_FORMATS[component_type]
    component_count = _TYPE_COMPONENTS[accessor_type]
    element_size = component_size * component_count
    stride = view.get("byteStride", element_size)
    if isinstance(stride, bool) or not isinstance(stride, int) or stride < element_size:
        _fail("invalid_glb_accessor", f"{field} byteStride is invalid")
    view_offset = view.get("byteOffset", 0)
    accessor_offset = accessor.get("byteOffset", 0)
    view_length = view.get("byteLength")
    if not all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in (view_offset, accessor_offset, view_length)):
        _fail("invalid_glb_accessor", f"{field} byte range is invalid")
    start = view_offset + accessor_offset
    end = start + (count - 1) * stride + element_size
    if accessor_offset + (count - 1) * stride + element_size > view_length or end > len(buffers[buffer_index]):
        _fail("invalid_glb_accessor", f"{field} accessor exceeds its bufferView")
    unpack_format = "<" + (format_code * component_count)
    normalized = accessor.get("normalized") is True and component_type != 5126
    values: List[Tuple[float, ...]] = []
    for item_index in range(count):
        raw = struct.unpack_from(unpack_format, buffers[buffer_index], start + item_index * stride)
        if normalized:
            converted = tuple(_normalized_integer(value, component_type) for value in raw)
        else:
            converted = tuple(float(value) for value in raw)
        if not all(math.isfinite(value) for value in converted):
            _fail("invalid_glb_accessor", f"{field} accessor contains non-finite values")
        values.append(converted)
    return values, component_type, str(accessor_type)


def _embedded_image_bytes(
    root: Mapping[str, Any], buffers: Sequence[bytes], image_index: int
) -> bytes:
    images = root.get("images")
    views = root.get("bufferViews")
    if not isinstance(images, list) or not 0 <= image_index < len(images):
        _fail("invalid_pbr_image", f"image {image_index} does not exist")
    image = images[image_index]
    if not isinstance(image, dict):
        _fail("invalid_pbr_image", f"image {image_index} is invalid")
    uri = image.get("uri")
    mime_type = image.get("mimeType")
    if isinstance(uri, str):
        header = uri.split(",", 1)[0]
        if not header.startswith("data:image/"):
            _fail("invalid_pbr_image", f"image {image_index} data URI has no image MIME type")
        if mime_type is not None and (
            not isinstance(mime_type, str) or not mime_type.startswith("image/")
        ):
            _fail("invalid_pbr_image", f"image {image_index} MIME type is invalid")
        content = _decode_data_uri(uri, f"image {image_index}")
    else:
        if not isinstance(mime_type, str) or not mime_type.startswith("image/"):
            _fail("invalid_pbr_image", f"image {image_index} has no image MIME type")
        view_index = image.get("bufferView")
        if not isinstance(views, list) or not isinstance(view_index, int) or not 0 <= view_index < len(views):
            _fail("invalid_pbr_image", f"image {image_index} is not embedded")
        view = views[view_index]
        if not isinstance(view, dict):
            _fail("invalid_pbr_image", f"image {image_index} bufferView is invalid")
        buffer_index = view.get("buffer", 0)
        offset = view.get("byteOffset", 0)
        length = view.get("byteLength")
        if not all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in (buffer_index, offset, length)):
            _fail("invalid_pbr_image", f"image {image_index} byte range is invalid")
        if not 0 <= buffer_index < len(buffers) or offset + length > len(buffers[buffer_index]):
            _fail("invalid_pbr_image", f"image {image_index} exceeds its buffer")
        content = buffers[buffer_index][offset : offset + length]
    if not content:
        _fail("invalid_pbr_image", f"image {image_index} is empty")
    return content


def _texture_image_index(root: Mapping[str, Any], texture_index: Any) -> int:
    textures = root.get("textures")
    if not isinstance(texture_index, int) or isinstance(texture_index, bool):
        _fail("invalid_pbr_texture", "PBR texture index is invalid")
    if not isinstance(textures, list) or not 0 <= texture_index < len(textures):
        _fail("invalid_pbr_texture", f"texture {texture_index} does not exist")
    texture = textures[texture_index]
    if not isinstance(texture, dict):
        _fail("invalid_pbr_texture", f"texture {texture_index} is invalid")
    source = texture.get("source")
    if source is None:
        extensions = texture.get("extensions")
        if isinstance(extensions, dict):
            basisu = extensions.get("KHR_texture_basisu")
            if isinstance(basisu, dict):
                source = basisu.get("source")
    if not isinstance(source, int) or isinstance(source, bool):
        _fail("invalid_pbr_texture", f"texture {texture_index} has no image source")
    return source


def _target_geometry(
    root: Mapping[str, Any], buffers: Sequence[bytes]
) -> Tuple[List[Tuple[float, float, float]], List[Tuple[int, int, int]], Dict[str, Any]]:
    meshes = root.get("meshes")
    materials = root.get("materials")
    images = root.get("images")
    if not isinstance(meshes, list) or not meshes:
        _fail("missing_target_mesh", "appearance target GLB has no meshes")
    if not isinstance(materials, list) or not materials:
        _fail("missing_pbr_material", "appearance target GLB has no materials")
    if not isinstance(images, list) or not images:
        _fail("missing_pbr_image", "appearance target GLB has no images")

    positions: List[Tuple[float, float, float]] = []
    triangles: List[Tuple[int, int, int]] = []
    primitive_count = 0
    used_materials = set()
    used_images = set()
    for mesh_index, mesh in enumerate(meshes):
        primitives = mesh.get("primitives") if isinstance(mesh, dict) else None
        if not isinstance(primitives, list) or not primitives:
            _fail("invalid_target_mesh", f"mesh {mesh_index} has no primitives")
        for primitive_index, primitive in enumerate(primitives):
            field = f"mesh {mesh_index} primitive {primitive_index}"
            if not isinstance(primitive, dict) or primitive.get("mode", 4) != 4:
                _fail("non_triangular_target", f"{field} is not TRIANGLES mode")
            attributes = primitive.get("attributes")
            if not isinstance(attributes, dict) or "POSITION" not in attributes:
                _fail("missing_target_positions", f"{field} has no POSITION accessor")
            if "TEXCOORD_0" not in attributes:
                _fail("missing_target_uv", f"{field} has no TEXCOORD_0 accessor")
            primitive_positions, _, position_type = _read_accessor(
                root, buffers, attributes["POSITION"], field=f"{field} POSITION"
            )
            if position_type != "VEC3":
                _fail("invalid_target_positions", f"{field} POSITION must be VEC3")
            primitive_uvs, _, uv_type = _read_accessor(
                root, buffers, attributes["TEXCOORD_0"], field=f"{field} TEXCOORD_0"
            )
            if uv_type != "VEC2" or len(primitive_uvs) != len(primitive_positions):
                _fail("invalid_target_uv", f"{field} UV count/type does not match POSITION")
            raw_indices, component_type, index_type = _read_accessor(
                root, buffers, primitive.get("indices"), field=f"{field} indices"
            )
            if index_type != "SCALAR" or component_type not in (5121, 5123, 5125):
                _fail("invalid_target_indices", f"{field} indices must be unsigned SCALAR")
            indices = [int(value[0]) for value in raw_indices]
            if len(indices) % 3:
                _fail("non_triangular_target", f"{field} index count is not divisible by three")
            if any(index < 0 or index >= len(primitive_positions) for index in indices):
                _fail("invalid_target_indices", f"{field} index is out of range")
            material_index = primitive.get("material")
            if not isinstance(material_index, int) or isinstance(material_index, bool) or not 0 <= material_index < len(materials):
                _fail("missing_pbr_material", f"{field} has no valid material")
            material = materials[material_index]
            pbr = material.get("pbrMetallicRoughness") if isinstance(material, dict) else None
            if not isinstance(pbr, dict):
                _fail("missing_pbr_material", f"{field} material has no PBR metallic-roughness declaration")
            texture_infos = [
                pbr.get("baseColorTexture"),
                pbr.get("metallicRoughnessTexture"),
            ]
            texture_infos = [value for value in texture_infos if isinstance(value, dict)]
            if not texture_infos:
                _fail("missing_pbr_texture", f"{field} PBR material has no embedded appearance texture")
            for texture_info in texture_infos:
                texcoord = texture_info.get("texCoord", 0)
                if not isinstance(texcoord, int) or f"TEXCOORD_{texcoord}" not in attributes:
                    _fail("missing_target_uv", f"{field} lacks the UV set used by its PBR texture")
                image_index = _texture_image_index(root, texture_info.get("index"))
                _embedded_image_bytes(root, buffers, image_index)
                used_images.add(image_index)
            base_index = len(positions)
            positions.extend(
                (value[0], value[1], value[2]) for value in primitive_positions
            )
            for offset in range(0, len(indices), 3):
                triangle = tuple(base_index + indices[offset + axis] for axis in range(3))
                if len(set(triangle)) != 3:
                    _fail("degenerate_target_face", f"{field} contains a degenerate triangle")
                triangles.append(triangle)  # type: ignore[arg-type]
            primitive_count += 1
            used_materials.add(material_index)
    if not positions or not triangles:
        _fail("empty_target_mesh", "appearance target GLB has no indexed triangle geometry")
    return positions, triangles, {
        "meshCount": len(meshes),
        "primitiveCount": primitive_count,
        "materialCount": len(materials),
        "usedMaterialCount": len(used_materials),
        "imageCount": len(images),
        "usedEmbeddedImageCount": len(used_images),
        "hasTexcoord0OnEveryPrimitive": True,
        "hasPbrMaterialOnEveryPrimitive": True,
        "hasEmbeddedPbrImage": True,
    }


class _PositionGroups:
    def __init__(self, tolerance: float):
        self.tolerance = tolerance
        self.representatives: List[Tuple[float, float, float]] = []
        self.cells: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)

    def _cell(self, position: Sequence[float]) -> Tuple[int, int, int]:
        return tuple(math.floor(value / self.tolerance) for value in position)  # type: ignore[return-value]

    def find(self, position: Sequence[float]) -> Tuple[int, float] | None:
        cell = self._cell(position)
        candidates = []
        for x_offset in (-1, 0, 1):
            for y_offset in (-1, 0, 1):
                for z_offset in (-1, 0, 1):
                    neighbor = (cell[0] + x_offset, cell[1] + y_offset, cell[2] + z_offset)
                    for group in self.cells.get(neighbor, ()):
                        delta = max(
                            abs(position[axis] - self.representatives[group][axis])
                            for axis in range(3)
                        )
                        if delta <= self.tolerance:
                            candidates.append((delta, group))
        if not candidates:
            return None
        delta, group = min(candidates)
        return group, delta

    def add_or_find(self, position: Tuple[float, float, float]) -> int:
        match = self.find(position)
        if match is not None:
            return match[0]
        group = len(self.representatives)
        self.representatives.append(position)
        self.cells[self._cell(position)].append(group)
        return group


def _topology_digest(counter: Counter[Tuple[int, int, int]]) -> str:
    expanded: List[List[int]] = []
    for triangle, count in sorted(counter.items()):
        expanded.extend([list(triangle)] * count)
    payload = json.dumps(expanded, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _mapping_evidence(
    source_positions: Sequence[Tuple[float, float, float]],
    source_faces: Sequence[Tuple[int, int, int]],
    target_positions: Sequence[Tuple[float, float, float]],
    target_faces: Sequence[Tuple[int, int, int]],
) -> Dict[str, Any]:
    groups = _PositionGroups(POSITION_TOLERANCE_M)
    source_vertex_groups = [groups.add_or_find(position) for position in source_positions]
    target_vertex_groups: List[int] = []
    max_delta = 0.0
    for target_index, position in enumerate(target_positions):
        match = groups.find(position)
        if match is None:
            _fail(
                "target_position_unmapped",
                f"appearance target position {target_index} has no connected-source match at 1e-6",
            )
        group, delta = match
        target_vertex_groups.append(group)
        max_delta = max(max_delta, delta)

    covered_groups = set(target_vertex_groups)
    missing_groups = set(source_vertex_groups) - covered_groups
    if missing_groups:
        _fail(
            "source_position_uncovered",
            f"{len(missing_groups)} connected-source position groups are absent from the target",
        )

    source_topology: Counter[Tuple[int, int, int]] = Counter()
    for face in source_faces:
        signature = tuple(sorted(source_vertex_groups[index] for index in face))
        if len(set(signature)) != 3:
            _fail("degenerate_source_geometry", "source face collapses within the 1e-6 mapping tolerance")
        source_topology[signature] += 1
    target_topology: Counter[Tuple[int, int, int]] = Counter()
    for face in target_faces:
        signature = tuple(sorted(target_vertex_groups[index] for index in face))
        if len(set(signature)) != 3:
            _fail("degenerate_target_geometry", "target face collapses within the 1e-6 mapping tolerance")
        target_topology[signature] += 1
    if source_topology != target_topology:
        missing_faces = sum((source_topology - target_topology).values())
        extra_faces = sum((target_topology - source_topology).values())
        _fail(
            "face_topology_mismatch",
            f"face topology multiset differs: {missing_faces} missing, {extra_faces} extra",
        )

    target_group_counts = Counter(target_vertex_groups)
    split_groups = sum(1 for count in target_group_counts.values() if count > 1)
    duplicate_vertices = sum(count - 1 for count in target_group_counts.values() if count > 1)
    topology_sha256 = _topology_digest(source_topology)
    return {
        "method": "exact_position_and_face_topology_v1",
        "positionToleranceM": POSITION_TOLERANCE_M,
        "sourceVertexCount": len(source_positions),
        "sourcePositionGroupCount": len(set(source_vertex_groups)),
        "sourcePositionGroupsCovered": len(set(source_vertex_groups)),
        "targetVertexOccurrenceCount": len(target_positions),
        "targetVerticesMapped": len(target_vertex_groups),
        "maxPositionDeltaM": max_delta,
        "fullSourcePositionCoverage": True,
        "fullTargetPositionCoverage": True,
        "uvSplitDuplicateCorrespondenceComplete": True,
        "uvSplitSourcePositionGroupCount": split_groups,
        "uvSplitExtraTargetVertexCount": duplicate_vertices,
        "sourceTriangleCount": len(source_faces),
        "targetTriangleCount": len(target_faces),
        "faceTopologyMultisetIdentity": True,
        "canonicalFaceTopologySha256": topology_sha256,
    }


def validate_paired_rig_source_dry_run(
    *,
    connected_source_obj: bytes,
    appearance_target_glb: bytes,
    expected_connected_source_sha256: str,
    expected_connected_source_bytes: int,
    expected_appearance_target_sha256: str,
    expected_appearance_target_bytes: int,
) -> Dict[str, Any]:
    """Validate a pinned connected OBJ/textured GLB pair without side effects."""
    source_artifact = _verified_artifact(
        connected_source_obj,
        expected_sha256=expected_connected_source_sha256,
        expected_bytes=expected_connected_source_bytes,
        field="connected source",
    )
    target_artifact = _verified_artifact(
        appearance_target_glb,
        expected_sha256=expected_appearance_target_sha256,
        expected_bytes=expected_appearance_target_bytes,
        field="appearance target",
    )
    if source_artifact["sha256"] == target_artifact["sha256"]:
        _fail("identical_artifacts", "connected source and appearance target must be distinct")

    source_positions, source_faces = _parse_obj(connected_source_obj)
    components = _source_component_evidence(source_positions, source_faces)
    root, chunks = _parse_glb(appearance_target_glb)
    buffers = _resolve_buffers(root, chunks)
    target_positions, target_faces, pbr_evidence = _target_geometry(root, buffers)
    mapping = _mapping_evidence(
        source_positions,
        source_faces,
        target_positions,
        target_faces,
    )
    result = {
        "schema": DRY_RUN_VALIDATION_SCHEMA,
        "valid": True,
        "dry_run": True,
        "credits_charged": 0,
        "task_created": False,
        "persisted_uploads": False,
        "temporary_buffers_discarded": True,
        "createsTask": False,
        "chargesCredits": False,
        "persistsUpload": False,
        "artifacts": {
            "connectedSource": {
                **source_artifact,
                "format": "obj",
                "role": "connected_pretexture_mesh",
            },
            "appearanceTarget": {
                **target_artifact,
                "format": "glb",
                "role": "textured_pbr_uv_split_mesh",
            },
        },
        "sourceTopology": {
            "triangleOnly": True,
            "componentPolicy": "one_or_more_watertight_components",
            "componentCount": len(components),
            "eachComponentWatertight": True,
            "components": components,
        },
        "appearanceTarget": pbr_evidence,
        "mapping": mapping,
        "prediction": {
            "model_type": "animal",
            "animal_type": "horse",
            "task_type": "only_rig",
            "view": "front",
            "expected_clips": list(REQUIRED_EXPECTED_CLIPS),
        },
        "transfer": {
            "enabled": True,
            "mode": TRANSFER_MODE,
            "position_tolerance_m": POSITION_TOLERANCE_M,
            "correspondence": {
                "full_source_position_coverage": True,
                "full_target_position_coverage": True,
                "face_topology_multiset_identity": True,
                "uv_split_duplicate_correspondence_complete": True,
            },
            "preservation": preservation_declarations(),
        },
        "requiredExpectedClips": list(REQUIRED_EXPECTED_CLIPS),
        "preservationDeclarations": preservation_declarations(),
    }
    # This is a contract assertion, not serialization as a side effect.
    json.dumps(result, allow_nan=False, separators=(",", ":"))
    return result
