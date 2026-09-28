"""Strict, side-effect-free quality validation for Renderfin GLB artifacts.

The validator deliberately understands only the uncompressed GLB 2.0 subset
emitted by the Hunyuan workers.  Unsupported encodings are rejected instead of
being guessed at.  Apart from Pillow (already pinned by the backend), the
implementation uses only the Python standard library.

The semantic gate was calibrated on Renderfin character output.  A small
normalised surface area is not sufficient on its own: the model is rejected
only when it is accompanied by a sparse orthographic silhouette.  This avoids
turning useful component-count and polygon-count signals into brittle rules.
"""
from __future__ import annotations

import base64
from collections import Counter
from dataclasses import asdict, dataclass
import hashlib
from io import BytesIO
import json
import math
from pathlib import Path
import struct
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import warnings

from PIL import Image, ImageDraw


GLB_QUALITY_SCHEMA = "renderfin.glb-quality.v1"
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
_UNSUPPORTED_GEOMETRY_EXTENSIONS = {
    "EXT_meshopt_compression",
    "KHR_draco_mesh_compression",
    "KHR_mesh_quantization",
}
_IMAGE_FORMAT_MIME = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "WEBP": "image/webp",
}


@dataclass(frozen=True)
class GlbQualityPolicy:
    """Bounded validation and calibrated Renderfin character thresholds."""

    max_file_bytes: int = 512 * 1024 * 1024
    max_json_bytes: int = 16 * 1024 * 1024
    max_accessor_elements: int = 50_000_000
    max_total_components: int = 150_000_000
    max_triangles: int = 5_000_000
    max_scene_instances: int = 100_000
    max_image_bytes: int = 128 * 1024 * 1024
    max_image_pixels: int = 67_108_864
    silhouette_size: int = 192
    min_bbox_diagonal: float = 1e-8
    degenerate_area_scale: float = 1e-12
    max_degenerate_ratio: float = 0.01
    semantic_normalized_area_below: float = 0.10
    semantic_silhouette_occupancy_below: float = 0.06
    semantic_silhouette_bbox_fill_below: float = 0.10
    diagnostic_triangle_count_below: int = 25_000
    diagnostic_largest_welded_fraction_below: float = 0.48
    require_embedded_image: bool = False
    require_texture: bool = False


DEFAULT_POLICY = GlbQualityPolicy()
HUNYUAN_STANDARD_PBR_POLICY = GlbQualityPolicy(
    require_embedded_image=True,
    require_texture=True,
)


class GlbQualityError(ValueError):
    """Fail-closed error carrying a stable code and JSON-safe evidence."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})

    def as_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "message": str(self), "details": self.details}


# A descriptive alias makes call sites that treat this as transport validation
# read naturally without splitting the exception contract.
GlbValidationError = GlbQualityError


def _fail(
    code: str,
    message: str,
    *,
    details: Optional[Mapping[str, Any]] = None,
) -> None:
    raise GlbQualityError(code, message, details=details)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _nonnegative_int(value: Any, field: str) -> int:
    if not _is_int(value) or value < 0:
        _fail("invalid_glb_schema", f"{field} must be a non-negative integer")
    return value


def _positive_int(value: Any, field: str) -> int:
    if not _is_int(value) or value <= 0:
        _fail("invalid_glb_schema", f"{field} must be a positive integer")
    return value


def _index(value: Any, length: int, field: str) -> int:
    if not _is_int(value) or not 0 <= value < length:
        _fail("invalid_glb_reference", f"{field} references a missing item")
    return value


def _array(root: Mapping[str, Any], field: str) -> List[Any]:
    value = root.get(field, [])
    if not isinstance(value, list):
        _fail("invalid_glb_schema", f"{field} must be an array")
    return value


def _finite_vector(
    value: Any,
    length: int,
    field: str,
    *,
    default: Sequence[float],
) -> Tuple[float, ...]:
    if value is None:
        return tuple(float(item) for item in default)
    if not isinstance(value, list) or len(value) != length:
        _fail("invalid_node_transform", f"{field} must contain {length} numbers")
    converted: List[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            _fail("invalid_node_transform", f"{field} contains a non-number")
        number = float(item)
        if not math.isfinite(number):
            _fail("invalid_node_transform", f"{field} contains a non-finite number")
        converted.append(number)
    return tuple(converted)


def _parse_glb(
    data: bytes, policy: GlbQualityPolicy
) -> Tuple[Dict[str, Any], Optional[bytes], List[Dict[str, Any]]]:
    if not isinstance(data, bytes):
        _fail("invalid_input", "GLB payload must be bytes")
    if len(data) > policy.max_file_bytes:
        _fail(
            "glb_limit_exceeded",
            "GLB exceeds the configured byte limit",
            details={"bytes": len(data), "limit": policy.max_file_bytes},
        )
    if len(data) < 20:
        _fail("invalid_glb_header", "GLB is shorter than its header and first chunk")
    magic, version, declared_length = struct.unpack_from("<4sII", data, 0)
    if magic != b"glTF":
        _fail("invalid_glb_header", "GLB magic is not glTF")
    if version != 2:
        _fail("unsupported_glb_version", f"GLB version {version} is not supported")
    if declared_length != len(data):
        _fail(
            "glb_length_mismatch",
            "GLB declared length does not exactly match the payload",
            details={"declared": declared_length, "actual": len(data)},
        )

    offset = 12
    chunks: List[Dict[str, Any]] = []
    json_payload: Optional[bytes] = None
    bin_payload: Optional[bytes] = None
    while offset < len(data):
        if offset + 8 > len(data):
            _fail("truncated_glb_chunk", "GLB has a truncated chunk header")
        chunk_length, chunk_type = struct.unpack_from("<II", data, offset)
        offset += 8
        if chunk_length % 4:
            _fail("misaligned_glb_chunk", "GLB chunk length is not a multiple of four")
        chunk_end = offset + chunk_length
        if chunk_end > len(data):
            _fail("truncated_glb_chunk", "GLB chunk extends beyond the declared payload")
        payload = data[offset:chunk_end]
        offset = chunk_end
        chunk_index = len(chunks)
        if chunk_type == _JSON_CHUNK_TYPE:
            if chunk_index != 0 or json_payload is not None:
                _fail("invalid_glb_chunks", "GLB must contain exactly one first JSON chunk")
            if chunk_length > policy.max_json_bytes:
                _fail("glb_limit_exceeded", "GLB JSON chunk exceeds the configured limit")
            json_payload = payload
            label = "JSON"
        elif chunk_type == _BIN_CHUNK_TYPE:
            if chunk_index == 0 or bin_payload is not None:
                _fail("invalid_glb_chunks", "GLB may contain at most one BIN chunk after JSON")
            bin_payload = payload
            label = "BIN"
        else:
            _fail(
                "unsupported_glb_chunk",
                f"GLB chunk type 0x{chunk_type:08x} is unsupported",
            )
        chunks.append({"index": chunk_index, "type": label, "bytes": chunk_length})
    if offset != len(data):
        _fail("glb_length_mismatch", "GLB chunk scan did not end at the declared length")
    if json_payload is None:
        _fail("invalid_glb_chunks", "GLB has no JSON chunk")

    try:
        json_text = json_payload.rstrip(b"\x00 \t\r\n").decode("utf-8")
        root = json.loads(
            json_text,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON number {value}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        _fail("invalid_glb_json", f"GLB JSON chunk is invalid: {exc}")
    if not isinstance(root, dict):
        _fail("invalid_glb_json", "GLB JSON root must be an object")
    asset = root.get("asset")
    if not isinstance(asset, dict) or str(asset.get("version") or "") != "2.0":
        _fail("invalid_glb_asset", "GLB asset.version must be exactly 2.0")
    required_extensions = root.get("extensionsRequired", [])
    if not isinstance(required_extensions, list) or not all(
        isinstance(item, str) for item in required_extensions
    ):
        _fail("invalid_glb_schema", "extensionsRequired must be an array of strings")
    unsupported = sorted(
        _UNSUPPORTED_GEOMETRY_EXTENSIONS.intersection(required_extensions)
    )
    if unsupported:
        _fail(
            "unsupported_geometry_encoding",
            "GLB requires a geometry encoding this validator cannot decode",
            details={"extensions": unsupported},
        )
    return root, bin_payload, chunks


def _decode_data_uri(uri: str, field: str) -> Tuple[str, bytes]:
    if not uri.startswith("data:") or "," not in uri:
        _fail("external_glb_resource", f"{field} must be embedded as a data URI")
    header, encoded = uri.split(",", 1)
    parts = header[5:].split(";")
    mime = parts[0].lower()
    if "base64" not in parts[1:]:
        _fail("invalid_glb_resource", f"{field} data URI must use base64")
    try:
        content = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as exc:
        _fail("invalid_glb_resource", f"{field} has invalid base64: {exc}")
    return mime, content


class _GlbDocument:
    def __init__(
        self,
        root: Mapping[str, Any],
        bin_payload: Optional[bytes],
        policy: GlbQualityPolicy,
    ) -> None:
        self.root = root
        self.policy = policy
        self.buffers = self._resolve_buffers(bin_payload)
        self.views = _array(root, "bufferViews")
        self.accessors = _array(root, "accessors")
        self._collected_accessors: Dict[int, List[Tuple[float, ...]]] = {}
        self._accessor_metadata: Dict[int, Dict[str, Any]] = {}
        self._total_components = 0
        self._validate_views()

    def _resolve_buffers(self, bin_payload: Optional[bytes]) -> List[bytes]:
        raw_buffers = _array(self.root, "buffers")
        if not raw_buffers:
            _fail("invalid_glb_buffers", "GLB has no buffers")
        resolved: List[bytes] = []
        used_bin = False
        for buffer_index, item in enumerate(raw_buffers):
            if not isinstance(item, dict):
                _fail("invalid_glb_buffers", f"buffer {buffer_index} is not an object")
            byte_length = _positive_int(
                item.get("byteLength"), f"buffer {buffer_index}.byteLength"
            )
            uri = item.get("uri")
            if uri is None:
                if buffer_index != 0 or used_bin or bin_payload is None:
                    _fail(
                        "invalid_glb_buffers",
                        f"buffer {buffer_index} has no corresponding BIN chunk",
                    )
                content = bin_payload
                used_bin = True
                excess_limit = 3
            elif isinstance(uri, str):
                _mime, content = _decode_data_uri(uri, f"buffer {buffer_index}")
                excess_limit = 0
            else:
                _fail("invalid_glb_buffers", f"buffer {buffer_index}.uri is invalid")
            if byte_length > len(content):
                _fail(
                    "invalid_glb_buffers",
                    f"buffer {buffer_index} is shorter than its declared byteLength",
                )
            if len(content) - byte_length > excess_limit:
                _fail(
                    "invalid_glb_buffers",
                    f"buffer {buffer_index} contains excess undeclared bytes",
                )
            if uri is None and any(content[byte_length:]):
                _fail(
                    "invalid_glb_buffers",
                    f"buffer {buffer_index} has non-zero BIN padding",
                )
            resolved.append(content[:byte_length])
        if bin_payload is not None and not used_bin:
            _fail("invalid_glb_buffers", "GLB contains an unreferenced BIN chunk")
        return resolved

    def _validate_views(self) -> None:
        for view_index, item in enumerate(self.views):
            if not isinstance(item, dict):
                _fail("invalid_buffer_view", f"bufferView {view_index} is not an object")
            buffer_index = _index(
                item.get("buffer"), len(self.buffers), f"bufferView {view_index}.buffer"
            )
            offset = _nonnegative_int(
                item.get("byteOffset", 0), f"bufferView {view_index}.byteOffset"
            )
            length = _positive_int(
                item.get("byteLength"), f"bufferView {view_index}.byteLength"
            )
            if offset + length > len(self.buffers[buffer_index]):
                _fail(
                    "buffer_view_out_of_bounds",
                    f"bufferView {view_index} exceeds buffer {buffer_index}",
                )
            if "byteStride" in item:
                stride = _positive_int(
                    item["byteStride"], f"bufferView {view_index}.byteStride"
                )
                if not 4 <= stride <= 252 or stride % 4:
                    _fail(
                        "invalid_buffer_view",
                        f"bufferView {view_index}.byteStride must be a multiple of four in [4, 252]",
                    )
            if "target" in item and item["target"] not in (34962, 34963):
                _fail("invalid_buffer_view", f"bufferView {view_index}.target is invalid")

    def view_bytes(self, view_index: Any, field: str) -> memoryview:
        resolved = _index(view_index, len(self.views), field)
        item = self.views[resolved]
        buffer_index = item["buffer"]
        start = item.get("byteOffset", 0)
        end = start + item["byteLength"]
        return memoryview(self.buffers[buffer_index])[start:end]

    @staticmethod
    def _layout(accessor_type: str, component_size: int) -> Tuple[List[int], int]:
        component_count = _TYPE_COMPONENTS[accessor_type]
        if not accessor_type.startswith("MAT"):
            offsets = [index * component_size for index in range(component_count)]
            return offsets, component_count * component_size
        dimension = int(accessor_type[-1])
        column_stride = ((dimension * component_size + 3) // 4) * 4
        offsets = [
            column * column_stride + row * component_size
            for column in range(dimension)
            for row in range(dimension)
        ]
        return offsets, dimension * column_stride

    def read_accessor(
        self,
        accessor_index: Any,
        *,
        field: str,
        collect: bool = True,
    ) -> Tuple[List[Tuple[float, ...]], Dict[str, Any]]:
        resolved = _index(accessor_index, len(self.accessors), f"{field} accessor")
        if collect and resolved in self._collected_accessors:
            return self._collected_accessors[resolved], self._accessor_metadata[resolved]
        if not collect and resolved in self._accessor_metadata:
            return [], self._accessor_metadata[resolved]
        accessor = self.accessors[resolved]
        if not isinstance(accessor, dict):
            _fail("invalid_accessor", f"accessor {resolved} is not an object")
        if "sparse" in accessor:
            _fail("unsupported_accessor", f"accessor {resolved} is sparse")
        if "bufferView" not in accessor:
            _fail("unsupported_accessor", f"accessor {resolved} has no dense bufferView")
        component_type = accessor.get("componentType")
        accessor_type = accessor.get("type")
        if component_type not in _COMPONENT_FORMATS or accessor_type not in _TYPE_COMPONENTS:
            _fail("unsupported_accessor", f"accessor {resolved} has an unsupported format")
        count = _positive_int(accessor.get("count"), f"accessor {resolved}.count")
        if count > self.policy.max_accessor_elements:
            _fail("glb_limit_exceeded", f"accessor {resolved} exceeds the element limit")
        format_code, component_size = _COMPONENT_FORMATS[component_type]
        component_offsets, element_size = self._layout(accessor_type, component_size)
        component_count = len(component_offsets)
        if resolved not in self._accessor_metadata:
            self._total_components += count * component_count
            if self._total_components > self.policy.max_total_components:
                _fail("glb_limit_exceeded", "GLB exceeds the decoded-component limit")
        view_index = _index(
            accessor.get("bufferView"), len(self.views), f"accessor {resolved}.bufferView"
        )
        view = self.views[view_index]
        raw = self.view_bytes(view_index, f"accessor {resolved}.bufferView")
        stride = view.get("byteStride", element_size)
        if stride < element_size or stride % component_size:
            _fail("invalid_accessor", f"accessor {resolved} has an invalid byte stride")
        accessor_offset = _nonnegative_int(
            accessor.get("byteOffset", 0), f"accessor {resolved}.byteOffset"
        )
        absolute_offset = view.get("byteOffset", 0) + accessor_offset
        if absolute_offset % component_size:
            _fail("invalid_accessor", f"accessor {resolved} is not component-aligned")
        required = accessor_offset + (count - 1) * stride + element_size
        if required > len(raw):
            _fail("accessor_out_of_bounds", f"accessor {resolved} exceeds its bufferView")
        normalized = accessor.get("normalized", False)
        if not isinstance(normalized, bool):
            _fail("invalid_accessor", f"accessor {resolved}.normalized must be boolean")
        if normalized and component_type == 5126:
            _fail("invalid_accessor", f"float accessor {resolved} cannot be normalized")

        unpack_format = "<" + format_code
        values: List[Tuple[float, ...]] = []
        actual_min = [math.inf] * component_count
        actual_max = [-math.inf] * component_count
        for item_index in range(count):
            start = accessor_offset + item_index * stride
            item_values: List[float] = []
            for component_index, component_offset in enumerate(component_offsets):
                raw_value = struct.unpack_from(
                    unpack_format, raw, start + component_offset
                )[0]
                value = float(raw_value)
                if not math.isfinite(value):
                    _fail(
                        "nonfinite_accessor",
                        f"accessor {resolved} contains a non-finite value",
                        details={"accessor": resolved, "element": item_index},
                    )
                actual_min[component_index] = min(actual_min[component_index], value)
                actual_max[component_index] = max(actual_max[component_index], value)
                item_values.append(value)
            if collect:
                values.append(tuple(item_values))

        for label, actual in (("min", actual_min), ("max", actual_max)):
            declared = accessor.get(label)
            if declared is None:
                continue
            if not isinstance(declared, list) or len(declared) != component_count:
                _fail(
                    "invalid_accessor_bounds",
                    f"accessor {resolved}.{label} has the wrong component count",
                )
            for component_index, (expected, observed) in enumerate(zip(declared, actual)):
                if isinstance(expected, bool) or not isinstance(expected, (int, float)):
                    _fail(
                        "invalid_accessor_bounds",
                        f"accessor {resolved}.{label} contains a non-number",
                    )
                expected_float = float(expected)
                if not math.isfinite(expected_float) or not math.isclose(
                    expected_float, observed, rel_tol=2e-5, abs_tol=2e-6
                ):
                    _fail(
                        "accessor_bounds_mismatch",
                        f"accessor {resolved}.{label} does not match its data",
                        details={
                            "component": component_index,
                            "declared": expected_float,
                            "actual": observed,
                        },
                    )
        metadata = {
            "index": resolved,
            "count": count,
            "type": accessor_type,
            "component_type": component_type,
            "normalized": normalized,
            "min": actual_min,
            "max": actual_max,
        }
        self._accessor_metadata[resolved] = metadata
        if collect:
            self._collected_accessors[resolved] = values
        return values, metadata

    def validate_all_accessors(self) -> None:
        for accessor_index in range(len(self.accessors)):
            if accessor_index in self._accessor_metadata:
                continue
            self.read_accessor(
                accessor_index,
                field=f"accessor {accessor_index}",
                collect=False,
            )


def _identity_matrix() -> Tuple[float, ...]:
    return (
        1.0, 0.0, 0.0, 0.0,
        0.0, 1.0, 0.0, 0.0,
        0.0, 0.0, 1.0, 0.0,
        0.0, 0.0, 0.0, 1.0,
    )


def _matrix_multiply(left: Sequence[float], right: Sequence[float]) -> Tuple[float, ...]:
    return tuple(
        sum(left[row * 4 + inner] * right[inner * 4 + column] for inner in range(4))
        for row in range(4)
        for column in range(4)
    )


def _node_matrix(node: Mapping[str, Any], node_index: int) -> Tuple[float, ...]:
    if "matrix" in node:
        if any(field in node for field in ("translation", "rotation", "scale")):
            _fail(
                "invalid_node_transform",
                f"node {node_index} combines matrix with TRS fields",
            )
        values = _finite_vector(
            node.get("matrix"), 16, f"node {node_index}.matrix", default=_identity_matrix()
        )
        # glTF serialises matrices column-major; internal matrices are row-major.
        matrix = tuple(values[column * 4 + row] for row in range(4) for column in range(4))
    else:
        tx, ty, tz = _finite_vector(
            node.get("translation"), 3, f"node {node_index}.translation", default=(0, 0, 0)
        )
        sx, sy, sz = _finite_vector(
            node.get("scale"), 3, f"node {node_index}.scale", default=(1, 1, 1)
        )
        x, y, z, w = _finite_vector(
            node.get("rotation"), 4, f"node {node_index}.rotation", default=(0, 0, 0, 1)
        )
        norm_squared = x * x + y * y + z * z + w * w
        if norm_squared <= 1e-30:
            _fail("invalid_node_transform", f"node {node_index} has a zero quaternion")
        factor = 2.0 / norm_squared
        rotation = (
            1.0 - factor * (y * y + z * z), factor * (x * y - z * w), factor * (x * z + y * w), 0.0,
            factor * (x * y + z * w), 1.0 - factor * (x * x + z * z), factor * (y * z - x * w), 0.0,
            factor * (x * z - y * w), factor * (y * z + x * w), 1.0 - factor * (x * x + y * y), 0.0,
            0.0, 0.0, 0.0, 1.0,
        )
        scale = (
            sx, 0.0, 0.0, 0.0,
            0.0, sy, 0.0, 0.0,
            0.0, 0.0, sz, 0.0,
            0.0, 0.0, 0.0, 1.0,
        )
        translation = (
            1.0, 0.0, 0.0, tx,
            0.0, 1.0, 0.0, ty,
            0.0, 0.0, 1.0, tz,
            0.0, 0.0, 0.0, 1.0,
        )
        matrix = _matrix_multiply(_matrix_multiply(translation, rotation), scale)
    if any(not math.isfinite(value) for value in matrix):
        _fail("invalid_node_transform", f"node {node_index} matrix is non-finite")
    if not all(
        math.isclose(matrix[12 + column], expected, rel_tol=0.0, abs_tol=1e-9)
        for column, expected in enumerate((0.0, 0.0, 0.0, 1.0))
    ):
        _fail("unsupported_node_transform", f"node {node_index} matrix is projective")
    return matrix


def _transform_position(matrix: Sequence[float], position: Sequence[float]) -> Tuple[float, float, float]:
    x, y, z = position
    result = (
        matrix[0] * x + matrix[1] * y + matrix[2] * z + matrix[3],
        matrix[4] * x + matrix[5] * y + matrix[6] * z + matrix[7],
        matrix[8] * x + matrix[9] * y + matrix[10] * z + matrix[11],
    )
    if not all(math.isfinite(value) for value in result):
        _fail("nonfinite_position", "node transform produced a non-finite POSITION")
    return result


def _scene_instances(
    root: Mapping[str, Any], policy: GlbQualityPolicy
) -> Tuple[List[Tuple[int, int, Tuple[float, ...]]], Dict[str, int]]:
    scenes = _array(root, "scenes")
    nodes = _array(root, "nodes")
    meshes = _array(root, "meshes")
    if not scenes:
        _fail("no_active_scene", "GLB has no scene")
    if not nodes:
        _fail("no_active_scene", "GLB has no nodes")
    if len(nodes) > policy.max_scene_instances:
        _fail("glb_limit_exceeded", "GLB has too many nodes")
    scene_index = _index(root.get("scene", 0), len(scenes), "scene")

    parent_counts = [0] * len(nodes)
    for node_index, node in enumerate(nodes):
        if not isinstance(node, dict):
            _fail("invalid_node", f"node {node_index} is not an object")
        children = node.get("children", [])
        if not isinstance(children, list):
            _fail("invalid_node", f"node {node_index}.children must be an array")
        seen_children = set()
        for child in children:
            child_index = _index(child, len(nodes), f"node {node_index}.children")
            if child_index in seen_children:
                _fail("invalid_node", f"node {node_index} contains a duplicate child")
            seen_children.add(child_index)
            parent_counts[child_index] += 1
            if parent_counts[child_index] > 1:
                _fail("invalid_node_hierarchy", f"node {child_index} has multiple parents")
        if "mesh" in node:
            _index(node["mesh"], len(meshes), f"node {node_index}.mesh")
        _node_matrix(node, node_index)

    # Check all node hierarchies, including inactive ones, for cycles.  Use an
    # explicit stack so a hostile deep hierarchy cannot escape as RecursionError.
    colours = [0] * len(nodes)
    for start_index in range(len(nodes)):
        if colours[start_index] == 2:
            continue
        stack: List[Tuple[int, bool]] = [(start_index, False)]
        while stack:
            node_index, exiting = stack.pop()
            if exiting:
                colours[node_index] = 2
                continue
            if colours[node_index] == 1:
                _fail("node_cycle", f"node cycle detected at node {node_index}")
            if colours[node_index] == 2:
                continue
            colours[node_index] = 1
            stack.append((node_index, True))
            for child in reversed(nodes[node_index].get("children", [])):
                if colours[child] == 1:
                    _fail("node_cycle", f"node cycle detected at node {child}")
                if colours[child] == 0:
                    stack.append((child, False))

    scene = scenes[scene_index]
    if not isinstance(scene, dict):
        _fail("invalid_glb_schema", f"scene {scene_index} is not an object")
    roots = scene.get("nodes", [])
    if not isinstance(roots, list) or not roots:
        _fail("no_active_scene", f"scene {scene_index} has no root nodes")
    root_indices: List[int] = []
    for raw_root in roots:
        root_index = _index(raw_root, len(nodes), f"scene {scene_index}.nodes")
        if parent_counts[root_index]:
            _fail("invalid_node_hierarchy", f"scene root node {root_index} has a parent")
        if root_index in root_indices:
            _fail("invalid_node_hierarchy", f"scene {scene_index} repeats root node {root_index}")
        root_indices.append(root_index)

    instances: List[Tuple[int, int, Tuple[float, ...]]] = []
    reachable_nodes = set()

    walk_stack: List[Tuple[int, Tuple[float, ...]]] = [
        (root_index, _identity_matrix()) for root_index in reversed(root_indices)
    ]
    while walk_stack:
        node_index, parent = walk_stack.pop()
        if len(reachable_nodes) >= policy.max_scene_instances:
            _fail("glb_limit_exceeded", "active scene exceeds the traversal limit")
        reachable_nodes.add(node_index)
        node = nodes[node_index]
        world = _matrix_multiply(parent, _node_matrix(node, node_index))
        if "mesh" in node:
            instances.append((node_index, node["mesh"], world))
            if len(instances) > policy.max_scene_instances:
                _fail("glb_limit_exceeded", "active scene has too many mesh instances")
        for child in reversed(node.get("children", [])):
            walk_stack.append((child, world))
    if not instances:
        _fail("no_reachable_mesh", "active scene has no reachable mesh")
    return instances, {
        "active_scene": scene_index,
        "reachable_nodes": len(reachable_nodes),
        "unreachable_nodes": len(nodes) - len(reachable_nodes),
        "mesh_instances": len(instances),
    }


class _UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.size = [1] * size

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if self.size[left_root] < self.size[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        self.size[left_root] += self.size[right_root]


def _component_diagnostics(
    positions: Sequence[Tuple[float, float, float]],
    triangles: Sequence[Tuple[int, int, int]],
    extent: Sequence[float],
) -> Dict[str, Any]:
    used = {index for triangle in triangles for index in triangle}
    raw_union = _UnionFind(len(positions))
    for first, second, third in triangles:
        raw_union.union(first, second)
        raw_union.union(second, third)
    raw_sizes = sorted(
        Counter(raw_union.find(index) for index in used).values(), reverse=True
    )

    weld_tolerance = max(max(extent) * 1e-6, 1e-9)
    welded_lookup: Dict[Tuple[int, int, int], int] = {}
    welded_indices: List[int] = []
    for position in positions:
        key = tuple(int(round(value / weld_tolerance)) for value in position)
        welded_indices.append(welded_lookup.setdefault(key, len(welded_lookup)))
    welded_union = _UnionFind(len(welded_lookup))
    welded_used = set()
    for first, second, third in triangles:
        mapped = (welded_indices[first], welded_indices[second], welded_indices[third])
        welded_used.update(mapped)
        welded_union.union(mapped[0], mapped[1])
        welded_union.union(mapped[1], mapped[2])
    welded_sizes = sorted(
        Counter(welded_union.find(index) for index in welded_used).values(), reverse=True
    )
    welded_total = sum(welded_sizes)
    return {
        "component_count": len(raw_sizes),
        "largest_component_vertex_fraction": (
            raw_sizes[0] / len(used) if raw_sizes and used else 0.0
        ),
        "largest_components_vertices": raw_sizes[:20],
        "weld_tolerance": weld_tolerance,
        "welded_component_count": len(welded_sizes),
        "welded_largest_component_vertex_fraction": (
            welded_sizes[0] / welded_total if welded_sizes and welded_total else 0.0
        ),
        "welded_largest_components_vertices": welded_sizes[:20],
    }


def _triangles_from_indices(indices: Sequence[int], mode: int, field: str) -> List[Tuple[int, int, int]]:
    if mode == 4:
        if len(indices) % 3:
            _fail("invalid_triangle_indices", f"{field} triangle index count is not divisible by three")
        return [
            (indices[offset], indices[offset + 1], indices[offset + 2])
            for offset in range(0, len(indices), 3)
        ]
    if mode == 5:
        if len(indices) < 3:
            _fail("invalid_triangle_indices", f"{field} triangle strip is too short")
        return [
            ((indices[offset + 1], indices[offset], indices[offset + 2]) if offset % 2 else
             (indices[offset], indices[offset + 1], indices[offset + 2]))
            for offset in range(len(indices) - 2)
        ]
    if mode == 6:
        if len(indices) < 3:
            _fail("invalid_triangle_indices", f"{field} triangle fan is too short")
        return [
            (indices[0], indices[offset], indices[offset + 1])
            for offset in range(1, len(indices) - 1)
        ]
    _fail("unsupported_primitive_mode", f"{field} uses non-triangle primitive mode {mode}")


def _triangle_area(
    first: Sequence[float], second: Sequence[float], third: Sequence[float]
) -> float:
    ab = (second[0] - first[0], second[1] - first[1], second[2] - first[2])
    ac = (third[0] - first[0], third[1] - first[1], third[2] - first[2])
    cross = (
        ab[1] * ac[2] - ab[2] * ac[1],
        ab[2] * ac[0] - ab[0] * ac[2],
        ab[0] * ac[1] - ab[1] * ac[0],
    )
    return 0.5 * math.sqrt(sum(value * value for value in cross))


def _validate_images(document: _GlbDocument) -> List[Dict[str, Any]]:
    root = document.root
    images = _array(root, "images")
    reports: List[Dict[str, Any]] = []
    for image_index, item in enumerate(images):
        if not isinstance(item, dict):
            _fail("invalid_embedded_image", f"image {image_index} is not an object")
        has_uri = "uri" in item
        has_view = "bufferView" in item
        if has_uri == has_view:
            _fail(
                "invalid_embedded_image",
                f"image {image_index} must have exactly one embedded source",
            )
        declared_mime = item.get("mimeType")
        if has_uri:
            uri = item["uri"]
            if not isinstance(uri, str):
                _fail("invalid_embedded_image", f"image {image_index}.uri is invalid")
            data_mime, content = _decode_data_uri(uri, f"image {image_index}")
            mime = str(declared_mime or data_mime).lower()
            if not data_mime.startswith("image/"):
                _fail("invalid_embedded_image", f"image {image_index} data URI is not an image")
            if declared_mime is not None and str(declared_mime).lower() != data_mime:
                _fail("image_mime_mismatch", f"image {image_index} MIME declarations disagree")
            source = "data_uri"
        else:
            if not isinstance(declared_mime, str) or not declared_mime.startswith("image/"):
                _fail("invalid_embedded_image", f"image {image_index} has no image MIME type")
            mime = declared_mime.lower()
            content = bytes(
                document.view_bytes(item["bufferView"], f"image {image_index}.bufferView")
            )
            source = "buffer_view"
        if not content:
            _fail("invalid_embedded_image", f"image {image_index} is empty")
        if len(content) > document.policy.max_image_bytes:
            _fail("glb_limit_exceeded", f"image {image_index} exceeds the byte limit")
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", Image.DecompressionBombWarning)
                with Image.open(BytesIO(content)) as opened:
                    image_format = str(opened.format or "").upper()
                    width, height = opened.size
                    if width <= 0 or height <= 0 or width * height > document.policy.max_image_pixels:
                        _fail("invalid_embedded_image", f"image {image_index} dimensions are unsafe")
                    opened.verify()
        except GlbQualityError:
            raise
        except Exception as exc:
            _fail("invalid_embedded_image", f"image {image_index} cannot be decoded: {exc}")
        decoded_mime = _IMAGE_FORMAT_MIME.get(image_format)
        if decoded_mime is None:
            _fail("unsupported_embedded_image", f"image {image_index} format {image_format!r} is unsupported")
        if mime != decoded_mime:
            _fail(
                "image_mime_mismatch",
                f"image {image_index} declares {mime} but decodes as {decoded_mime}",
            )
        reports.append(
            {
                "index": image_index,
                "source": source,
                "bytes": len(content),
                "mime_type": decoded_mime,
                "width": width,
                "height": height,
            }
        )

    textures = _array(root, "textures")
    for texture_index, texture in enumerate(textures):
        if not isinstance(texture, dict):
            _fail("invalid_texture", f"texture {texture_index} is not an object")
        source = texture.get("source")
        basisu = texture.get("extensions", {}).get("KHR_texture_basisu") if isinstance(texture.get("extensions"), dict) else None
        if source is None and isinstance(basisu, dict):
            source = basisu.get("source")
        _index(source, len(images), f"texture {texture_index}.source")
    return reports


def _validate_material_texture_references(root: Mapping[str, Any]) -> None:
    textures = _array(root, "textures")
    materials = _array(root, "materials")
    for material_index, material in enumerate(materials):
        if not isinstance(material, dict):
            _fail("invalid_material", f"material {material_index} is not an object")
        slots: List[Tuple[str, Any]] = []
        pbr = material.get("pbrMetallicRoughness")
        if pbr is not None:
            if not isinstance(pbr, dict):
                _fail("invalid_material", f"material {material_index}.pbrMetallicRoughness is invalid")
            slots.extend(
                (name, pbr.get(name))
                for name in ("baseColorTexture", "metallicRoughnessTexture")
                if name in pbr
            )
        slots.extend(
            (name, material.get(name))
            for name in ("normalTexture", "occlusionTexture", "emissiveTexture")
            if name in material
        )
        for name, slot in slots:
            if not isinstance(slot, dict):
                _fail("invalid_material", f"material {material_index}.{name} is invalid")
            _index(slot.get("index"), len(textures), f"material {material_index}.{name}.index")


def _silhouette_metrics(
    triangles: Sequence[
        Tuple[
            Tuple[float, float, float],
            Tuple[float, float, float],
            Tuple[float, float, float],
        ]
    ],
    bounds_min: Sequence[float],
    bounds_max: Sequence[float],
    size: int,
) -> Dict[str, Any]:
    if size < 32 or size > 2048:
        _fail("invalid_quality_policy", "silhouette_size must be in [32, 2048]")
    views: Dict[str, Dict[str, Any]] = {}
    for label, axes in (("front_xy", (0, 1)), ("side_zy", (2, 1)), ("top_xz", (0, 2))):
        lower = (bounds_min[axes[0]], bounds_min[axes[1]])
        upper = (bounds_max[axes[0]], bounds_max[axes[1]])
        span = (upper[0] - lower[0], upper[1] - lower[1])
        longest = max(span)
        if longest <= 0.0:
            occupied = 0
            pixel_bbox = None
            fill = 0.0
        else:
            scale = 0.9 * size / longest
            centre = ((lower[0] + upper[0]) * 0.5, (lower[1] + upper[1]) * 0.5)
            canvas = Image.new("L", (size, size), 0)
            draw = ImageDraw.Draw(canvas)
            for triangle in triangles:
                projected = [
                    (
                        (point[axes[0]] - centre[0]) * scale + size * 0.5,
                        size - ((point[axes[1]] - centre[1]) * scale + size * 0.5),
                    )
                    for point in triangle
                ]
                draw.polygon(projected, fill=255)
            histogram = canvas.histogram()
            occupied = int(histogram[255])
            raw_bbox = canvas.getbbox()
            if raw_bbox is None:
                pixel_bbox = None
                fill = 0.0
            else:
                left, upper_pixel, right, lower_pixel = raw_bbox
                bbox_pixels = max(1, (right - left) * (lower_pixel - upper_pixel))
                pixel_bbox = [left, upper_pixel, right - 1, lower_pixel - 1]
                fill = occupied / bbox_pixels
        views[label] = {
            "occupied_pixels": occupied,
            "occupied_fraction": occupied / float(size * size),
            "pixel_bbox": pixel_bbox,
            "fill_fraction_in_bbox": fill,
        }
    return {
        "resolution": [size, size],
        "views": views,
        "max_occupancy": max(item["occupied_fraction"] for item in views.values()),
        "max_bbox_fill": max(item["fill_fraction_in_bbox"] for item in views.values()),
    }


def _validate_policy(policy: GlbQualityPolicy) -> None:
    for field in ("require_embedded_image", "require_texture"):
        if not isinstance(getattr(policy, field), bool):
            _fail("invalid_quality_policy", f"{field} must be boolean")
    for field in (
        "max_file_bytes", "max_json_bytes", "max_accessor_elements",
        "max_total_components", "max_triangles", "max_scene_instances",
        "max_image_bytes", "max_image_pixels", "silhouette_size",
        "diagnostic_triangle_count_below",
    ):
        if not _is_int(getattr(policy, field)) or getattr(policy, field) <= 0:
            _fail("invalid_quality_policy", f"{field} must be a positive integer")
    for field in (
        "min_bbox_diagonal", "degenerate_area_scale", "max_degenerate_ratio",
        "semantic_normalized_area_below", "semantic_silhouette_occupancy_below",
        "semantic_silhouette_bbox_fill_below",
        "diagnostic_largest_welded_fraction_below",
    ):
        value = getattr(policy, field)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)) or value < 0:
            _fail("invalid_quality_policy", f"{field} must be a finite non-negative number")


def validate_glb_bytes(
    data: bytes,
    *,
    policy: GlbQualityPolicy = DEFAULT_POLICY,
) -> Dict[str, Any]:
    """Validate a complete GLB and return deterministic JSON-safe evidence.

    ``GlbQualityError`` is raised for every malformed, unsupported, or
    calibrated-corrupt result.  The function performs no file or database
    mutations.
    """

    if not isinstance(policy, GlbQualityPolicy):
        _fail("invalid_quality_policy", "policy must be GlbQualityPolicy")
    _validate_policy(policy)
    root, bin_payload, chunks = _parse_glb(data, policy)
    document = _GlbDocument(root, bin_payload, policy)
    images = _validate_images(document)
    if policy.require_embedded_image and not images:
        _fail("missing_embedded_image", "GLB has no embedded texture image")
    if policy.require_texture and not _array(root, "textures"):
        _fail("missing_texture", "GLB has no texture referencing an embedded image")
    _validate_material_texture_references(root)
    instances, scene_report = _scene_instances(root, policy)
    meshes = _array(root, "meshes")
    materials = _array(root, "materials")

    bounds_min = [math.inf, math.inf, math.inf]
    bounds_max = [-math.inf, -math.inf, -math.inf]
    world_triangles: List[
        Tuple[
            Tuple[float, float, float],
            Tuple[float, float, float],
            Tuple[float, float, float],
        ]
    ] = []
    primitive_reports: List[Dict[str, Any]] = []
    total_vertices = 0
    total_indices = 0
    total_triangles = 0
    total_surface_area = 0.0
    minimum_triangle_area = math.inf
    maximum_triangle_area = 0.0
    total_degenerate = 0
    total_components = 0
    total_welded_components = 0
    weighted_welded_fraction = 0.0

    for node_index, mesh_index, world in instances:
        mesh = meshes[mesh_index]
        if not isinstance(mesh, dict):
            _fail("invalid_mesh", f"mesh {mesh_index} is not an object")
        primitives = mesh.get("primitives")
        if not isinstance(primitives, list) or not primitives:
            _fail("invalid_mesh", f"mesh {mesh_index} has no primitives")
        for primitive_index, primitive in enumerate(primitives):
            field = f"mesh {mesh_index} primitive {primitive_index}"
            if not isinstance(primitive, dict):
                _fail("invalid_primitive", f"{field} is not an object")
            if "extensions" in primitive and isinstance(primitive["extensions"], dict):
                unsupported = sorted(
                    _UNSUPPORTED_GEOMETRY_EXTENSIONS.intersection(primitive["extensions"])
                )
                if unsupported:
                    _fail(
                        "unsupported_geometry_encoding",
                        f"{field} uses an unsupported geometry extension",
                        details={"extensions": unsupported},
                    )
            attributes = primitive.get("attributes")
            if not isinstance(attributes, dict) or "POSITION" not in attributes:
                _fail("missing_position", f"{field} has no POSITION accessor")
            positions_raw, position_meta = document.read_accessor(
                attributes["POSITION"], field=f"{field}.POSITION"
            )
            if position_meta["type"] != "VEC3" or position_meta["component_type"] != 5126 or position_meta["normalized"]:
                _fail("invalid_position", f"{field}.POSITION must be an unnormalised float VEC3")
            if len(positions_raw) < 3:
                _fail("invalid_position", f"{field}.POSITION has fewer than three vertices")
            for attribute_name, accessor_index in attributes.items():
                if not isinstance(attribute_name, str):
                    _fail("invalid_primitive", f"{field} has a non-string attribute name")
                _values, metadata = document.read_accessor(
                    accessor_index, field=f"{field}.{attribute_name}", collect=False
                )
                if metadata["count"] != len(positions_raw):
                    _fail("attribute_count_mismatch", f"{field}.{attribute_name} count differs from POSITION")
            targets = primitive.get("targets", [])
            if not isinstance(targets, list):
                _fail("invalid_primitive", f"{field}.targets must be an array")
            for target_index, target in enumerate(targets):
                if not isinstance(target, dict):
                    _fail("invalid_primitive", f"{field}.targets[{target_index}] is invalid")
                for attribute_name, accessor_index in target.items():
                    _values, metadata = document.read_accessor(
                        accessor_index,
                        field=f"{field}.targets[{target_index}].{attribute_name}",
                        collect=False,
                    )
                    if metadata["count"] != len(positions_raw):
                        _fail("attribute_count_mismatch", f"{field} morph target count differs from POSITION")

            positions = [_transform_position(world, item) for item in positions_raw]
            for position in positions:
                for axis in range(3):
                    bounds_min[axis] = min(bounds_min[axis], position[axis])
                    bounds_max[axis] = max(bounds_max[axis], position[axis])
            if "indices" in primitive:
                raw_indices, index_meta = document.read_accessor(
                    primitive["indices"], field=f"{field}.indices"
                )
                if index_meta["type"] != "SCALAR" or index_meta["component_type"] not in (5121, 5123, 5125) or index_meta["normalized"]:
                    _fail("invalid_indices", f"{field}.indices must be an unsigned unnormalised SCALAR")
                indices = [int(item[0]) for item in raw_indices]
            else:
                indices = list(range(len(positions)))
            for offset, vertex_index in enumerate(indices):
                if not 0 <= vertex_index < len(positions):
                    _fail(
                        "index_out_of_bounds",
                        f"{field}.indices references a missing POSITION",
                        details={"offset": offset, "index": vertex_index, "vertices": len(positions)},
                    )
            mode = primitive.get("mode", 4)
            if not _is_int(mode):
                _fail("invalid_primitive", f"{field}.mode must be an integer")
            triangles = _triangles_from_indices(indices, mode, field)
            if not triangles:
                _fail("empty_triangle_geometry", f"{field} has no triangles")
            total_triangles += len(triangles)
            if total_triangles > policy.max_triangles:
                _fail("glb_limit_exceeded", "GLB exceeds the triangle limit")
            extent = [
                max(position[axis] for position in positions) - min(position[axis] for position in positions)
                for axis in range(3)
            ]
            scale_squared = max(sum(value * value for value in extent), float.fromhex("0x0.0000000000001p-1022"))
            degenerate_limit = scale_squared * policy.degenerate_area_scale
            primitive_area = 0.0
            primitive_minimum_area = math.inf
            primitive_maximum_area = 0.0
            primitive_degenerate = 0
            for triangle in triangles:
                points = (positions[triangle[0]], positions[triangle[1]], positions[triangle[2]])
                area = _triangle_area(*points)
                if not math.isfinite(area):
                    _fail("nonfinite_triangle", f"{field} produced a non-finite triangle area")
                primitive_area += area
                primitive_minimum_area = min(primitive_minimum_area, area)
                primitive_maximum_area = max(primitive_maximum_area, area)
                minimum_triangle_area = min(minimum_triangle_area, area)
                maximum_triangle_area = max(maximum_triangle_area, area)
                if area * 2.0 <= degenerate_limit:
                    primitive_degenerate += 1
                world_triangles.append(points)
            components = _component_diagnostics(positions, triangles, extent)
            total_components += components["component_count"]
            total_welded_components += components["welded_component_count"]
            weighted_welded_fraction += len(positions) * components["welded_largest_component_vertex_fraction"]
            material = primitive.get("material")
            if material is not None:
                _index(material, len(materials), f"{field}.material")
            primitive_reports.append(
                {
                    "node": node_index,
                    "mesh": mesh_index,
                    "primitive": primitive_index,
                    "mode": mode,
                    "material": material,
                    "attributes": sorted(attributes),
                    "vertices": len(positions),
                    "indices": len(indices),
                    "triangles": len(triangles),
                    "surface_area": primitive_area,
                    "mean_triangle_area": primitive_area / len(triangles),
                    "min_triangle_area": primitive_minimum_area,
                    "max_triangle_area": primitive_maximum_area,
                    "degenerate_triangles": primitive_degenerate,
                    "degenerate_ratio": primitive_degenerate / len(triangles),
                    "bbox_extents": extent,
                    **components,
                }
            )
            total_vertices += len(positions)
            total_indices += len(indices)
            total_surface_area += primitive_area
            total_degenerate += primitive_degenerate

    document.validate_all_accessors()
    if not world_triangles or total_triangles == 0:
        _fail("empty_triangle_geometry", "active scene has no triangle geometry")
    extents = [bounds_max[axis] - bounds_min[axis] for axis in range(3)]
    diagonal_squared = sum(value * value for value in extents)
    diagonal = math.sqrt(diagonal_squared)
    if not math.isfinite(diagonal) or diagonal <= policy.min_bbox_diagonal:
        _fail(
            "degenerate_bbox",
            "active scene has a zero or non-finite bounding box",
            details={"extents": extents},
        )
    normalized_surface_area = total_surface_area / diagonal_squared
    degenerate_ratio = total_degenerate / total_triangles
    if degenerate_ratio > policy.max_degenerate_ratio:
        _fail(
            "excessive_degenerate_triangles",
            "GLB contains too many zero-area triangles",
            details={
                "degenerate_triangles": total_degenerate,
                "triangles": total_triangles,
                "ratio": degenerate_ratio,
                "limit": policy.max_degenerate_ratio,
            },
        )
    silhouette = _silhouette_metrics(
        world_triangles, bounds_min, bounds_max, policy.silhouette_size
    )
    largest_welded_fraction = weighted_welded_fraction / total_vertices
    warnings_list: List[Dict[str, Any]] = []
    if normalized_surface_area < policy.semantic_normalized_area_below:
        warnings_list.append(
            {
                "code": "low_normalized_surface_area",
                "value": normalized_surface_area,
                "threshold": policy.semantic_normalized_area_below,
            }
        )
    if total_triangles < policy.diagnostic_triangle_count_below:
        warnings_list.append(
            {
                "code": "low_triangle_count",
                "value": total_triangles,
                "threshold": policy.diagnostic_triangle_count_below,
            }
        )
    if largest_welded_fraction < policy.diagnostic_largest_welded_fraction_below:
        warnings_list.append(
            {
                "code": "fragmented_welded_components",
                "value": largest_welded_fraction,
                "threshold": policy.diagnostic_largest_welded_fraction_below,
            }
        )

    report: Dict[str, Any] = {
        "schema": GLB_QUALITY_SCHEMA,
        "ok": True,
        "sha256": hashlib.sha256(data).hexdigest(),
        "file_bytes": len(data),
        "glb_version": 2,
        "chunks": chunks,
        "counts": {
            field: len(_array(root, field))
            for field in (
                "scenes", "nodes", "meshes", "accessors", "bufferViews",
                "buffers", "materials", "textures", "images", "skins", "animations",
            )
        },
        "scene": scene_report,
        "resources": {
            "all_embedded": True,
            "images": images,
        },
        "geometry": {
            "vertices_total_instances": total_vertices,
            "indices_total_instances": total_indices,
            "triangles_total_instances": total_triangles,
            "surface_area": total_surface_area,
            "normalized_surface_area": normalized_surface_area,
            "mean_triangle_area": total_surface_area / total_triangles,
            "min_triangle_area": minimum_triangle_area,
            "max_triangle_area": maximum_triangle_area,
            "degenerate_triangles": total_degenerate,
            "degenerate_ratio": degenerate_ratio,
            "bbox_min": bounds_min,
            "bbox_max": bounds_max,
            "bbox_extents": extents,
            "bbox_diagonal": diagonal,
            "bbox_diagonal_squared": diagonal_squared,
            "bbox_volume": extents[0] * extents[1] * extents[2],
            "primitive_count": len(primitive_reports),
            "component_count_sum": total_components,
            "welded_component_count_sum": total_welded_components,
            "weighted_welded_largest_component_fraction": largest_welded_fraction,
        },
        "silhouette": silhouette,
        "primitives": primitive_reports,
        "warnings": warnings_list,
        "policy": asdict(policy),
    }
    silhouette_is_sparse = (
        silhouette["max_occupancy"] < policy.semantic_silhouette_occupancy_below
        or silhouette["max_bbox_fill"] < policy.semantic_silhouette_bbox_fill_below
    )
    if (
        normalized_surface_area < policy.semantic_normalized_area_below
        and silhouette_is_sparse
    ):
        report["ok"] = False
        _fail(
            "semantic_geometry_corrupt",
            "GLB geometry is sparse and fragmented rather than a usable character surface",
            details={"report": report},
        )
    return report


def validate_glb_path(
    path: Path | str,
    *,
    policy: GlbQualityPolicy = DEFAULT_POLICY,
) -> Dict[str, Any]:
    """Read and validate a GLB path without modifying the artifact."""

    if not isinstance(policy, GlbQualityPolicy):
        _fail("invalid_quality_policy", "policy must be GlbQualityPolicy")
    _validate_policy(policy)
    resolved = Path(path)
    try:
        file_size = resolved.stat().st_size
    except OSError as exc:
        _fail("glb_read_failed", f"cannot stat GLB {resolved}: {exc}")
    if file_size > policy.max_file_bytes:
        _fail(
            "glb_limit_exceeded",
            "GLB exceeds the configured byte limit",
            details={"bytes": file_size, "limit": policy.max_file_bytes},
        )
    try:
        data = resolved.read_bytes()
    except OSError as exc:
        _fail("glb_read_failed", f"cannot read GLB {resolved}: {exc}")
    report = validate_glb_bytes(data, policy=policy)
    report["path"] = str(resolved)
    return report


__all__ = [
    "DEFAULT_POLICY",
    "GLB_QUALITY_SCHEMA",
    "HUNYUAN_STANDARD_PBR_POLICY",
    "GlbQualityError",
    "GlbQualityPolicy",
    "GlbValidationError",
    "validate_glb_bytes",
    "validate_glb_path",
]
