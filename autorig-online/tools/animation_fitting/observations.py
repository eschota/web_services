from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

import numpy as np

from .errors import ContractError, DependencyUnavailableError


OBSERVATIONS_SCHEMA = "autorig-fitting-observations.v1"
ANCHOR_MAP_SCHEMA = "autorig-tracker-anchor-map.v1"


@dataclass(frozen=True)
class TrackPoint:
    frame: int
    xy: np.ndarray
    visible: bool
    confidence: Optional[float] = None


@dataclass(frozen=True)
class PointTrack:
    id: str
    anchor_id: str
    query_frame: int
    points: tuple[TrackPoint, ...]


@dataclass(frozen=True)
class SilhouetteObservation:
    frame: int
    path: Path
    foreground: np.ndarray
    outside_distance: np.ndarray


@dataclass(frozen=True)
class DepthObservation:
    frame: int
    path: Path
    camera_depth: np.ndarray
    valid: np.ndarray
    mode: str
    scale: float
    offset: float


@dataclass(frozen=True)
class ContactObservation:
    anchor_id: str
    frames: tuple[int, ...]
    ground_height: Optional[float]
    weight: float


@dataclass(frozen=True)
class ObservationSet:
    path: Path
    sha256: str
    frame_count: int
    width: int
    height: int
    fps: float
    tracks: tuple[PointTrack, ...]
    silhouettes: Dict[int, SilhouetteObservation]
    depths: Dict[int, DepthObservation]
    contacts: tuple[ContactObservation, ...]
    provenance: dict

    @property
    def anchor_ids(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(track.anchor_id for track in self.tracks))

    @property
    def track_by_anchor(self) -> Dict[str, PointTrack]:
        result: Dict[str, PointTrack] = {}
        for track in self.tracks:
            if track.anchor_id in result:
                raise ContractError(
                    f"More than one point track maps to anchor {track.anchor_id!r}; "
                    "merge or select the track explicitly before fitting"
                )
            result[track.anchor_id] = track
        return result


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContractError(f"Invalid JSON {path}: {exc}") from exc


def _finite_number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{field} must be a finite number") from exc
    if not np.isfinite(number):
        raise ContractError(f"{field} must be a finite number")
    return number


def _resolve_artifact(root: Path, raw_path: Any, field: str) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise ContractError(f"{field} must be a non-empty path")
    resolved_root = root.resolve()
    path = (resolved_root / raw_path).resolve()
    try:
        path.relative_to(resolved_root)
    except ValueError as exc:
        raise ContractError(f"{field} escapes the observation root: {raw_path}") from exc
    if not path.is_file():
        raise ContractError(f"{field} does not exist: {path}")
    return path


def _load_grayscale(path: Path) -> np.ndarray:
    try:
        from PIL import Image
    except ImportError as exc:
        raise DependencyUnavailableError(
            "Pillow is required to read mask/depth images. Install it with: python -m pip install Pillow"
        ) from exc
    try:
        with Image.open(path) as image:
            return np.asarray(image)
    except Exception as exc:
        raise ContractError(f"Cannot read image observation {path}: {exc}") from exc


def _load_silhouette(path: Path, frame: int, width: int, height: int) -> SilhouetteObservation:
    array = _load_grayscale(path)
    if array.ndim == 3:
        array = np.any(array != 0, axis=2)
    elif array.ndim == 2:
        array = array != 0
    else:
        raise ContractError(f"Silhouette {path} must be a 2D image")
    if array.shape != (height, width):
        raise ContractError(
            f"Silhouette {path} has {array.shape[1]}x{array.shape[0]}, expected {width}x{height}"
        )
    try:
        from scipy.ndimage import distance_transform_edt
    except ImportError as exc:
        raise DependencyUnavailableError(
            "SciPy is required for silhouette distance fields. Install it with: python -m pip install scipy"
        ) from exc
    foreground = np.asarray(array, dtype=bool)
    outside_distance = distance_transform_edt(~foreground).astype(np.float64, copy=False)
    return SilhouetteObservation(
        frame=frame,
        path=path,
        foreground=foreground,
        outside_distance=outside_distance,
    )


def _load_depth(
    root: Path,
    raw: dict,
    frame: int,
    width: int,
    height: int,
) -> DepthObservation:
    path = _resolve_artifact(root, raw.get("path"), f"depth[{frame}].path")
    if path.suffix.lower() == ".npy":
        try:
            array = np.load(path, allow_pickle=False)
        except Exception as exc:
            raise ContractError(f"Cannot read depth array {path}: {exc}") from exc
    else:
        array = _load_grayscale(path)
    if array.ndim != 2 or array.shape != (height, width):
        raise ContractError(f"Depth {path} must be a {height}x{width} scalar image/array")
    array = np.asarray(array, dtype=np.float64)
    mode = str(raw.get("mode") or "")
    if mode == "camera_z":
        if "scale" in raw or "offset" in raw:
            raise ContractError("camera_z depth must not declare scale/offset")
        scale, offset = 1.0, 0.0
    elif mode == "affine_to_camera_z":
        if "scale" not in raw or "offset" not in raw:
            raise ContractError("affine_to_camera_z depth requires explicit scale and offset")
        scale = _finite_number(raw.get("scale"), f"depth[{frame}].scale")
        offset = _finite_number(raw.get("offset"), f"depth[{frame}].offset")
        if scale == 0.0:
            raise ContractError("affine_to_camera_z depth scale must be non-zero")
    else:
        raise ContractError(
            f"depth[{frame}].mode must be 'camera_z' or 'affine_to_camera_z'; "
            "relative depth is not metrically usable without an explicit calibration"
        )
    valid = np.isfinite(array)
    if "invalid_value" in raw:
        invalid_value = _finite_number(raw.get("invalid_value"), f"depth[{frame}].invalid_value")
        valid &= array != invalid_value
    camera_depth = array * scale + offset
    valid &= np.isfinite(camera_depth) & (camera_depth > 0.0)
    if not np.any(valid):
        raise ContractError(f"Depth observation {path} contains no valid positive camera-space depth")
    return DepthObservation(
        frame=frame,
        path=path,
        camera_depth=camera_depth,
        valid=valid,
        mode=mode,
        scale=scale,
        offset=offset,
    )


def _frame_number(value: Any, frame_count: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{field} must be an integer")
    if value < 0 or value >= frame_count:
        raise ContractError(f"{field}={value} is outside [0, {frame_count - 1}]")
    return value


def load_observations(path: str | Path) -> ObservationSet:
    source = Path(path).resolve()
    payload = _read_json(source)
    if not isinstance(payload, dict) or payload.get("schema") != OBSERVATIONS_SCHEMA:
        raise ContractError(f"Unsupported observation schema; expected {OBSERVATIONS_SCHEMA}")
    try:
        frame_count = int(payload.get("frame_count"))
        width = int(payload.get("width"))
        height = int(payload.get("height"))
    except (TypeError, ValueError) as exc:
        raise ContractError("frame_count, width and height must be integers") from exc
    fps = _finite_number(payload.get("fps"), "fps")
    if frame_count <= 0 or width <= 0 or height <= 0 or fps <= 0:
        raise ContractError("frame_count, width, height and fps must be positive")

    raw_tracks = payload.get("tracks")
    if not isinstance(raw_tracks, list) or not raw_tracks:
        raise ContractError("Observation set must contain at least one point track")
    tracks: list[PointTrack] = []
    track_ids: set[str] = set()
    for track_index, raw_track in enumerate(raw_tracks):
        if not isinstance(raw_track, dict):
            raise ContractError(f"tracks[{track_index}] must be an object")
        track_id = raw_track.get("id")
        anchor_id = raw_track.get("anchor_id")
        if not isinstance(track_id, str) or not track_id or track_id in track_ids:
            raise ContractError("Track IDs must be non-empty and unique")
        if not isinstance(anchor_id, str) or not anchor_id:
            raise ContractError(f"Track {track_id} requires anchor_id")
        track_ids.add(track_id)
        query_frame = _frame_number(
            raw_track.get("query_frame", 0), frame_count, f"track {track_id}.query_frame"
        )
        raw_points = raw_track.get("points")
        if not isinstance(raw_points, list) or not raw_points:
            raise ContractError(f"Track {track_id} has no points")
        points: list[TrackPoint] = []
        seen_frames: set[int] = set()
        for point_index, raw_point in enumerate(raw_points):
            if not isinstance(raw_point, dict):
                raise ContractError(f"Track {track_id} point {point_index} must be an object")
            frame = _frame_number(raw_point.get("frame"), frame_count, f"track {track_id}.frame")
            if frame in seen_frames:
                raise ContractError(f"Track {track_id} has duplicate frame {frame}")
            seen_frames.add(frame)
            xy = np.asarray((raw_point.get("x"), raw_point.get("y")), dtype=np.float64)
            if xy.shape != (2,) or not np.all(np.isfinite(xy)):
                raise ContractError(f"Track {track_id} frame {frame} has non-finite coordinates")
            visible = raw_point.get("visible")
            if not isinstance(visible, bool):
                raise ContractError(f"Track {track_id} frame {frame}.visible must be boolean")
            confidence: Optional[float] = None
            if "confidence" in raw_point:
                confidence = _finite_number(
                    raw_point.get("confidence"), f"track {track_id} frame {frame}.confidence"
                )
                if confidence < 0.0:
                    raise ContractError("Track confidence must be non-negative")
            points.append(TrackPoint(frame=frame, xy=xy, visible=visible, confidence=confidence))
        tracks.append(
            PointTrack(
                id=track_id,
                anchor_id=anchor_id,
                query_frame=query_frame,
                points=tuple(sorted(points, key=lambda point: point.frame)),
            )
        )

    silhouettes: Dict[int, SilhouetteObservation] = {}
    for index, raw in enumerate(payload.get("silhouettes") or []):
        if not isinstance(raw, dict):
            raise ContractError(f"silhouettes[{index}] must be an object")
        frame = _frame_number(raw.get("frame"), frame_count, f"silhouettes[{index}].frame")
        if frame in silhouettes:
            raise ContractError(f"Duplicate silhouette frame {frame}")
        artifact = _resolve_artifact(source.parent, raw.get("path"), f"silhouettes[{index}].path")
        silhouettes[frame] = _load_silhouette(artifact, frame, width, height)

    depths: Dict[int, DepthObservation] = {}
    for index, raw in enumerate(payload.get("depth") or []):
        if not isinstance(raw, dict):
            raise ContractError(f"depth[{index}] must be an object")
        frame = _frame_number(raw.get("frame"), frame_count, f"depth[{index}].frame")
        if frame in depths:
            raise ContractError(f"Duplicate depth frame {frame}")
        depths[frame] = _load_depth(source.parent, raw, frame, width, height)

    contacts: list[ContactObservation] = []
    for index, raw in enumerate(payload.get("contacts") or []):
        if not isinstance(raw, dict):
            raise ContractError(f"contacts[{index}] must be an object")
        anchor_id = raw.get("anchor_id")
        frames = raw.get("frames")
        if not isinstance(anchor_id, str) or not anchor_id:
            raise ContractError(f"contacts[{index}].anchor_id is required")
        if not isinstance(frames, list) or not frames:
            raise ContractError(f"contacts[{index}].frames must be non-empty")
        parsed_frames = tuple(
            sorted({_frame_number(frame, frame_count, f"contacts[{index}].frames") for frame in frames})
        )
        ground_height = None
        if "ground_height" in raw:
            ground_height = _finite_number(raw.get("ground_height"), f"contacts[{index}].ground_height")
        weight = _finite_number(raw.get("weight", 1.0), f"contacts[{index}].weight")
        if weight <= 0.0:
            raise ContractError("Contact weight must be positive")
        contacts.append(
            ContactObservation(
                anchor_id=anchor_id,
                frames=parsed_frames,
                ground_height=ground_height,
                weight=weight,
            )
        )

    provenance = payload.get("provenance") or {}
    if not isinstance(provenance, dict):
        raise ContractError("provenance must be an object")
    return ObservationSet(
        path=source,
        sha256=_sha256(source),
        frame_count=frame_count,
        width=width,
        height=height,
        fps=fps,
        tracks=tuple(tracks),
        silhouettes=silhouettes,
        depths=depths,
        contacts=tuple(contacts),
        provenance=provenance,
    )


def _load_anchor_map(path: str | Path) -> Dict[str, str]:
    source = Path(path).resolve()
    payload = _read_json(source)
    if not isinstance(payload, dict) or payload.get("schema") != ANCHOR_MAP_SCHEMA:
        raise ContractError(f"Unsupported anchor map schema; expected {ANCHOR_MAP_SCHEMA}")
    rows = payload.get("tracks")
    if not isinstance(rows, list) or not rows:
        raise ContractError("Anchor map must contain tracks")
    result: Dict[str, str] = {}
    used_anchors: set[str] = set()
    for row in rows:
        track_id = row.get("track_id") if isinstance(row, dict) else None
        anchor_id = row.get("anchor_id") if isinstance(row, dict) else None
        if not isinstance(track_id, (str, int)) or not str(track_id):
            raise ContractError("Anchor map track_id must be a non-empty string/integer")
        if not isinstance(anchor_id, str) or not anchor_id:
            raise ContractError("Anchor map anchor_id must be a non-empty string")
        key = str(track_id)
        if key in result:
            raise ContractError(f"Duplicate anchor-map track_id: {key}")
        if anchor_id in used_anchors:
            raise ContractError(f"Anchor map assigns more than one track to {anchor_id}")
        result[key] = anchor_id
        used_anchors.add(anchor_id)
    return result


TrackerAdapter = Callable[..., dict]
_TRACKER_ADAPTERS: Dict[str, TrackerAdapter] = {}


def register_tracker_adapter(name: str, adapter: TrackerAdapter) -> None:
    if not name or name in _TRACKER_ADAPTERS:
        raise ValueError(f"Tracker adapter is already registered or invalid: {name!r}")
    _TRACKER_ADAPTERS[name] = adapter


def _layout_tn(array: Any, layout: str, field: str, trailing: tuple[int, ...]) -> np.ndarray:
    result = np.asarray(array)
    if result.ndim != 2 + len(trailing) or tuple(result.shape[2:]) != trailing:
        if trailing:
            raise ContractError(f"{field} must have shape T,N,{','.join(map(str, trailing))} or N,T,...")
        raise ContractError(f"{field} must have shape T,N or N,T")
    if layout == "T,N,2" or layout == "T,N":
        return result
    if layout == "N,T,2" or layout == "N,T":
        return np.swapaxes(result, 0, 1)
    raise ContractError(f"Unsupported explicit layout {layout!r}")


def _visibility_bool(array: np.ndarray, *, semantics: str, threshold: Optional[float], field: str) -> np.ndarray:
    if array.dtype == np.bool_:
        return array if semantics == "visible" else ~array
    if threshold is None:
        raise ContractError(
            f"{field} is numeric. Supply --visibility-threshold explicitly; no tracker threshold is guessed."
        )
    numeric = np.asarray(array, dtype=np.float64)
    if not np.all(np.isfinite(numeric)):
        raise ContractError(f"{field} contains non-finite values")
    return numeric >= threshold if semantics == "visible" else numeric < threshold


def _track_ids(payload: dict, count: int) -> list[str]:
    ids = payload.get("track_ids")
    if ids is None:
        return [str(index) for index in range(count)]
    if not isinstance(ids, list) or len(ids) != count:
        raise ContractError(f"track_ids must contain exactly {count} entries")
    result = [str(value) for value in ids]
    if any(not value for value in result) or len(set(result)) != len(result):
        raise ContractError("track_ids must be non-empty and unique")
    return result


def _canonical_from_arrays(
    *,
    source: Path,
    adapter: str,
    points: np.ndarray,
    visible: np.ndarray,
    confidence: Optional[np.ndarray],
    ids: list[str],
    anchor_map: Dict[str, str],
    query_frames: Optional[Iterable[Any]],
    width: int,
    height: int,
    fps: float,
    threshold: Optional[float],
    layout: str,
) -> dict:
    frame_count, track_count, coordinate_count = points.shape
    if coordinate_count != 2 or visible.shape != (frame_count, track_count):
        raise ContractError("Tracker points and visibility dimensions do not match")
    points = np.asarray(points, dtype=np.float64)
    if not np.all(np.isfinite(points)):
        raise ContractError("Tracker coordinates contain non-finite values")
    if confidence is not None:
        confidence = np.asarray(confidence, dtype=np.float64)
        if confidence.shape != (frame_count, track_count) or not np.all(np.isfinite(confidence)):
            raise ContractError("Tracker confidence dimensions/values are invalid")
        if np.any(confidence < 0.0):
            raise ContractError("Tracker confidence values must be non-negative")
    if set(ids) != set(anchor_map):
        missing = sorted(set(ids).difference(anchor_map))
        extra = sorted(set(anchor_map).difference(ids))
        raise ContractError(f"Anchor map must match tracker IDs exactly; missing={missing}, extra={extra}")
    if query_frames is None:
        queries = [0] * track_count
    else:
        queries = list(query_frames)
        if len(queries) != track_count:
            raise ContractError("query_frames length does not match track count")
        if any(isinstance(value, bool) or not isinstance(value, int) for value in queries):
            raise ContractError("query_frames must contain integers")
        if any(value < 0 or value >= frame_count for value in queries):
            raise ContractError("query_frames contain an out-of-range frame")
    tracks = []
    for track_index, track_id in enumerate(ids):
        canonical_points = []
        for frame in range(frame_count):
            point: dict[str, Any] = {
                "frame": frame,
                "x": float(points[frame, track_index, 0]),
                "y": float(points[frame, track_index, 1]),
                "visible": bool(visible[frame, track_index]),
            }
            if confidence is not None:
                point["confidence"] = float(confidence[frame, track_index])
            canonical_points.append(point)
        tracks.append(
            {
                "id": track_id,
                "anchor_id": anchor_map[track_id],
                "query_frame": int(queries[track_index]),
                "points": canonical_points,
            }
        )
    provenance: dict[str, Any] = {
        "adapter": adapter,
        "source": str(source),
        "source_sha256": _sha256(source),
        "input_layout": layout,
    }
    if threshold is not None:
        provenance["visibility_threshold"] = float(threshold)
    return {
        "schema": OBSERVATIONS_SCHEMA,
        "frame_count": frame_count,
        "width": width,
        "height": height,
        "fps": fps,
        "tracks": tracks,
        "silhouettes": [],
        "depth": [],
        "contacts": [],
        "provenance": provenance,
    }


def _cotracker_adapter(
    payload: dict,
    *,
    source: Path,
    anchor_map: Dict[str, str],
    layout: str,
    width: int,
    height: int,
    fps: float,
    visibility_threshold: Optional[float],
) -> dict:
    points = _layout_tn(payload.get("tracks"), layout, "tracks", (2,))
    visibility_layout = layout.replace(",2", "")
    visible_raw = _layout_tn(payload.get("visibility"), visibility_layout, "visibility", ())
    visible = _visibility_bool(
        visible_raw, semantics="visible", threshold=visibility_threshold, field="visibility"
    )
    confidence = None
    if payload.get("confidence") is not None:
        confidence = _layout_tn(payload.get("confidence"), visibility_layout, "confidence", ())
    ids = _track_ids(payload, points.shape[1])
    return _canonical_from_arrays(
        source=source,
        adapter="cotracker-json-v1",
        points=points,
        visible=visible,
        confidence=confidence,
        ids=ids,
        anchor_map=anchor_map,
        query_frames=payload.get("query_frames"),
        width=width,
        height=height,
        fps=fps,
        threshold=visibility_threshold,
        layout=layout,
    )


def _tap_adapter(
    payload: dict,
    *,
    source: Path,
    anchor_map: Dict[str, str],
    layout: str,
    width: int,
    height: int,
    fps: float,
    visibility_threshold: Optional[float],
) -> dict:
    points = _layout_tn(payload.get("tracks"), layout, "tracks", (2,))
    visibility_layout = layout.replace(",2", "")
    occluded_raw = _layout_tn(payload.get("occluded"), visibility_layout, "occluded", ())
    visible = _visibility_bool(
        occluded_raw, semantics="occluded", threshold=visibility_threshold, field="occluded"
    )
    confidence = None
    if payload.get("confidence") is not None:
        confidence = _layout_tn(payload.get("confidence"), visibility_layout, "confidence", ())
    ids = _track_ids(payload, points.shape[1])
    return _canonical_from_arrays(
        source=source,
        adapter="tap-json-v1",
        points=points,
        visible=visible,
        confidence=confidence,
        ids=ids,
        anchor_map=anchor_map,
        query_frames=payload.get("query_frames"),
        width=width,
        height=height,
        fps=fps,
        threshold=visibility_threshold,
        layout=layout,
    )


register_tracker_adapter("cotracker", _cotracker_adapter)
register_tracker_adapter("tap", _tap_adapter)


def adapt_tracker_json(
    input_path: str | Path,
    *,
    adapter: str,
    anchor_map_path: str | Path,
    output_path: str | Path,
    layout: str,
    width: int,
    height: int,
    fps: float,
    visibility_threshold: Optional[float] = None,
) -> Path:
    source = Path(input_path).resolve()
    payload = _read_json(source)
    if not isinstance(payload, dict):
        raise ContractError("Tracker export must be a JSON object")
    implementation = _TRACKER_ADAPTERS.get(adapter)
    if implementation is None:
        raise ContractError(
            f"Unknown tracker adapter {adapter!r}; available: {', '.join(sorted(_TRACKER_ADAPTERS))}"
        )
    if width <= 0 or height <= 0 or not np.isfinite(fps) or fps <= 0:
        raise ContractError("width, height and fps must be positive")
    if visibility_threshold is not None and not np.isfinite(visibility_threshold):
        raise ContractError("visibility_threshold must be finite")
    canonical = implementation(
        payload,
        source=source,
        anchor_map=_load_anchor_map(anchor_map_path),
        layout=layout,
        width=int(width),
        height=int(height),
        fps=float(fps),
        visibility_threshold=visibility_threshold,
    )
    destination = Path(output_path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(canonical, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    load_observations(destination)
    return destination
