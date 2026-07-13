from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import numpy as np
from PIL import Image
import pytest
from jsonschema import Draft202012Validator

from animation_fitting.cli import main as fitting_cli_main
from animation_fitting.errors import ContractError, OptimizationError
from animation_fitting.observations import adapt_tracker_json, load_observations
from animation_fitting.optimizer import FittingConfig, fit_sequence
from animation_fitting.rig import load_rig_bundle


def _flat(matrix: np.ndarray) -> list[float]:
    return [float(value) for value in matrix.reshape(-1)]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _artifact_record(path: Path) -> dict:
    return {"filename": path.name, "bytes": path.stat().st_size, "sha256": _sha(path)}


def _repin_bundle(bundle: Path) -> None:
    metadata_path = bundle / "fitting_bundle.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    for record in metadata["artifacts"].values():
        artifact = bundle / record["filename"]
        record.update(_artifact_record(artifact))
    _write_json(metadata_path, metadata)
    filenames = [metadata_path.name] + sorted(
        {record["filename"] for record in metadata["artifacts"].values()}
    )
    _write_json(
        bundle / "immutable_manifest.json",
        {
            "schema": "autorig-fitting-immutable-bundle.v1",
            "files": [_artifact_record(bundle / filename) for filename in filenames],
        },
    )


def _replace_vertex_weights(bundle: Path, vertex_id: int, weights: list[dict]) -> None:
    skin_path = bundle / "skin_weights.json.gz"
    with gzip.open(skin_path, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    row = next(row for row in payload["vertices"] if row["vertex_id"] == vertex_id)
    row["weights"] = weights
    with gzip.open(skin_path, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream)
    _repin_bundle(bundle)


def _replace_anchor_weight(bundle: Path, anchor_id: str, weight: float) -> None:
    anchors_path = bundle / "surface_anchors.json"
    payload = json.loads(anchors_path.read_text(encoding="utf-8"))
    point = next(
        point
        for group in payload["bones"]
        for point in group["points"]
        if point["id"] == anchor_id
    )
    point["weight"] = weight
    _write_json(anchors_path, payload)
    _repin_bundle(bundle)


def _build_synthetic_horse_bundle(root: Path) -> Path:
    bundle = root / "horse_bundle"
    bundle.mkdir()
    identity = np.eye(4)
    body_local = np.eye(4)
    body_local[:3, 3] = (0.0, 0.0, 1.0)
    leg_local = np.eye(4)
    leg_local[:3, 3] = (0.6, 0.0, -0.2)
    skeleton = {
        "armatures": [
            {
                "name": "SyntheticHorse",
                "matrix_world": _flat(identity),
                "bones": [
                    {
                        "name": "HorseBody",
                        "parent": None,
                        "use_deform": True,
                        "helper": False,
                        "length": 1.2,
                        "parent_relative_matrix": _flat(body_local),
                        "joint_limits": [],
                    },
                    {
                        "name": "HorseFrontLeg",
                        "parent": "HorseBody",
                        "use_deform": True,
                        "helper": False,
                        "length": 0.8,
                        "parent_relative_matrix": _flat(leg_local),
                        "joint_limits": [
                            {
                                "type": "LIMIT_ROTATION",
                                "space": "LOCAL",
                                "use_limit_x": False,
                                "use_limit_y": True,
                                "use_limit_z": False,
                                "min": [-0.0, -0.8, -0.0],
                                "max": [0.0, 0.8, 0.0],
                            }
                        ],
                    },
                ],
            }
        ]
    }
    skeleton_path = bundle / "skeleton.json"
    _write_json(skeleton_path, skeleton)

    world_points = {
        "body_front": (0.65, 0.0, 1.25),
        "body_back": (-0.65, 0.0, 1.2),
        "body_top": (0.0, 0.15, 1.55),
        "front_hoof": (0.6, 0.0, 0.0),
    }
    vertices = []
    for vertex_id, (anchor_id, world) in enumerate(world_points.items()):
        bone = "HorseFrontLeg" if anchor_id == "front_hoof" else "HorseBody"
        vertices.append(
            {
                "vertex_id": vertex_id,
                "world": list(world),
                "local": list(world),
                "weights": [{"bone": bone, "weight": 1.0}],
            }
        )
    skin_path = bundle / "skin_weights.json.gz"
    with gzip.open(skin_path, "wt", encoding="utf-8") as stream:
        json.dump({"vertices": vertices}, stream)

    anchor_groups = []
    for bone in ("HorseBody", "HorseFrontLeg"):
        points = []
        for vertex_id, (anchor_id, world) in enumerate(world_points.items()):
            expected_bone = "HorseFrontLeg" if anchor_id == "front_hoof" else "HorseBody"
            if expected_bone == bone:
                points.append(
                    {
                        "id": anchor_id,
                        "vertex_id": vertex_id,
                        "weight": 1.0,
                        "world": list(world),
                    }
                )
        anchor_groups.append({"bone": bone, "points": points})
    anchors_path = bundle / "surface_anchors.json"
    _write_json(anchors_path, {"bones": anchor_groups})

    rgb_path = bundle / "reference_rgb.png"
    Image.new("RGB", (320, 240), (32, 32, 32)).save(rgb_path)
    mask_path = bundle / "reference_mask.png"
    Image.new("L", (320, 240), 255).save(mask_path)

    world_to_camera = np.asarray(
        (
            (1.0, 0.0, 0.0, 0.0),
            (0.0, 0.0, 1.0, 0.0),
            (0.0, 1.0, 0.0, -8.0),
            (0.0, 0.0, 0.0, 1.0),
        )
    )
    metadata = {
        "schema": "autorig-actionless-fitting-bundle.v1",
        "actionless": {"actionless": True, "animation_count": 0},
        "camera": {
            "resolution": [320, 240],
            "intrinsics": {"fx": 260.0, "fy": 260.0, "cx": 160.0, "cy": 120.0},
            "world_to_camera": _flat(world_to_camera),
        },
        "ground_plane": {"normal": [0.0, 0.0, 1.0], "height": 0.0},
        "artifacts": {
            "rgb": _artifact_record(rgb_path),
            "mask": _artifact_record(mask_path),
            "skeleton": _artifact_record(skeleton_path),
            "skin_weights": _artifact_record(skin_path),
            "surface_anchors": _artifact_record(anchors_path),
        },
    }
    _write_json(bundle / "fitting_bundle.json", metadata)
    _repin_bundle(bundle)
    return bundle


def _build_observations(root: Path, bundle: Path) -> tuple[Path, np.ndarray]:
    rig = load_rig_bundle(bundle)
    frame_count = 7
    phase = np.linspace(0.0, 2.0 * np.pi, frame_count)
    truth = 0.35 * np.sin(phase)
    tracks = {
        anchor_id: {"id": str(index), "anchor_id": anchor_id, "query_frame": 0, "points": []}
        for index, anchor_id in enumerate(rig.anchors)
    }
    depth_rows = []
    silhouette_rows = []
    observation_root = root / "observations"
    observation_root.mkdir()
    for frame, angle in enumerate(truth):
        root_translation = np.asarray((0.08 * np.sin(phase[frame]), 0.0, 0.0))
        world, _ = rig.forward_kinematics(
            root_translation,
            np.zeros(3),
            {"HorseFrontLeg": np.asarray((0.0, angle, 0.0))},
        )
        anchors = rig.anchor_positions(world)
        depth = np.full((rig.camera.height, rig.camera.width), np.nan, dtype=np.float32)
        mask = np.zeros((rig.camera.height, rig.camera.width), dtype=np.uint8)
        for anchor_id, point_world in anchors.items():
            xy, camera_depth = rig.camera.project(point_world)
            tracks[anchor_id]["points"].append(
                {
                    "frame": frame,
                    "x": float(xy[0]),
                    "y": float(xy[1]),
                    "visible": True,
                }
            )
            x, y = int(round(float(xy[0]))), int(round(float(xy[1])))
            depth[y, x] = camera_depth
            yy, xx = np.ogrid[: rig.camera.height, : rig.camera.width]
            mask[(xx - x) ** 2 + (yy - y) ** 2 <= 5**2] = 255
        depth_path = observation_root / f"depth_{frame:03d}.npy"
        np.save(depth_path, depth)
        depth_rows.append({"frame": frame, "path": depth_path.name, "mode": "camera_z"})
        mask_path = observation_root / f"mask_{frame:03d}.png"
        Image.fromarray(mask).save(mask_path)
        silhouette_rows.append({"frame": frame, "path": mask_path.name})
    observations = {
        "schema": "autorig-fitting-observations.v1",
        "frame_count": frame_count,
        "width": rig.camera.width,
        "height": rig.camera.height,
        "fps": 24.0,
        "tracks": list(tracks.values()),
        "silhouettes": silhouette_rows,
        "depth": depth_rows,
        "contacts": [
            {"anchor_id": "front_hoof", "frames": [0, frame_count - 1], "ground_height": 0.0}
        ],
        "provenance": {"generator": "deterministic-synthetic-horse-test"},
    }
    path = observation_root / "observations.json"
    _write_json(path, observations)
    return path, truth


def test_synthetic_horse_temporal_fit_exports_all_local_bones(tmp_path: Path) -> None:
    bundle = _build_synthetic_horse_bundle(tmp_path)
    observations_path, truth = _build_observations(tmp_path, bundle)
    rig = load_rig_bundle(bundle)
    observations = load_observations(observations_path)
    config = FittingConfig(
        depth_weight=10.0,
        silhouette_weight=0.0,
        joint_rest_weight=0.0,
        joint_velocity_weight=0.0001,
        joint_acceleration_weight=0.0001,
        root_velocity_weight=0.0001,
        root_acceleration_weight=0.0001,
        contact_height_weight=2.0,
        contact_slide_weight=0.0,
        loop_pose_weight=10.0,
        loop_velocity_weight=1.0,
        robust_loss="linear",
        max_nfev=200,
    )
    result = fit_sequence(rig, observations, loop=True, config=config)
    assert result.optimizer["success"] is True
    assert result.qa["decision"] is None
    assert result.qa["tracks"]["reprojection_px"]["rmse"] < 0.05
    assert result.qa["loop_seam"]["parameter_pose_l2"] < 0.01
    fitted_angles = result.parameters.reshape(len(truth), result.frame_width)[:, 6]
    assert np.max(np.abs(fitted_angles - truth)) < 0.02
    assert result.qa["joint_limit_violation_radians"]["max"] == pytest.approx(0.0)

    output = result.save(tmp_path / "horse_fitted.json")
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema"] == "autorig-fitted-animation.v1"
    assert len(payload["frames"]) == len(truth)
    assert set(payload["frames"][3]["bones"]) == {"HorseBody", "HorseFrontLeg"}
    assert len(payload["frames"][3]["bones"]["HorseFrontLeg"]["local_matrix"]) == 16
    schema_path = Path(__file__).resolve().parents[1] / "schemas" / "fitted-animation.v1.schema.json"
    Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8"))).validate(payload)


def test_max_nfev_one_fails_closed_and_cli_writes_no_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _build_synthetic_horse_bundle(tmp_path)
    observations_path, _ = _build_observations(tmp_path, bundle)
    rig = load_rig_bundle(bundle)
    observations = load_observations(observations_path)
    with pytest.raises(OptimizationError, match=r"status=0"):
        fit_sequence(
            rig,
            observations,
            loop=True,
            config=FittingConfig(max_nfev=1),
        )

    config_path = tmp_path / "max-nfev-one.json"
    _write_json(
        config_path,
        {"schema": "autorig-fitting-config.v1", "max_nfev": 1},
    )
    output = tmp_path / "must-not-exist.json"
    exit_code = fitting_cli_main(
        [
            "fit",
            "--bundle",
            str(bundle),
            "--observations",
            str(observations_path),
            "--output",
            str(output),
            "--loop",
            "--config",
            str(config_path),
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "status=0" in captured.err
    assert not output.exists()


def test_anchor_lbs_uses_every_normalized_bone_influence(tmp_path: Path) -> None:
    bundle = _build_synthetic_horse_bundle(tmp_path)
    _replace_vertex_weights(
        bundle,
        0,
        [
            {"bone": "HorseBody", "weight": 2.0},
            {"bone": "HorseFrontLeg", "weight": 2.0},
        ],
    )
    _replace_anchor_weight(bundle, "body_front", 2.0)
    rig = load_rig_bundle(bundle)
    anchor = rig.anchors["body_front"]
    assert anchor.bone == "HorseBody"
    assert [influence.bone for influence in anchor.influences] == [
        "HorseBody",
        "HorseFrontLeg",
    ]
    assert [influence.weight for influence in anchor.influences] == pytest.approx([0.5, 0.5])

    rest_world, _ = rig.forward_kinematics(np.zeros(3), np.zeros(3), {})
    assert rig.anchor_positions(rest_world)[anchor.id] == pytest.approx(anchor.rest_world)

    posed_world, _ = rig.forward_kinematics(
        np.zeros(3),
        np.zeros(3),
        {"HorseFrontLeg": np.asarray((0.0, 0.6, 0.0))},
    )
    blended = rig.anchor_positions(posed_world)[anchor.id]
    expected = sum(
        (
            influence.weight
            * (
                posed_world[influence.bone]
                @ np.append(influence.bone_local, 1.0)
            )[:3]
        )
        for influence in anchor.influences
    )
    primary_local = (
        np.linalg.inv(rig.bones[anchor.bone].rest_world)
        @ np.append(anchor.rest_world, 1.0)
    )[:3]
    primary_only = (posed_world[anchor.bone] @ np.append(primary_local, 1.0))[:3]
    assert blended == pytest.approx(expected)
    assert not np.allclose(blended, primary_only)
    assert "HorseFrontLeg" in rig.select_active_bones([anchor.id])


@pytest.mark.parametrize(
    ("weights", "message"),
    [
        ([], "between one and four"),
        (
            [
                {"bone": "HorseBody", "weight": 0.5},
                {"bone": "HorseBody", "weight": 0.5},
            ],
            "duplicate skin weight",
        ),
        ([{"bone": "MissingBone", "weight": 1.0}], "unknown bone"),
        ([{"bone": "HorseBody", "weight": -0.1}], "invalid skin weight"),
        ([{"bone": "HorseBody", "weight": float("nan")}], "invalid skin weight"),
        (
            [
                {"bone": "HorseBody", "weight": 0.0},
                {"bone": "HorseFrontLeg", "weight": 0.0},
            ],
            "positive finite sum",
        ),
    ],
)
def test_invalid_skin_weight_contract_fails_closed(
    tmp_path: Path,
    weights: list[dict],
    message: str,
) -> None:
    bundle = _build_synthetic_horse_bundle(tmp_path)
    _replace_vertex_weights(bundle, 0, weights)
    with pytest.raises(ContractError, match=message):
        load_rig_bundle(bundle)


def test_cotracker_adapter_requires_explicit_numeric_visibility_threshold(tmp_path: Path) -> None:
    tracker = tmp_path / "tracker.json"
    _write_json(
        tracker,
        {
            "track_ids": ["hoof"],
            "tracks": [[[10.0, 20.0]], [[11.0, 21.0]]],
            "visibility": [[0.9], [0.4]],
        },
    )
    anchor_map = tmp_path / "map.json"
    _write_json(
        anchor_map,
        {
            "schema": "autorig-tracker-anchor-map.v1",
            "tracks": [{"track_id": "hoof", "anchor_id": "front_hoof"}],
        },
    )
    with pytest.raises(ContractError, match="visibility-threshold"):
        adapt_tracker_json(
            tracker,
            adapter="cotracker",
            anchor_map_path=anchor_map,
            output_path=tmp_path / "observations.json",
            layout="T,N,2",
            width=320,
            height=240,
            fps=24.0,
        )
    output = adapt_tracker_json(
        tracker,
        adapter="cotracker",
        anchor_map_path=anchor_map,
        output_path=tmp_path / "observations.json",
        layout="T,N,2",
        width=320,
        height=240,
        fps=24.0,
        visibility_threshold=0.5,
    )
    loaded = load_observations(output)
    assert [point.visible for point in loaded.tracks[0].points] == [True, False]
    assert loaded.provenance["visibility_threshold"] == 0.5


def test_tap_adapter_converts_occlusion_boolean_without_guessing(tmp_path: Path) -> None:
    tracker = tmp_path / "tap.json"
    _write_json(
        tracker,
        {
            "track_ids": ["hoof"],
            "tracks": [[[10.0, 20.0], [11.0, 21.0]]],
            "occluded": [[False, True]],
            "query_frames": [0],
        },
    )
    anchor_map = tmp_path / "map.json"
    _write_json(
        anchor_map,
        {
            "schema": "autorig-tracker-anchor-map.v1",
            "tracks": [{"track_id": "hoof", "anchor_id": "front_hoof"}],
        },
    )
    output = adapt_tracker_json(
        tracker,
        adapter="tap",
        anchor_map_path=anchor_map,
        output_path=tmp_path / "tap_observations.json",
        layout="N,T,2",
        width=320,
        height=240,
        fps=24.0,
    )
    loaded = load_observations(output)
    assert [point.visible for point in loaded.tracks[0].points] == [True, False]
    assert loaded.provenance["adapter"] == "tap-json-v1"


def test_relative_depth_without_explicit_affine_calibration_is_rejected(tmp_path: Path) -> None:
    depth = tmp_path / "relative.npy"
    np.save(depth, np.ones((2, 2), dtype=np.float32))
    observations = tmp_path / "observations.json"
    _write_json(
        observations,
        {
            "schema": "autorig-fitting-observations.v1",
            "frame_count": 2,
            "width": 2,
            "height": 2,
            "fps": 24,
            "tracks": [
                {
                    "id": "0",
                    "anchor_id": "hoof",
                    "query_frame": 0,
                    "points": [
                        {"frame": 0, "x": 0, "y": 0, "visible": True},
                        {"frame": 1, "x": 0, "y": 0, "visible": True}
                    ]
                }
            ],
            "depth": [{"frame": 0, "path": depth.name, "mode": "relative"}]
        },
    )
    with pytest.raises(ContractError, match="relative depth"):
        load_observations(observations)


def _write_escape_observations(path: Path, artifact_path: str) -> None:
    _write_json(
        path,
        {
            "schema": "autorig-fitting-observations.v1",
            "frame_count": 1,
            "width": 2,
            "height": 2,
            "fps": 24,
            "tracks": [
                {
                    "id": "0",
                    "anchor_id": "hoof",
                    "query_frame": 0,
                    "points": [
                        {"frame": 0, "x": 0, "y": 0, "visible": True},
                    ],
                }
            ],
            "depth": [{"frame": 0, "path": artifact_path, "mode": "camera_z"}],
        },
    )


def test_observation_artifact_cannot_escape_root_with_parent_path(tmp_path: Path) -> None:
    outside = tmp_path / "outside.npy"
    np.save(outside, np.ones((2, 2), dtype=np.float32))
    root = tmp_path / "observations"
    root.mkdir()
    observations = root / "observations.json"
    _write_escape_observations(observations, "../outside.npy")
    with pytest.raises(ContractError, match="escapes the observation root"):
        load_observations(observations)


def test_observation_artifact_cannot_escape_root_through_symlink(tmp_path: Path) -> None:
    outside = tmp_path / "outside.npy"
    np.save(outside, np.ones((2, 2), dtype=np.float32))
    root = tmp_path / "observations"
    root.mkdir()
    link = root / "linked.npy"
    try:
        link.symlink_to(outside)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    observations = root / "observations.json"
    _write_escape_observations(observations, link.name)
    with pytest.raises(ContractError, match="escapes the observation root"):
        load_observations(observations)


def test_all_json_schemas_parse() -> None:
    schema_root = Path(__file__).resolve().parents[1] / "schemas"
    for schema in schema_root.glob("*.schema.json"):
        payload = json.loads(schema.read_text(encoding="utf-8"))
        assert payload["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        Draft202012Validator.check_schema(payload)
