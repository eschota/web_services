from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import sys
import uuid
import zipfile

import numpy as np
import pytest


TOOLS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOLS_ROOT.parent))

from animation_fitting.bridge_browser_animation_motion import (  # noqa: E402
    BLENDER_TO_GLTF_Y_UP,
    BridgeError,
    RESULT_SCHEMA as BRIDGE_RESULT_SCHEMA,
    _canonical,
    _resolve_final_approval,
    _skeleton_fingerprint,
    _validate_glb_skeleton,
    bridge_browser_animation_motions,
)
from animation_fitting.motion_export_contract import load_motion  # noqa: E402
from animation_fitting.package_browser_animation_glb import (  # noqa: E402
    INPUT_SCHEMA as GLB_INPUT_SCHEMA,
    TAXONOMY_PATH,
    _parse_glb,
    load_animal_taxonomy,
    package_browser_animation_glb,
)
from animation_fitting.package_browser_animation_release import (  # noqa: E402
    FBX_RECEIPT_SCHEMA,
    INPUT_SCHEMA,
    PLAN_SCHEMA,
    RESULT_SCHEMA,
    SERVER_CONFIG_SCHEMA,
    FilePin,
    ReleaseError,
    _load_release_inputs,
    _source_identity,
    build_export_plan,
    publish_release,
)
import animation_fitting.package_browser_animation_release as release_module  # noqa: E402


TAXONOMY_PIN, CLIP_IDS, TAXONOMY = load_animal_taxonomy()
CLIP_IDS = tuple(CLIP_IDS)
LIBRARY_REVISION = "horse-browser-library-v2"
TASK_ID = "10000000-0000-4000-8000-000000000001"
TASK_GUID = "20000000-0000-4000-8000-000000000001"
BLENDER_VERSION = "4.3.3"
BLENDER_43 = Path(
    os.environ.get(
        "AUTORIG_BLENDER_43",
        r"C:\Program Files\Blender Foundation\Blender 4.3\blender.exe",
    )
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _path_pin(path: Path, *, relative_to: Path | None = None) -> dict:
    data = path.read_bytes()
    name = path.relative_to(relative_to) if relative_to is not None else path.resolve()
    return {"path": str(name), "bytes": len(data), "sha256": _sha(data)}


def _filename_pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"filename": path.name, "bytes": len(data), "sha256": _sha(data)}


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical(value))


def _identity_value(value: dict) -> dict:
    identity = _sha(_canonical(value, newline=False))
    return {**value, "identity_sha256": identity}


def _matrix_bytes(document: dict, binary: bytes) -> bytes:
    json_bytes = json.dumps(
        document, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    json_bytes += b" " * ((-len(json_bytes)) % 4)
    binary += b"\x00" * ((-len(binary)) % 4)
    total = 12 + 8 + len(json_bytes) + 8 + len(binary)
    return b"".join(
        (
            struct.pack("<4sII", b"glTF", 2, total),
            struct.pack("<II", len(json_bytes), 0x4E4F534A),
            json_bytes,
            struct.pack("<II", len(binary), 0x004E4942),
            binary,
        )
    )


def _trs(
    translation: tuple[float, float, float] = (0.0, 0.0, 0.0),
    *,
    axis: str = "z",
    angle: float = 0.0,
    scale: tuple[float, float, float] = (1.0, 1.0, 1.0),
) -> np.ndarray:
    c, s = math.cos(angle), math.sin(angle)
    rotations = {
        "x": np.asarray(((1, 0, 0), (0, c, -s), (0, s, c)), dtype=float),
        "y": np.asarray(((c, 0, s), (0, 1, 0), (-s, 0, c)), dtype=float),
        "z": np.asarray(((c, -s, 0), (s, c, 0), (0, 0, 1)), dtype=float),
    }
    result = np.eye(4)
    result[:3, :3] = rotations[axis] @ np.diag(scale)
    result[:3, 3] = translation
    return result


def _flat(matrix: np.ndarray) -> list[float]:
    return [float(value) for value in matrix.flat]


def _node_matrix(matrix: np.ndarray) -> list[float]:
    return [float(value) for value in matrix.T.flat]


def _skeleton_value(*, complex_hierarchy: bool) -> tuple[dict, np.ndarray, list[dict]]:
    if complex_hierarchy:
        armature_world = _trs(
            (0.25, -0.1, 0.5), axis="y", angle=0.23, scale=(1.2, 0.8, 1.1)
        )
        rows = [
            {
                "name": "RootA",
                "parent": None,
                "use_deform": True,
                "helper": False,
                "relative": _trs(),
            },
            {
                "name": "ChildA",
                "parent": "RootA",
                "use_deform": True,
                "helper": False,
                "relative": _trs((0, 1, 0), angle=0.19, scale=(1.0, 1.35, 0.75)),
            },
            {
                "name": "HelperA",
                "parent": "ChildA",
                "use_deform": False,
                "helper": True,
                "relative": _trs((0.15, 0.5, 0), axis="x", angle=-0.11),
            },
            {
                "name": "RootB",
                "parent": None,
                "use_deform": False,
                "helper": False,
                "relative": _trs((1.0, 0, 0), axis="z", angle=-0.15),
            },
            {
                "name": "ChildB",
                "parent": "RootB",
                "use_deform": True,
                "helper": False,
                "relative": _trs((0, 0.7, 0), axis="y", angle=0.09),
            },
        ]
    else:
        armature_world = _trs((1.0, 2.0, 0.25), axis="z", angle=0.15)
        # Blender bones use +Y as their longitudinal local axis.  The real
        # canary creates both bones along armature-space +Z, so Root's rest
        # basis rotates local +Y onto +Z and Child is one local-Y unit below
        # its parent.  These are the actual Bone.matrix_local relationships,
        # not the edit-bone head/tail coordinates.
        root_bone_rest = np.asarray(
            (
                (1.0, 0.0, 0.0, 0.0),
                (0.0, 0.0, -1.0, 0.0),
                (0.0, 1.0, 0.0, 0.0),
                (0.0, 0.0, 0.0, 1.0),
            ),
            dtype=float,
        )
        rows = [
            {
                "name": "Root",
                "parent": None,
                "use_deform": True,
                "helper": False,
                "relative": root_bone_rest,
            },
            {
                "name": "Child",
                "parent": "Root",
                "use_deform": True,
                "helper": False,
                "relative": _trs((0, 1, 0)),
            },
        ]
    bones = [
        {
            "name": row["name"],
            "parent": row["parent"],
            "use_deform": row["use_deform"],
            "helper": row["helper"],
            "parent_relative_matrix": _flat(row["relative"]),
            "length": 1.0,
            "joint_limits": [],
        }
        for row in rows
    ]
    return (
        {
            "armatures": [
                {
                    "name": "HorseRig",
                    "matrix_world": _flat(armature_world),
                    "bones": bones,
                }
            ]
        },
        armature_world,
        rows,
    )


def _source_glb(
    skeleton_sha256: str,
    armature_world: np.ndarray,
    rows: list[dict],
) -> bytes:
    nodes: list[dict] = [
        {
            "name": "HorseRig",
            "matrix": _node_matrix(armature_world),
            "mesh": 0,
            "skin": 0,
            "children": [],
        }
    ]
    index_by_name: dict[str, int] = {}
    for row in rows:
        index_by_name[row["name"]] = len(nodes)
        nodes.append(
            {
                "name": row["name"],
                "matrix": _node_matrix(row["relative"]),
            }
        )
    for row in rows:
        index = index_by_name[row["name"]]
        parent = row["parent"]
        parent_index = 0 if parent is None else index_by_name[parent]
        nodes[parent_index].setdefault("children", []).append(index)
    binary = b"SOURCE-BIN-BROWSER-ASSET" + b"\x00" * 96
    document = {
        "asset": {
            "version": "2.0",
            "extras": {
                "sourceRigType": "horse",
                "sourceOrientation": "front",
                "templateSkeletonSha256": skeleton_sha256,
            },
        },
        "scene": 0,
        "scenes": [{"nodes": [0]}],
        "nodes": nodes,
        "skins": [{"joints": list(index_by_name.values())}],
        "meshes": [
            {
                "primitives": [
                    {
                        "attributes": {"POSITION": 1, "JOINTS_0": 2, "WEIGHTS_0": 3},
                        "indices": 4,
                    }
                ]
            }
        ],
        "buffers": [{"byteLength": len(binary)}],
        "bufferViews": [{"buffer": 0, "byteOffset": 0, "byteLength": len(binary)}],
        "accessors": [
            {
                "bufferView": 0,
                "componentType": 5126,
                "count": len(rows),
                "type": "MAT4",
            },
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "VEC3"},
            {"bufferView": 0, "componentType": 5123, "count": 1, "type": "VEC4"},
            {"bufferView": 0, "componentType": 5126, "count": 1, "type": "VEC4"},
            {"bufferView": 0, "componentType": 5123, "count": 3, "type": "SCALAR"},
        ],
    }
    return _matrix_bytes(document, binary)


def _clip_value(
    semantic_id: str,
    index: int,
    *,
    bones: list[dict],
    timing_mode: str = "valid",
) -> dict:
    taxonomy_row = TAXONOMY["clips"][index]
    frame_count = taxonomy_row["frame_profile"]
    fps = 30
    if timing_mode == "bad_frame_count" and index == 0:
        frame_count -= 1
    if timing_mode == "bad_fps" and index == 0:
        fps = 24
    times = [frame / fps for frame in range(frame_count)]
    loop = bool(taxonomy_row["loop"])
    amplitude = 0.08 + index * 0.0007
    angles = []
    positions = []
    for frame in range(frame_count):
        phase = frame / max(1, frame_count - 1)
        value = math.sin(phase * 2 * math.pi) if loop else phase
        if timing_mode == "bad_loop_closure" and index == 0:
            value = phase
        angles.append(amplitude * value)
        positions.append(0.025 * value)
    root_name = bones[0]["name"]
    child_name = bones[1]["name"]
    child_translation = bones[1]["relative"][:3, 3].tolist()
    rotations = []
    for angle in angles:
        rotations.extend([0.0, 0.0, math.sin(angle / 2), math.cos(angle / 2)])
    root_positions = []
    for position in positions:
        root_positions.extend([position, 0.0, 0.0])
    child_positions = child_translation * frame_count
    return {
        "name": f"Browser_{semantic_id}",
        "duration": times[-1],
        "tracks": [
            {
                "name": f"{child_name}.quaternion",
                "type": "quaternion",
                "times": times,
                "values": rotations,
            },
            {
                "name": f"{root_name}.position",
                "type": "vector",
                "times": times,
                "values": root_positions,
            },
            {
                "name": f"{child_name}.position",
                "type": "vector",
                "times": times,
                "values": child_positions,
            },
        ],
    }


def _candidate_id(index: int) -> str:
    return str(uuid.UUID(int=(1 << 62) | (index + 1), version=4))


class ReleaseFixture:
    def __init__(
        self,
        root: Path,
        *,
        timing_mode: str = "valid",
        complex_hierarchy: bool = True,
        defer_export_runtime: bool = False,
    ) -> None:
        self.root = root.resolve()
        self.root.mkdir(exist_ok=True)
        self.fitting_root = self.root / "fitting-jobs"
        self.fitting_root.mkdir()
        self.validation_root = self.root / "trusted-fbx-validation"
        self.validation_root.mkdir()
        self.glb_root = self.root / "glb"
        self.glb_root.mkdir()
        self.skeleton = self.root / "skeleton.json"
        skeleton_value, self.armature_world, self.bone_rows = _skeleton_value(
            complex_hierarchy=complex_hierarchy
        )
        _write_json(self.skeleton, skeleton_value)
        self.skeleton_sha = _sha(self.skeleton.read_bytes())
        self.source_glb = self.root / "source.glb"
        self.source_glb.write_bytes(
            _source_glb(self.skeleton_sha, self.armature_world, self.bone_rows)
        )
        self.approvals: list[dict] = []
        self.approval_jobs: dict[str, str] = {}
        self.clip_paths: list[Path] = []
        for index, semantic_id in enumerate(CLIP_IDS):
            job_id = f"30000000-0000-4000-8000-{index + 1:012x}"
            self.approval_jobs[semantic_id] = job_id
            clip_value = _clip_value(
                semantic_id,
                index,
                bones=self.bone_rows,
                timing_mode=timing_mode,
            )
            candidate_identity, candidate_id, clip_path, approval = (
                self._write_approval(
                    index=index,
                    semantic_id=semantic_id,
                    job_id=job_id,
                    clip_value=clip_value,
                )
            )
            assert candidate_identity
            assert candidate_id == approval["candidate_id"]
            self.clip_paths.append(clip_path)
            self.approvals.append(approval)
        self.glb_input = self.glb_root / "package-input.json"
        self.write_glb_input()
        self.multi_glb = self.glb_root / "animations.glb"
        self.glb_result = self.glb_root / "package-result.json"
        package_browser_animation_glb(
            input_manifest=self.glb_input,
            input_manifest_sha256=_sha(self.glb_input.read_bytes()),
            output=self.multi_glb,
            result_manifest=self.glb_result,
        )
        self.bridge_dir = self.root / "browser-motion-bridge"
        bridge_browser_animation_motions(
            package_input=self.glb_input,
            package_input_sha256=_sha(self.glb_input.read_bytes()),
            skeleton=self.skeleton,
            skeleton_sha256=self.skeleton_sha,
            fitting_jobs_root=self.fitting_root,
            approval_job_ids=self.approval_jobs,
            output_dir=self.bridge_dir,
        )
        self.bridge_result = self.bridge_dir / "motion-bridge-result.json"
        bridge_value = json.loads(self.bridge_result.read_text(encoding="utf-8"))
        self.motion_paths = [
            Path(row["motion"]["path"]) for row in bridge_value["clips"]
        ]
        self.server_config: Path | None = None
        self.release_input: Path | None = None
        self.work = self.root / "work"
        self.work.mkdir()
        self.plan = self.root / "export-plan.json"
        if not defer_export_runtime:
            source_blend = self.root / "horse.blend"
            source_blend.write_bytes(b"SERVER-OWNED-BLENDER-SOURCE")
            fake_blender = self.root / "blender.exe"
            fake_blender.write_bytes(b"ALLOWLISTED-FAKE-BLENDER-4.3.3")
            self.configure_export_runtime(source_blend, fake_blender, BLENDER_VERSION)

    def _write_approval(
        self,
        *,
        index: int,
        semantic_id: str,
        job_id: str,
        clip_value: dict,
    ) -> tuple[str, str, Path, dict]:
        clip_payload = _canonical(clip_value)
        clip_pin = {
            "filename": "three-clip.json",
            "bytes": len(clip_payload),
            "sha256": _sha(clip_payload),
        }
        manifest_unsigned = {
            "schema": "autorig.browser-animation-candidate-bundle.v1",
            "library": {
                "version_id": f"40000000-0000-4000-8000-{index + 1:012x}",
                "revision": LIBRARY_REVISION,
                "rig_type": "horse",
                "template_skeleton_sha256": self.skeleton_sha,
            },
            "fitting_job": {
                "id": job_id,
                "semantic_id": semantic_id,
                "workflow_name": "browser-fitting",
                "workflow_fingerprint": _sha(f"workflow-{index}".encode()),
            },
            "source_task": {"id": TASK_ID, "guid": TASK_GUID},
            "candidate": {
                "candidate_index": 0,
                "seed": index + 100,
                "source_rig_type": "Horse_2",
                "source_model_sha256": _sha(self.source_glb.read_bytes()),
                "source_skeleton_sha256": self.skeleton_sha,
                "frame_count": TAXONOMY["clips"][index]["frame_profile"],
                "fps": 30,
            },
            "controlled_generation": {"server_owned": True},
            "artifacts": {"three-clip.json": clip_pin},
        }
        manifest = _identity_value(manifest_unsigned)
        candidate_identity = manifest["identity_sha256"]
        candidate_dir = (
            self.fitting_root
            / job_id
            / "browser-candidates"
            / candidate_identity[:2]
            / candidate_identity
        )
        candidate_dir.mkdir(parents=True)
        clip_path = candidate_dir / "three-clip.json"
        clip_path.write_bytes(clip_payload)
        manifest_path = candidate_dir / "candidate-manifest.json"
        _write_json(manifest_path, manifest)
        manifest_pin = _filename_pin(manifest_path)
        review_unsigned = {
            "schema": "autorig.browser-animation-human-review.v1",
            "candidate": {
                "identity_sha256": candidate_identity,
                "manifest": manifest_pin,
            },
            "server_validation": {"trusted": True},
            "lifecycle_binding_sha256": _sha(f"lifecycle-{index}".encode()),
            "review": {
                "decision": "PASS",
                "reviewer_id": "qa@autorig.online",
                "reviewed_at": "2026-07-16T00:00:00+00:00",
            },
        }
        review = _identity_value(review_unsigned)
        review_dir = (
            self.fitting_root
            / job_id
            / "browser-candidate-reviews"
            / candidate_identity[:2]
            / candidate_identity
            / "human-review"
        )
        review_dir.mkdir(parents=True)
        review_path = review_dir / "human-review-receipt.json"
        _write_json(review_path, review)
        review_pin = _filename_pin(review_path)
        candidate_id = _candidate_id(index)
        descriptor = {
            "schema": "autorig.browser-animation-package-descriptor.v1",
            "package_id": candidate_id,
            "candidate_id": candidate_id,
            "candidate_bundle_sha256": _sha(manifest_path.read_bytes()),
            "human_review_sha256": _sha(review_path.read_bytes()),
            "semantic_id": semantic_id,
            "clip": clip_pin,
            "review_identity_sha256": review["identity_sha256"],
            "library": manifest["library"],
            "fitting_job": manifest["fitting_job"],
            "source_task": manifest["source_task"],
            "candidate_identity_sha256": candidate_identity,
            "server_validation_identity_sha256": _sha(f"validation-{index}".encode()),
            "review": review["review"],
            "pins": {
                "candidate_manifest": manifest_pin,
                "three_clip": clip_pin,
                "task_model": _filename_pin(self.source_glb),
                "task_skeleton": _filename_pin(self.skeleton),
                "server_validation_receipt": {
                    "filename": "server-validation-receipt.json",
                    "bytes": 1,
                    "sha256": _sha(f"validation-receipt-{index}".encode()),
                },
                "server_qa_metrics": {
                    "filename": "server-qa-metrics.json",
                    "bytes": 1,
                    "sha256": _sha(f"metrics-{index}".encode()),
                },
                "human_review_receipt": review_pin,
            },
        }
        descriptor_path = review_dir / "package-descriptor.json"
        _write_json(descriptor_path, descriptor)
        descriptor_pin = _filename_pin(descriptor_path)
        dummy_two = _sha(f"{semantic_id}-rank2".encode())
        dummy_three = _sha(f"{semantic_id}-rank3".encode())
        selection_unsigned = {
            "schema": "autorig.browser-animation-candidate-selection.v1",
            "state": "FINAL",
            "mode": "production",
            "job": {
                "id": job_id,
                "semantic_id": semantic_id,
                "library_revision": LIBRARY_REVISION,
                "rig_type": "horse",
            },
            "contracts": {"server_owned": True},
            "inventory": {"generation_closed": True},
            "candidates": [
                {
                    "candidate_identity_sha256": candidate_identity,
                    "candidate_manifest": manifest_pin,
                    "human_review": {
                        "identity_sha256": review["identity_sha256"],
                        "receipt": review_pin,
                        "decision": "PASS",
                        "package_descriptor": descriptor_pin,
                        "candidate_id": candidate_id,
                    },
                    "ranking": {"eligible": True, "rank": 1},
                },
                {
                    "candidate_identity_sha256": dummy_two,
                    "human_review": {"decision": "PASS"},
                    "ranking": {"eligible": True, "rank": 2},
                },
                {
                    "candidate_identity_sha256": dummy_three,
                    "human_review": {"decision": "PASS"},
                    "ranking": {"eligible": True, "rank": 3},
                },
            ],
            "selection": {
                "top_candidate_identity_sha256": candidate_identity,
                "top_k_candidate_identity_sha256": [
                    candidate_identity,
                    dummy_two,
                    dummy_three,
                ],
                "comparative_selection": True,
                "production_eligible": True,
                "finalization_reason": "target_and_top_k_satisfied",
                "finalized_by": "qa@autorig.online",
            },
        }
        selection = _identity_value(selection_unsigned)
        selection_path = (
            self.fitting_root
            / job_id
            / "browser-candidate-selection"
            / "final"
            / "selection-receipt.json"
        )
        _write_json(selection_path, selection)
        approval = {
            "candidate_id": candidate_id,
            "candidate_bundle_sha256": _sha(manifest_path.read_bytes()),
            "human_review_sha256": _sha(review_path.read_bytes()),
        }
        return candidate_identity, candidate_id, clip_path, approval

    def glb_input_value(self) -> dict:
        source_pin = _path_pin(self.source_glb)
        source_pin["orientation"] = "front"
        return {
            "schema": GLB_INPUT_SCHEMA,
            "taxonomy": _path_pin(Path(TAXONOMY_PATH)),
            "library_revision": LIBRARY_REVISION,
            "rig_type": "horse",
            "template_skeleton_sha256": self.skeleton_sha,
            "source": source_pin,
            "clips": [
                {
                    "semantic_id": semantic_id,
                    **_path_pin(clip_path),
                    **self.approvals[index],
                }
                for index, (semantic_id, clip_path) in enumerate(
                    zip(CLIP_IDS, self.clip_paths, strict=True)
                )
            ],
        }

    def write_glb_input(self, value: dict | None = None) -> None:
        _write_json(self.glb_input, value or self.glb_input_value())

    def configure_export_runtime(
        self, source_blend: Path, blender_executable: Path, blender_version: str
    ) -> None:
        self.source_blend = source_blend.resolve()
        self.blender = blender_executable.resolve()
        self.target = self.root / "target.json"
        browser_identity = _source_identity(
            task_id=TASK_ID,
            task_guid=TASK_GUID,
            task_model=release_module._snapshot_checked(self.source_glb, "test model"),
            skeleton=release_module._snapshot_checked(self.skeleton, "test skeleton"),
            rig_type="horse",
            orientation="front",
        )
        target_value = {
            "schema": "autorig-motion-target.v1",
            "source_sha256": _sha(self.source_blend.read_bytes()),
            "armature_name": "HorseRig",
            "bone_names": [row["name"] for row in self.bone_rows],
            "bone_parents": {row["name"]: row["parent"] for row in self.bone_rows},
            "rig_type": "horse",
            "orientation": "front",
            "source_asset": {
                "identity_sha256": browser_identity,
                "task_id": TASK_ID,
                "task_guid": TASK_GUID,
                "task_model_sha256": _sha(self.source_glb.read_bytes()),
                "task_skeleton_sha256": self.skeleton_sha,
            },
        }
        _write_json(self.target, target_value)
        export_identity = release_module._export_identity(
            browser_identity,
            release_module._snapshot_checked(self.source_blend, "test blend"),
            release_module._snapshot_checked(self.target, "test target"),
        )
        self.server_config = self.root / "server-config.json"
        config = {
            "schema": SERVER_CONFIG_SCHEMA,
            "config_id": "horse-release-test-v1",
            "fitting_jobs_root": str(self.fitting_root),
            "fbx_validation_root": str(self.validation_root),
            "approval_jobs": [
                {"semantic_id": semantic_id, "job_id": self.approval_jobs[semantic_id]}
                for semantic_id in CLIP_IDS
            ],
            "source_asset": {
                "task_id": TASK_ID,
                "task_guid": TASK_GUID,
                "task_model": _path_pin(self.source_glb),
                "task_skeleton": _path_pin(self.skeleton),
                "source_blend": _path_pin(self.source_blend),
                "target_manifest": _path_pin(self.target),
                "browser_asset_identity_sha256": browser_identity,
                "export_asset_identity_sha256": export_identity,
            },
            "blender": {
                "selected_id": "blender-4.3.3",
                "allowlist": [
                    {
                        "id": "blender-4.3.3",
                        "executable": _path_pin(self.blender),
                        "version": blender_version,
                    }
                ],
            },
            "canonical_applier_sha256": _sha(
                (TOOLS_ROOT / "apply_fitted_motion.py").read_bytes()
            ),
            "canonical_validator_sha256": _sha(
                (TOOLS_ROOT / "package_browser_animation_release.py").read_bytes()
            ),
        }
        _write_json(self.server_config, config)
        self.release_input = self.root / "release-input.json"
        self.write_release_input()

    def release_input_value(self) -> dict:
        assert self.release_input is not None
        return {
            "schema": INPUT_SCHEMA,
            "taxonomy": _path_pin(Path(TAXONOMY_PATH)),
            "library_revision": LIBRARY_REVISION,
            "rig_type": "horse",
            "orientation": "front",
            "template_skeleton_sha256": self.skeleton_sha,
            "multi_clip_glb_result": _path_pin(self.glb_result),
            "browser_motion_bridge_result": _path_pin(self.bridge_result),
            "clips": [
                {"semantic_id": semantic_id, "fitted_motion": _path_pin(motion)}
                for semantic_id, motion in zip(CLIP_IDS, self.motion_paths, strict=True)
            ],
        }

    def write_release_input(self, value: dict | None = None) -> None:
        assert self.release_input is not None
        _write_json(self.release_input, value or self.release_input_value())

    def build_plan(self) -> dict:
        assert self.server_config is not None and self.release_input is not None
        return build_export_plan(
            input_manifest=self.release_input,
            input_manifest_sha256=_sha(self.release_input.read_bytes()),
            server_config=self.server_config,
            server_config_sha256=_sha(self.server_config.read_bytes()),
            working_root=self.work,
            output_plan=self.plan,
        )

    def _one_clip_glb(self, index: int) -> bytes:
        snapshot = release_module._snapshot_checked(self.multi_glb, "test multi GLB")
        document, binary = release_module._glb_chunks(snapshot.data, "test multi GLB")
        document = copy.deepcopy(document)
        document["animations"] = [document["animations"][index]]
        return _matrix_bytes(document, binary)

    def _write_trusted_receipt(
        self,
        *,
        job: dict,
        clip_index: int,
        fbx: Path,
        inputs: release_module.ReleaseInputs,
    ) -> None:
        clip = inputs.clips[clip_index]
        binding = {
            "schema": FBX_RECEIPT_SCHEMA,
            "job_id": job["job_id"],
            "semantic_id": clip.semantic_id,
            "source_asset_identity_sha256": inputs.server.export_asset_identity_sha256,
            "skeleton_fingerprint_sha256": inputs.skeleton_fingerprint_sha256,
            "fbx": _filename_pin(fbx),
            "motion": clip.motion.filename_pin(),
            "skeleton": inputs.skeleton.filename_pin(),
            "producer": {
                "validator": inputs.server.validator.descriptor(),
                "blender_executable": inputs.server.blender_executable.descriptor(),
                "blender_version": inputs.server.blender_version,
                "background": True,
            },
            "take": {
                "semantic_id": clip.semantic_id,
                "action_names": [clip.semantic_id],
                "imported_action_name": clip.semantic_id,
                "armature_name": inputs.armature_name,
                "bone_names": list(inputs.bone_order),
                "bone_parents": {
                    name: inputs.bones[name].parent for name in inputs.bone_order
                },
                "frame_count": clip.frame_profile,
                "fps": 30,
                "duration_seconds": (clip.frame_profile - 1) / 30,
                "loop": clip.loop,
            },
            "validation": {
                "fbx_imported": True,
                "single_semantic_action": True,
                "exact_skeleton_hierarchy": True,
                "exact_timing": True,
            },
        }
        receipt = _identity_value(binding)
        _write_json(Path(job["expected_fbx_validation_receipt"]), receipt)

    def write_worker_outputs(self, plan: dict) -> None:
        assert self.release_input is not None and self.server_config is not None
        inputs = _load_release_inputs(
            self.release_input,
            _sha(self.release_input.read_bytes()),
            server_config=self.server_config,
            server_config_sha256=_sha(self.server_config.read_bytes()),
        )
        for index, (job, clip) in enumerate(
            zip(plan["jobs"], inputs.clips, strict=True)
        ):
            output = Path(job["output_dir"])
            output.mkdir()
            blend = output / f"{clip.semantic_id}.blend"
            fbx = output / f"{clip.semantic_id}.fbx"
            glb = output / f"{clip.semantic_id}.glb"
            blend.write_bytes(
                (f"opaque-derived-blend-{clip.semantic_id}" * 32).encode()
            )
            # Deliberately not an FBX header/string fixture.  Only the trusted,
            # server-root Blender import receipt is authoritative.
            fbx.write_bytes(os.urandom(2048) + index.to_bytes(2, "little"))
            glb.write_bytes(self._one_clip_glb(index))
            manifest = {
                "schema": "autorig-fitted-asset-bundle.v1",
                "semantic_action_id": clip.semantic_id,
                "source": {
                    "path": str(inputs.source_blend.path),
                    "sha256": inputs.source_blend.sha256,
                },
                "motion": {
                    "path": str(clip.motion.path),
                    "sha256": clip.motion.sha256,
                    "schema": "autorig-fitted-animation.v1",
                    "input_fps": 30,
                },
                "target": {
                    "manifest": str(inputs.target_manifest.path),
                    "manifest_sha256": inputs.target_manifest.sha256,
                    "armature_name": inputs.armature_name,
                    "bone_count": len(inputs.bone_order),
                },
                "blender": {
                    "version": inputs.server.blender_version,
                    "background": True,
                },
                "timing": {
                    "frame_count": clip.frame_profile,
                    "frame_start": 0,
                    "frame_end": clip.frame_profile - 1,
                    "fps": 30,
                    "duration_seconds": (clip.frame_profile - 1) / 30,
                    "loop": clip.loop,
                },
                "action": {"name": clip.semantic_id, "action_datablock_count": 1},
                "skin": {"max_influences": 4, "unweighted_vertex_count": 0},
                "artifacts": {
                    "blend": _filename_pin(blend),
                    "fbx": _filename_pin(fbx),
                    "glb": _filename_pin(glb),
                },
            }
            _write_json(Path(job["expected_manifest"]), manifest)
            self._write_trusted_receipt(
                job=job, clip_index=index, fbx=fbx, inputs=inputs
            )


def test_bridge_resolves_server_final_pass_and_exact_30fps_profiles(
    tmp_path: Path,
) -> None:
    fixture = ReleaseFixture(tmp_path)
    result = json.loads(fixture.bridge_result.read_text(encoding="utf-8"))
    assert result["schema"] == BRIDGE_RESULT_SCHEMA
    assert result["browser_only"] is True
    assert result["blender_used"] is False
    assert result["clip_count"] == 30
    assert tuple(row["semantic_id"] for row in result["clips"]) == CLIP_IDS
    for row, taxonomy in zip(result["clips"], TAXONOMY["clips"], strict=True):
        motion = load_motion(row["motion"]["path"])
        assert motion.fps == 30
        assert motion.frame_count == taxonomy["frame_profile"]
        assert motion.loop is taxonomy["loop"]
        assert row["approval"]["selection_receipt"]["sha256"] == _sha(
            Path(row["approval"]["selection_receipt"]["path"]).read_bytes()
        )
        assert row["approval"]["browser_clip"]["sha256"] == _sha(
            Path(row["approval"]["browser_clip"]["path"]).read_bytes()
        )


@pytest.mark.parametrize(
    ("armature_name", "action_names", "error"),
    [
        ("ForeignRig", ["idle_neutral"], "armature name differs"),
        ("HorseRig", ["HorseRig|idle_neutral"], "actions must be exactly one"),
        (
            "HorseRig",
            ["HorseRig|HorseRig|idle_neutral_suffix"],
            "actions must be exactly one",
        ),
        (
            "HorseRig",
            ["idle_neutral", "extra_action"],
            "actions must be exactly one",
        ),
    ],
)
def test_imported_fbx_identity_rejects_foreign_or_inexact_action_contract(
    armature_name: str, action_names: list[str], error: str
) -> None:
    with pytest.raises(ReleaseError, match=error):
        release_module._validate_imported_fbx_identity(
            imported_armature_name=armature_name,
            imported_action_names=action_names,
            skeleton_armature_name="HorseRig",
            semantic_id="idle_neutral",
        )


@pytest.mark.parametrize(
    "action_name",
    ["idle_neutral", "HorseRig|HorseRig|idle_neutral"],
)
def test_imported_fbx_identity_accepts_only_exact_supported_round_trip_names(
    action_name: str,
) -> None:
    assert (
        release_module._validate_imported_fbx_identity(
            imported_armature_name="HorseRig",
            imported_action_names=[action_name],
            skeleton_armature_name="HorseRig",
            semantic_id="idle_neutral",
        )
        == action_name
    )


def test_fake_caller_approval_is_rejected_against_server_final(tmp_path: Path) -> None:
    fixture = ReleaseFixture(tmp_path)
    value = fixture.glb_input_value()
    value["clips"][0]["human_review_sha256"] = "f" * 64
    forged = tmp_path / "forged-package-input.json"
    _write_json(forged, value)
    with pytest.raises(BridgeError, match="approval binding mismatch"):
        bridge_browser_animation_motions(
            package_input=forged,
            package_input_sha256=_sha(forged.read_bytes()),
            skeleton=fixture.skeleton,
            skeleton_sha256=fixture.skeleton_sha,
            fitting_jobs_root=fixture.fitting_root,
            approval_job_ids=fixture.approval_jobs,
            output_dir=tmp_path / "forged-output",
        )


@pytest.mark.parametrize("mutation", ["open", "hold", "rank"])
def test_non_final_non_pass_or_wrong_rank_server_approval_fails(
    tmp_path: Path, mutation: str
) -> None:
    fixture = ReleaseFixture(tmp_path)
    job_id = fixture.approval_jobs[CLIP_IDS[0]]
    path = (
        fixture.fitting_root
        / job_id
        / "browser-candidate-selection"
        / "final"
        / "selection-receipt.json"
    )
    value = json.loads(path.read_text(encoding="utf-8"))
    value.pop("identity_sha256")
    if mutation == "open":
        value["state"] = "OPEN"
    elif mutation == "hold":
        value["candidates"][0]["human_review"]["decision"] = "HOLD"
    else:
        value["candidates"][0]["ranking"]["rank"] = 2
    _write_json(path, _identity_value(value))
    with pytest.raises(BridgeError, match="FINAL|PASS|rank"):
        _resolve_final_approval(
            fitting_jobs_root=fixture.fitting_root,
            job_id=job_id,
            semantic_id=CLIP_IDS[0],
        )


@pytest.mark.parametrize(
    "timing_mode", ["bad_frame_count", "bad_fps", "bad_loop_closure"]
)
def test_frame_profile_fps_and_loop_closure_are_fail_closed(
    tmp_path: Path, timing_mode: str
) -> None:
    with pytest.raises(BridgeError, match="frame|FPS|closure|timing"):
        ReleaseFixture(tmp_path, timing_mode=timing_mode)


def test_skeleton_sha_must_equal_package_template(tmp_path: Path) -> None:
    fixture = ReleaseFixture(tmp_path)
    changed = tmp_path / "changed-skeleton.json"
    value = json.loads(fixture.skeleton.read_text(encoding="utf-8"))
    value["armatures"][0]["bones"][0]["length"] = 2.0
    _write_json(changed, value)
    with pytest.raises(BridgeError, match="template_skeleton_sha256"):
        bridge_browser_animation_motions(
            package_input=fixture.glb_input,
            package_input_sha256=_sha(fixture.glb_input.read_bytes()),
            skeleton=changed,
            skeleton_sha256=_sha(changed.read_bytes()),
            fitting_jobs_root=fixture.fitting_root,
            approval_job_ids=fixture.approval_jobs,
            output_dir=tmp_path / "changed-output",
        )


def test_glb_rest_hierarchy_detects_helper_non_deform_multi_root_drift(
    tmp_path: Path,
) -> None:
    fixture = ReleaseFixture(tmp_path)
    snapshot = release_module._snapshot_checked(fixture.source_glb, "source")
    document, _ = _parse_glb(snapshot)
    armature_name, armature_world, bones, order = release_module._load_skeleton(
        release_module._snapshot_checked(fixture.skeleton, "skeleton")
    )
    assert {name for name in order if bones[name].helper} == {"HelperA"}
    assert {name for name in order if not bones[name].use_deform} == {
        "HelperA",
        "RootB",
    }
    assert sum(bones[name].parent is None for name in order) == 2
    fingerprint = _validate_glb_skeleton(
        document,
        armature_name=armature_name,
        armature_world=armature_world,
        bones=bones,
        bone_order=order,
        field="test GLB",
    )
    assert fingerprint == _skeleton_fingerprint(
        armature_name, armature_world, bones, order
    )
    y_up = copy.deepcopy(document)
    y_up["nodes"][0]["matrix"] = _node_matrix(BLENDER_TO_GLTF_Y_UP @ armature_world)
    assert _validate_glb_skeleton(
        y_up,
        armature_name=armature_name,
        armature_world=armature_world,
        bones=bones,
        bone_order=order,
        field="Y-up GLB",
    ) == _skeleton_fingerprint(armature_name, armature_world, bones, order)

    mixed = copy.deepcopy(y_up)
    index_by_name = {node["name"]: index for index, node in enumerate(mixed["nodes"])}
    root_b_index = index_by_name["RootB"]
    root_b_parent_world = BLENDER_TO_GLTF_Y_UP @ armature_world
    mixed["nodes"][root_b_index]["matrix"] = _node_matrix(
        np.linalg.inv(root_b_parent_world) @ bones["RootB"].rest_world
    )
    with pytest.raises(BridgeError, match="asset-wide"):
        _validate_glb_skeleton(
            mixed,
            armature_name=armature_name,
            armature_world=armature_world,
            bones=bones,
            bone_order=order,
            field="mixed-space GLB",
        )

    other_axis = _trs(axis="y", angle=math.pi / 2)
    translated = _trs((0.01, 0.0, 0.0)) @ BLENDER_TO_GLTF_Y_UP
    scaled = _trs(scale=(1.01, 1.0, 1.0)) @ BLENDER_TO_GLTF_Y_UP
    for label, basis in (
        ("inverse", np.linalg.inv(BLENDER_TO_GLTF_Y_UP)),
        ("other-axis", other_axis),
        ("translated", translated),
        ("scaled", scaled),
    ):
        wrong_basis = copy.deepcopy(document)
        wrong_basis["nodes"][0]["matrix"] = _node_matrix(basis @ armature_world)
        with pytest.raises(BridgeError, match="rest matrix mismatch"):
            _validate_glb_skeleton(
                wrong_basis,
                armature_name=armature_name,
                armature_world=armature_world,
                bones=bones,
                bone_order=order,
                field=f"{label} GLB",
            )

    rest_drift = copy.deepcopy(document)
    rest_drift["nodes"][2]["matrix"][12] += 0.01
    with pytest.raises(BridgeError, match="rest matrix mismatch"):
        _validate_glb_skeleton(
            rest_drift,
            armature_name=armature_name,
            armature_world=armature_world,
            bones=bones,
            bone_order=order,
            field="drift GLB",
        )
    hierarchy_drift = copy.deepcopy(document)
    hierarchy_drift["nodes"][1]["children"].remove(2)
    hierarchy_drift["nodes"][0]["children"].append(2)
    with pytest.raises(BridgeError, match="parent mismatch"):
        _validate_glb_skeleton(
            hierarchy_drift,
            armature_name=armature_name,
            armature_world=armature_world,
            bones=bones,
            bone_order=order,
            field="hierarchy GLB",
        )


def test_plan_uses_only_allowlisted_blender_and_exact_canonical_applier(
    tmp_path: Path,
) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    assert plan["schema"] == PLAN_SCHEMA
    assert len(plan["jobs"]) == 30
    for job in plan["jobs"]:
        assert job["export_command"]["shell"] is False
        assert job["export_command"]["argv"][0] == str(fixture.blender)
        assert job["export_command"]["argv"][4] == str(
            (TOOLS_ROOT / "apply_fitted_motion.py").resolve()
        )
        assert job["fbx_validation_command"]["argv"][4] == str(
            (TOOLS_ROOT / "package_browser_animation_release.py").resolve()
        )
    config = json.loads(fixture.server_config.read_text(encoding="utf-8"))
    config["blender"]["selected_id"] = "not-allowlisted"
    bad = tmp_path / "bad-config.json"
    _write_json(bad, config)
    with pytest.raises(ReleaseError, match="selected Blender"):
        build_export_plan(
            input_manifest=fixture.release_input,
            input_manifest_sha256=_sha(fixture.release_input.read_bytes()),
            server_config=bad,
            server_config_sha256=_sha(bad.read_bytes()),
            working_root=fixture.work,
            output_plan=tmp_path / "bad-plan.json",
        )


def test_noncanonical_applier_pin_is_rejected(tmp_path: Path) -> None:
    fixture = ReleaseFixture(tmp_path)
    config = json.loads(fixture.server_config.read_text(encoding="utf-8"))
    config["canonical_applier_sha256"] = "0" * 64
    bad = tmp_path / "bad-applier-config.json"
    _write_json(bad, config)
    with pytest.raises(ReleaseError, match="canonical apply_fitted_motion"):
        build_export_plan(
            input_manifest=fixture.release_input,
            input_manifest_sha256=_sha(fixture.release_input.read_bytes()),
            server_config=bad,
            server_config_sha256=_sha(bad.read_bytes()),
            working_root=fixture.work,
            output_plan=tmp_path / "bad-plan.json",
        )


def test_release_is_atomic_deterministic_zip_and_reopens_all_30_members(
    tmp_path: Path,
) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    fixture.write_worker_outputs(plan)
    output = tmp_path / "published"
    output.mkdir()
    result = publish_release(
        plan_manifest=fixture.plan,
        plan_manifest_sha256=_sha(fixture.plan.read_bytes()),
        output_dir=output,
    )
    assert result["schema"] == RESULT_SCHEMA
    release_dir = Path(result["release_directory"])
    assert release_dir.parent == output
    assert sorted(path.name for path in release_dir.iterdir()) == sorted(
        [
            result["artifacts"]["multi_clip_glb"]["filename"],
            result["artifacts"]["per_clip_fbx_zip"]["filename"],
            result["publication"]["manifest"],
        ]
    )
    archive_path = release_dir / result["artifacts"]["per_clip_fbx_zip"]["filename"]
    with zipfile.ZipFile(archive_path) as archive:
        assert archive.namelist() == [
            "fbx-index.json",
            *[
                f"animations/{index:02d}-{semantic_id}.fbx"
                for index, semantic_id in enumerate(CLIP_IDS, 1)
            ],
        ]
        index_value = json.loads(archive.read("fbx-index.json"))
        assert index_value["clip_count"] == 30
        for row in index_value["clips"]:
            assert _sha(archive.read(row["fbx_member"])) == row["fbx_sha256"]


def test_missing_trusted_receipt_and_fake_worker_receipt_are_rejected(
    tmp_path: Path,
) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    fixture.write_worker_outputs(plan)
    trusted = Path(plan["jobs"][0]["expected_fbx_validation_receipt"])
    fake = Path(plan["jobs"][0]["output_dir"]) / "fbx-validation-receipt.json"
    fake.write_bytes(trusted.read_bytes())
    trusted.unlink()
    output = tmp_path / "published"
    output.mkdir()
    with pytest.raises(ReleaseError, match="bundle inventory|trusted FBX receipt"):
        publish_release(
            plan_manifest=fixture.plan,
            plan_manifest_sha256=_sha(fixture.plan.read_bytes()),
            output_dir=output,
        )
    assert not list(output.iterdir())


def test_malformed_one_clip_glb_rejected_even_when_manifest_and_hash_are_updated(
    tmp_path: Path,
) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    fixture.write_worker_outputs(plan)
    job = plan["jobs"][0]
    glb = Path(job["expected_glb"])
    glb.write_bytes(b"glTF" + b"malformed")
    manifest_path = Path(job["expected_manifest"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"]["glb"] = _filename_pin(glb)
    _write_json(manifest_path, manifest)
    output = tmp_path / "published"
    output.mkdir()
    with pytest.raises(ReleaseError, match="GLB.*header|truncated"):
        publish_release(
            plan_manifest=fixture.plan,
            plan_manifest_sha256=_sha(fixture.plan.read_bytes()),
            output_dir=output,
        )
    assert not list(output.iterdir())


def test_fbx_bytes_changed_after_trusted_receipt_are_rejected(tmp_path: Path) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    fixture.write_worker_outputs(plan)
    fbx = Path(plan["jobs"][0]["expected_fbx"])
    fbx.write_bytes(fbx.read_bytes() + b"tamper")
    manifest_path = Path(plan["jobs"][0]["expected_manifest"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"]["fbx"] = _filename_pin(fbx)
    _write_json(manifest_path, manifest)
    output = tmp_path / "published"
    output.mkdir()
    with pytest.raises(ReleaseError, match="trusted FBX receipt binding"):
        publish_release(
            plan_manifest=fixture.plan,
            plan_manifest_sha256=_sha(fixture.plan.read_bytes()),
            output_dir=output,
        )
    assert not list(output.iterdir())


def test_json_toctou_and_symlink_path_escape_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = ReleaseFixture(tmp_path)
    original = release_module._json_checked
    original_release_bytes = fixture.release_input.read_bytes()
    changed = False

    def mutate_after_read(snapshot, field, **kwargs):
        nonlocal changed
        value = original(snapshot, field, **kwargs)
        if field == "release input manifest" and not changed:
            changed = True
            snapshot.path.write_bytes(snapshot.path.read_bytes() + b" ")
        return value

    monkeypatch.setattr(release_module, "_json_checked", mutate_after_read)
    with pytest.raises(ReleaseError, match="changed|SHA|descriptor"):
        fixture.build_plan()
    monkeypatch.setattr(release_module, "_json_checked", original)
    fixture.release_input.write_bytes(original_release_bytes)
    if not hasattr(os, "symlink"):
        pytest.skip("symlink is unavailable")
    link = tmp_path / "linked-work"
    try:
        os.symlink(fixture.work, link, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is not permitted")
    with pytest.raises(ReleaseError, match="symlink|reparse"):
        build_export_plan(
            input_manifest=fixture.release_input,
            input_manifest_sha256=_sha(fixture.release_input.read_bytes()),
            server_config=fixture.server_config,
            server_config_sha256=_sha(fixture.server_config.read_bytes()),
            working_root=link,
            output_plan=tmp_path / "link-plan.json",
        )


def test_zip_source_toctou_is_detected_and_no_release_is_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    fixture.write_worker_outputs(plan)
    original = release_module._stream_copy_checked
    invoked = False

    def tamper_stream(reader, writer, expected: FilePin):
        nonlocal invoked
        if not invoked:
            invoked = True
            expected.path.write_bytes(expected.path.read_bytes() + b"changed")
        return original(reader, writer, expected)

    monkeypatch.setattr(release_module, "_stream_copy_checked", tamper_stream)
    output = tmp_path / "published"
    output.mkdir()
    with pytest.raises(ReleaseError, match="ZIP source changed"):
        publish_release(
            plan_manifest=fixture.plan,
            plan_manifest_sha256=_sha(fixture.plan.read_bytes()),
            output_dir=output,
        )
    assert not [path for path in output.iterdir() if not path.name.startswith(".")]


def test_manifest_staging_failure_never_requires_public_unlink_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    fixture.write_worker_outputs(plan)
    original = release_module._write_new

    def fail_manifest(path: Path, payload: bytes):
        if path.name.endswith("release-manifest.json"):
            raise OSError("injected manifest staging failure")
        return original(path, payload)

    monkeypatch.setattr(release_module, "_write_new", fail_manifest)
    output = tmp_path / "published"
    output.mkdir()
    with pytest.raises(OSError, match="manifest staging failure"):
        publish_release(
            plan_manifest=fixture.plan,
            plan_manifest_sha256=_sha(fixture.plan.read_bytes()),
            output_dir=output,
        )
    assert not [path for path in output.iterdir() if not path.name.startswith(".")]


def test_release_directory_collision_preserves_existing_bytes(tmp_path: Path) -> None:
    fixture = ReleaseFixture(tmp_path)
    plan = fixture.build_plan()
    fixture.write_worker_outputs(plan)
    output = tmp_path / "published"
    output.mkdir()
    prefix = release_module._portable_prefix(
        _load_release_inputs(
            fixture.release_input,
            _sha(fixture.release_input.read_bytes()),
            server_config=fixture.server_config,
            server_config_sha256=_sha(fixture.server_config.read_bytes()),
        ),
        plan["plan_id"],
    )
    collision = output / prefix
    collision.mkdir()
    marker = collision / "existing.bin"
    marker.write_bytes(b"preserve")
    with pytest.raises(ReleaseError, match="collision"):
        publish_release(
            plan_manifest=fixture.plan,
            plan_manifest_sha256=_sha(fixture.plan.read_bytes()),
            output_dir=output,
        )
    assert marker.read_bytes() == b"preserve"


def _run_blender(*args: str, timeout: int = 240) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(BLENDER_43), *args],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Blender failed ({result.returncode})\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def _installed_blender_version() -> str:
    result = _run_blender("--version", timeout=30)
    first_line = result.stdout.splitlines()[0].strip()
    prefix = "Blender "
    assert first_line.startswith(prefix), first_line
    return first_line.removeprefix(prefix).strip()


@pytest.mark.skipif(
    not BLENDER_43.is_file(), reason="Local Blender 4.3 is not installed"
)
def test_real_blender_export_and_independent_fbx_import_receipt_canary(
    tmp_path: Path,
) -> None:
    fixture = ReleaseFixture(
        tmp_path / "fixture",
        complex_hierarchy=False,
        defer_export_runtime=True,
    )
    canary = tmp_path / "blender-source"
    _run_blender(
        "--background",
        "--factory-startup",
        "--python",
        str(TOOLS_ROOT / "tests" / "blender_create_two_bone_canary.py"),
        "--",
        "--output-dir",
        str(canary),
    )
    fixture.configure_export_runtime(
        canary / "horse_source.blend", BLENDER_43, _installed_blender_version()
    )
    plan = fixture.build_plan()
    job = plan["jobs"][0]
    export = _run_blender(*job["export_command"]["argv"][1:])
    assert "AUTORIG_FITTED_MOTION=" in export.stdout
    validate = _run_blender(*job["fbx_validation_command"]["argv"][1:])
    assert "autorig.browser-animation-fbx-validation-receipt.v1" in validate.stdout
    receipt = Path(job["expected_fbx_validation_receipt"])
    assert receipt.is_file() and receipt.stat().st_size > 0
    inputs = _load_release_inputs(
        fixture.release_input,
        _sha(fixture.release_input.read_bytes()),
        server_config=fixture.server_config,
        server_config_sha256=_sha(fixture.server_config.read_bytes()),
    )
    output_dir = Path(job["output_dir"])
    snapshot_root = tmp_path / "real-snapshots"
    snapshot_root.mkdir()
    validated = release_module._validate_worker_bundle(
        job,
        inputs.clips[0],
        inputs,
        snapshot_root=snapshot_root,
        working_root=fixture.work,
    )
    assert validated[1].bytes > 1024
    assert validated[2].sha256 == _sha((output_dir / "idle_neutral.glb").read_bytes())


@pytest.mark.skipif(
    not BLENDER_43.is_file(), reason="Local Blender 4.3 is not installed"
)
def test_malformed_fbx_cannot_produce_trusted_blender_receipt(tmp_path: Path) -> None:
    fixture = ReleaseFixture(tmp_path / "fixture")
    # Reconfigure to the actual allowlisted executable; source blend is not
    # opened by the validator, but remains server-pinned.
    fixture.configure_export_runtime(
        fixture.source_blend, BLENDER_43, _installed_blender_version()
    )
    plan = fixture.build_plan()
    job = plan["jobs"][0]
    output = Path(job["output_dir"])
    output.mkdir()
    malformed = Path(job["expected_fbx"])
    malformed.write_bytes(
        b"Kaydara FBX Binary  \x00\x1a\x00idle_neutral" + b"\x00" * 2048
    )
    result = subprocess.run(
        job["fbx_validation_command"]["argv"],
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert result.returncode != 0
    assert not Path(job["expected_fbx_validation_receipt"]).exists()
