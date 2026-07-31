import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.responses import Response


BACKEND_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
for import_dir in (BACKEND_DIR, TESTS_DIR):
    if str(import_dir) not in sys.path:
        sys.path.insert(0, str(import_dir))

import main  # noqa: E402
from test_paired_rig_source_dry_run import (  # noqa: E402
    TETRA_OBJ,
    _synthetic_textured_glb,
)


EXPECTED_PIN_FIELDS = {
    "expected_connected_source_sha256",
    "expected_connected_source_bytes",
    "expected_appearance_target_sha256",
    "expected_appearance_target_bytes",
}
SERVER_TRANSFER_MARKER = main.SERVER_AUTHORIZATION_KEY
SERVER_VALIDATION_EVIDENCE = main.SERVER_VALIDATION_KEY


def _raw_endpoint(function):
    """Unwrap SlowAPI/FastAPI decorators for deterministic direct-call tests."""
    while hasattr(function, "__wrapped__"):
        function = function.__wrapped__
    return function


class _Upload:
    def __init__(self, filename: str, payload: bytes):
        self.filename = filename
        self._payload = payload
        self._offset = 0
        self.closed = False
        self.read_calls = 0

    async def read(self, size: int = -1) -> bytes:
        self.read_calls += 1
        if size < 0:
            size = len(self._payload) - self._offset
        start = self._offset
        self._offset = min(len(self._payload), self._offset + size)
        return self._payload[start : self._offset]

    async def close(self) -> None:
        self.closed = True


class _FormRequest:
    def __init__(self, form, *, via_api_key=True, api_key_anon_id=None):
        self._form = form
        self.state = SimpleNamespace(
            auth_via_api_key=via_api_key,
            api_key_anon_id=api_key_anon_id,
        )
        self.headers = {"content-type": "multipart/form-data; boundary=test"}

    async def form(self):
        return self._form


class _BodyRequest:
    def __init__(self, body: bytes):
        self._body = body

    async def body(self) -> bytes:
        return self._body


class _CommitSpy:
    def __init__(self):
        self.commit_count = 0

    async def commit(self):
        self.commit_count += 1


class _FakeTask:
    def __init__(self, **values):
        self.__dict__.update(values)


class _AtomicTaskDb:
    def __init__(self):
        self.added = []
        self.commit_snapshots = []
        self.refresh_count = 0

    def add(self, value):
        self.added.append(value)

    async def commit(self):
        self.commit_snapshots.append(
            [
                {
                    "id": task.id,
                    "status": task.status,
                    "viewer_settings": json.loads(task.viewer_settings),
                }
                for task in self.added
            ]
        )

    async def refresh(self, value):
        self.refresh_count += 1


def _admin(*, is_admin=True):
    return SimpleNamespace(
        email="admin@autorig.test" if is_admin else "operator@autorig.test",
        is_admin=is_admin,
    )


def _paired_form(
    *,
    source_payload=TETRA_OBJ,
    target_payload=None,
    expected_overrides=None,
):
    target_payload = _synthetic_textured_glb() if target_payload is None else target_payload
    source = _Upload("connected-white.obj", source_payload)
    target = _Upload("textured-target.glb", target_payload)
    form = {
        "dry_run": "true",
        "rig_source_transfer": "position-and-triangle-topology",
        "type": "animal",
        "pipeline": "rig",
        "animal_type": "horse",
        "mode": "only_rig",
        "rig_source_file": source,
        "appearance_target_file": target,
        "expected_connected_source_sha256": hashlib.sha256(source_payload).hexdigest(),
        "expected_connected_source_bytes": str(len(source_payload)),
        "expected_appearance_target_sha256": hashlib.sha256(target_payload).hexdigest(),
        "expected_appearance_target_bytes": str(len(target_payload)),
    }
    form.update(expected_overrides or {})
    return form, source, target


def _paired_create_form(
    *,
    source_payload=TETRA_OBJ,
    target_payload=None,
    source_filename="connected-white.obj",
    expected_overrides=None,
):
    form, source, target = _paired_form(
        source_payload=source_payload,
        target_payload=target_payload,
        expected_overrides=expected_overrides,
    )
    source.filename = source_filename
    form.pop("dry_run")
    form["source"] = "upload"
    form["file"] = form.pop("appearance_target_file")
    return form, source, target


def _json_response_body(response):
    return json.loads(bytes(response.body).decode("utf-8"))


class PairedRigSourceHttpHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_capability_requires_admin_api_key_and_declares_pin_fields(self):
        endpoint = _raw_endpoint(main.paired_rig_source_capabilities)

        with self.assertRaises(HTTPException) as missing_key:
            await endpoint(_FormRequest({}, via_api_key=False), _admin())
        self.assertEqual(missing_key.exception.status_code, 401)

        with self.assertRaises(HTTPException) as non_admin:
            await endpoint(_FormRequest({}), _admin(is_admin=False))
        self.assertEqual(non_admin.exception.status_code, 403)

        capability = await endpoint(_FormRequest({}), _admin())
        self.assertFalse(capability["createsTask"])
        self.assertFalse(capability["chargesCredits"])
        self.assertFalse(capability["persistsUpload"])
        declared_fields = set(capability["fieldMap"])
        self.assertTrue(EXPECTED_PIN_FIELDS.issubset(declared_fields))
        paired_fields = capability["pairedAnimalDryRun"]["formFields"]
        self.assertEqual(
            {
                paired_fields["expectedConnectedRigSourceSha256"],
                paired_fields["expectedConnectedRigSourceBytes"],
                paired_fields["expectedTexturedTargetSha256"],
                paired_fields["expectedTexturedTargetBytes"],
            },
            EXPECTED_PIN_FIELDS,
        )

    async def test_dry_run_pass_is_no_mutation_and_closes_both_uploads(self):
        form, source, target = _paired_form()
        endpoint = _raw_endpoint(main.paired_rig_source_dry_run)
        forbidden_async = AsyncMock(side_effect=AssertionError("dry-run must not mutate tasks"))

        with tempfile.TemporaryDirectory(prefix="autorig-paired-http-") as upload_dir:
            with (
                patch.object(main, "UPLOAD_DIR", upload_dir),
                patch.object(main, "create_conversion_task", new=forbidden_async),
                patch.object(main, "enforce_task_cache_max_size", new=forbidden_async),
                patch.object(main, "ensure_disk_headroom_for_new_task", new=forbidden_async),
                patch.object(main, "Task", side_effect=AssertionError("dry-run must not create Task")),
            ):
                evidence = await endpoint(_FormRequest(form), _admin())

            self.assertEqual(list(Path(upload_dir).iterdir()), [])

        self.assertTrue(evidence["valid"])
        self.assertTrue(evidence["dry_run"])
        self.assertEqual(evidence["credits_charged"], 0)
        self.assertFalse(evidence["task_created"])
        self.assertFalse(evidence["persisted_uploads"])
        self.assertTrue(evidence["temporary_buffers_discarded"])
        self.assertTrue(source.closed)
        self.assertTrue(target.closed)
        forbidden_async.assert_not_awaited()

    async def test_dry_run_rejects_client_pin_mismatch_before_geometry(self):
        endpoint = _raw_endpoint(main.paired_rig_source_dry_run)
        mismatches = (
            ("expected_connected_source_sha256", "0" * 64, "artifact_sha256_mismatch"),
            ("expected_connected_source_bytes", str(len(TETRA_OBJ) + 1), "artifact_bytes_mismatch"),
            ("expected_appearance_target_sha256", "f" * 64, "artifact_sha256_mismatch"),
            ("expected_appearance_target_bytes", "1", "artifact_bytes_mismatch"),
        )
        for field, bad_value, error_code in mismatches:
            with self.subTest(field=field):
                form, source, target = _paired_form(expected_overrides={field: bad_value})
                forbidden_async = AsyncMock(
                    side_effect=AssertionError("pin failure must not mutate tasks")
                )
                with tempfile.TemporaryDirectory(prefix="autorig-paired-http-") as upload_dir:
                    with (
                        patch.object(main, "UPLOAD_DIR", upload_dir),
                        patch.object(main, "create_conversion_task", new=forbidden_async),
                        patch.object(main, "enforce_task_cache_max_size", new=forbidden_async),
                        patch.object(main, "ensure_disk_headroom_for_new_task", new=forbidden_async),
                        patch.object(
                            main,
                            "Task",
                            side_effect=AssertionError("pin failure must not create Task"),
                        ),
                    ):
                        response = await endpoint(_FormRequest(form), _admin())
                    self.assertEqual(list(Path(upload_dir).iterdir()), [])
                body = _json_response_body(response)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(body["error"]["code"], error_code)
                self.assertFalse(body["task_created"])
                self.assertEqual(body["credits_charged"], 0)
                self.assertFalse(body["persisted_uploads"])
                self.assertTrue(body["temporary_buffers_discarded"])
                self.assertTrue(source.closed)
                self.assertTrue(target.closed)
                forbidden_async.assert_not_awaited()

    async def test_dry_run_geometry_failure_and_413_close_all_buffers_without_persistence(self):
        endpoint = _raw_endpoint(main.paired_rig_source_dry_run)

        invalid_target = b"glTF-invalid"
        form, source, target = _paired_form(target_payload=invalid_target)
        with tempfile.TemporaryDirectory(prefix="autorig-paired-http-") as upload_dir:
            with patch.object(main, "UPLOAD_DIR", upload_dir):
                response = await endpoint(_FormRequest(form), _admin())
            self.assertEqual(list(Path(upload_dir).iterdir()), [])
        body = _json_response_body(response)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(body["valid"])
        self.assertFalse(body["task_created"])
        self.assertTrue(body["temporary_buffers_discarded"])
        self.assertTrue(source.closed)
        self.assertTrue(target.closed)

        form, source, target = _paired_form()
        with tempfile.TemporaryDirectory(prefix="autorig-paired-http-") as upload_dir:
            with (
                patch.object(main, "UPLOAD_DIR", upload_dir),
                patch.object(main, "MAX_UPLOAD_SIZE_MB", 0),
            ):
                with self.assertRaises(HTTPException) as too_large:
                    await endpoint(_FormRequest(form), _admin())
            self.assertEqual(list(Path(upload_dir).iterdir()), [])
        self.assertEqual(too_large.exception.status_code, 413)
        self.assertTrue(source.closed)
        self.assertTrue(target.closed)

    async def test_paired_create_requires_admin_api_key_and_obj_source_before_writes(self):
        endpoint = _raw_endpoint(main.api_create_task)
        cases = (
            (
                "browser admin",
                _admin(),
                {"via_api_key": False},
                "connected-white.obj",
                403,
            ),
            (
                "non-admin api key",
                _admin(is_admin=False),
                {"via_api_key": True},
                "connected-white.obj",
                403,
            ),
            (
                "anonymous api key",
                None,
                {"via_api_key": True, "api_key_anon_id": "anon-api-key"},
                "connected-white.obj",
                403,
            ),
            (
                "glb connected source",
                _admin(),
                {"via_api_key": True},
                "connected-white.glb",
                400,
            ),
        )
        for label, user, request_kwargs, source_filename, expected_status in cases:
            with self.subTest(label=label):
                form, source, target = _paired_create_form(
                    source_filename=source_filename,
                )
                db = _AtomicTaskDb()
                with tempfile.TemporaryDirectory(prefix="autorig-paired-create-") as upload_dir:
                    with patch.object(main, "UPLOAD_DIR", upload_dir):
                        with self.assertRaises(HTTPException) as rejected:
                            await endpoint(
                                _FormRequest(form, **request_kwargs),
                                Response(),
                                user,
                                db,
                            )
                    self.assertEqual(list(Path(upload_dir).iterdir()), [])
                self.assertEqual(rejected.exception.status_code, expected_status)
                self.assertEqual(source.read_calls, 0)
                self.assertEqual(target.read_calls, 0)
                self.assertEqual(db.added, [])
                self.assertEqual(db.commit_snapshots, [])

    async def test_paired_create_reruns_full_validator_before_task_insert(self):
        target_payload = _synthetic_textured_glb(alter_face=True)
        form, source, target = _paired_create_form(target_payload=target_payload)
        db = _AtomicTaskDb()
        endpoint = _raw_endpoint(main.api_create_task)
        forbidden_async = AsyncMock(
            side_effect=AssertionError("invalid pair must not create or dispatch a task")
        )

        with tempfile.TemporaryDirectory(prefix="autorig-paired-create-") as upload_dir:
            with (
                patch.object(main, "UPLOAD_DIR", upload_dir),
                patch.object(main, "APP_URL", "https://autorig.test"),
                patch.object(main, "ensure_request_disk_headroom", new=AsyncMock()),
                patch.object(main, "create_conversion_task", new=forbidden_async),
                patch.object(main, "get_global_queue_status", new=forbidden_async),
                patch.object(main, "get_dispatchable_workers", new=forbidden_async),
                patch.object(main, "start_task_on_worker", new=forbidden_async),
                patch.object(
                    main,
                    "Task",
                    side_effect=AssertionError("invalid pair must not construct Task"),
                ),
            ):
                with self.assertRaises(HTTPException) as rejected:
                    await endpoint(
                        _FormRequest(form),
                        Response(),
                        _admin(),
                        db,
                    )
            self.assertEqual(list(Path(upload_dir).iterdir()), [])

        self.assertEqual(rejected.exception.status_code, 400)
        self.assertIn("Paired validation failed", str(rejected.exception.detail))
        self.assertGreater(source.read_calls, 0)
        self.assertGreater(target.read_calls, 0)
        self.assertEqual(db.added, [])
        self.assertEqual(db.commit_snapshots, [])
        forbidden_async.assert_not_awaited()

    async def test_paired_create_commits_full_contract_once_and_skips_immediate_dispatch(self):
        form, source, target = _paired_create_form()
        db = _AtomicTaskDb()
        endpoint = _raw_endpoint(main.api_create_task)
        forbidden_create = AsyncMock(
            side_effect=AssertionError("paired path must not call create_conversion_task")
        )
        forbidden_dispatch = AsyncMock(
            side_effect=AssertionError("paired path must not enter immediate dispatch")
        )

        with tempfile.TemporaryDirectory(prefix="autorig-paired-create-") as upload_dir:
            with (
                patch.object(main, "UPLOAD_DIR", upload_dir),
                patch.object(main, "APP_URL", "https://autorig.test"),
                patch.object(main, "Task", _FakeTask),
                patch.object(main, "ensure_request_disk_headroom", new=AsyncMock()),
                patch.object(main, "enforce_task_cache_max_size", new=AsyncMock()),
                patch.object(main, "ensure_disk_headroom_for_new_task", new=AsyncMock()),
                patch.object(main, "create_conversion_task", new=forbidden_create),
                patch.object(main, "_save_preflight_render_image"),
                patch.object(main, "_select_viewer_theme_from_metadata", return_value=None),
                patch.object(main, "get_global_queue_status", new=forbidden_dispatch),
                patch.object(main, "get_dispatchable_workers", new=forbidden_dispatch),
                patch.object(main, "start_task_on_worker", new=forbidden_dispatch),
            ):
                result = await endpoint(
                    _FormRequest(form),
                    Response(),
                    _admin(),
                    db,
                )
            persisted_sizes = [
                path.stat().st_size
                for path in Path(upload_dir).rglob("*")
                if path.is_file()
            ]

        self.assertEqual(result.status, "created")
        self.assertEqual(len(db.added), 1)
        self.assertEqual(len(db.commit_snapshots), 1)
        committed = db.commit_snapshots[0][0]
        settings = committed["viewer_settings"]
        self.assertEqual(committed["status"], "created")
        self.assertEqual(settings[main.SERVER_AUTHORIZATION_KEY], main.SERVER_AUTHORIZATION_VALUE)
        self.assertTrue(settings[main.SERVER_VALIDATION_KEY]["valid"])
        self.assertEqual(
            settings["rig_v2_animal_detection"]["animal_type"],
            "horse",
        )
        transfer = settings["rig_source_transfer"]
        self.assertEqual(
            transfer["connected_source"]["sha256"],
            hashlib.sha256(TETRA_OBJ).hexdigest(),
        )
        self.assertEqual(
            transfer["appearance_target"]["sha256"],
            hashlib.sha256(_synthetic_textured_glb()).hexdigest(),
        )
        self.assertEqual(len(persisted_sizes), 2)
        self.assertTrue(all(size > 0 for size in persisted_sizes))
        self.assertGreater(source.read_calls, 0)
        self.assertGreater(target.read_calls, 0)
        forbidden_create.assert_not_awaited()
        forbidden_dispatch.assert_not_awaited()

    async def test_viewer_settings_reject_client_internal_transfer_fields(self):
        original = json.dumps(
            {
                "rig_source_transfer": {"schema": "server-contract"},
                SERVER_TRANSFER_MARKER: main.SERVER_AUTHORIZATION_VALUE,
                SERVER_VALIDATION_EVIDENCE: {"valid": True},
                "rig_v2_animal_detection": {"animal_type": "horse"},
            }
        )
        endpoint = _raw_endpoint(main.api_set_task_viewer_settings)

        for protected_key in (
            "rig_source_transfer",
            SERVER_TRANSFER_MARKER,
            SERVER_VALIDATION_EVIDENCE,
        ):
            with self.subTest(protected_key=protected_key):
                task = SimpleNamespace(viewer_settings=original)
                db = _CommitSpy()
                body = json.dumps({"camera": {"fov": 35}, protected_key: {"forged": True}}).encode()
                with (
                    patch.object(main, "get_task_by_id", new=AsyncMock(return_value=task)),
                    patch.object(main, "get_anon_session", new=AsyncMock(return_value=None)),
                    patch.object(main, "_is_task_owner_or_admin", return_value=True),
                ):
                    with self.assertRaises(HTTPException) as rejected:
                        await endpoint(
                            "task-1",
                            _BodyRequest(body),
                            Response(),
                            _admin(),
                            db,
                        )
                self.assertEqual(rejected.exception.status_code, 400)
                self.assertEqual(task.viewer_settings, original)
                self.assertEqual(db.commit_count, 0)

    async def test_viewer_settings_preserve_server_transfer_contract_and_marker(self):
        transfer = main.build_rig_source_transfer(
            connected_source_url="https://autorig.test/u/token/rig_source_connected.obj",
            connected_source_sha256="1" * 64,
            connected_source_bytes=123,
            appearance_target_url="https://autorig.test/u/token/textured.glb",
            appearance_target_sha256="2" * 64,
            appearance_target_bytes=456,
        )
        marker = main.SERVER_AUTHORIZATION_VALUE
        validation = {
            "schema": "autorig.paired-rig-source-dry-run-validation.v1",
            "valid": True,
            "artifacts": {
                "connectedSource": {"sha256": "1" * 64, "bytes": 123},
                "appearanceTarget": {"sha256": "2" * 64, "bytes": 456},
            },
        }
        task = SimpleNamespace(
            viewer_settings=json.dumps(
                {
                    "rig_source_transfer": transfer,
                    SERVER_TRANSFER_MARKER: marker,
                    SERVER_VALIDATION_EVIDENCE: validation,
                    "rig_v2_animal_detection": {"animal_type": "horse"},
                    "viewer_theme_selection": {"theme_id": "studio"},
                    "old_public_key": "replaceable",
                }
            )
        )
        db = _CommitSpy()
        endpoint = _raw_endpoint(main.api_set_task_viewer_settings)
        with (
            patch.object(main, "get_task_by_id", new=AsyncMock(return_value=task)),
            patch.object(main, "get_anon_session", new=AsyncMock(return_value=None)),
            patch.object(main, "_is_task_owner_or_admin", return_value=True),
        ):
            result = await endpoint(
                "task-1",
                _BodyRequest(json.dumps({"camera": {"fov": 42}}).encode()),
                Response(),
                _admin(),
                db,
            )

        saved = json.loads(task.viewer_settings)
        self.assertEqual(result, {"ok": True})
        self.assertEqual(db.commit_count, 1)
        self.assertEqual(saved["rig_source_transfer"], transfer)
        self.assertEqual(saved[SERVER_TRANSFER_MARKER], marker)
        self.assertEqual(saved[SERVER_VALIDATION_EVIDENCE], validation)
        self.assertEqual(saved["rig_v2_animal_detection"], {"animal_type": "horse"})
        self.assertEqual(saved["viewer_theme_selection"], {"theme_id": "studio"})
        self.assertEqual(saved["camera"], {"fov": 42})
        self.assertNotIn("old_public_key", saved)

    async def test_viewer_settings_get_never_exposes_server_transfer_state(self):
        task = SimpleNamespace(
            viewer_settings=json.dumps(
                {
                    "rig_source_transfer": {"secret": "worker contract"},
                    SERVER_TRANSFER_MARKER: main.SERVER_AUTHORIZATION_VALUE,
                    SERVER_VALIDATION_EVIDENCE: {"secret": "validation evidence"},
                    "camera": {"fov": 37},
                }
            )
        )
        endpoint = _raw_endpoint(main.api_get_task_viewer_settings)
        with (
            patch.object(main, "get_task_by_id", new=AsyncMock(return_value=task)),
            patch.object(main, "get_anon_session", new=AsyncMock(return_value=None)),
            patch.object(main, "_is_task_owner_or_admin", return_value=True),
            patch.object(main, "_read_global_viewer_camera_preset", return_value=None),
        ):
            result = await endpoint(
                "task-1",
                _BodyRequest(b""),
                Response(),
                _admin(),
                _CommitSpy(),
            )

        self.assertEqual(result["camera"], {"fov": 37})
        self.assertNotIn("rig_source_transfer", result)
        self.assertNotIn(SERVER_TRANSFER_MARKER, result)
        self.assertNotIn(SERVER_VALIDATION_EVIDENCE, result)


if __name__ == "__main__":
    unittest.main()
