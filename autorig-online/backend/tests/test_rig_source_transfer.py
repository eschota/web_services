import copy
import unittest

from rig_source_transfer import (
    MAX_POSITION_DELTA_M,
    RIG_SOURCE_TRANSFER_SCHEMA,
    WORKER_TRANSPORT_KEY,
    RigSourceTransferContractError,
    build_rig_source_transfer,
    normalize_rig_source_transfer,
    safe_pair_upload_basename,
)
from viewer_environment import build_viewer_environment_from_settings
from worker_payloads import build_worker_task_payload


SOURCE_SHA = "1" * 64
TARGET_SHA = "2" * 64


def valid_contract():
    return build_rig_source_transfer(
        connected_source_url="https://autorig.online/u/pair/rig_source_white_mesh.obj",
        connected_source_sha256=SOURCE_SHA,
        connected_source_bytes=1_462_410,
        appearance_target_url="https://autorig.online/u/pair/andalusian.glb",
        appearance_target_sha256=TARGET_SHA,
        appearance_target_bytes=10_722_104,
    )


class RigSourceTransferTests(unittest.TestCase):
    def test_builder_pins_both_artifacts_and_fail_closed_mapping_policy(self):
        value = valid_contract()
        self.assertEqual(value["schema"], RIG_SOURCE_TRANSFER_SCHEMA)
        self.assertEqual(value["connected_source"]["sha256"], SOURCE_SHA)
        self.assertEqual(value["appearance_target"]["sha256"], TARGET_SHA)
        self.assertEqual(
            value["mapping"]["source_component_policy"],
            "one_or_more_watertight_components",
        )
        self.assertTrue(value["mapping"]["require_each_source_component_watertight"])
        self.assertTrue(value["mapping"]["require_face_topology_identity"])
        self.assertTrue(value["mapping"]["require_duplicate_weight_identity"])
        self.assertEqual(
            value["mapping"]["max_position_delta_m"],
            MAX_POSITION_DELTA_M,
        )
        self.assertEqual(value["output_revision_policy"], "new_task_immutable")

    def test_contract_rejects_unpinned_or_relaxed_sources(self):
        cases = []
        insecure = valid_contract()
        insecure["connected_source"]["url"] = "http://autorig.online/u/pair/white.obj"
        cases.append(insecure)
        uppercase = valid_contract()
        uppercase["connected_source"]["sha256"] = "A" * 64
        cases.append(uppercase)
        relaxed = valid_contract()
        relaxed["mapping"]["max_position_delta_m"] = MAX_POSITION_DELTA_M * 2
        cases.append(relaxed)
        non_watertight = valid_contract()
        non_watertight["mapping"]["require_each_source_component_watertight"] = False
        cases.append(non_watertight)
        wrong_policy = valid_contract()
        wrong_policy["mapping"]["source_component_policy"] = "single_component"
        cases.append(wrong_policy)

        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(RigSourceTransferContractError):
                    normalize_rig_source_transfer(value)

    def test_contract_rejects_same_source_and_target_artifact(self):
        value = valid_contract()
        value["appearance_target"]["sha256"] = SOURCE_SHA
        with self.assertRaises(RigSourceTransferContractError):
            normalize_rig_source_transfer(value)

    def test_paired_upload_basename_is_cross_platform_and_fail_closed(self):
        self.assertEqual(
            safe_pair_upload_basename(
                r"C:\cache\white_mesh.obj",
                default="connected.obj",
            ),
            "white_mesh.obj",
        )
        self.assertEqual(
            safe_pair_upload_basename(
                "../../textured_mesh.glb",
                default="appearance.glb",
            ),
            "textured_mesh.glb",
        )
        with self.assertRaises(RigSourceTransferContractError):
            safe_pair_upload_basename("..", default="appearance.glb")

    def test_task_settings_transport_is_lifted_out_of_viewer_environment(self):
        transfer = valid_contract()
        environment = build_viewer_environment_from_settings(
            {"rig_source_transfer": transfer},
            app_url="https://autorig.online",
        )
        self.assertEqual(environment[WORKER_TRANSPORT_KEY], transfer)

        payload = build_worker_task_payload(
            "https://autorig.online/u/pair/andalusian.glb",
            "animal",
            pipeline_kind="rig",
            animal_type="horse",
            viewer_environment=environment,
        )
        self.assertEqual(payload["rig_source_transfer"], transfer)
        self.assertEqual(
            payload["input_url"],
            transfer["connected_source"]["url"],
        )
        self.assertEqual(
            payload["rig_source_transfer"]["appearance_target"]["url"],
            "https://autorig.online/u/pair/andalusian.glb",
        )
        self.assertNotIn("viewer_environment", payload)
        self.assertNotIn(WORKER_TRANSPORT_KEY, payload)

    def test_transport_rejects_contract_for_a_different_appearance_target(self):
        environment = {WORKER_TRANSPORT_KEY: valid_contract()}
        with self.assertRaisesRegex(ValueError, "appearance_target.url"):
            build_worker_task_payload(
                "https://autorig.online/u/pair/different.glb",
                "animal",
                pipeline_kind="rig",
                animal_type="horse",
                viewer_environment=environment,
            )

    def test_transport_does_not_mutate_durable_task_settings(self):
        transfer = valid_contract()
        settings = {"rig_source_transfer": copy.deepcopy(transfer)}
        environment = build_viewer_environment_from_settings(settings)
        payload = build_worker_task_payload(
            "https://autorig.online/u/pair/andalusian.glb",
            "animal",
            pipeline_kind="rig",
            animal_type="horse",
            viewer_environment=environment,
        )
        payload["rig_source_transfer"]["mapping"]["max_position_delta_m"] = 1e-9
        self.assertEqual(settings["rig_source_transfer"], transfer)

    def test_convert_payload_never_receives_paired_rig_source(self):
        environment = {WORKER_TRANSPORT_KEY: valid_contract()}
        payload = build_worker_task_payload(
            "https://autorig.online/u/pair/andalusian.glb",
            "animal",
            pipeline_kind="convert",
            viewer_environment=environment,
        )
        self.assertNotIn("rig_source_transfer", payload)
        self.assertNotIn("viewer_environment", payload)

    def test_paired_transport_preserves_existing_orientation_fields(self):
        environment = {WORKER_TRANSPORT_KEY: valid_contract()}
        orientation = {"front_axis": "+Y", "up_axis": "+Z"}
        payload = build_worker_task_payload(
            "https://autorig.online/u/pair/andalusian.glb",
            "animal",
            {
                "local_rotation": [0.0, 0.0, 0.0],
                "local_rotation_authoritative": True,
                "rig_orientation": orientation,
            },
            pipeline_kind="rig",
            animal_type="horse",
            viewer_environment=environment,
        )
        self.assertEqual(payload["local_rotation"], [0.0, 0.0, 0.0])
        self.assertIs(payload["local_rotation_authoritative"], True)
        self.assertEqual(payload["rig_orientation"], orientation)
        payload["rig_orientation"]["front_axis"] = "-Y"
        self.assertEqual(orientation["front_axis"], "+Y")


if __name__ == "__main__":
    unittest.main()
