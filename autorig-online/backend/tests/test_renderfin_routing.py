import unittest

from renderfin.models import RenderPrompt, RenderServer
from renderfin.routing import (
    WORKFLOW_ANIMATION_DEFAULT,
    WORKFLOW_GEN_IMAGE,
    WORKFLOW_IMAGE_TO_3D,
    WORKFLOW_INPAINT,
    WORKFLOW_OPEN_POSE,
    WORKFLOW_T_POSE,
    WORKFLOW_Z_DEPTH,
    clamp_image_dims,
    is_image_request,
    output_extension,
    resolve_runtime_workflow,
    resolve_workflow_file,
    scheduling_token,
    select_animation_workflow,
    server_can_run,
)


class RoutingImageBranchTests(unittest.TestCase):
    def test_no_image_url_is_image_request(self):
        p = RenderPrompt(prompt="a knight")
        self.assertTrue(is_image_request(p))
        self.assertEqual(output_extension(p), ".png")
        self.assertEqual(scheduling_token(p), WORKFLOW_GEN_IMAGE)
        self.assertEqual(resolve_workflow_file(p), (WORKFLOW_GEN_IMAGE, None))

    def test_typed_request_with_image_is_image_request(self):
        p = RenderPrompt(prompt="x", type="t_pose", image_url="https://h/render/masks/t_pose.jpg")
        self.assertTrue(is_image_request(p))
        self.assertEqual(output_extension(p), ".png")

    def test_t_pose_forces_1024_without_aspect_ratio(self):
        p = RenderPrompt(type="t_pose", image_url="https://h/m.jpg")
        wf, forced = resolve_workflow_file(p)
        self.assertEqual(wf, WORKFLOW_T_POSE)
        self.assertEqual(forced, (1024, 1024))

    def test_t_poses_alias(self):
        p = RenderPrompt(type="t_poses", image_url="https://h/m.jpg")
        wf, forced = resolve_workflow_file(p)
        self.assertEqual(wf, WORKFLOW_T_POSE)
        self.assertEqual(forced, (1024, 1024))

    def test_t_pose_with_aspect_ratio_falls_to_gen_image(self):
        p = RenderPrompt(type="t_pose", image_url="https://h/m.jpg", aspect_ratio=1.5)
        wf, forced = resolve_workflow_file(p)
        self.assertEqual(wf, WORKFLOW_GEN_IMAGE)
        self.assertIsNone(forced)

    def test_type_map(self):
        for ptype, expected in (
            ("z_depth", WORKFLOW_Z_DEPTH),
            ("open_pose", WORKFLOW_OPEN_POSE),
            ("inpaint", WORKFLOW_INPAINT),
            ("material", WORKFLOW_GEN_IMAGE),
        ):
            p = RenderPrompt(type=ptype, image_url="https://h/m.jpg")
            self.assertEqual(resolve_workflow_file(p)[0], expected, ptype)

    def test_sphere_png_routes_to_z_depth(self):
        # sphere.png in image_url wins over the default gen_image branch for any
        # typed image request that has no more specific mapping
        for ptype in ("material", "whatever"):
            p = RenderPrompt(prompt="x", type=ptype, image_url="https://h/masks/sphere.png")
            self.assertEqual(resolve_workflow_file(p)[0], WORKFLOW_Z_DEPTH, ptype)
        p2 = RenderPrompt(prompt="x", type="material", image_url="https://h/other.png")
        self.assertEqual(resolve_workflow_file(p2)[0], WORKFLOW_GEN_IMAGE)

    def test_image_to_3d(self):
        p = RenderPrompt(type="image_to_3d", image_url="https://h/iso.png")
        self.assertEqual(output_extension(p), ".glb")
        self.assertEqual(scheduling_token(p), WORKFLOW_IMAGE_TO_3D)
        self.assertEqual(resolve_workflow_file(p), (WORKFLOW_IMAGE_TO_3D, None))


class RoutingVideoBranchTests(unittest.TestCase):
    def test_image_url_without_type_is_video(self):
        p = RenderPrompt(prompt="run", image_url="https://h/frame.png")
        self.assertFalse(is_image_request(p))
        self.assertEqual(output_extension(p), ".mp4")
        self.assertEqual(scheduling_token(p), WORKFLOW_ANIMATION_DEFAULT)

    def test_canonical_animation_names_pass_through(self):
        self.assertEqual(
            select_animation_workflow("autorig_animal_loop_v1"), "autorig_animal_loop_v1"
        )
        self.assertEqual(select_animation_workflow(""), WORKFLOW_ANIMATION_DEFAULT)
        self.assertEqual(
            select_animation_workflow("gen_animation_by_url_ltx_19b_static_lora.json"),
            "gen_animation_by_url_ltx_19b_static_lora.json",
        )

    def test_unsafe_requested_workflow_falls_back(self):
        self.assertEqual(select_animation_workflow("../../etc/passwd"), WORKFLOW_ANIMATION_DEFAULT)


class RuntimeWorkflowTests(unittest.TestCase):
    def test_override_applies(self):
        s = RenderServer(
            render_server_name="w4090",
            workflow_overrides={"autorig_animal_loop_v1": "autorig_ltx2_animal_loop_v1_api.json"},
        )
        self.assertEqual(
            resolve_runtime_workflow(s, "autorig_animal_loop_v1"),
            "autorig_ltx2_animal_loop_v1_api.json",
        )

    def test_no_override_appends_json(self):
        s = RenderServer(render_server_name="w")
        self.assertEqual(resolve_runtime_workflow(s, "gen_image.json"), "gen_image.json")

    def test_unsafe_override_rejected(self):
        s = RenderServer(
            render_server_name="w",
            workflow_overrides={"x.json": "../evil.json"},
        )
        with self.assertRaises(ValueError):
            resolve_runtime_workflow(s, "x.json")

    def test_server_can_run(self):
        s = RenderServer(render_server_name="w", available_workflows=["gen_image.json"])
        self.assertTrue(server_can_run(s, "gen_image.json"))
        self.assertFalse(server_can_run(s, "image_to_3d.json"))


class ClampTests(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(clamp_image_dims(0, 0), (1024, 1024))

    def test_clamp_and_round(self):
        self.assertEqual(clamp_image_dims(2000, 50), (1024, 64))
        self.assertEqual(clamp_image_dims(1000, 700), (992, 704))


class PromptModelTests(unittest.TestCase):
    def test_frame_count_clamped(self):
        self.assertEqual(RenderPrompt(frame_count=999).frame_count, 300)
        self.assertEqual(RenderPrompt(frame_count=-5).frame_count, 0)

    def test_user_name_sanitized(self):
        self.assertEqual(RenderPrompt(user_name="../evil user").user_name, "..eviluser")
        self.assertEqual(RenderPrompt(user_name="").user_name, "default_user")


if __name__ == "__main__":
    unittest.main()
