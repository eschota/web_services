import json
import unittest
from pathlib import Path

from pydantic import ValidationError

from renderfin.models import RenderPrompt
from renderfin.queue import _inject_avatar_reference_images
from renderfin.templating import render_workflow_text


WORKFLOW = (
    Path(__file__).parents[1]
    / "renderfin"
    / "assets"
    / "workflows"
    / "gen_image_flux2_avatar.json"
)


class AvatarPromptContractTests(unittest.TestCase):
    def test_reference_urls_preserve_order_and_survive_roundtrip(self):
        urls = ["https://example.test/front.png", "https://example.test/scene.jpg"]
        prompt = RenderPrompt(reference_image_urls=urls)
        self.assertEqual(prompt.reference_image_urls, urls)
        self.assertEqual(
            RenderPrompt(**prompt.model_dump()).reference_image_urls,
            urls,
        )

    def test_reference_urls_are_bounded_to_one_through_four(self):
        with self.assertRaises(ValidationError):
            RenderPrompt(reference_image_urls=[f"https://e/{i}.png" for i in range(5)])
        with self.assertRaises(ValidationError):
            RenderPrompt(reference_image_urls=["https://e/ok.png", "  "])
        self.assertEqual(RenderPrompt(reference_image_urls=[]).reference_image_urls, [])
        self.assertEqual(RenderPrompt().reference_image_urls, [])


class AvatarWorkflowTests(unittest.TestCase):
    def _workflow(self):
        return render_workflow_text(
            WORKFLOW.read_text(encoding="utf-8"),
            width=960,
            height=540,
            prompt="Keep the identity from image 1 in the scene from image 2",
            negative_prompt="",
            image_filename="",
            output_prefix="avatar-test",
            randomize_seeds=False,
        )

    def test_builds_only_real_reference_branches_in_order(self):
        workflow = self._workflow()
        _inject_avatar_reference_images(workflow, ["front.png", "scene.png"])

        self.assertEqual(workflow["avatar_reference_1_image"]["inputs"]["image"], "front.png")
        self.assertEqual(workflow["avatar_reference_2_image"]["inputs"]["image"], "scene.png")
        self.assertEqual(
            workflow["avatar_reference_1_conditioning"]["inputs"]["conditioning"],
            ["positive", 0],
        )
        self.assertEqual(
            workflow["avatar_reference_2_conditioning"]["inputs"]["conditioning"],
            ["avatar_reference_1_conditioning", 0],
        )
        self.assertEqual(
            workflow["guider"]["inputs"]["conditioning"],
            ["avatar_reference_2_conditioning", 0],
        )
        loads = [node for node in workflow.values() if node.get("class_type") == "LoadImage"]
        self.assertEqual(len(loads), 2)

    def test_rejects_missing_or_excess_references(self):
        for filenames in ([], ["a", "b", "c", "d", "e"]):
            with self.assertRaises(ValueError):
                _inject_avatar_reference_images(self._workflow(), filenames)

    def test_template_is_valid_after_substitution(self):
        workflow = self._workflow()
        self.assertEqual(workflow["latent"]["inputs"]["width"], 960)
        self.assertEqual(workflow["latent"]["inputs"]["height"], 540)
        json.dumps(workflow)


if __name__ == "__main__":
    unittest.main()
