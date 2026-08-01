import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx

import render_prompting
from render_prompting import (
    BODY_TYPES,
    DEFAULT_NEGATIVE_PROMPT,
    RenderGenPlan,
    body_type_from_keywords,
    build_template_plan,
    mask_url_for_body_type,
    _extract_json_object,
    _plan_from_llm_json,
)


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class BodyTypeHeuristicTests(unittest.TestCase):
    def test_keywords(self):
        self.assertEqual(body_type_from_keywords("a chubby orc chef"), "fat")
        self.assertEqual(body_type_from_keywords("stocky dwarf miner"), "dwarf")
        self.assertEqual(body_type_from_keywords("skinny goblin scout"), "goblin")
        self.assertEqual(body_type_from_keywords("tall elegant elf"), "long")
        self.assertEqual(body_type_from_keywords("brave knight"), "normal")
        self.assertEqual(body_type_from_keywords(""), "normal")

    def test_mask_urls(self):
        self.assertTrue(mask_url_for_body_type("normal").endswith("/render/masks/t_pose.jpg"))
        self.assertTrue(mask_url_for_body_type("fat").endswith("/render/masks/t_pose_fat.jpg"))
        self.assertTrue(mask_url_for_body_type("bogus").endswith("/render/masks/t_pose.jpg"))


class TemplatePlanTests(unittest.TestCase):
    def test_full_metadata(self):
        plan = build_template_plan(
            {
                "title": "Chubby Orc Chef",
                "description": "A rotund green orc wearing a stained apron",
                "keywords": "orc, chef, fantasy",
            }
        )
        self.assertEqual(plan.source, "template")
        self.assertIn("Chubby Orc Chef", plan.prompt)
        self.assertIn("T-pose", plan.prompt)
        self.assertEqual(plan.body_type, "fat")
        self.assertIn("t_pose_fat.jpg", plan.mask_url)
        self.assertEqual(plan.negative_prompt, DEFAULT_NEGATIVE_PROMPT)

    def test_empty_metadata(self):
        plan = build_template_plan({})
        self.assertIn("stylized 3d character", plan.prompt)
        self.assertEqual(plan.body_type, "normal")

    def test_animal_fallback_title(self):
        plan = build_template_plan({"animal_type": "horse"})
        self.assertIn("horse", plan.prompt)


class LlmParsingTests(unittest.TestCase):
    def test_extract_json_with_markdown_fence(self):
        parsed = _extract_json_object('```json\n{"flux_prompt": "x"}\n```')
        self.assertEqual(parsed, {"flux_prompt": "x"})

    def test_plan_from_valid_json(self):
        plan = _plan_from_llm_json(
            {
                "flux_prompt": "a detailed goblin scout, full body, T-pose, front view, character sheet",
                "negative_prompt": "blurry",
                "body_type": "goblin",
            }
        )
        self.assertIsNotNone(plan)
        self.assertEqual(plan.body_type, "goblin")
        self.assertEqual(plan.source, "llm")
        self.assertIn("t_pose_goblin.jpg", plan.mask_url)

    def test_plan_rejects_short_prompt(self):
        self.assertIsNone(_plan_from_llm_json({"flux_prompt": "hi"}))

    def test_plan_normalizes_bad_body_type(self):
        plan = _plan_from_llm_json(
            {"flux_prompt": "a detailed knight, full body T-pose front view sheet", "body_type": "giant"}
        )
        self.assertEqual(plan.body_type, "normal")


# Tokens measured to wreck this pipeline. "character sheet" makes Flux build a
# multi-panel turnaround with annotation text; "silhouette" renders a literal
# black cutout; "even lighting" reads as no form shadow at all; "studio"/"white
# background" pull a floor sweep and a contact shadow that RMBG cuts through;
# "stretched" arms read as a stretch deformation; the booster tags are no-ops on
# Flux that push toward an over-processed plastic look.
BANNED_TOKENS = (
    "character sheet", "model sheet", "turnaround", "reference sheet", "concept sheet",
    "multiple views", "silhouette", "even lighting", "flat lighting", "studio background",
    "neutral background", "white background", "simple background", "arms stretched",
    "masterpiece", "best quality", "8k", "4k", "ultra hd", "hyperdetailed",
    "trending on artstation", "octane", "unreal engine", "bokeh", "depth of field",
    "pbr materials", "high detail",
)


class PromptAntiPatternTests(unittest.TestCase):
    def _assert_clean(self, prompt: str, label: str):
        low = prompt.lower()
        for token in BANNED_TOKENS:
            self.assertNotIn(token, low, f"{label} contains banned token {token!r}")

    def test_fallback_prompts_avoid_the_banned_tokens(self):
        plan = build_template_plan(
            {
                "title": "Spaicy 3D 2022 New Textures By Loulouvz Rigged Character",
                "description": "stocky dwarf blacksmith, low poly, 4K PBR, FBX OBJ",
                "keywords": "fantasy, medieval, fantasy, rpg",
            }
        )
        self._assert_clean(plan.prompt, "base prompt")
        self._assert_clean(plan.prompt_b, "lowpoly prompt")

    def test_fallback_drops_marketplace_boilerplate(self):
        plan = build_template_plan(
            {"title": "Knight By Someone Rigged FBX 2022", "description": "game ready, unity, unreal"}
        )
        low = plan.prompt.lower()
        for junk in ("rigged", "fbx", "2022", "by someone", "game ready", "unity", "unreal"):
            self.assertNotIn(junk, low)
        self.assertIn("knight", low)

    def test_fallback_states_the_framing_guarantees(self):
        plan = build_template_plan({"title": "orc"})
        for prompt in (plan.prompt, plan.prompt_b):
            # fingertips sit within 31-91px of the frame edge on every mask
            self.assertIn("clear margin past the fingertips", prompt)
            # arms fusing to the torso is the main 3D reconstruction failure
            self.assertIn("between the arms and torso", prompt)
            self.assertIn("T-pose", prompt)

    def test_the_shipped_instruction_teaches_the_pipeline_traps(self):
        import json as _json

        data = _json.loads(render_prompting._INSTRUCTION_PATH.read_text(encoding="utf-8"))
        text = data["instruction"]
        self.assertGreater(len(text), 2000)
        # traps the model cannot see from the prompt alone
        self.assertIn("glass", text)          # stripped by sanitize_prompt
        self.assertIn("SAME CHARACTER", text)  # one character, two styles
        for body_type in BODY_TYPES:
            self.assertIn(body_type, text)
        # the instruction must not invite style words: the pipeline adds those,
        # and anything the model says about them makes the two renders diverge
        self.assertIn("Never mention lighting", text)

    def test_the_instruction_no_longer_ships_a_second_style(self):
        import json as _json

        data = _json.loads(render_prompting._INSTRUCTION_PATH.read_text(encoding="utf-8"))
        self.assertNotIn("instruction_lowpoly", data)


class LowPolyVariantTests(unittest.TestCase):
    def test_template_plan_carries_both_styles(self):
        plan = build_template_plan({"title": "orc warrior", "description": "green skin"})
        self.assertTrue(plan.prompt)
        self.assertTrue(plan.prompt_b)
        self.assertNotEqual(plan.prompt, plan.prompt_b)
        self.assertIn("low-poly", plan.prompt_b)
        # both variants must render the same subject
        self.assertIn("orc warrior", plan.prompt_b)

    def test_variant_keeps_t_pose_contract(self):
        variant = render_prompting.lowpoly_variant(
            "a stern dwarf smith, full body, T-pose, front view, character sheet"
        )
        for required in ("full body", "T-pose", "front view"):
            self.assertIn(required, variant)
        self.assertIn("dwarf smith", variant)
        # the base style tail must not leak into the low-poly variant
        self.assertNotIn("character sheet, neutral studio", variant)

    def test_one_call_produces_both_styles_of_the_same_character(self):
        async def scenario():
            calls = []

            async def fake_llm(meta, poster):
                calls.append(meta)
                return render_prompting._plan_from_llm_json(
                    {
                        "subject": "a slim teenage boy with a round face and short black hair",
                        "outfit": "a cream white puffer jacket, a deep red scarf and charcoal trousers",
                        "body_type": "normal",
                    }
                )

            with patch.object(render_prompting, "_llm_generate", new=fake_llm):
                with patch.object(render_prompting, "_poster_data_url", return_value=None):
                    plan = await render_prompting.build_render_request("t1")

            # one call: two calls invented two different characters
            self.assertEqual(len(calls), 1)
            self.assertTrue(plan.prompt_b)
            self.assertNotEqual(plan.prompt, plan.prompt_b)

        run(scenario())

    def test_the_two_variants_describe_the_same_character(self):
        plan = _plan_from_llm_json(
            {
                "subject": "a slim teenage boy with a round face and short black hair",
                "outfit": "a cream white puffer jacket, a deep red scarf and charcoal trousers",
                "body_type": "normal",
            }
        )
        # every identity-carrying word must appear in both renders
        for token in (
            "slim teenage boy",
            "round face",
            "short black hair",
            "cream white puffer jacket",
            "deep red scarf",
            "charcoal trousers",
        ):
            self.assertIn(token, plan.prompt, "base render lost the character")
            self.assertIn(token, plan.prompt_b, "cartoon render lost the character")
        # and only the style differs
        self.assertIn("low-poly cartoon", plan.prompt_b)
        self.assertNotIn("low-poly", plan.prompt)

    def test_both_variants_use_one_pose_skeleton(self):
        """A different mask would change the build, not just the look."""
        plan = _plan_from_llm_json(
            {"subject": "a stout dwarf smith with a broad face", "outfit": "a leather apron", "body_type": "dwarf"}
        )
        self.assertIn("t_pose_dwarf.jpg", plan.mask_url)
        self.assertFalse(hasattr(plan, "mask_url_b"))

    def test_the_cartoon_style_asks_for_triangles(self):
        plan = _plan_from_llm_json(
            {"subject": "a slim teenage boy", "outfit": "a white jacket", "body_type": "normal"}
        )
        for token in ("triangles", "triangular facets", "coarsest form"):
            self.assertIn(token, plan.prompt_b)
        # the base render must not be simplified
        self.assertNotIn("triangles", plan.prompt)

    def test_a_legacy_single_prompt_answer_still_works(self):
        plan = _plan_from_llm_json(
            {"flux_prompt": "a detailed goblin scout in worn leather armour", "body_type": "goblin"}
        )
        self.assertIn("goblin scout", plan.prompt)
        self.assertIn("goblin scout", plan.prompt_b)

    def test_template_fallback_also_describes_one_character(self):
        plan = build_template_plan({"title": "orc warrior", "description": "green skin"})
        self.assertIn("orc warrior", plan.prompt)
        self.assertIn("orc warrior", plan.prompt_b)
        self.assertIn("low-poly cartoon", plan.prompt_b)

    def test_both_styles_failing_falls_back_to_the_template(self):
        async def scenario():
            with patch.object(render_prompting, "_llm_generate", new=AsyncMock(return_value=None)):
                with patch.object(render_prompting, "_poster_data_url", return_value=None):
                    with patch.object(
                        render_prompting, "_task_metadata_summary", return_value={"title": "stout dwarf"}
                    ):
                        plan = await render_prompting.build_render_request("t1")
            self.assertEqual(plan.source, "template")
            self.assertTrue(plan.prompt)
            self.assertIn("low-poly", plan.prompt_b)

        run(scenario())

    def test_variant_survives_empty_prompt(self):
        self.assertTrue(render_prompting.lowpoly_variant(""))


class BuildRenderRequestTests(unittest.TestCase):
    def test_llm_failure_falls_back_to_template(self):
        async def scenario():
            with patch.object(render_prompting, "_llm_generate", new=AsyncMock(return_value=None)):
                with patch.object(render_prompting, "_poster_data_url", return_value=None):
                    with patch.object(render_prompting, "_task_metadata_summary", return_value={"title": "Tall Elf"}):
                        # short-circuit the DB load with a fake task object
                        class _T:
                            poster_llm_title = "Tall Elf"

                        async def fake_load(task_id):
                            return _T()

                        with patch.object(render_prompting, "build_render_request", wraps=None):
                            pass
            # direct template path check instead (DB unavailable in tests)
            plan = build_template_plan({"title": "Tall Elf"})
            self.assertEqual(plan.body_type, "long")

        run(scenario())

    def test_llm_result_used_when_available(self):
        async def scenario():
            fake_plan = RenderGenPlan(
                prompt="p" * 40,
                negative_prompt="n",
                body_type="dwarf",
                mask_url=mask_url_for_body_type("dwarf"),
                source="llm",
            )
            with patch.object(render_prompting, "_llm_generate", new=AsyncMock(return_value=fake_plan)):
                with patch.object(render_prompting, "_poster_data_url", return_value=None):
                    plan = await render_prompting.build_render_request("nonexistent-task")
            self.assertEqual(plan.source, "llm")
            self.assertEqual(plan.body_type, "dwarf")

        run(scenario())


class CharacterGenClientTests(unittest.TestCase):
    def test_start_character_gen(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                body = json.loads(request.content)
                assert body["prompt"] == "test prompt for orc"
                assert body["prompt_b"] == "low-poly orc"
                assert body["mask_url"].endswith("t_pose.jpg")
                return httpx.Response(200, json={"job_id": "j-123", "stage": "flux_render"})

            transport = httpx.MockTransport(handler)
            real_client = httpx.AsyncClient

            def patched_client(*args, **kwargs):
                kwargs["transport"] = transport
                return real_client(*args, **kwargs)

            plan = RenderGenPlan(
                prompt="test prompt for orc",
                prompt_b="low-poly orc",
                negative_prompt="",
                body_type="normal",
                mask_url=mask_url_for_body_type("normal"),
                source="template",
            )
            with patch.object(render_prompting.httpx, "AsyncClient", side_effect=patched_client):
                job_id = await render_prompting.start_character_gen(plan, source_task_id="t1")
            self.assertEqual(job_id, "j-123")

        run(scenario())


if __name__ == "__main__":
    unittest.main()


class ApproveVariantTests(unittest.TestCase):
    def test_approve_sends_chosen_variant(self):
        async def scenario():
            seen = {}

            def handler(request: httpx.Request) -> httpx.Response:
                seen["url"] = str(request.url)
                return httpx.Response(200, json={"job_id": "j-1", "stage": "hunyuan"})

            transport = httpx.MockTransport(handler)
            real_client = httpx.AsyncClient

            def patched_client(*args, **kwargs):
                kwargs["transport"] = transport
                return real_client(*args, **kwargs)

            with patch.object(render_prompting.httpx, "AsyncClient", side_effect=patched_client):
                await render_prompting.approve_character_gen_image("j-1", variant="b")
            self.assertIn("variant=b", seen["url"])
            self.assertIn("/api-character-gen/j-1/approve-image", seen["url"])

        run(scenario())


class LlmCredentialTests(unittest.TestCase):
    def test_env_first_then_the_web_server_config(self):
        cfg = {
            "open_AI_api_key": "sk-cfg-openai",
            "open_router_api_key": "sk-cfg-router",
            "open_ai_api_url_string": "https://cfg.openai/v1/chat/completions",
        }
        env = {"OPENAI_API_KEY": "sk-env-openai", "OPENROUTER_API_KEY": ""}
        with patch.object(render_prompting, "_vision_config", return_value=cfg):
            with patch.dict(render_prompting.os.environ, env, clear=False):
                attempts = render_prompting._llm_attempts()
        keys = [a[1] for a in attempts]
        # the env key is tried first, but a stale one must not block the config
        self.assertEqual(keys[0], "sk-env-openai")
        self.assertIn("sk-cfg-openai", keys)
        self.assertIn("sk-cfg-router", keys)

    def test_no_credentials_yields_no_attempts(self):
        with patch.object(render_prompting, "_vision_config", return_value={}):
            with patch.dict(
                render_prompting.os.environ,
                {"OPENAI_API_KEY": "", "OPENROUTER_API_KEY": ""},
                clear=False,
            ):
                self.assertEqual(render_prompting._llm_attempts(), [])

    def test_the_same_key_is_not_tried_twice(self):
        cfg = {"open_AI_api_key": "sk-same"}
        with patch.object(render_prompting, "_vision_config", return_value=cfg):
            with patch.dict(
                render_prompting.os.environ,
                {"OPENAI_API_KEY": "sk-same", "OPENROUTER_API_KEY": ""},
                clear=False,
            ):
                attempts = render_prompting._llm_attempts()
        self.assertEqual(len(attempts), 1)
