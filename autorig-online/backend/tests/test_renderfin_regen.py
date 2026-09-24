"""AutoRig Regen v1: re-pose an existing task's character with Qwen-Image-Edit.

Routing, the qwen_edit workflow, the prompts, the job model, the regen_source
stage, the edit renders of the flux stage, the API and the delivery captions.
The queue's handling of the edit bundle is in test_renderfin_regen_quality.py,
the source fetching in test_renderfin_regen_source.py and the bot button in
test_renderfin_regen_telegram.py.
"""
import io
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from PIL import Image, ImageDraw

from renderfin import (
    character_gen,
    comfy_adapter,
    config,
    regen_prompts,
    regen_source,
    routing,
    telegram_delivery,
    templating,
    turntable,
)
from renderfin.character_gen import CharacterGenManager
from renderfin.models import (
    CHARGEN_KIND_GENERATE,
    CHARGEN_KIND_REGEN,
    CHARGEN_STAGE_AWAITING_IMAGE,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_FLUX,
    CHARGEN_STAGE_READY,
    CHARGEN_STAGE_REGEN_SOURCE,
    RENDER_TYPE_QWEN_EDIT,
    RENDER_TYPE_T_POSE,
    TASK_DONE,
    CharacterGenJob,
    RenderPrompt,
    RenderServer,
)
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry
from renderfin.telegram_delivery import TelegramDeliveryService

from test_render_prompting import BANNED_TOKENS
from test_renderfin_character_gen import (
    _Env,
    _idle_job,
    _InstantQueue,
    _wait_stage,
    run,
)
from test_renderfin_telegram_delivery import _FakeManager

TASK_ID = "c8691854-bdd2-4503-9281-fdc8cafdb0d7"
WORKFLOW = config.WORKFLOWS_DIR / routing.WORKFLOW_QWEN_EDIT


def _png(size=1024, *, figure=True, fmt="PNG", aspect=(1, 1)) -> bytes:
    width, height = size * aspect[0], size * aspect[1]
    image = Image.new("RGB", (width, height), (127, 127, 127))
    if figure:
        ImageDraw.Draw(image).rectangle(
            (width // 3, height // 6, 2 * width // 3, 5 * height // 6),
            fill=(180, 90, 40),
        )
    buffer = io.BytesIO()
    image.save(buffer, fmt)
    return buffer.getvalue()


def _glb() -> bytes:
    body = b"\x00" * 64
    return b"glTF" + (2).to_bytes(4, "little") + (12 + len(body)).to_bytes(4, "little") + body


async def _fake_fetch_glb(client, task_id, dest):
    Path(dest).parent.mkdir(parents=True, exist_ok=True)
    Path(dest).write_bytes(_glb())
    return Path(dest), "main app prepared.glb"


async def _fake_render_still(glb_path, out_png, *, view="front", size=1024, **kwargs):
    Path(out_png).write_bytes(_png(size))
    return Path(out_png)


async def _fake_turntable(glb_path, out_path, **kwargs):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_bytes(b"MP4!" * 500)
    return Path(out_path)


class _RegenEnv(_Env):
    """_Env plus every main-app path and URL the regen stage could reach."""

    def __init__(self):
        super().__init__()
        root = Path(self.tmp.name)
        self.patches += [
            patch.object(config, "MAIN_GLB_CACHE_DIR", root / "glb_cache"),
            patch.object(config, "MAIN_ARTIFACT_CACHE_DIR", root / "artifact-cache"),
            patch.object(config, "PREFLIGHT_RENDER_DIR", root / "preflight"),
            patch.object(config, "MAIN_APP_INTERNAL_URL", "http://main.invalid"),
            patch.object(config, "MAIN_APP_PUBLIC_URL", "https://site.invalid"),
        ]


class _InstantEditQueue(_InstantQueue):
    """_InstantQueue whose qwen_edit renders also leave their RMBG cut-out."""

    async def enqueue(self, prompt):
        task = await super().enqueue(prompt)
        if prompt.type == RENDER_TYPE_QWEN_EDIT and task.status == TASK_DONE:
            user_dir = config.RENDER_DIR / prompt.user_name
            (user_dir / f"{task.id}_Isolated.png").write_bytes(b"ISO" * 400)
            task.extra_outputs["isolated"] = (
                f"{config.PUBLIC_BASE_URL}/render/{prompt.user_name}/{task.id}_Isolated.png"
            )
            await self._persist(task)
        return task


def _queue_and_manager():
    """The queue keeps its own sqlite file: a second manager opened on a file
    the queue holds blocks for the whole busy_timeout (gotchas.md)."""
    registry = ServerRegistry()
    queue = _InstantEditQueue(registry, db_path=config.DB_DIR / "queue.db")
    return queue, CharacterGenManager(queue, db_path=config.DB_PATH)


async def _regen_job(manager, **fields):
    base = dict(
        kind=CHARGEN_KIND_REGEN,
        render_type=RENDER_TYPE_QWEN_EDIT,
        stage=CHARGEN_STAGE_REGEN_SOURCE,
        source_task_id=TASK_ID,
        regen_view="front",
        prompt_b="b",
    )
    base.update(fields)
    return await _idle_job(manager, **base)


class RoutingTests(unittest.TestCase):
    def test_qwen_edit_gets_its_own_scheduling_token(self):
        prompt = RenderPrompt(type="qwen_edit", prompt="x", image_url="https://h/src.png")
        self.assertEqual(routing.scheduling_token(prompt), "qwen_edit.json")
        self.assertEqual(routing.WORKFLOW_QWEN_EDIT, "qwen_edit.json")

    def test_qwen_edit_template_has_no_forced_size(self):
        prompt = RenderPrompt(type="qwen_edit", image_url="https://h/src.png")
        self.assertEqual(routing.select_image_workflow(prompt), ("qwen_edit.json", None))
        self.assertEqual(routing.resolve_workflow_file(prompt), ("qwen_edit.json", None))
        self.assertEqual(routing.output_extension(prompt), ".png")

    def test_t_pose_routing_is_unchanged(self):
        prompt = RenderPrompt(type="t_pose", image_url="https://h/render/masks/t_pose.jpg")
        self.assertEqual(routing.scheduling_token(prompt), "gen_image.json")
        self.assertEqual(routing.select_image_workflow(prompt), ("t_pose.json", (1024, 1024)))
        to_3d = RenderPrompt(type="image_to_3d", image_url="https://h/iso.png")
        self.assertEqual(routing.scheduling_token(to_3d), "image_to_3d.json")

    def test_only_boxes_advertising_qwen_edit_receive_it(self):
        with _Env():
            registry = ServerRegistry()
            for name, workflows in (
                ("flux-box", ["gen_image.json"]),
                ("qwen-box", ["gen_image.json", "qwen_edit.json"]),
            ):
                registry.save(RenderServer(
                    render_server_name=name,
                    render_server_url="http://127.0.0.1:1",
                    status="online",
                    available_workflows=workflows,
                ))
            queue = RenderQueue(registry, db_path=config.DB_PATH)
            picked = queue._pick_server("qwen_edit.json", {"flux-box": 0, "qwen-box": 50})
            self.assertEqual(picked.render_server_name, "qwen-box")
            registry.delete("qwen-box")
            self.assertIsNone(queue._pick_server("qwen_edit.json", {"flux-box": 0}))

    def test_a_box_can_point_the_token_at_its_own_workflow_file(self):
        server = RenderServer(
            render_server_name="big-box",
            available_workflows=["qwen_edit.json"],
            workflow_overrides={"qwen_edit.json": "qwen_edit_bf16"},
        )
        self.assertEqual(
            routing.resolve_runtime_workflow(server, "qwen_edit.json"), "qwen_edit_bf16.json"
        )
        plain = RenderServer(render_server_name="box", available_workflows=["qwen_edit.json"])
        self.assertEqual(routing.resolve_runtime_workflow(plain, "qwen_edit.json"), "qwen_edit.json")


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.text = WORKFLOW.read_text(encoding="utf-8")

    def _render(self, prompt="orc"):
        return templating.render_workflow_text(
            self.text,
            width=1024,
            height=1024,
            prompt=prompt,
            negative_prompt="ignored",
            image_filename="abc_src.png",
            output_prefix="task-1",
            workflow_type="qwen_edit",
        )

    def test_placeholders_are_exactly_the_edit_set(self):
        # $output_url_ is how the scanner reads $output_url_Isolated
        self.assertEqual(
            templating.workflow_placeholders(self.text),
            ("$image", "$output_url", "$output_url_", "$prompt"),
        )
        self.assertIn("$output_url_Isolated", self.text)
        self.assertNotIn("$negative_prompt", self.text)

    def test_valid_json_before_and_after_templating(self):
        json.loads(self.text)
        workflow = self._render('a "quoted"\nprompt')
        self.assertEqual(workflow["151"]["inputs"]["prompt"], 'a "quoted"\nprompt')
        self.assertNotIn("$", json.dumps(workflow))

    def test_exactly_one_load_image_reads_the_input(self):
        raw = json.loads(self.text)
        loaders = [node for node in raw.values() if node["class_type"] == "LoadImage"]
        self.assertEqual(len(loaders), 1)
        self.assertEqual(loaders[0]["inputs"], {"image": "$image"})
        rendered = [n for n in self._render().values() if n["class_type"] == "LoadImage"]
        self.assertEqual(rendered[0]["inputs"]["image"], "abc_src.png")

    def test_outputs_mirror_t_pose_full_and_isolated(self):
        workflow = self._render()
        saves = {
            node["inputs"]["filename_prefix"]: node["inputs"]["images"][0]
            for node in workflow.values()
            if node["class_type"] == "SaveImage"
        }
        self.assertEqual(set(saves), {"task-1", "task-1_Isolated"})
        rmbg = workflow[saves["task-1_Isolated"]]
        self.assertEqual(rmbg["class_type"], "RMBG")
        # the full render and the cut-out come from the same decoded image
        self.assertEqual(rmbg["inputs"]["image"][0], saves["task-1"])
        t_pose = json.loads((config.WORKFLOWS_DIR / "t_pose.json").read_text(encoding="utf-8"))
        expected = {k: v for k, v in t_pose["300"]["inputs"].items() if k != "image"}
        self.assertEqual({k: v for k, v in rmbg["inputs"].items() if k != "image"}, expected)

    def test_lightning_path_and_model_files(self):
        raw = json.loads(self.text)
        by_class = {}
        for node in raw.values():
            by_class.setdefault(node["class_type"], []).append(node["inputs"])
        self.assertEqual(
            by_class["UNETLoader"][0]["unet_name"], "qwen_image_edit_2511_fp8mixed.safetensors"
        )
        self.assertEqual(
            by_class["CLIPLoader"][0],
            {"clip_name": "qwen_2.5_vl_7b_fp8_scaled.safetensors", "type": "qwen_image", "device": "default"},
        )
        self.assertEqual(by_class["VAELoader"][0]["vae_name"], "qwen_image_vae.safetensors")
        self.assertEqual(
            by_class["LoraLoaderModelOnly"][0]["lora_name"],
            "Qwen-Image-Edit-2511-Lightning-4steps-V1.0-bf16.safetensors",
        )
        sampler = by_class["KSampler"][0]
        self.assertEqual(
            (sampler["steps"], sampler["cfg"], sampler["sampler_name"], sampler["scheduler"]),
            (4, 1, "euler", "simple"),
        )
        methods = by_class["FluxKontextMultiReferenceLatentMethod"]
        self.assertEqual({m["reference_latents_method"] for m in methods}, {"index_timestep_zero"})
        self.assertEqual(len(methods), 2)
        encoders = by_class["TextEncodeQwenImageEditPlus"]
        self.assertEqual(sorted(e["prompt"] for e in encoders), ["", "$prompt"])

    def test_every_link_targets_an_existing_node(self):
        workflow = self._render()
        for node_id, node in workflow.items():
            for name, value in node["inputs"].items():
                if isinstance(value, list):
                    self.assertIn(value[0], workflow, f"{node_id}.{name} -> {value}")

    def test_the_seed_is_randomised_or_pinned(self):
        pinned = templating.render_workflow_text(
            self.text, width=1024, height=1024, prompt="x", negative_prompt="",
            image_filename="a.png", output_prefix="t", workflow_type="qwen_edit", seed=4242,
        )
        self.assertEqual(pinned["169"]["inputs"]["seed"], 4242)


class PromptTests(unittest.TestCase):
    def _all(self):
        for view in regen_source.REGEN_VIEWS:
            faithful, clean = regen_prompts.build_regen_prompts(view)
            yield f"{view}/a", faithful
            yield f"{view}/b", clean
        yield "base body", regen_prompts.BASE_BODY_PROMPT

    def test_regen_prompts_avoid_the_banned_tokens(self):
        for label, prompt in self._all():
            low = prompt.lower()
            for token in BANNED_TOKENS:
                self.assertNotIn(token, low, f"{label} contains banned token {token!r}")
            self.assertNotIn("stretch", low, label)

    def test_prompts_survive_templating_unchanged(self):
        for label, prompt in self._all():
            # the templater silently deletes "glass"/"glasses" and clamps length
            self.assertNotIn("glass", prompt.lower(), label)
            self.assertEqual(templating.sanitize_prompt(prompt), prompt, label)

    def test_both_variants_state_the_load_bearing_clauses(self):
        for prompt in regen_prompts.build_regen_prompts("front"):
            self.assertIn("T-pose", prompt)
            self.assertIn("between each arm and the torso and between the two legs", prompt)
            self.assertIn("at shoulder height", prompt)
            self.assertIn("palms facing down", prompt)
            self.assertIn("clear margin", prompt)
            self.assertIn("mid-grey backdrop", prompt)
            self.assertIn("Keep the same character", prompt)

    def test_only_variant_b_cleans_up_for_3d(self):
        faithful, clean = regen_prompts.build_regen_prompts("front")
        self.assertNotIn("locks", faithful)
        self.assertNotIn("weapons", faithful)
        for phrase in ("large solid locks", "crisp, clean edges", "weapons, props", "see-through or fuzzy"):
            self.assertIn(phrase, clean)

    def test_a_non_front_view_asks_to_turn_around(self):
        front, _ = regen_prompts.build_regen_prompts("front")
        back, _ = regen_prompts.build_regen_prompts("back")
        self.assertNotIn("turn", front)
        self.assertIn("turn it around to face the viewer", back)

    def test_base_body_prompt_never_asks_for_skin(self):
        low = regen_prompts.BASE_BODY_PROMPT.lower()
        self.assertIn("mannequin bodysuit", low)
        self.assertIn("bald head", low)
        for word in ("nude", "naked", "bare", "skin ", "underwear"):
            self.assertNotIn(word, low)


class JobModelTests(unittest.TestCase):
    NEW_FIELDS = ("kind", "render_type", "source_image_url", "regen_view")

    def test_old_job_json_without_regen_fields_still_loads(self):
        async def scenario():
            with _RegenEnv():
                legacy = CharacterGenJob(prompt="orc", user_name="bot", seq=7).model_dump()
                for field in self.NEW_FIELDS:
                    legacy.pop(field)
                legacy["stage"] = CHARGEN_STAGE_AWAITING_IMAGE
                config.DB_DIR.mkdir(parents=True, exist_ok=True)
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()  # creates the schema
                await manager.stop()
                with sqlite3.connect(str(config.DB_PATH)) as db:
                    db.execute(
                        "INSERT INTO chargen_jobs(id, payload, stage, created_at) VALUES(?,?,?,?)",
                        (legacy["id"], json.dumps(legacy), legacy["stage"], legacy["created_at"]),
                    )
                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager2.start()
                try:
                    job = manager2.get(legacy["id"])
                    self.assertIsNotNone(job, "a pre-regen row was skipped on load")
                    self.assertEqual(job.kind, CHARGEN_KIND_GENERATE)
                    self.assertEqual(job.render_type, RENDER_TYPE_T_POSE)
                    self.assertEqual((job.source_image_url, job.regen_view), ("", ""))
                    self.assertEqual(job.stage, CHARGEN_STAGE_AWAITING_IMAGE)
                finally:
                    await manager2.stop()
                    await queue.stop()

        run(scenario())

    def test_public_dict_reports_regen_fields(self):
        generated = CharacterGenJob(prompt="orc").public_dict()
        self.assertEqual((generated["kind"], generated["render_type"]), ("generate", "t_pose"))
        self.assertIsNone(generated["source_image_url"])
        regen = CharacterGenJob(
            kind="regen", render_type="qwen_edit", source_task_id=TASK_ID,
            source_image_url="https://x/src.png", regen_view="front",
        ).public_dict()
        self.assertEqual(
            (regen["kind"], regen["render_type"], regen["source_image_url"], regen["regen_view"]),
            ("regen", "qwen_edit", "https://x/src.png", "front"),
        )


class CreateRegenTests(unittest.TestCase):
    def test_create_regen_starts_at_regen_source_with_both_edit_prompts(self):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    with patch.object(manager, "_spawn") as spawn:
                        job = await manager.create_regen(
                            TASK_ID.upper(), telegram_chat_id=777
                        )
                    spawn.assert_called_once_with(job)
                    faithful, clean = regen_prompts.build_regen_prompts("front")
                    self.assertEqual(job.kind, CHARGEN_KIND_REGEN)
                    self.assertEqual(job.render_type, RENDER_TYPE_QWEN_EDIT)
                    self.assertEqual(job.stage, CHARGEN_STAGE_REGEN_SOURCE)
                    self.assertEqual(job.source_task_id, TASK_ID)  # canonical
                    self.assertEqual(job.regen_view, "front")
                    self.assertEqual((job.prompt, job.prompt_b), (faithful, clean))
                    self.assertEqual((job.negative_prompt, job.mask_url), ("", ""))
                    self.assertEqual(job.source_image_url, "")
                    self.assertEqual(job.user_name, "autorig-bot")
                    self.assertEqual(job.telegram_chat_id, 777)
                    self.assertEqual(job.queue_class, "interactive")
                    self.assertGreater(job.seq, 0)
                    self.assertIs(manager.get(job.id), job)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_bad_ids_and_views_are_refused_before_anything_is_stored(self):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    for task_id, view in (
                        ("../../etc/passwd", "front"),
                        ("", "front"),
                        ("not-a-uuid", "front"),
                        (TASK_ID, "top_side_45"),
                        (TASK_ID, "sideways"),
                    ):
                        with self.subTest(task_id=task_id, view=view):
                            with self.assertRaises(ValueError):
                                await manager.create_regen(task_id, view=view)
                    self.assertEqual(manager.all_jobs(), [])
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_user_name_gets_the_render_path_safety(self):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    with patch.object(manager, "_spawn"):
                        job = await manager.create_regen(TASK_ID, user_name="../../root")
                    self.assertNotIn("/", job.user_name)
                    path = manager._regen_source_path(job).resolve()
                    self.assertIn(config.RENDER_DIR.resolve(), path.parents)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())


class RegenSourceStageTests(unittest.TestCase):
    """regen_source: render the still, fall back to the poster, park on a blip."""

    def _scenario(self, body):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    await body(queue, manager)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_success_renders_the_still_into_the_job_render_dir(self):
        async def body(queue, manager):
            job = await _regen_job(manager)
            render = AsyncMock(side_effect=_fake_render_still)
            with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                    patch.object(turntable, "render_still", render, create=True):
                await manager._stage_regen_source(job)

            out = config.RENDER_DIR / "bot" / f"{job.id}_regen_source.png"
            self.assertEqual(job.stage, CHARGEN_STAGE_FLUX)
            self.assertTrue(out.is_file())
            self.assertEqual(
                job.source_image_url,
                f"{config.PUBLIC_BASE_URL}/render/bot/{job.id}_regen_source.png",
            )
            self.assertEqual(job.warning, "")
            glb_path = render.await_args.args[0]
            self.assertEqual(render.await_args.kwargs, {"view": "front", "size": 1024})
            # the downloaded model is scratch, the still is the artifact
            self.assertFalse(Path(glb_path).exists())
            self.assertEqual(list(config.TMP_DIR.glob("regen_*")), [])
            self.assertEqual([p.name for p in out.parent.glob(".*")], [])

            # the queue reads it as an own artifact: from disk, never over HTTP
            def no_network(request):
                raise AssertionError(f"fetched {request.url} over HTTP")

            async with httpx.AsyncClient(transport=httpx.MockTransport(no_network)) as client:
                name, data = await comfy_adapter.download_input_image(client, job.source_image_url)
            self.assertEqual(data, out.read_bytes())

        self._scenario(body)

    def test_the_requested_view_reaches_the_still_renderer(self):
        async def body(queue, manager):
            job = await _regen_job(manager, regen_view="back")
            render = AsyncMock(side_effect=_fake_render_still)
            with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                    patch.object(turntable, "render_still", render, create=True):
                await manager._stage_regen_source(job)
            self.assertEqual(render.await_args.kwargs["view"], "back")

        self._scenario(body)

    def test_no_model_falls_back_to_the_poster_with_a_warning(self):
        async def body(queue, manager):
            job = await _regen_job(manager)
            render = AsyncMock(side_effect=_fake_render_still)
            poster = _png(360, fmt="JPEG", aspect=(2, 1))  # 720x360 gallery poster
            with patch.object(
                regen_source, "fetch_glb",
                side_effect=regen_source.RegenSourceError(f"task {TASK_ID} has no model GLB"),
            ), patch.object(
                regen_source, "fetch_poster", AsyncMock(return_value=(poster, "main app thumb"))
            ), patch.object(turntable, "render_still", render, create=True):
                await manager._stage_regen_source(job)

            render.assert_not_awaited()
            self.assertEqual(job.stage, CHARGEN_STAGE_FLUX)
            self.assertIn("постер", job.warning)
            self.assertIn("нет модели", job.warning)
            with Image.open(manager._regen_source_path(job)) as still:
                self.assertEqual((still.format, still.size), ("PNG", (1024, 1024)))
                # letterboxed onto the still renderer's backdrop
                self.assertEqual(still.convert("RGB").getpixel((5, 5)), (127, 127, 127))

        self._scenario(body)

    def test_an_unreachable_model_parks_without_spending_an_attempt(self):
        async def body(queue, manager):
            job = await _regen_job(manager)
            poster = AsyncMock()
            with patch.object(
                regen_source, "fetch_glb",
                side_effect=regen_source.RegenSourceUnavailable(
                    "model unreachable (main app prepared.glb: ConnectError)"
                ),
            ), patch.object(regen_source, "fetch_poster", poster):
                await manager._run(job)

            poster.assert_not_awaited()  # a blip is waited out, not settled
            self.assertEqual(job.stage, CHARGEN_STAGE_REGEN_SOURCE)
            self.assertEqual(job.attempts, {})
            self.assertEqual(job.error, "")
            self.assertIn("unreachable", job.last_error)
            self.assertAlmostEqual(
                job.retry_at, time.time() + character_gen.REGEN_SOURCE_PARK_SECONDS, delta=5
            )
            # the window keeps running while parked: it is what ends the wait
            self.assertEqual(job.timed_stage, CHARGEN_STAGE_REGEN_SOURCE)
            self.assertGreater(job.stage_started_at, 0)

        self._scenario(body)

    def test_the_stage_clock_survives_a_restart(self):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                job = await _regen_job(manager)
                with patch.object(
                    regen_source, "fetch_glb",
                    side_effect=regen_source.RegenSourceUnavailable("model unreachable"),
                ):
                    await manager._run(job)
                started_at, retry_at = job.stage_started_at, job.retry_at
                job.stage_started_at -= 600  # ten minutes of waiting already
                await manager._persist(job)
                await manager.stop()

                manager2 = CharacterGenManager(queue, db_path=config.DB_PATH)
                await manager2.start()
                try:
                    revived = manager2.get(job.id)
                    self.assertEqual(revived.stage, CHARGEN_STAGE_REGEN_SOURCE)
                    self.assertEqual(revived.timed_stage, CHARGEN_STAGE_REGEN_SOURCE)
                    self.assertAlmostEqual(revived.stage_started_at, started_at - 600, delta=0.01)
                    self.assertAlmostEqual(revived.retry_at, retry_at, delta=0.01)
                    # parked jobs are left to the retry loop, not re-run at start
                    self.assertNotIn(job.id, manager2._runners)
                    budget = manager2._stage_budget(revived, character_gen.REGEN_SOURCE_STAGE_TIMEOUT)
                    self.assertLess(
                        budget, character_gen.REGEN_SOURCE_STAGE_TIMEOUT - 590,
                        "restart handed the stage a fresh window",
                    )
                finally:
                    await manager2.stop()
                    await queue.stop()

        run(scenario())

    def test_a_spent_window_stops_waiting_and_uses_the_poster(self):
        async def body(queue, manager):
            job = await _regen_job(
                manager,
                timed_stage=CHARGEN_STAGE_REGEN_SOURCE,
                stage_started_at=time.time() - character_gen.REGEN_SOURCE_STAGE_TIMEOUT - 5,
            )
            with patch.object(
                regen_source, "fetch_glb",
                side_effect=regen_source.RegenSourceUnavailable("model unreachable (status 502)"),
            ), patch.object(
                regen_source, "fetch_poster", AsyncMock(return_value=(_png(512), "site thumb"))
            ):
                await manager._stage_regen_source(job)
            self.assertEqual(job.stage, CHARGEN_STAGE_FLUX)
            self.assertIn("модель недоступна", job.warning)
            self.assertTrue(manager._regen_source_ready(job))

        self._scenario(body)

    def test_an_unreachable_poster_after_the_window_spends_an_attempt(self):
        async def body(queue, manager):
            job = await _regen_job(
                manager,
                timed_stage=CHARGEN_STAGE_REGEN_SOURCE,
                stage_started_at=time.time() - character_gen.REGEN_SOURCE_STAGE_TIMEOUT - 5,
            )
            with patch.object(
                regen_source, "fetch_glb",
                side_effect=regen_source.RegenSourceUnavailable("model unreachable"),
            ), patch.object(
                regen_source, "fetch_poster",
                side_effect=regen_source.RegenSourceUnavailable("poster unreachable"),
            ):
                await manager._run(job)
            self.assertEqual(job.attempts, {CHARGEN_STAGE_REGEN_SOURCE: 1})
            self.assertEqual(job.stage, CHARGEN_STAGE_REGEN_SOURCE)
            self.assertEqual(job.error, "")
            # a counted retry earns a fresh window
            self.assertEqual(job.stage_started_at, 0)

        self._scenario(body)

    def test_a_first_render_failure_is_retried_with_a_neutral_error(self):
        async def body(queue, manager):
            job = await _regen_job(manager)
            # the renderer's own output can read like a farm outage
            failing = AsyncMock(side_effect=turntable.TurntableError(
                "still render exited 1: Navigation timed out after 30000 ms"
            ))
            poster = AsyncMock()
            with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                    patch.object(regen_source, "fetch_poster", poster), \
                    patch.object(turntable, "render_still", failing, create=True):
                await manager._run(job)
            poster.assert_not_awaited()
            self.assertEqual(job.attempts, {CHARGEN_STAGE_REGEN_SOURCE: 1})
            self.assertEqual(job.last_error, "still render failed (TurntableError)")
            # an ordinary backoff, not the half-hour farm-breakage park
            self.assertLessEqual(
                job.retry_at, time.time() + character_gen.RETRY_BACKOFF_SECONDS[0] + 5
            )
            job.error = job.last_error
            self.assertFalse(character_gen._failed_on_recoverable_infrastructure(job))

        self._scenario(body)

    def test_a_second_render_failure_uses_the_poster(self):
        async def body(queue, manager):
            job = await _regen_job(manager, attempts={CHARGEN_STAGE_REGEN_SOURCE: 1})
            failing = AsyncMock(side_effect=turntable.TurntableError("still render is blank"))
            with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                    patch.object(
                        regen_source, "fetch_poster", AsyncMock(return_value=(_png(640), "main app thumb"))
                    ), patch.object(turntable, "render_still", failing, create=True):
                await manager._stage_regen_source(job)
            self.assertEqual(job.stage, CHARGEN_STAGE_FLUX)
            self.assertIn("модель не рендерится", job.warning)

        self._scenario(body)

    def test_a_blank_still_counts_as_a_failed_render(self):
        async def body(queue, manager):
            job = await _regen_job(manager)

            async def blank(glb_path, out_png, **kwargs):
                Path(out_png).write_bytes(_png(1024, figure=False))
                return Path(out_png)

            with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                    patch.object(turntable, "render_still", AsyncMock(side_effect=blank), create=True):
                await manager._run(job)
            self.assertEqual(job.attempts, {CHARGEN_STAGE_REGEN_SOURCE: 1})
            self.assertIn("blank", job.last_error)
            self.assertFalse(manager._regen_source_path(job).exists())

        self._scenario(body)

    def test_a_missing_still_renderer_uses_the_poster(self):
        async def body(queue, manager):
            job = await _regen_job(manager)
            with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                    patch.object(turntable, "render_still", None, create=True), \
                    patch.object(
                        regen_source, "fetch_poster", AsyncMock(return_value=(_png(512), "preflight render"))
                    ):
                await manager._stage_regen_source(job)
            self.assertEqual(job.stage, CHARGEN_STAGE_FLUX)
            self.assertIn("нет рендера стилла", job.warning)

        self._scenario(body)

    def test_nothing_to_show_fails_after_the_attempts_and_is_not_revived(self):
        async def body(queue, manager):
            job = await _regen_job(
                manager, attempts={CHARGEN_STAGE_REGEN_SOURCE: character_gen.MAX_STAGE_ATTEMPTS - 1}
            )
            with patch.object(
                regen_source, "fetch_glb",
                side_effect=regen_source.RegenSourceError(f"task {TASK_ID} has no model GLB"),
            ), patch.object(
                regen_source, "fetch_poster",
                side_effect=regen_source.RegenSourceError(
                    f"task {TASK_ID} has neither a model nor a poster"
                ),
            ):
                await manager._run(job)
            self.assertEqual(job.stage, CHARGEN_STAGE_FAILED)
            self.assertIn("neither a model nor a poster", job.error)
            self.assertFalse(character_gen._failed_on_recoverable_infrastructure(job))

        self._scenario(body)


class RegenFluxStageTests(unittest.TestCase):
    def _scenario(self, body):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    await body(queue, manager)
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    async def _ready_regen_job(self, manager):
        job = await _regen_job(manager, stage=CHARGEN_STAGE_FLUX, prompt_b="clean")
        job.prompt = "faithful"  # _idle_job fixes the prompt
        source = manager._regen_source_path(job)
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(_png(1024))
        job.source_image_url = f"{config.PUBLIC_BASE_URL}/render/bot/{source.name}"
        return job

    def test_both_variants_edit_the_same_source_still(self):
        async def body(queue, manager):
            job = await self._ready_regen_job(manager)
            await manager._stage_flux(job)
            self.assertEqual(job.stage, CHARGEN_STAGE_AWAITING_IMAGE, job.error)
            self.assertEqual(len(queue.enqueued), 2)
            for task in queue.enqueued:
                self.assertEqual(task.prompt.type, RENDER_TYPE_QWEN_EDIT)
                self.assertEqual(task.prompt.image_url, job.source_image_url)
                self.assertEqual(task.prompt.negative_prompt, "")
                self.assertEqual(task.workflow, "qwen_edit.json")
                self.assertEqual(task.workflow_file, "qwen_edit.json")
            self.assertEqual(
                [t.prompt.prompt for t in queue.enqueued], ["faithful", "clean"]
            )
            self.assertTrue(job.isolated_url and job.isolated_url_b)

        self._scenario(body)

    def test_t_pose_jobs_still_render_flux_on_their_mask(self):
        async def body(queue, manager):
            job = await _idle_job(
                manager, stage=CHARGEN_STAGE_FLUX, mask_url="https://x/render/masks/t_pose_fat.jpg"
            )
            task = await manager._enqueue_flux(job, job.prompt)
            self.assertEqual(task.prompt.type, "t_pose")
            self.assertEqual(task.prompt.image_url, "https://x/render/masks/t_pose_fat.jpg")
            self.assertEqual(task.workflow_file, "t_pose.json")

        self._scenario(body)

    def test_a_missing_source_sends_the_job_back_to_regen_source(self):
        async def body(queue, manager):
            job = await _regen_job(
                manager, stage=CHARGEN_STAGE_FLUX,
                source_image_url=f"{config.PUBLIC_BASE_URL}/render/bot/gone.png",
            )
            await manager._stage_flux(job)
            self.assertEqual(queue.enqueued, [])
            self.assertEqual(job.stage, CHARGEN_STAGE_REGEN_SOURCE)
            self.assertEqual(job.source_image_url, "")
            self.assertLessEqual(job.retry_at, time.time())
            self.assertGreater(job.retry_at, 0)

        self._scenario(body)

    def test_a_pose_miss_spends_an_attempt_but_a_box_fault_does_not(self):
        async def body(queue, manager):
            content = await _regen_job(manager, stage=CHARGEN_STAGE_FLUX)
            await manager._handle_stage_error(content, RuntimeError(
                "render task t1 failed: render artifact quality rejected on qwen-box: "
                "qwen_edit_pose_not_spread: the figure is not spread like a T-pose"
            ))
            self.assertEqual(content.attempts, {CHARGEN_STAGE_FLUX: 1})

            box = await _regen_job(manager, stage=CHARGEN_STAGE_FLUX)
            await manager._handle_stage_error(box, RuntimeError(
                "render task t2 failed: render artifact quality rejected on qwen-box: "
                "isolated_rgba_required: isolated image must decode as RGBA"
            ))
            self.assertEqual(box.attempts, {})
            self.assertAlmostEqual(
                box.retry_at, time.time() + character_gen.SLOT_WAIT_SECONDS, delta=5
            )

        self._scenario(body)


class RegenPipelineTests(unittest.TestCase):
    def test_regen_runs_end_to_end_through_the_shared_stages(self):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                            patch.object(
                                turntable, "render_still",
                                AsyncMock(side_effect=_fake_render_still), create=True,
                            ), patch.object(
                                turntable, "render_turntable", AsyncMock(side_effect=_fake_turntable)
                            ):
                        job = await manager.create_regen(TASK_ID, telegram_chat_id=777)
                        job = await _wait_stage(
                            manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE, CHARGEN_STAGE_FAILED}
                        )
                        self.assertEqual(job.stage, CHARGEN_STAGE_AWAITING_IMAGE, job.last_error)
                        variant_b = job.isolated_url_b
                        job, moved = await manager.approve_image(job.id, "b")
                        self.assertTrue(moved)
                        job = await _wait_stage(
                            manager, job.id, {CHARGEN_STAGE_READY, CHARGEN_STAGE_FAILED}
                        )
                    self.assertEqual(job.stage, CHARGEN_STAGE_READY, job.error)
                    self.assertEqual(job.chosen_variant, "b")
                    self.assertTrue(job.glb_url.endswith(".glb"))
                    self.assertTrue(job.video_url.endswith("_turntable.mp4"))
                    to_3d = [t for t in queue.enqueued if t.prompt.type == "image_to_3d"]
                    self.assertEqual([t.prompt.image_url for t in to_3d], [variant_b])
                    edits = [t for t in queue.enqueued if t.prompt.type == RENDER_TYPE_QWEN_EDIT]
                    self.assertEqual(len(edits), 2)
                    payload = job.public_dict()
                    # the bot auto-submits ready jobs owned by autorig-bot
                    self.assertEqual((payload["kind"], payload["user_name"]), ("regen", "autorig-bot"))
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_resume_before_the_still_existed_rebuilds_it(self):
        async def scenario():
            with _RegenEnv(), patch.object(character_gen, "RETRY_TICK_SECONDS", 0.05):
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _regen_job(
                        manager, stage=CHARGEN_STAGE_FAILED, error="task has no model GLB",
                        attempts={CHARGEN_STAGE_REGEN_SOURCE: 3},
                    )
                    with patch.object(regen_source, "fetch_glb", side_effect=_fake_fetch_glb), \
                            patch.object(
                                turntable, "render_still",
                                AsyncMock(side_effect=_fake_render_still), create=True,
                            ):
                        _, resumed = await manager.resume(job.id)
                        self.assertTrue(resumed)
                        job = await _wait_stage(
                            manager, job.id, {CHARGEN_STAGE_AWAITING_IMAGE, CHARGEN_STAGE_FAILED},
                            timeout=10.0,
                        )
                    self.assertEqual(job.stage, CHARGEN_STAGE_AWAITING_IMAGE, job.last_error)
                    self.assertTrue(manager._regen_source_ready(job))
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())

    def test_discard_removes_the_source_still(self):
        async def scenario():
            with _RegenEnv():
                queue, manager = _queue_and_manager()
                await queue.start()
                await manager.start()
                try:
                    job = await _regen_job(manager, stage=CHARGEN_STAGE_AWAITING_IMAGE)
                    source = manager._regen_source_path(job)
                    source.parent.mkdir(parents=True, exist_ok=True)
                    source.write_bytes(_png(256))
                    job.source_image_url = f"{config.PUBLIC_BASE_URL}/render/bot/{source.name}"
                    await manager.discard(job.id)
                    self.assertFalse(source.exists())
                finally:
                    await manager.stop()
                    await queue.stop()

        run(scenario())


class RegenApiTests(unittest.TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient

        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [
            patch.object(config, "DATA_DIR", root),
            patch.object(config, "RENDER_DIR", root / "render"),
            patch.object(config, "DB_DIR", root / "db"),
            patch.object(config, "TMP_DIR", root / "tmp"),
            patch.object(config, "SERVERS_DIR", root / "servers"),
            patch.object(config, "DB_PATH", root / "db" / "renderfin.db"),
            patch.object(config, "HUNYUAN_WORKERS_FILE", root / "no-workers.json"),
            # the endpoint is under test, not the stage: nothing may run it
            patch.object(CharacterGenManager, "_spawn"),
        ]
        for p in self.patches:
            p.start()
        from renderfin.app import app  # noqa: WPS433 (import after patching)

        self.client = TestClient(app)
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None, None, None)
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def test_regen_endpoint_creates_a_regen_job(self):
        resp = self.client.post(
            "/renderfin/api-character-gen/regen",
            json={"task_id": TASK_ID, "telegram_chat_id": 777},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        data = resp.json()
        self.assertEqual(data["kind"], "regen")
        self.assertEqual(data["render_type"], "qwen_edit")
        self.assertEqual(data["stage"], "regen_source")
        self.assertEqual(data["source_task_id"], TASK_ID)
        self.assertEqual(data["regen_view"], "front")
        self.assertEqual(data["telegram_chat_id"], 777)
        status = self.client.get(f"/renderfin/api-character-gen/{data['job_id']}")
        self.assertEqual(status.json()["stage"], "regen_source")
        listed = self.client.get("/renderfin/api-character-gen").json()["jobs"]
        self.assertIn(data["job_id"], [j["job_id"] for j in listed])

    def test_regen_endpoint_takes_a_view(self):
        resp = self.client.post(
            "/renderfin/api-character-gen/regen", json={"task_id": TASK_ID, "view": "left"}
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["regen_view"], "left")

    def test_regen_endpoint_rejects_a_bad_task_id(self):
        for task_id in ("../../etc/passwd", "12345", ""):
            with self.subTest(task_id=task_id):
                resp = self.client.post(
                    "/renderfin/api-character-gen/regen", json={"task_id": task_id}
                )
                self.assertEqual(resp.status_code, 400)
                self.assertIn("UUID", resp.json()["detail"])

    def test_regen_endpoint_rejects_an_unknown_view(self):
        resp = self.client.post(
            "/renderfin/api-character-gen/regen", json={"task_id": TASK_ID, "view": "top"}
        )
        self.assertEqual(resp.status_code, 400)

    def test_regen_endpoint_requires_a_task_id(self):
        resp = self.client.post("/renderfin/api-character-gen/regen", json={})
        self.assertEqual(resp.status_code, 422)


class RegenDeliveryTests(unittest.TestCase):
    def _job(self, **fields):
        base = dict(
            kind=CHARGEN_KIND_REGEN,
            render_type=RENDER_TYPE_QWEN_EDIT,
            source_task_id=TASK_ID,
            prompt="Redraw the character from image 1 standing in a clean T-pose.",
            telegram_chat_id=777,
            seq=12,
        )
        base.update(fields)
        return CharacterGenJob(**base)

    def test_the_variant_album_names_the_task_and_the_variants(self):
        async def scenario():
            sent = []

            def handler(request):
                sent.append((request.url.path, dict(httpx.QueryParams(request.content.decode()))))
                if request.url.path.endswith("sendMediaGroup"):
                    return httpx.Response(
                        200, json={"ok": True, "result": [{"message_id": 1}, {"message_id": 2}]}
                    )
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 3}})

            job = self._job(
                stage=CHARGEN_STAGE_AWAITING_IMAGE,
                image_url="https://x/a.png", isolated_url="https://x/a_Isolated.png",
                image_url_b="https://x/b.png", isolated_url_b="https://x/b_Isolated.png",
                warning="исходник — постер задачи: у задачи нет модели",
            )
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            service = TelegramDeliveryService(_FakeManager([job]), client=client)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()

            album = json.loads(dict(sent[0][1])["media"])
            self.assertIn("♻️ Regen задачи c8691854", album[0]["caption"])
            self.assertIn("1️⃣ точная T-поза", album[0]["caption"])
            self.assertIn("постер", album[0]["caption"])
            self.assertEqual(album[1]["caption"], "2️⃣ чистая под 3D")
            # the edit instruction is boilerplate, not a subject
            self.assertNotIn("Redraw", album[0]["caption"])
            choice = json.loads(dict(sent[1][1])["reply_markup"])
            data = [b["callback_data"] for row in choice["inline_keyboard"] for b in row]
            self.assertEqual(data[:2], [f"rfa:{job.id}:a", f"rfa:{job.id}:b"])

        run(scenario())

    def test_queue_progress_and_failure_cards_name_the_task(self):
        job = self._job(stage=CHARGEN_STAGE_REGEN_SOURCE)
        digest = telegram_delivery.digest_text([job])
        self.assertIn("рендер исходника", digest)
        self.assertIn("♻️ Regen задачи c8691854", digest)
        self.assertIn("♻️ Regen задачи c8691854", telegram_delivery.progress_text(job))
        self.assertIn(CHARGEN_STAGE_REGEN_SOURCE, telegram_delivery.UNFINISHED_STAGES)

        async def failure():
            sent = []

            def handler(request):
                sent.append(dict(httpx.QueryParams(request.content.decode())))
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 5}})

            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                    await telegram_delivery.deliver_failure(
                        client, self._job(stage=CHARGEN_STAGE_FAILED, error="boom")
                    )
            return sent[0]["text"]

        self.assertIn("♻️ Regen задачи c8691854", run(failure()))

    def test_generated_jobs_keep_their_prompt_as_subject(self):
        job = CharacterGenJob(prompt="orc warrior, green skin", telegram_chat_id=777)
        self.assertIn("orc warrior", telegram_delivery.progress_text(job))
        self.assertNotIn("Regen", telegram_delivery.digest_text([job]))


if __name__ == "__main__":
    unittest.main()
