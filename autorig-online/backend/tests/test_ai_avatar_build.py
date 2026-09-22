"""Automatic Avatar builder: one source in, a saved Avatar v2 with views out."""
import asyncio
import io
import json
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

import httpx
from fastapi import FastAPI, Header
from PIL import Image

import ai_avatar_build as build
from ai_avatar_assets import AvatarAssetStore
from ai_avatars import AvatarDraft, AvatarOwner, AvatarStore

ALICE = AvatarOwner(owner_type="user", owner_id="alice@example.com")
DESCRIPTION = {"display_name": "Auburn bob woman", "identity_prompt": "adult woman, auburn bob, green eyes",
               "appearance": "freckles", "body": "slim, average height",
               "wardrobe": "yellow raincoat, jeans", "negative_identity_prompt": "long hair",
               "production_notes": "back not visible"}


def png(color=(200, 120, 40), size=(64, 96)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", size, color).save(out, "PNG")
    return out.getvalue()


class FakeFarm:
    """The internal API, the farm's render folder and the asset URLs."""

    def __init__(self, assets: AvatarAssetStore, scores=None, fail_engines=()):
        self.assets = assets
        self.scores = dict(scores or {})
        self.fail_engines = set(fail_engines)
        self.renders = {}
        self.vision = {}
        self.posts = []
        self.counter = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        path = request.url.path
        if request.method == "POST" and path in ("/api/image", "/api/qwen-image"):
            body = json.loads(request.content)
            engine = "klein" if path == "/api/image" else "qwen"
            self.posts.append((engine, body))
            if engine in self.fail_engines:
                return httpx.Response(400, json={"detail": {"message_string": "no box"}})
            self.counter += 1
            task = f"00000000-0000-0000-0000-{self.counter:012d}"
            prompt = body["prompt"].replace("picture 1", "image 1")
            slot = next((name for name, spec in build.VIEW_SPECS.items() if spec["text"] in prompt), "?")
            self.renders[task] = (slot, engine)
            return httpx.Response(200, json={"task_id_string": task,
                                             "image_url_string": f"https://farm.test/render/{task}.png",
                                             "effective_params_object": {"work_flow": "wf.json"}})
        if path.startswith("/api/ai/render-status/"):
            task = path.rsplit("/", 1)[1]
            return httpx.Response(200, json={"status_string": "completed", "node_string": "f5",
                                             "output_url_string": f"https://farm.test/render/{task}.png"})
        if url.startswith("https://farm.test/render/"):
            # The colour says which view and engine drew it, so the fake
            # judge can answer for the right picture whatever the order.
            slot, engine = self.renders[path.rsplit("/", 1)[1][:-4]]
            index = list(build.VIEW_SPECS).index(slot)
            return httpx.Response(200, content=png((20 + index * 28, 40 if engine == "klein" else 200, 90)))
        if request.method == "POST" and path == "/api/vision":
            body = json.loads(request.content)
            key = f"f13.{len(self.vision) + 1}"
            if body["prompt"].startswith("Describe"):
                answer = "```json\n" + json.dumps(DESCRIPTION) + "\n```"
            else:
                slot = self._judged_slot(body["image_base64"])
                answer = json.dumps({"same_person": self.scores.get(slot, 8), "view": slot if slot in (
                    "front", "back", "profile_left", "profile_right", "three_quarter_left",
                    "three_quarter_right") else "front", "people": 1, "issues": ""})
            self.vision[key] = answer
            return httpx.Response(200, json={"task_id_string": key, "finished_bool": False})
        if path.startswith("/api/ai/status/"):
            key = path.rsplit("/", 1)[1]
            return httpx.Response(200, json={"finished_bool": True, "answer_string": self.vision[key]})
        match = re.fullmatch(r"/api/ai/avatar-assets/([a-f0-9]{32})/([a-f0-9]{64})\.(png|jpg|webp)", path)
        if match:
            file, _ = self.assets.resolve_capability(*match.groups())
            return httpx.Response(200, content=file.read_bytes())
        return httpx.Response(404, json={"detail": "not here: " + url})

    def _judged_slot(self, data_url):
        import base64
        raw = base64.b64decode(data_url.split(",", 1)[1])
        with Image.open(io.BytesIO(raw)) as image:
            image = image.convert("RGB")
            red, green, _ = image.getpixel((image.width - 3, image.height // 2))
        slot = list(build.VIEW_SPECS)[max(0, min(7, round((red - 20) / 28)))]
        engine = "klein" if green < 120 else "qwen"
        if (slot, engine) in self.scores:
            self.scores[slot] = self.scores[(slot, engine)]
        return slot


class BuilderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp.name)
        self.avatars = AvatarStore(root / "avatars")
        self.assets = AvatarAssetStore(root / "assets", max_assets_per_owner=500)
        self.jobs = build.BuildJobStore(root / "build")
        self.root = root
        # Flat test pictures have no face; the detector is tested on its own.
        self._detect = build.detect_face_kind
        self._box = build.face_box
        build.detect_face_kind = lambda data: "unknown"
        # A face a third of the picture tall: every crop view keeps its framing.
        build.face_box = lambda data: [10, 10, 30, 32]

    def tearDown(self):
        build.detect_face_kind = self._detect
        build.face_box = self._box
        self.temp.cleanup()

    def builder(self, farm):
        return build.AvatarBuilder(
            avatar_store=self.avatars, asset_store=self.assets, job_store=self.jobs,
            http_client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(farm.handler)),
            api_base="http://internal", poll_seconds=0)

    def start(self, builder, views=None, **extra):
        source = self.root / "source.png"
        source.write_bytes(png())
        body = build.BuildRequest(views=views, **extra)
        identity = build._identity("image", "sha-of-source", body, None)
        identity["local_file"] = str(source)
        job, hit = self.jobs.create(ALICE, identity)
        self.assertFalse(hit)
        return asyncio.run(builder.run(job["job_id"]))

    def test_full_build_saves_v2_avatar_with_every_view(self):
        farm = FakeFarm(self.assets)
        job = self.start(self.builder(farm))
        self.assertEqual(job["status"], "completed", job.get("error"))
        status = build.public_status(job)
        self.assertTrue(status["avatar_string"].startswith("av_"))
        for slot in build.CANONICAL_VIEW_SLOTS:
            self.assertTrue(status[f"{slot}_url_string"].startswith("https://autorig.online/api/ai/avatar-assets/"), slot)
        self.assertTrue(status["sheet_url_string"])
        avatar_id = status["avatar_string"].split("@")[0]
        profile = self.avatars.get(avatar_id, ALICE)
        self.assertEqual(profile.format_version, 2)
        self.assertEqual(set(profile.views), set(build.CANONICAL_VIEW_SLOTS))
        self.assertEqual(profile.views["front"].qa.status, "passed")
        self.assertEqual(profile.views["front"].qa.identity_method, "vision_judge:qwen35-9b-uncensored")
        self.assertEqual(profile.body, "slim, average height")
        self.assertEqual([ref.role for ref in profile.references], ["face", "body"])
        summary = self.avatars.list(ALICE)[0]
        self.assertEqual(summary.view_count, 8)
        self.assertEqual(summary.cover_url, profile.views["front"].canonical_url)

    def test_the_full_body_anchor_is_made_first_and_anchors_the_other_views(self):
        farm = FakeFarm(self.assets)
        job = self.start(self.builder(farm), views="back,profile_left,face_closeup")
        engines = [(engine, body) for engine, body in farm.posts]
        first = engines[0][1]
        self.assertIn(build.VIEW_SPECS[build.ANCHOR_SLOT]["text"], first["prompt"])
        self.assertNotIn("reference_image_urls", first)
        anchor = job["views"][build.ANCHOR_SLOT]["final"]["canonical_url"]
        for engine, body in engines[1:]:
            self.assertEqual(engine, "klein")
            self.assertEqual(body["checkpoint"], build.KLEIN_CHECKPOINT)
        back = next(body for _, body in engines if build.VIEW_SPECS["back"]["text"] in body["prompt"])
        self.assertEqual(back["image_url"], anchor)
        self.assertNotIn("reference_image_urls", back)  # the back never sees the face
        profile = next(body for _, body in engines if build.VIEW_SPECS["profile_left"]["text"] in body["prompt"])
        self.assertEqual(profile["image_url"], anchor)
        # The close-up starts from a crop of the anchor, with the source for the face.
        face = next(body for _, body in engines if build.VIEW_SPECS["face_closeup"]["text"] in body["prompt"])
        self.assertNotIn("shoes", face["prompt"])  # a clause that pulled klein back to full length
        self.assertEqual(face["image_url"], job["views"]["face_closeup"]["crop_url"])
        self.assertNotEqual(face["image_url"], anchor)
        self.assertEqual(face["reference_image_urls"], [job["source"]["frame_url"]])

    def test_failed_view_is_retried_once_on_the_other_engine(self):
        farm = FakeFarm(self.assets, scores={("back", "klein"): 2, ("back", "qwen"): 9})
        job = self.start(self.builder(farm), views="front,back")
        back = job["views"]["back"]
        self.assertEqual([item["engine"] for item in back["attempts"]], ["klein", "qwen"])
        self.assertEqual(back["final"]["qa"]["status"], "passed")
        self.assertEqual(back["final"]["qa"]["attempts"], 2)
        self.assertEqual(back["final"]["provenance"]["engine"], "qwen-image-edit-2511")

    def test_a_view_that_fails_twice_is_kept_and_marked_failed(self):
        farm = FakeFarm(self.assets, scores={("back", "klein"): 3, ("back", "qwen"): 1})
        job = self.start(self.builder(farm), views="front,back")
        self.assertEqual(job["status"], "completed")
        final = job["views"]["back"]["final"]
        self.assertEqual(final["qa"]["status"], "failed")
        self.assertEqual(final["qa"]["identity_score"], 0.3)  # the better of the two

    def test_resume_does_not_resubmit_finished_renders(self):
        farm = FakeFarm(self.assets)
        builder = self.builder(farm)
        job = self.start(builder, views="front,face_closeup")
        posts = len(farm.posts)
        job["finished"] = False
        job["steps"].pop("save")
        job.pop("avatar_string")
        self.jobs.write(job)
        again = asyncio.run(builder.run(job["job_id"]))
        self.assertEqual(len(farm.posts), posts)
        self.assertEqual(again["status"], "completed")

    def test_no_engine_available_fails_the_build_honestly(self):
        farm = FakeFarm(self.assets, fail_engines=("klein", "qwen"))
        job = self.start(self.builder(farm), views="front")
        self.assertEqual(job["status"], "failed")
        self.assertIn("anchor", job["error"])

    def test_a_zoomed_out_close_up_falls_back_to_the_anchor_crop(self):
        farm = FakeFarm(self.assets)
        # Every render comes back with a face 1/40 of the height: klein zoomed out.
        build.face_box = lambda data: [10, 10, 2, 2]
        job = self.start(self.builder(farm), views="face_closeup")
        state = job["views"]["face_closeup"]
        self.assertEqual([item["engine"] for item in state["attempts"]], ["klein", "qwen", "crop"])
        final = state["final"]
        self.assertEqual(final["provenance"]["engine"], "crop")
        self.assertEqual(final["qa"]["status"], "accepted_with_warnings")
        self.assertIn("framing drifted", state["attempts"][0]["qa"]["notes"])

    def test_unknown_view_is_refused(self):
        with self.assertRaises(Exception):
            build.parse_views("front,left_ear")
        self.assertEqual(build.parse_views("back, front")[0], build.ANCHOR_SLOT)


class CropTests(unittest.TestCase):
    def test_crops_have_the_view_size_and_fall_back_without_a_face(self):
        tall = png((120, 130, 140), size=(832, 1216))
        for framing, size in (("upper", (832, 1216)), ("face", (1024, 1024))):
            data = build.crop_to_framing(tall, framing, *size)
            with Image.open(io.BytesIO(data)) as image:
                self.assertEqual(image.size, size)


class VerdictTests(unittest.TestCase):
    def test_back_view_with_a_frontal_face_fails(self):
        verdict = build.judge_verdict("back", {"same_person": 9, "view": "back", "people": 1}, "frontal")
        self.assertEqual(verdict["status"], "failed")
        self.assertFalse(verdict["angle_ok"])

    def test_profile_seen_as_three_quarter_is_a_warning_not_a_failure(self):
        verdict = build.judge_verdict("profile_left", {"same_person": 8, "view": "three_quarter_left",
                                                       "people": 1}, "profile")
        self.assertEqual(verdict["status"], "accepted_with_warnings")

    def test_two_people_fail(self):
        verdict = build.judge_verdict("front", {"same_person": 9, "view": "front", "people": 2}, "frontal")
        self.assertEqual(verdict["status"], "failed")

    def test_json_is_recovered_from_fences_and_missing_brace(self):
        self.assertEqual(build._extract_json('```json\n{"a": 1}\n```'), {"a": 1})
        self.assertEqual(build._extract_json('Sure: {"a": "b"'), {"a": "b"})


try:
    import multipart  # noqa: F401  (python-multipart: the upload route needs it; prod has it)
    HAVE_MULTIPART = True
except ImportError:
    HAVE_MULTIPART = False


@unittest.skipUnless(HAVE_MULTIPART, "python-multipart is not installed")
class RouterTests(unittest.TestCase):
    def test_other_owner_cannot_read_a_build_and_bad_source_is_refused(self):
        with tempfile.TemporaryDirectory() as folder:
            root = pathlib.Path(folder)
            builder = build.AvatarBuilder(avatar_store=AvatarStore(root / "a"),
                                          asset_store=AvatarAssetStore(root / "b"),
                                          job_store=build.BuildJobStore(root / "c"))
            builder.ensure_running = lambda job_id: None

            async def owner(x_test_owner: str = Header(...)):
                return AvatarOwner(owner_type="user", owner_id=x_test_owner)

            app = FastAPI()
            app.include_router(build.build_avatar_build_router(owner, builder=builder))

            async def scenario():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                             base_url="https://test") as client:
                    refused = await client.post("/api/ai/avatar-build", json={
                        "image_url": "https://evil.example/x.png"}, headers={"X-Test-Owner": "a"})
                    self.assertEqual(refused.status_code, 400)
                    accepted = await client.post("/api/ai/avatar-build", json={
                        "image_url": "https://autorig.online/dev/api/scratch/x.png"},
                        headers={"X-Test-Owner": "a"})
                    self.assertEqual(accepted.status_code, 202)
                    body = accepted.json()
                    self.assertTrue(body["task_id_string"].startswith("avb_"))
                    again = await client.post("/api/ai/avatar-build", json={
                        "image_url": "https://autorig.online/dev/api/scratch/x.png"},
                        headers={"X-Test-Owner": "a"})
                    self.assertEqual(again.json()["task_id_string"], body["task_id_string"])
                    self.assertTrue(again.json()["cache_hit_bool"])
                    hidden = await client.get(body["status_url_string"], headers={"X-Test-Owner": "b"})
                    self.assertEqual(hidden.status_code, 404)
                    mine = await client.get(body["status_url_string"], headers={"X-Test-Owner": "a"})
                    self.assertEqual(mine.status_code, 202)
            asyncio.run(scenario())


class GraphOutputsTests(unittest.TestCase):
    def test_a_node_result_keeps_every_output_by_field(self):
        from ai_graph import NodeResult
        record = NodeResult.model_validate({
            "status": "done", "type": "avatar", "value": "av_" + "a" * 24 + "@1",
            "outputs": {"avatar_string": "av_" + "a" * 24 + "@1",
                        "front_url_string": "https://autorig.online/api/ai/avatar-assets/x.png",
                        "description_string": "adult woman, auburn bob"}})
        self.assertEqual(record.model_dump()["outputs"]["front_url_string"],
                         "https://autorig.online/api/ai/avatar-assets/x.png")
        with self.assertRaises(Exception):
            NodeResult.model_validate({"outputs": {"front_url_string": "data:image/png;base64,AAAA"}})
        with self.assertRaises(Exception):
            NodeResult.model_validate({"outputs": {"Bad Key": "x"}})


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is not installed")
class VideoFrameTests(unittest.TestCase):
    def test_sampling_returns_frames_and_duration(self):
        with tempfile.TemporaryDirectory() as folder:
            root = pathlib.Path(folder)
            video = root / "clip.mp4"
            subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=320x240:rate=10",
                            "-t", "2", "-pix_fmt", "yuv420p", str(video)], check=True)
            frames, meta = build.sample_video(video, root / "frames", count=6)
            self.assertGreaterEqual(len(frames), 5)
            self.assertAlmostEqual(meta["duration"], 2.0, delta=0.2)
            try:
                import cv2  # noqa: F401
            except ImportError:
                return
            picked = build.pick_best_frame(video, root / "frames2")
            self.assertIn("score", picked["best"])


if __name__ == "__main__":
    unittest.main()
