import asyncio
import base64
from pathlib import Path

from ai_request_cache import AIRequestCache, canonical_request_hash


def test_concurrent_identical_requests_submit_once(tmp_path: Path) -> None:
    async def scenario() -> None:
        cache = AIRequestCache(tmp_path / "cache.sqlite3")
        calls = 0

        async def submit():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.02)
            return {"task_id": "task-1", "status": "accepted"}

        payload = {"prompt": "same", "channel": "canny", "seed": 0}
        results = await asyncio.gather(
            *(cache.run_cached("controlnet", payload, submit) for _ in range(12))
        )

        assert calls == 1
        assert sum(item["cache_hit_bool"] is False for item in results) == 1
        assert all(item["task_id"] == "task-1" for item in results)

    asyncio.run(scenario())


def test_cache_survives_new_instance_and_completed_result(tmp_path: Path) -> None:
    async def scenario() -> None:
        database = tmp_path / "cache.sqlite3"
        first = AIRequestCache(database)
        calls = 0

        async def submit():
            nonlocal calls
            calls += 1
            return {"task_id_string": "vision-7", "status_string": "pending"}

        payload = {"prompt": "describe", "image_url": "https://example.test/a.png"}
        accepted = await first.run_cached("vision", payload, submit)
        assert accepted["cache_hit_bool"] is False
        assert first.note_result("vision-7", "completed", {"text": "a result"})

        second = AIRequestCache(database)
        hit = await second.run_cached("vision", payload, submit)
        assert calls == 1
        assert hit["cache_hit_bool"] is True
        assert hit["text"] == "a result"

    asyncio.run(scenario())


def test_changed_meaningful_parameter_is_a_miss(tmp_path: Path) -> None:
    async def scenario() -> None:
        cache = AIRequestCache(tmp_path / "cache.sqlite3")
        calls = 0

        async def submit():
            nonlocal calls
            calls += 1
            return {"task_id": f"task-{calls}", "status": "accepted"}

        common = {"prompt": "portrait", "seed": 41, "model": "pony"}
        first = await cache.run_cached("image", common, submit)
        second = await cache.run_cached("image", {**common, "steps": 30}, submit)
        third = await cache.run_cached("image", {**common, "wait_seconds": 99}, submit)

        assert calls == 2
        assert first["cache_hit_bool"] is False
        assert second["cache_hit_bool"] is False
        assert third["cache_hit_bool"] is True
        assert third["task_id"] == first["task_id"]

    asyncio.run(scenario())


def test_failed_task_is_invalidated(tmp_path: Path) -> None:
    async def scenario() -> None:
        cache = AIRequestCache(tmp_path / "cache.sqlite3")
        calls = 0

        async def submit():
            nonlocal calls
            calls += 1
            return {"task_id": f"task-{calls}", "status": "accepted"}

        payload = {"prompt": "edges", "channel": "canny"}
        first = await cache.run_cached("controlnet", payload, submit)
        assert cache.note_result(first["task_id"], "failed", {"error": "worker"})
        second = await cache.run_cached("controlnet", payload, submit)

        assert calls == 2
        assert second["cache_hit_bool"] is False
        assert second["task_id"] != first["task_id"]

    asyncio.run(scenario())


def test_random_image_without_seed_is_never_cached(tmp_path: Path) -> None:
    async def scenario() -> None:
        cache = AIRequestCache(tmp_path / "cache.sqlite3")
        calls = 0

        async def submit():
            nonlocal calls
            calls += 1
            return {"task_id": f"random-{calls}"}

        payload = {"prompt": "random portrait", "seed": 0}
        first = await cache.run_cached("generate_image", payload, submit)
        second = await cache.run_cached("generate_image", payload, submit)

        assert calls == 2
        assert first["cache_hit_bool"] is False
        assert second["cache_hit_bool"] is False

    asyncio.run(scenario())


def test_inline_media_is_hashed_and_secrets_are_ignored() -> None:
    raw = b"identical pixels"
    plain = base64.b64encode(raw).decode("ascii")
    data_url = "data:image/png;base64," + plain
    first = canonical_request_hash(
        "vision", {"image_base64": plain, "authorization": "Bearer first"}
    )
    second = canonical_request_hash(
        "vision", {"image_base64": data_url, "authorization": "Bearer second"}
    )
    assert first == second


def test_clear_removes_metadata_only(tmp_path: Path) -> None:
    async def scenario() -> None:
        external = tmp_path / "source.png"
        external.write_bytes(b"keep me")
        cache = AIRequestCache(tmp_path / "cache.sqlite3")

        async def submit():
            return {"task_id": "task-clear", "status": "accepted"}

        await cache.run_cached(
            "vision", {"image_url": external.as_uri(), "prompt": "inspect"}, submit
        )
        assert cache.clear() == 1
        assert external.read_bytes() == b"keep me"

    asyncio.run(scenario())
