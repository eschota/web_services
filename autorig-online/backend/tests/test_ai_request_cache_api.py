from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import ai_controlnet_api
import ai_request_cache
import ai_vision_api


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(ai_request_cache, "_default_cache", ai_request_cache.AIRequestCache(tmp_path / "cache.db"))
    app = FastAPI()
    app.include_router(ai_vision_api.router)
    app.include_router(ai_controlnet_api.router)
    with TestClient(app) as test_client:
        yield test_client


def test_repeated_vision_request_reuses_task_and_completed_answer(client, monkeypatch):
    submit = AsyncMock(return_value={"success_bool": True, "task_id_string": "f13.test",
                                    "status_string": "pending", "finished_bool": False})
    monkeypatch.setattr(ai_vision_api, "_uncached_api_vision", submit)
    body = {"prompt": "Describe the picture", "image_url": "https://example.com/image.png"}
    first = client.post("/api/vision", json=body).json()
    second = client.post("/api/vision", json=body).json()
    assert not first["cache_hit_bool"]
    assert second["cache_hit_bool"]
    assert submit.await_count == 1
    ai_request_cache.note_result("f13.test", "completed", {
        "status_string": "completed", "finished_bool": True, "answer_string": "A red truck"})
    completed = client.post("/api/vision", json=body).json()
    assert completed["answer_string"] == "A red truck"
    assert completed["finished_bool"]


def test_control_channel_changes_invalidate_cached_request(client, monkeypatch):
    submit = AsyncMock(return_value={"task_id_string": "control-test", "status_string": "pending"})
    monkeypatch.setattr(ai_controlnet_api, "_uncached_api_controlnet", submit)
    body = {"channel": "pose", "image_url": "https://example.com/image.png"}
    assert not client.post("/api/controlnet", json=body).json()["cache_hit_bool"]
    assert client.post("/api/controlnet", json=body).json()["cache_hit_bool"]
    assert not client.post("/api/controlnet", json={**body, "channel": "depth"}).json()["cache_hit_bool"]
    assert submit.await_count == 2


def test_clear_forgets_requests_without_touching_graph_files(client, monkeypatch, tmp_path):
    graph = tmp_path / "graph.json"
    graph.write_text('{"name":"Keep this graph"}')
    submit = AsyncMock(return_value={"task_id_string": "control-test", "status_string": "pending"})
    monkeypatch.setattr(ai_controlnet_api, "_uncached_api_controlnet", submit)
    body = {"channel": "canny", "image_url": "https://example.com/image.png"}
    client.post("/api/controlnet", json=body)
    assert client.delete("/api/ai/request-cache").json()["entries_removed_int"] == 1
    assert graph.exists()
    assert not client.post("/api/controlnet", json=body).json()["cache_hit_bool"]
