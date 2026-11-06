import asyncio
import json

import civitai_media as cm


def run(coro):
    return asyncio.run(coro)


def test_non_civitai_unchanged():
    r = run(cm.resolve("https://autorig.online/dev/api/scratch/a.png"))
    assert r["url"] == "https://autorig.online/dev/api/scratch/a.png" and not r["changed_bool"]


def test_cdn_transform_becomes_original():
    u = ("https://image.civitai.com/xG1nkqKTMzGDvpLrqFT7WA/75f2a328-d6be-4bf4-a28d-2a61f2ae2044/"
         "transcode=true,width=450,optimized=true/joined.webm")
    r = run(cm.resolve(u))
    assert r["url"].endswith("/75f2a328-d6be-4bf4-a28d-2a61f2ae2044/original=true/joined.webm")
    assert r["type"] == "video"


def test_page_hosts_detected():
    for u in ("https://civitai.com/images/1", "https://civitai.red/images/1", "https://www.civitai.green/images/1",
              "https://image.civitai.red/a/75f2a328-d6be-4bf4-a28d-2a61f2ae2044/width=1/x.jpeg"):
        assert cm.is_civitai(u)
    assert not cm.is_civitai("https://notcivitai.com/images/1")


def test_model_page_refused():
    try:
        run(cm.resolve("https://civitai.com/models/12345/some-model"))
    except cm.CivitaiResolveError:
        return
    raise AssertionError("model page should be refused")


def test_middleware_rewrites_json_body(monkeypatch):
    async def fake_resolve(url, client=None):
        return {"url": "https://image.civitai.com/k/75f2a328-d6be-4bf4-a28d-2a61f2ae2044/original=true/v.mp4",
                "type": "video", "source": url, "changed_bool": True}
    monkeypatch.setattr(cm, "resolve", fake_resolve)
    seen = {}

    async def app(scope, receive, send):
        message = await receive()
        seen["body"] = json.loads(message["body"])
        seen["len"] = dict(scope["headers"])[b"content-length"]

    body = json.dumps({"image_url": "https://civitai.red/images/143788350", "prompt": "x"}).encode()
    scope = {"type": "http", "method": "POST", "path": "/api/vision",
             "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]}
    sent = [{"type": "http.request", "body": body, "more_body": False}]

    async def receive():
        return sent.pop(0)

    async def send(message):
        pass

    run(cm.CivitaiMediaMiddleware(app)(scope, receive, send))
    assert "image_url" not in seen["body"]
    assert seen["body"]["video_url"].endswith("original=true/v.mp4")
    assert int(seen["len"]) == len(json.dumps(seen["body"]).encode())
