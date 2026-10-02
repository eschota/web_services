"""Subgraph · Character swap (owner 2026-10-02: «сделай уже Subgraf, чтобы можно было пайплайн выстраивать только
на уровне инпут-аутпут, а под капотом мощный логический AI Vision, который добивается конкретного результата …
максимизировать сходство стиля, сюжета, но с заменой персонажа консистентно»).

One node with inputs and outputs only. In: a reference picture (its look and its story) and the character (one
or two pictures, optionally her look in words and a Z-Image LoRA). Out: the reference re-made with the character,
and a report. Under the hood a Vision loop:
  1. Vision reads the reference: safety, medium, lighting, palette, camera, setting, mood, people, action, clothes.
  2. Engine: a photo-like reference with a character LoRA -> Z-Image Turbo + the LoRA + the reference's Z_depth
     (composition and action); anything else -> Qwen-Image 2.1 edit with the reference as the canvas (image 1) and
     the character as images 2-3.
  3. Each attempt is scored side by side by Vision: LOOK (reference | result), STORY (reference | result),
     CHARACTER (character | result), plus anatomy, adults only and no text on the result itself.
  4. Corrections between attempts: story short -> firmer depth or "keep the pose"; character short -> heavier
     LoRA, identity emphasis; look short twice on Z-Image -> the Qwen canvas; every attempt has a new seed.
  5. Stops when LOOK, STORY and CHARACTER all reach the target with anatomy >= 8, else after the attempts; when
     only CHARACTER falls short a last face pass (Qwen, the character as reference) is tried.
The best attempt lands at the URL announced at start (the node editor waits for that file); the report keeps
every attempt with its knobs, scores and the decision. Starting a job is for admins and for scripts on this
server: the node can put a person into any scene, so it is not open to anonymous visitors.
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from PIL import Image, ImageDraw
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

LOCAL = "http://127.0.0.1:8200"
PUBLIC = os.getenv("AUTORIG_PUBLIC_URL", "https://autorig.online").rstrip("/")
ROOT = Path(os.getenv("CHARACTER_SWAP_DIR", "/srv/autorig/data/var/character_swap"))
API = "/api/ai/character-swap"
VISION_MODEL = "bonsai2-27b"
ZIT = "z_image_turbo_fp8_e4m3fn.safetensors"
MAX_JOBS = 4                       # 6 starved the farm's Vision (2026-10-02)
AREA = 1.0e6                       # ~1 MP renders in the reference's aspect
NEGATIVE = ("child, teen, text, watermark, signature, logo, extra people, deformed hands, extra fingers, extra limbs, "
            "missing limbs, deformed genitals, blurry")

DESCRIBE = (
    "Describe this picture so another artist can re-create it with a different character. Answer exactly:\n"
    "SAFE: <no if it shows anyone who could be under 18 or non-consensual content; yes otherwise>\n"
    "MEDIUM: <photo / 3d render / anime / illustration / painting / other, then a few words on the rendering>\n"
    "LIGHTING: <direction, quality, colour temperature, key, rim and fill, time of day: one line>\n"
    "PALETTE: <main colours, contrast and grading: one line>\n"
    "CAMERA: <shot size, angle, lens feel, depth of field: one line>\n"
    "SETTING: <place and background: one line>\n"
    "MOOD: <a few words>\n"
    "PEOPLE: <number of people>\n"
    "ACTION: <what the main person does and how she is posed - the story of the picture, one line>\n"
    "CLOTHING: <what the main person wears, or nude>")
QC = ("Check this picture. Answer exactly:\nADULT: <yes if every person clearly has an adult face and body>\n"
      "ANATOMY: <0-10: hands, fingers, limbs, joints, bodies, genitals; 10 = flawless>\n"
      "TEXT: <yes if a watermark, logo, signature or caption is overlaid on the picture; tattoos and lettering on "
      "objects do not count>\nPEOPLE: <number of people>\n"
      "NUDE: <yes if the main woman wears no clothes>\nNOTE: <one line>")
LOOK_Q = ("Left: a reference picture. Right: a new picture showing another person. Compare ONLY the look: medium and "
          "rendering style, lighting, colour palette and grading, camera framing. Ignore who is shown and what they "
          "wear. Answer exactly:\nSCORE: <0-10, 10 = the same look>\nNOTE: <one line>")
STORY_Q = ("Left: a reference picture. Right: a new picture showing another person. Compare ONLY the story: what the "
           "main person does, the pose, the composition and the setting. Ignore the person's identity, face, clothes "
           "and the drawing style. Answer exactly:\nSCORE: <0-10, 10 = the same story>\nNOTE: <one line>")
CHAR_Q = ("Left: a picture of a character. Right: a new picture, possibly in another drawing style. Is the main "
          "person on the right the same character as on the left (face, hair, eyes, body, tattoos, jewellery)? "
          "Ignore style, pose, clothes and place. Answer exactly:\nSCORE: <0-10, 10 = clearly the same>\n"
          "NOTE: <one line>")
CANVAS_SYSTEM = ("Image 1 is the scene: keep its medium and rendering style, lighting, colour palette, camera framing, "
                 "composition, background and the main person's pose and action exactly. Replace only the main person "
                 "with the character shown in {chars}: her face, hair, eyes, body shape, skin, tattoos, piercings and "
                 "jewellery. Nothing of image 1's person stays except the pose{keep_clothes}. Remove every signature, "
                 "watermark, logo and caption of image 1: the result carries no text at all.")
# Z-Image Turbo runs at cfg 1, where a negative prompt does nothing: the clean-picture line rides in the prompt.
CLEAN_LINE = "a clean picture without any text, signature, logo or watermark"
FACE_SYSTEM = ("Edit image 1 minimally: give the main person exactly the face, eyes and hair of the character in image "
               "2. Keep everything else of image 1 - style, light, colours, pose, body, clothes, background - as it is.")

_slots: Optional[asyncio.Semaphore] = None
_running: set = set()


class SwapRequest(BaseModel):
    image_url: str = Field(..., description="The reference: its look and its story")
    reference_image_urls: List[str] = Field(default_factory=list, description="The character: one or two pictures")
    prompt: str = Field("", description="The character's look in words (optional; needed for the photo engine)")
    lora: str = Field("", description="<lora:FILE:W> stack for the photo engine (Z-Image)")
    clothing: str = Field("reference", description="reference (as the reference) or nude")
    engine: str = Field("auto", description="auto, photo (Z-Image + LoRA + depth) or edit (Qwen canvas)")
    target: float = Field(8.0, ge=5, le=10, description="LOOK, STORY and CHARACTER must reach this")
    attempts: int = Field(5, ge=1, le=8)
    width: int = Field(0, ge=0, le=2048)
    height: int = Field(0, ge=0, le=2048)
    seed: int = Field(0, ge=0, le=2 ** 31 - 1)


# ------------------------------------------------------------------ helpers
def _job_dir(job: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{12}", job or ""):
        raise HTTPException(status_code=404, detail="unknown job")
    return ROOT / job


def _file_url(job: str, name: str) -> str:
    return f"{PUBLIC}{API}/file/{job}/{name}"


def _field(text: str, key: str) -> str:
    m = re.search(rf"{key}:\s*(.+)", text or "", re.I)
    return m.group(1).strip() if m else ""


def _int(text: str, key: str) -> int:
    m = re.search(rf"{key}:\s*(\d+)", text or "", re.I)
    return int(m.group(1)) if m else 0


def _size(w: int, h: int, body: SwapRequest) -> Tuple[int, int]:
    if body.width and body.height:
        return body.width // 16 * 16, body.height // 16 * 16
    s = (AREA / max(1, w * h)) ** 0.5
    return max(512, min(1536, int(w * s) // 16 * 16)), max(512, min(1536, int(h * s) // 16 * 16))


def _scale_loras(stack: str, scale: float) -> str:
    def one(m):
        weight = float(m.group(2)) if m.group(2) else 1.0
        return f"<lora:{m.group(1)}:{min(1.2, round(weight * scale, 2))}>"
    return re.sub(r"<lora:([^:>]+)(?::([0-9.]+))?>", one, stack or "")


async def _download(client: httpx.AsyncClient, url: str) -> bytes:
    for _ in range(3):
        try:
            got = await client.get(url, timeout=120, follow_redirects=True)
            if got.status_code == 200 and got.content:
                return got.content
        except httpx.HTTPError:
            pass
        await asyncio.sleep(3)
    raise RuntimeError(f"could not download {url[:120]}")


async def _wait_file(client: httpx.AsyncClient, url: str, task: str = "", minutes: float = 12) -> bytes:
    deadline = time.time() + minutes * 60
    while time.time() < deadline:
        if task:
            try:
                st = (await client.get(f"{LOCAL}/api/ai/render-status/{task}", timeout=30)).json()
                if st.get("status_string") in ("failed", "cancelled"):
                    raise RuntimeError(f"render {st.get('status_string')}: {str(st.get('error_string'))[:160]}")
            except (httpx.HTTPError, ValueError):
                pass
        try:
            got = await client.get(url, timeout=120)
            if got.status_code == 200 and got.content:
                return got.content
        except httpx.HTTPError:
            pass
        await asyncio.sleep(4)
    raise RuntimeError("the render did not land in time")


async def _render(client: httpx.AsyncClient, path: str, body: Dict[str, Any]) -> Tuple[str, bytes]:
    answer = (await client.post(f"{LOCAL}{path}", json=body, timeout=300)).json()
    url = str(answer.get("image_url_string") or "")
    task = str(answer.get("task_id_string") or "")
    if not url:
        raise RuntimeError(f"{path} refused: {json.dumps(answer)[:200]}")
    return url, await _wait_file(client, url, task)


async def _vision_once(client: httpx.AsyncClient, url: str, question: str) -> str:
    answer = (await client.post(f"{LOCAL}/api/vision", json={"image_url": url, "prompt": question,
                                                            "model": VISION_MODEL}, timeout=300)).json()
    text, task = str(answer.get("answer_string") or ""), str(answer.get("task_id_string") or "")
    for _ in range(90):
        if text or not task:
            break
        await asyncio.sleep(4)
        try:
            st = (await client.get(f"{LOCAL}/api/ai/status/{task}", timeout=60)).json()
        except (httpx.HTTPError, ValueError):
            continue
        text = str(st.get("answer_string") or "")
        if st.get("error_string") and not text:
            break
    return text


async def _vision(client: httpx.AsyncClient, url: str, question: str) -> str:
    """Vision under load sometimes answers nothing (2026-10-02: whole jobs scored 0/0/0): ask again before giving up,
    each time with a distinct suffix so the request cache cannot hand back the empty answer."""
    for attempt in range(3):
        try:
            text = await _vision_once(client, url, question if not attempt else f"{question}\n(retry {attempt})")
        except (httpx.HTTPError, ValueError):
            text = ""
        if re.search(r"[A-Z_]+:\s*\S", text):
            return text
        await asyncio.sleep(20 + 20 * attempt)
    return ""


def _save_jpeg(data: bytes, path: Path, max_side: int = 1536) -> Tuple[int, int]:
    im = Image.open(io.BytesIO(data)).convert("RGB")
    full = im.size
    im.thumbnail((max_side, max_side))
    im.save(path, quality=92)
    return full


def _pair(left: Path, right: Path, out: Path) -> None:
    a, b = Image.open(left).convert("RGB"), Image.open(right).convert("RGB")
    h = 768
    a = a.resize((max(1, a.width * h // a.height), h))
    b = b.resize((max(1, b.width * h // b.height), h))
    pair = Image.new("RGB", (a.width + b.width + 16, h), "white")
    pair.paste(a, (0, 0))
    pair.paste(b, (a.width + 16, 0))
    pair.save(out, quality=88)


def _placeholder(path: Path, text: str) -> None:
    im = Image.new("RGB", (768, 512), (40, 16, 16))
    d = ImageDraw.Draw(im)
    y = 24
    for line in re.findall(r".{1,60}(?:\s|$)", text)[:16]:
        d.text((24, y), line.strip(), fill=(255, 220, 220))
        y += 28
    im.save(path, format="PNG")


# ------------------------------------------------------------------ the loop
class Job:
    def __init__(self, job: str, body: SwapRequest):
        self.job, self.body, self.dir = job, body, ROOT / job
        self.report: Dict[str, Any] = {"job": job, "started": time.time(), "stage": "queued", "attempts": [],
                                       "request": body.model_dump()}

    def write(self) -> None:
        tmp = self.dir / "report.tmp"
        tmp.write_text(json.dumps(self.report, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(self.dir / "report.json")

    def stage(self, text: str) -> None:
        self.report["stage"] = text
        self.write()

    async def score(self, client, n: int, png: Path) -> Dict[str, Any]:
        ref, char = self.dir / "reference.jpg", self.dir / "character_1.jpg"
        _pair(ref, png, self.dir / f"look_{n}.jpg")
        _pair(ref, png, self.dir / f"story_{n}.jpg")
        _pair(char, png, self.dir / f"char_{n}.jpg")
        qc, look, story, ch = await asyncio.gather(
            _vision(client, _file_url(self.job, png.name), QC + f" ({self.job}-{n})"),
            _vision(client, _file_url(self.job, f"look_{n}.jpg"), LOOK_Q),
            _vision(client, _file_url(self.job, f"story_{n}.jpg"), STORY_Q),
            _vision(client, _file_url(self.job, f"char_{n}.jpg"), CHAR_Q))
        s = {"look": _int(look, "SCORE"), "story": _int(story, "SCORE"), "character": _int(ch, "SCORE"),
             "anatomy": _int(qc, "ANATOMY"), "adult": _field(qc, "ADULT").lower().startswith("yes"),
             "text": _field(qc, "TEXT").lower().startswith("yes"), "nude": _field(qc, "NUDE").lower().startswith("yes"),
             "notes": {"look": _field(look, "NOTE")[:160], "story": _field(story, "NOTE")[:160],
                       "character": _field(ch, "NOTE")[:160], "qc": _field(qc, "NOTE")[:160]}}
        s["vision_silent"] = not any((look, story, ch, qc))
        t = self.body.target
        s["passed"] = (s["look"] >= t and s["story"] >= t and s["character"] >= t and s["anatomy"] >= 8
                       and s["adult"] and not s["text"] and (self.body.clothing != "nude" or s["nude"]))
        return s

    async def attempt(self, client, n: int, knobs: Dict[str, Any], d: Dict[str, Any]) -> Dict[str, Any]:
        body, w, h = self.body, knobs["w"], knobs["h"]
        seed = (body.seed or 7001) + n * 7919
        nude = body.clothing == "nude"
        clothes = ("she is completely naked, no clothes and no underwear" if nude
                   else (f"wearing {d['clothing']}" if d["clothing"] and d["clothing"].lower() != "nude" else "nude"))
        if knobs["engine"] == "photo":
            prompt = (f"{d['medium']}; lighting: {d['lighting']}; colour palette: {d['palette']}; camera: "
                      f"{d['camera']}; setting: {d['setting']}; mood: {d['mood']}. {body.prompt}. {clothes}. "
                      f"{d['action']}. {' '.join(knobs['emphasis'])} {CLEAN_LINE}.")
            req = {"prompt": prompt, "negative_prompt": NEGATIVE, "checkpoint": ZIT, "width": w, "height": h,
                   "seed": seed, "prompt_translate": False, "wait_seconds": 0,
                   "loras": _scale_loras(body.lora, knobs["lora_scale"])}
            if knobs.get("depth_url"):
                req.update(control_depth=knobs["depth_url"], control_strength=knobs["depth"], control_start=0.0,
                           control_end=0.95)
            url, data = await _render(client, "/api/image", req)
        else:
            chars = [_file_url(self.job, p.name) for p in sorted(self.dir.glob("character_*.jpg"))]
            names = " and ".join(f"image {i + 2}" for i in range(len(chars)))
            system = CANVAS_SYSTEM.format(chars=names, keep_clothes="" if nude else " and the clothes")
            # the character pictures are photos: without naming image 1's medium Qwen paints her photoreal into a
            # drawing (2026-10-02, look 0-4 on anime references)
            medium = (f"Draw her in exactly image 1's medium and style - {d['medium']} - with the same line art, "
                      "shading, colours and finish; if image 1 is a drawing she is drawn too, not a photograph. "
                      if not knobs.get("photo_like") else "")
            prompt = (f"{medium}The character: {body.prompt or 'the woman of ' + names}. {clothes}. {d['action']}. "
                      f"{' '.join(knobs['emphasis'])}")
            req = {"prompt": prompt, "system_prompt": system, "mode": "edit",
                   "image_url": _file_url(self.job, "reference.jpg"), "reference_image_urls": chars, "width": w,
                   "height": h, "seed": seed, "prompt_translate": False, "negative_prompt": NEGATIVE,
                   "wait_seconds": 0}
            url, data = await _render(client, "/api/qwen-image", req)
        png = self.dir / f"attempt_{n}.png"
        png.write_bytes(data)
        row = {"n": n, "engine": knobs["engine"], "seed": seed, "url": url, "prompt": prompt[:1200],
               "knobs": {k: v for k, v in knobs.items() if k not in ("w", "h", "depth_url")}}
        row.update(await self.score(client, n, png))
        if row["vision_silent"]:            # the picture is fine as far as we know: score it again later
            await asyncio.sleep(60)
            row.update(await self.score(client, n, png))
        return row

    def correct(self, knobs: Dict[str, Any], row: Dict[str, Any], history: List[Dict[str, Any]]) -> str:
        t, why = self.body.target, []
        knobs["emphasis"] = []
        if row.get("vision_silent"):
            return "Vision не ответил (перегрузка): попытка не засчитана, новый сид"
        weak_story = [r for r in history if r["engine"] == "photo" and r["story"] < t]
        if knobs["engine"] == "photo" and len(weak_story) >= 2 and self.report.get("characters"):
            knobs["engine"] = "edit"
            knobs["emphasis"] = []
            return f"сюжет {row['story']} < {t:g} дважды на Z-Image: переход на холст Qwen"
        if row["story"] < t:
            if knobs["engine"] == "photo" and knobs.get("depth_url"):
                knobs["depth"] = round(min(0.84, knobs["depth"] + 0.07), 2)
                why.append(f"сюжет {row['story']} < {t:g}: depth → {knobs['depth']}")
            else:
                knobs["emphasis"].append("Keep image 1's pose, framing and composition exactly.")
                why.append(f"сюжет {row['story']} < {t:g}: держать позу и композицию")
        if row["character"] < t:
            if knobs["engine"] == "photo":
                knobs["lora_scale"] = round(min(1.4, knobs["lora_scale"] + 0.15), 2)
                knobs["emphasis"].append("Her face exactly as described, unmistakably the same woman.")
                why.append(f"персонаж {row['character']} < {t:g}: LoRA ×{knobs['lora_scale']}")
            else:
                knobs["emphasis"].append("Her face must be exactly the face of the character pictures.")
                why.append(f"персонаж {row['character']} < {t:g}: акцент на лицо")
        if row["look"] < t:
            weak = [r for r in history if r["engine"] == "photo" and r["look"] < t]
            if knobs["engine"] == "photo" and len(weak) >= 2 and self.report.get("characters"):
                knobs["engine"] = "edit"
                why.append(f"стиль {row['look']} < {t:g} дважды на Z-Image: переход на холст Qwen")
            else:
                knobs["emphasis"].append("Match the reference's rendering style, light and colours exactly.")
                why.append(f"стиль {row['look']} < {t:g}: акцент на стиль")
        if row["anatomy"] < 8:
            knobs["emphasis"].append("Correct anatomy: natural hands with five fingers, natural arms and legs.")
            why.append(f"анатомия {row['anatomy']}: упор на руки и конечности, новый сид")
        if not row["adult"]:
            why.append("Vision не уверен, что все взрослые: новый сид")
        if row["text"]:
            why.append("вотермарк или подпись на кадре: новый сид")
        if self.body.clothing == "nude" and not row["nude"]:
            knobs["emphasis"].append("She wears nothing at all.")
            why.append("осталась одежда: акцент на наготу")
        return "; ".join(why) or "новый сид"

    async def face_pass(self, client, best: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        n = len(self.report["attempts"]) + 1
        w, h = Image.open(self.dir / f"attempt_{best['n']}.png").size
        req = {"prompt": "Fix only the face.", "system_prompt": FACE_SYSTEM, "mode": "edit",
               "image_url": _file_url(self.job, f"attempt_{best['n']}.png"),
               "reference_image_urls": [_file_url(self.job, "character_1.jpg")], "width": w // 16 * 16,
               "height": h // 16 * 16, "seed": best["seed"] + 77, "prompt_translate": False, "wait_seconds": 0}
        url, data = await _render(client, "/api/qwen-image", req)
        png = self.dir / f"attempt_{n}.png"
        png.write_bytes(data)
        row = {"n": n, "engine": "face pass", "seed": req["seed"], "url": url, "prompt": FACE_SYSTEM,
               "knobs": {"from": best["n"]}}
        row.update(await self.score(client, n, png))
        return row

    async def run(self) -> None:
        body = self.body
        async with httpx.AsyncClient() as client:
            self.stage("референс")
            ref = await _download(client, body.image_url)
            rw, rh = _save_jpeg(ref, self.dir / "reference.jpg")
            chars = body.reference_image_urls[:2]
            for i, url in enumerate(chars, start=1):
                _save_jpeg(await _download(client, url), self.dir / f"character_{i}.jpg")
            self.report["characters"] = len(chars)
            text = await _vision(client, _file_url(self.job, "reference.jpg"), DESCRIBE)
            d = {k.lower(): _field(text, k) for k in ("SAFE", "MEDIUM", "LIGHTING", "PALETTE", "CAMERA", "SETTING",
                                                      "MOOD", "PEOPLE", "ACTION", "CLOTHING")}
            m = re.search(r"\d+", d["people"])
            d["people"] = int(m.group(0)) if m else 0
            self.report["reference"] = d
            if not d["safe"].lower().startswith("yes"):
                raise PermissionError("Vision не пропустил референс (возможен несовершеннолетний или насилие)")
            medium = d["medium"].lower()
            photo_like = any(k in medium for k in ("photo", "cinematic", "realistic"))
            engine = body.engine if body.engine in ("photo", "edit") else ("photo" if photo_like and body.lora else "edit")
            if engine == "edit" and not chars:
                engine = "photo"
            if engine == "photo" and not (body.lora or body.prompt):
                raise ValueError("для фото-движка нужен LoRA или описание персонажа")
            w, h = _size(rw, rh, body)
            knobs: Dict[str, Any] = {"engine": engine, "depth": 0.68, "lora_scale": 1.0, "emphasis": [], "w": w, "h": h,
                                     "photo_like": photo_like}
            if d["people"] >= 1:
                self.stage("Z_depth референса")
                try:
                    answer = (await client.post(f"{LOCAL}/api/controlnet", json={
                        "image_url": _file_url(self.job, "reference.jpg"), "channel": "depth"}, timeout=300)).json()
                    if answer.get("image_url_string"):
                        await _wait_file(client, answer["image_url_string"], str(answer.get("task_id_string") or ""))
                        knobs["depth_url"] = answer["image_url_string"]
                except Exception as error:      # depth helps; the loop works without it
                    self.report["depth_error"] = str(error)[:200]
            history: List[Dict[str, Any]] = []
            for n in range(1, body.attempts + 1):
                self.stage(f"попытка {n}/{body.attempts} · {knobs['engine']}")
                try:
                    row = await self.attempt(client, n, knobs, d)
                except Exception as error:
                    history.append({"n": n, "engine": knobs["engine"], "error": str(error)[:240], "look": 0,
                                    "story": 0, "character": 0, "anatomy": 0, "passed": False})
                    self.report["attempts"] = history
                    continue
                history.append(row)
                self.report["attempts"] = history
                if row["passed"]:
                    row["decision"] = "цель достигнута"
                    break
                row["decision"] = self.correct(knobs, row, history)
                self.write()
            done = [r for r in history if "error" not in r and not r.get("vision_silent")] or \
                [r for r in history if "error" not in r]
            if not done:
                raise RuntimeError("ни одна попытка не отрендерилась: " + "; ".join(r["error"] for r in history)[:400])
            key = lambda r: (r["passed"], min(r["look"], r["story"], r["character"]),
                             r["look"] + r["story"] + r["character"], r["anatomy"])
            best = max(done, key=key)
            t = body.target
            if (not best["passed"] and best["character"] < t and best["look"] >= t - 1 and best["story"] >= t - 1
                    and best["anatomy"] >= 8 and chars):
                self.stage("финальная ретушь лица")
                try:
                    fixed = await self.face_pass(client, best)
                    history.append(fixed)
                    if fixed["character"] > best["character"] and fixed["look"] >= best["look"] - 1:
                        fixed["decision"] = "ретушь лица принята"
                        best = fixed
                    else:
                        fixed["decision"] = "ретушь лица не улучшила"
                except Exception as error:
                    history.append({"n": len(history) + 1, "engine": "face pass", "error": str(error)[:240]})
            self.report["attempts"] = history
            self.report["best"] = best["n"]
            (self.dir / "result.png").write_bytes((self.dir / f"attempt_{best['n']}.png").read_bytes())
            self.report["summary_string"] = (
                f"Сабграф · замена персонажа: {len([r for r in history if 'error' not in r])} попыток, лучшая "
                f"#{best['n']} ({best['engine']}): стиль {best['look']}, сюжет {best['story']}, персонаж "
                f"{best['character']}, анатомия {best['anatomy']}; цель {t:g} "
                f"{'достигнута' if best.get('passed') else 'не достигнута'}. Референс: {d['medium'][:60]}; "
                f"{d['action'][:80]}")
            self.report["stage"] = "готово"
            self.report["finished"] = time.time()
            self.write()


async def _run_job(job: Job) -> None:
    global _slots
    if _slots is None:
        _slots = asyncio.Semaphore(MAX_JOBS)
    async with _slots:
        try:
            await job.run()
        except Exception as error:
            logger.warning("character swap %s failed: %s", job.job, error)
            job.report.update(stage="ошибка", error_string=str(error)[:400], finished=time.time(),
                              summary_string=f"Сабграф · замена персонажа: остановлен — {str(error)[:300]}")
            _placeholder(job.dir / "result.png", job.report["summary_string"])
            job.write()


# ------------------------------------------------------------------ routes
def build_character_swap_router(current_user: Callable, is_admin_email: Callable[[Optional[str]], bool]) -> APIRouter:
    router = APIRouter()

    def internal(request: Request) -> bool:
        """A script on this server (direct to uvicorn): no proxy headers, the uvicorn host itself."""
        host = (request.headers.get("host") or "").split(":")[0]
        return (request.client is not None and request.client.host in ("127.0.0.1", "::1")
                and host in ("127.0.0.1", "localhost") and "x-forwarded-for" not in request.headers
                and "x-real-ip" not in request.headers)

    @router.post(API)
    async def start(body: SwapRequest, request: Request, user=Depends(current_user)):
        if not internal(request) and not (user and is_admin_email(getattr(user, "email", None))):
            raise HTTPException(status_code=403, detail={"error_string": "admin_only", "message_string":
                                "The character-swap subgraph is for admins: it can put a person into any scene."})
        if not body.reference_image_urls and not body.lora:
            raise HTTPException(status_code=400, detail={"error_string": "no_character", "message_string":
                                "Wire the character picture(s) (or give a LoRA for the photo engine)."})
        job = uuid.uuid4().hex[:12]
        j = Job(job, body)
        j.dir.mkdir(parents=True, exist_ok=True)
        j.write()
        task = asyncio.create_task(_run_job(j))
        _running.add(task)
        task.add_done_callback(_running.discard)
        return {"success_bool": True, "finished_bool": False, "job_id_string": job,
                "image_url_string": f"{PUBLIC}{API}/result/{job}.png",
                "report_url_string": f"{PUBLIC}{API}/report/{job}.json"}

    @router.api_route(API + "/result/{name}", methods=["GET", "HEAD"])
    async def result(name: str):
        path = _job_dir(name.removesuffix(".png")) / "result.png"
        if not path.is_file():
            raise HTTPException(status_code=404, detail="not ready")
        return FileResponse(path, media_type="image/png")

    @router.get(API + "/report/{name}")
    async def report(name: str):
        path = _job_dir(name.removesuffix(".json")) / "report.json"
        if not path.is_file():
            raise HTTPException(status_code=404, detail="unknown job")
        return JSONResponse(json.loads(path.read_text(encoding="utf-8")))

    @router.api_route(API + "/file/{job}/{name}", methods=["GET", "HEAD"])
    async def file(job: str, name: str):
        if not re.fullmatch(r"[a-z_]+(?:_\d+)?\.(?:jpg|png)", name):
            raise HTTPException(status_code=404, detail="no such file")
        path = _job_dir(job) / name
        if not path.is_file():
            raise HTTPException(status_code=404, detail="no such file")
        return FileResponse(path, media_type="image/png" if name.endswith(".png") else "image/jpeg")

    return router
