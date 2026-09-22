#!/usr/bin/env python3
"""Build a standalone, evidence-only Avatar/video benchmark review page.

The tool never decides whether a render passed. It renders dispositions already
present in manual review JSON. A completed render without such a disposition is
explicitly labelled "Требует проверки".
"""

from __future__ import annotations

import argparse
import html
import json
import os
import mimetypes
import uuid
import urllib.request
import sys
import subprocess
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import quote


REVIEW_LABELS = {
    "accepted": ("Принято", "accepted"),
    "candidate": ("Кандидат", "candidate"),
    "rejected": ("Отклонено", "rejected"),
}

SOURCE_TITLES_RU = {
    "walk-and-address-camera": "Мужчина говорит и жестикулирует в камеру",
    "seated-speaking": "Сидящая женщина объясняет руками",
    "single-hand-gesture": "Жест одной рукой крупным планом",
    "face-object-interaction": "Женщина пьёт из банки",
    "rear-view-walk": "Человек уходит, камера поднимается к башне",
    "two-person-cafe-conversation": "Разговор двух людей в кафе",
    "two-person-walk": "Двое идут рядом с крупными аксессуарами",
    "seated-couple-object-action": "Пара за столом разрезает торт",
    "user-meeting": "Групповая презентация в офисе",
}

RETUNE_MEDIA = {
    "seated-speaking-pose-predecode-crop": {
        "title": "Речь и жест после исправления хвоста",
        "source": "https://autorig.online/dev/api/scratch/44a6b18dee12.mp4",
        "output": "https://autorig.online/renderfin/render/default_user/b586fa0e-3687-4d61-847a-18f0984ceeb6.mp4",
    },
    "single-hand-gesture-aligned-canny-strength1": {
        "title": "Жест «палец вниз» после alignment и Canny",
        "source": "https://autorig.online/dev/api/scratch/c8ff2cd4ef3a.mp4",
        "output": "https://autorig.online/renderfin/render/default_user/3c6c6229-3f4f-4a9e-b3aa-4b7f7a828b24.mp4",
    },
}


def read_json(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return {}
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def json_block(value: Any) -> str:
    return esc(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def as_path(value: Any, base: Path) -> Optional[Path]:
    if not value:
        return None
    path = Path(str(value))
    if not path.is_absolute():
        from_cwd = path.resolve()
        path = from_cwd if from_cwd.exists() else (base / path).resolve()
    return path


def public_or_file(path: Optional[Path], public_url: str = "") -> str:
    if public_url:
        return public_url
    if path and path.is_file():
        return path.resolve().as_uri()
    return ""


def uploaded_url(result: Mapping[str, Any], path: Path) -> str:
    url = str(result.get("url") or "")
    if not url.startswith(("http://", "https://")):
        raise RuntimeError(f"Upload of {path} returned no public URL")
    return url


def upload_file(endpoint: str, path: Path, transport: str, timeout: int) -> str:
    if transport == "curl":
        executable = shutil.which("curl.exe") or shutil.which("curl")
        if not executable:
            raise RuntimeError("curl transport requested but curl.exe was not found")
        completed = subprocess.run([
            executable, "--silent", "--show-error", "--fail-with-body",
            "--max-time", str(timeout), "--request", "POST",
            "--form", f"file=@{path}", "--write-out", "\n%{http_code}", endpoint,
        ], capture_output=True, text=True, encoding="utf-8", errors="replace",
           check=False)
        output, _, status = completed.stdout.rpartition("\n")
        if completed.returncode != 0 or not status.startswith("2"):
            detail = (completed.stderr or output or f"HTTP {status}").strip()
            raise RuntimeError(f"curl upload failed ({completed.returncode}): {detail}")
        try:
            return uploaded_url(json.loads(output), path)
        except json.JSONDecodeError as error:
            raise RuntimeError(f"Upload of {path} returned invalid JSON") from error

    boundary = "----autorig-report-" + uuid.uuid4().hex
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    payload = path.read_bytes()
    body = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{path.name}\"\r\n"
        f"Content-Type: {mime}\r\n\r\n"
    ).encode("utf-8") + payload + f"\r\n--{boundary}--\r\n".encode("ascii")
    request = urllib.request.Request(endpoint, data=body, method="POST", headers={
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "User-Agent": "AutoRigBenchmarkReport/1",
    })
    # A transport error can happen after the server accepted the bytes. Never
    # retry this POST blindly: only a recorded successful response permits the
    # caller to mark the source id complete.
    with urllib.request.urlopen(request, timeout=timeout) as response:
        result = json.loads(response.read().decode("utf-8"))
    return uploaded_url(result, path)


def upload_source_segments(state_path: Path, manifest: Mapping[str, Any],
                           known: Dict[str, str], endpoint: str,
                           url_manifest_path: Path, transport: str,
                           timeout: int) -> Dict[str, str]:
    def persist() -> None:
        url_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = url_manifest_path.with_suffix(url_manifest_path.suffix + ".tmp")
        temporary.write_text(json.dumps({
            "schema": "autorig.avatar-benchmark-source-urls/v1",
            "sources": known,
        }, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, url_manifest_path)

    sources_dir = state_path.parent / "sources"
    for source in manifest.get("sources_array") or []:
        source_id = str(source.get("id_string") or "")
        if not source_id or known.get(source_id):
            continue
        segment = sources_dir / source_id / "segment.mp4"
        if not segment.is_file():
            continue
        try:
            known[source_id] = upload_file(endpoint, segment, transport, timeout)
            persist()
            print(f"uploaded {source_id}", flush=True)
        except Exception as error:
            print(f"upload failed for {source_id}: {error}", file=sys.stderr, flush=True)
    return known


def baseline_review_entry(review_set: Mapping[str, Any], case_id: str,
                          source_id: str = "") -> Dict[str, Any]:
    for container in (review_set, review_set.get("cases_object") or {},
                      review_set.get("verdicts_object") or {},
                      review_set.get("cases") or {}):
        value = None
        if isinstance(container, dict):
            value = container.get(case_id) or (container.get(source_id) if source_id else None)
        if isinstance(value, dict):
            normalized = dict(value)
            if "pipeline_disposition_string" not in normalized:
                normalized["pipeline_disposition_string"] = (
                    normalized.get("disposition") or normalized.get("verdict") or ""
                )
            if "notes_string" not in normalized:
                normalized["notes_string"] = (normalized.get("notes") or
                                                normalized.get("reason_ru") or
                                                normalized.get("summary") or "")
            if "defects_array" not in normalized:
                normalized["defects_array"] = normalized.get("defects") or []
            return normalized
    return {}


def review_for(case_id: str, source_id: str, case: Mapping[str, Any], state_root: Path,
               authoritative: Mapping[str, Any]) -> Dict[str, Any]:
    supplied = baseline_review_entry(authoritative, case_id, source_id)
    if supplied:
        return supplied
    artifacts = case.get("artifacts_object") or {}
    review_path = as_path(artifacts.get("manual_review_path_string"), state_root)
    if review_path and review_path.is_file():
        return read_json(review_path)
    return {}


def verdict(case: Mapping[str, Any], review: Mapping[str, Any]) -> Dict[str, str]:
    disposition = str(review.get("pipeline_disposition_string") or "").lower()
    if disposition in REVIEW_LABELS:
        label, css = REVIEW_LABELS[disposition]
        return {"label": label, "css": css}
    status = str(case.get("status_string") or "unknown").lower()
    if status == "completed":
        return {"label": "Требует проверки", "css": "review"}
    if status in {"pending", "queued", "rendering", "running", "processing"}:
        return {"label": "В процессе", "css": "pending"}
    if status in {"failed", "cancelled", "error"}:
        return {"label": "Ошибка рендера", "css": "failed"}
    return {"label": "Нет результата", "css": "missing"}


def source_index(manifest: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {str(item.get("id_string")): dict(item)
            for item in manifest.get("sources_array") or []}


def case_order(manifest: Mapping[str, Any], cases: Mapping[str, Any]) -> List[str]:
    ordered = []
    for spec in manifest.get("cases_array") or []:
        key = f"{spec.get('source_id_string')}--{spec.get('pipeline_id_string')}"
        if key in cases and key not in ordered:
            ordered.append(key)
    ordered.extend(key for key in cases if key not in ordered)
    return ordered


def story_clips(receipt: Mapping[str, Any]) -> List[Dict[str, str]]:
    envelope = receipt.get("graph_object") if isinstance(receipt.get("graph_object"), dict) else receipt
    nodes = {str(node.get("id")): node for node in envelope.get("nodes") or []}
    results = envelope.get("results") or {}
    clips = []
    for node_id, node in nodes.items():
        if node.get("service") != "avatar_video":
            continue
        result = results.get(node_id) or {}
        value = str(result.get("value") or "")
        clips.append({
            "id": node_id,
            "label": str((node.get("params") or {}).get("_label") or f"Сцена {len(clips) + 1}"),
            "url": value if value.startswith(("http://", "https://")) else "",
            "status": str(result.get("status") or "unknown"),
        })
    return clips


def find_final_story_url(*objects: Mapping[str, Any]) -> str:
    preferred = {"final_story_url_string", "story_url_string", "final_video_url_string"}

    def walk(value: Any) -> Iterable[str]:
        if isinstance(value, dict):
            for key, item in value.items():
                if key in preferred and isinstance(item, str):
                    yield item
                yield from walk(item)
        elif isinstance(value, list):
            for item in value:
                yield from walk(item)

    for obj in objects:
        for url in walk(obj):
            if url.startswith(("http://", "https://")):
                return url
    return ""


def case_html(
    case_id: str,
    case: Mapping[str, Any],
    source: Mapping[str, Any],
    source_url: str,
    review: Mapping[str, Any],
) -> str:
    result_url = str(case.get("output_url_string") or
                     (case.get("last_status_object") or {}).get("output_url_string") or "")
    status = verdict(case, review)
    defects = review.get("defects_array") or []
    notes = str(review.get("notes_string") or "")
    limitations = str(source.get("sample_limitations_string") or "")
    # source_url points at benchmark/sources/<id>/segment.mp4 (or its uploaded
    # copy), which is already trimmed. Original-video offsets remain provenance
    # in the manifest and must not be applied a second time during playback.
    start = 0.0
    duration = float(source.get("segment_duration_seconds_float") or 4)
    settings = case.get("submitted_request_object") or {}
    video_source = (f'<video controls loop muted playsinline preload="metadata" '
                    f'data-role="source" data-start="{start}" data-duration="{duration}" '
                    f'src="{esc(source_url)}"></video>') if source_url else '<div class="no-media">Источник не опубликован</div>'
    video_result = (f'<video controls loop muted playsinline preload="metadata" '
                    f'data-role="generated" src="{esc(result_url)}"></video>') if result_url else '<div class="no-media">Результат ещё не готов</div>'
    defect_html = "".join(f"<li>{esc(item)}</li>" for item in defects)
    review_details = (
        f'<p>{esc(notes)}</p><ul>{defect_html}</ul>'
        if notes or defects else '<p>Ручной verdict отсутствует.</p>'
    )
    return f"""
    <article class="case" id="{esc(case_id)}">
      <header><div><h2>{esc(SOURCE_TITLES_RU.get(str(source.get('id_string')), source.get('id_string') or case_id))}</h2><code>{esc(case_id)}</code></div>
      <span class="verdict {esc(status['css'])}">{esc(status['label'])}</span></header>
      <div class="pair" data-pair>
        <section><h3>Исходный фрагмент · {duration:g} с</h3>{video_source}</section>
        <section><h3>Сгенерированное видео</h3>{video_result}</section>
        <button type="button" class="sync" data-sync>▶ Синхронно воспроизвести</button>
      </div>
      <details><summary>Provenance: исходный промпт и точные настройки</summary>
        <h3>Исходный английский промпт</h3><p class="prompt">{esc(settings.get('prompt') or source.get('prompt_string') or '')}</p>
        <h3>Фактический запрос</h3><pre>{json_block(settings)}</pre>
      </details>
      <details><summary>Ручная оценка</summary>{review_details}</details>
      {f'<p class="limitation"><b>Ограничение источника:</b> {esc(limitations)}</p>' if limitations else ''}
    </article>"""


def retunes_html(review_set: Mapping[str, Any]) -> str:
    reviews = review_set.get("retunes") or {}
    cards = []
    for retune_id, media in RETUNE_MEDIA.items():
        review = reviews.get(retune_id)
        if not isinstance(review, dict):
            continue
        disposition = str(review.get("disposition") or review.get("verdict") or "").lower()
        label, css = REVIEW_LABELS.get(disposition, ("Требует проверки", "review"))
        reason = review.get("reason_ru") or review.get("notes") or ""
        scope = review.get("evidence_scope") or ""
        defects = "".join(f"<li>{esc(item)}</li>" for item in review.get("defects") or [])
        cards.append(f"""
        <article class="case retune" id="{esc(retune_id)}">
          <header><div><h3>{esc(media['title'])}</h3><code>{esc(retune_id)}</code></div>
          <span class="verdict {esc(css)}">{esc(label)}</span></header>
          <div class="pair" data-pair>
            <section><h3>Контрольное движение</h3><video controls loop muted playsinline preload="metadata" data-role="source" data-start="0" data-duration="4.041667" src="{esc(media['source'])}"></video></section>
            <section><h3>Результат после исправления</h3><video controls loop muted playsinline preload="metadata" data-role="generated" src="{esc(media['output'])}"></video></section>
            <button type="button" class="sync" data-sync>▶ Синхронно воспроизвести</button>
          </div>
          <p>{esc(reason)}</p>
          {f'<ul>{defects}</ul>' if defects else ''}
          <p class="limitation"><b>Границы этой оценки:</b> {esc(scope)}</p>
        </article>""")
    if not cards:
        return ""
    return ('<section class="story retunes"><h2>Контроль движения после исправлений</h2>'
            '<p class="lead">Два дополнительных прогона показывают результат исправления хвоста видео и выравнивания управляющего движения. Под каждой парой указано, что именно проверено.</p>'
            + "".join(cards) + '</section>')


def build_report(
    state: Mapping[str, Any],
    manifest: Mapping[str, Any],
    state_path: Path,
    source_urls: Mapping[str, Any],
    baseline_review: Mapping[str, Any],
    story_receipt: Mapping[str, Any],
    story_final_receipt: Mapping[str, Any],
    story_review: Mapping[str, Any],
    graph_links: List[str],
) -> str:
    cases = state.get("cases_object") or {}
    sources = source_index(manifest)
    state_root = state_path.parent.parent.parent
    sections = []
    for case_id in case_order(manifest, cases):
        case = cases[case_id]
        source_id = str(case.get("source_id_string") or case_id.split("--", 1)[0])
        source = sources.get(source_id, {"id_string": source_id})
        segment = state_path.parent / "sources" / source_id / "segment.mp4"
        source_url = public_or_file(segment, str(source_urls.get(source_id) or ""))
        sections.append(case_html(case_id, case, source, source_url,
                                  review_for(case_id, source_id, case, state_root, baseline_review)))

    clips = story_clips(story_receipt)
    shot_reviews = story_review.get("shots_object") or story_review.get("shots") or {}
    for index, clip in enumerate(clips):
        review = (shot_reviews.get(clip["id"]) or shot_reviews.get(f"shot-{index + 1}")
                  or shot_reviews.get(str(index + 1)) or {}) if isinstance(shot_reviews, dict) else {}
        caption = (review.get("caption_ru") or review.get("title_ru") or
                   review.get("story_caption_ru") or review.get("caption"))
        if caption:
            clip["label"] = str(caption)
    assembled_shots = story_final_receipt.get("shots") or []
    for index, clip in enumerate(clips):
        if index < len(assembled_shots) and assembled_shots[index].get("caption"):
            clip["label"] = str(assembled_shots[index]["caption"])
    story_url = find_final_story_url(story_review, story_final_receipt, story_receipt)
    story_disposition = str(story_review.get("pipeline_disposition_string") or
                            story_review.get("disposition") or "").lower()
    story_label = REVIEW_LABELS.get(story_disposition, ("Требует проверки", "review"))
    story_scope = str(story_review.get("evidence_scope_string") or
                      story_review.get("evidence_scope") or "")
    clip_cards = "".join(
        f'<article class="story-clip"><h3>{esc(item["label"])}</h3>'
        + (f'<video controls loop muted playsinline preload="metadata" src="{esc(item["url"])}"></video>'
           if item["url"] else f'<p>{esc(item["status"])}</p>') + '</article>'
        for item in clips[:4]
    )
    final_story = (f'<video class="final-story" controls loop muted playsinline preload="metadata" src="{esc(story_url)}"></video>'
                   f'<p><a href="{esc(story_url)}">Открыть итоговый ролик</a></p>') if story_url else (
        '<p class="no-media">Итоговый story-файл ещё не приложен. Этот блок обновится при повторной сборке с receipt/review.</p>')
    links = [
        "Wan template=https://autorig.online/nodes?g=SavedAvatarWanMotion",
        "LTX template=https://autorig.online/nodes?g=SavedAvatarVideoMotion",
    ] + graph_links
    receipt_graph_url = str(story_final_receipt.get("graph_url_string") or "")
    if receipt_graph_url.startswith(("http://", "https://")):
        links.append("Полный граф истории=" + receipt_graph_url)
    graph_html = "".join(
        f'<a href="{esc(item.split("=", 1)[1])}">{esc(item.split("=", 1)[0])}</a>'
        for item in links if "=" in item
    )
    count = len(sections)
    evidence = manifest.get("evidence_scope_string") or ""
    retunes = retunes_html(baseline_review)
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Видео: исходники и результаты</title>
<style>
:root{{--bg:#0e1020;--card:#17192d;--line:#303452;--text:#edf0ff;--muted:#a6acc5;--cyan:#38bdf8}}
*{{box-sizing:border-box}}body{{margin:0;background:radial-gradient(circle at 15% 0,#20264b 0,transparent 34%),var(--bg);color:var(--text);font:14px/1.5 Inter,Segoe UI,sans-serif}}
main{{width:min(1320px,calc(100% - 28px));margin:auto;padding:42px 0 80px}}h1{{font-size:clamp(30px,5vw,54px);margin:0}}.lead{{color:var(--muted);max-width:900px}}
.summary,.case,.story{{background:rgba(23,25,45,.9);border:1px solid var(--line);border-radius:16px;padding:18px;margin-top:20px}}.summary{{display:flex;gap:14px;flex-wrap:wrap}}
.summary b{{font-size:22px}}.case header{{display:flex;justify-content:space-between;gap:15px;align-items:start}}h2,h3{{margin:.15em 0}}code{{color:var(--muted)}}
.verdict{{padding:6px 10px;border-radius:999px;font-weight:700;white-space:nowrap}}.accepted{{background:#14532d}}.candidate{{background:#713f12}}.rejected,.failed{{background:#7f1d1d}}.review{{background:#164e63}}.pending,.missing{{background:#374151}}
.pair{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:16px}}.pair section{{min-width:0}}video{{width:100%;max-height:470px;background:#080912;border-radius:10px;display:block}}.sync{{grid-column:1/-1;justify-self:center;border:1px solid #4d58a4;background:#29316b;color:white;padding:9px 15px;border-radius:9px;cursor:pointer}}
details{{margin-top:12px;border-top:1px solid var(--line);padding-top:10px}}summary{{cursor:pointer;color:#c8d1ff}}pre,.prompt{{white-space:pre-wrap;overflow-wrap:anywhere;background:#0a0c18;padding:12px;border-radius:9px;color:#cbd5e1}}.limitation{{color:#fcd34d}}.no-media{{display:grid;place-items:center;min-height:180px;background:#0a0c18;color:var(--muted);border-radius:10px;padding:18px;text-align:center}}
.story-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}}.story-clip{{background:#101326;padding:10px;border-radius:11px}}.graphs{{display:flex;gap:10px;flex-wrap:wrap}}.graphs a,a{{color:#67e8f9}}@media(max-width:760px){{.pair{{grid-template-columns:1fr}}}}
</style></head><body><main>
<h1>Видео: исходники и результаты</h1>
<p class="lead">Здесь исходные четырёхсекундные движения стоят рядом с результатами генерации. Для каждого теста отдельно указаны оценка качества, найденные дефекты и объём проведённой проверки.</p>
<details class="summary"><summary>Как подготовлены исходники</summary><p>{esc(evidence)}</p></details>
<div class="summary"><span><b>{count}</b><br>Тестов</span><span><b>{len(clips[:4])}</b><br>Сцен истории</span><span><b>{sum(1 for key in case_order(manifest, cases) if verdict(cases[key], review_for(key, str(cases[key].get('source_id_string') or key.split('--', 1)[0]), cases[key], state_root, baseline_review))["css"] == "review")}</b><br>Ожидают оценки</span></div>
{''.join(sections)}
{retunes}
<section class="story"><header><h2>{esc(story_final_receipt.get('title') or story_review.get('title_string') or 'План на двоих')}</h2><span class="verdict {esc(story_label[1])}">{esc(story_label[0])}</span></header>
{f'<p class="lead"><b>Границы оценки:</b> {esc(story_scope)}</p>' if story_scope else ''}
<div class="story-grid">{clip_cards or '<p>Story receipt не приложен.</p>'}</div><h3>Итоговый ролик</h3>{final_story}</section>
<section class="story"><h2>Повторно используемые графы</h2><div class="graphs">{graph_html}</div></section>
</main><script>
document.querySelectorAll('[data-pair]').forEach(pair=>{{const button=pair.querySelector('[data-sync]');const source=pair.querySelector('[data-role="source"]');const generated=pair.querySelector('[data-role="generated"]');if(!source||!generated){{button.disabled=true;return}}const playLabel='▶ Синхронно воспроизвести';let lastSourceTime=0;button.addEventListener('click',async()=>{{if(!source.paused||!generated.paused){{source.pause();generated.pause();button.textContent=playLabel;return}}const start=Number(source.dataset.start||0);source.currentTime=start;generated.currentTime=0;lastSourceTime=start;source.muted=true;generated.muted=true;try{{await Promise.all([source.play(),generated.play()]);button.textContent='❚❚ Пауза'}}catch(e){{source.pause();generated.pause();button.textContent='Не удалось запустить'}}}});[source,generated].forEach(v=>v.addEventListener('pause',()=>{{if(source.paused&&generated.paused)button.textContent=playLabel}}));source.addEventListener('timeupdate',()=>{{const start=Number(source.dataset.start||0);const duration=Number(source.dataset.duration||4);const wrapped=source.currentTime+.2<lastSourceTime;let sourceElapsed=Math.max(0,source.currentTime-start);if(wrapped||sourceElapsed>=duration){{sourceElapsed=0;generated.currentTime=0;if(source.currentTime<start||source.currentTime>=start+duration)source.currentTime=start}}else if(!generated.paused&&Math.abs(generated.currentTime-sourceElapsed)>.12){{generated.currentTime=Math.min(sourceElapsed,Number.isFinite(generated.duration)?generated.duration:sourceElapsed)}}lastSourceTime=source.currentTime}})}});
</script></body></html>"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--source-url-manifest", type=Path)
    parser.add_argument("--upload-source-segments", action="store_true")
    parser.add_argument("--upload-endpoint", default="https://autorig.online/dev/api/scratch")
    parser.add_argument("--http-transport", choices=("urllib", "curl"), default="urllib")
    parser.add_argument("--upload-timeout", type=int, default=120)
    parser.add_argument("--baseline-review", type=Path)
    parser.add_argument("--story-receipt", type=Path)
    parser.add_argument("--story-final-receipt", type=Path)
    parser.add_argument("--story-review", type=Path)
    parser.add_argument("--graph-link", action="append", default=[], metavar="LABEL=URL")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = read_json(args.state)
    manifest = read_json(args.manifest)
    source_manifest = (read_json(args.source_url_manifest)
                       if args.source_url_manifest and args.source_url_manifest.exists() else {})
    source_urls = dict(source_manifest.get("sources") or source_manifest)
    if args.upload_source_segments:
        if not args.source_url_manifest:
            raise SystemExit("--upload-source-segments requires --source-url-manifest")
        source_urls = upload_source_segments(args.state.resolve(), manifest, source_urls,
                                             args.upload_endpoint,
                                             args.source_url_manifest,
                                             args.http_transport,
                                             max(5, args.upload_timeout))
    baseline_review = read_json(args.baseline_review)
    story_receipt = read_json(args.story_receipt)
    story_final_receipt = read_json(args.story_final_receipt)
    story_review = read_json(args.story_review)
    report = build_report(state, manifest, args.state.resolve(), source_urls, baseline_review,
                          story_receipt, story_final_receipt, story_review, args.graph_link)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8", newline="\n")
    print(json.dumps({
        "output": str(args.output.resolve()),
        "baseline_cases": len(state.get("cases_object") or {}),
        "story_clips": len(story_clips(story_receipt)),
        "final_story": bool(find_final_story_url(story_review, story_final_receipt, story_receipt)),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
