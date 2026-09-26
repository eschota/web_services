"""Fleet model manager: base models (checkpoints / diffusion models) by link.

The /lora page's "Models" tab. An administrator pastes a Civitai model or
version link (or a Hugging Face file link); the VPS resolves it through the
Civitai API with the site's token (the token never leaves the VPS), picks the
catalogue checkpoint of the same architecture as a *template* (its workflow,
sampling policy and recommended settings are what the new file runs with) and
starts a download on every render box that runs that template and that the VPS
can reach over SSH.

How a model travels
-------------------
1. Resolve: base model -> family (ai_lora_manager.family_for_base), file,
   SHA-256, size. No usable checkpoint of that family on the farm -> refused:
   there would be no workflow to load it.
2. Register: a row in model_catalogue.json with `managed_by: fleet-models`.
   It stays `usable: false` until at least one box holds the verified file;
   `validated_workers` follows the boxes that report it ready. Renderfin
   dispatches a checkpoint only to a box whose ComfyUI lists the file
   (renderfin/model_eligibility.py), so nothing else has to be told.
3. Download: per box, over SSH, a small PowerShell job is written to
   C:\\ProgramData\\AutoRig\\model-dl\\ and started detached (WMI, so it
   outlives the SSH session). It pulls a LAN peer first when one is configured
   (f15 serves its models folder on the LAN), then the short-lived presigned
   Civitai CDN link (authorises that one file only), checks SHA-256 and moves
   the file into ComfyUI\\models\\diffusion_models. It writes its state to a
   JSON file the VPS polls every ~20 s for the progress bar.
4. Delete: only rows this manager added. The row leaves the catalogue first
   (nothing can be queued on it any more), then each box stops its own
   download job for that hash and deletes the file - only when the file's size
   matches the registered one. Curated rows (Qwen-Image 2.1 turbo, Krea 2,
   Z-Image, LTX-2.5, MiniMax H3, ...) and every file a workflow template names
   are protected and cannot be deleted here.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import os
import pathlib
import re
import time
import urllib.parse
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

import ai_lora_manager as lm

logger = logging.getLogger(__name__)

router = APIRouter()

MANAGED_BY = "fleet-models"
MODEL_SUBDIR = "diffusion_models"
MAX_MODEL_BYTES = 60 * 1024 ** 3
FREE_MARGIN_BYTES = 10 * 1024 ** 3
POLL_SECONDS = 20
# Boxes this manager never pushes to, whatever the template says.
# f12 is under repair (2026-09-26); worker-4090 is the owner's workstation
# and has no SSH route from the VPS.
EXCLUDED_BOXES = {b.strip() for b in os.getenv("AUTORIG_FLEET_MODELS_EXCLUDE", "f12").split(",") if b.strip()}
# f15 serves D:\ComfyUI_windows_portable\ComfyUI\models on the LAN
# (python -m http.server 18998), see ai_lora_manager.F15_LAN_PEER.
F15_MODELS_PEER = lm.F15_LAN_PEER.rsplit("/loras/", 1)[0] + "/" + MODEL_SUBDIR + "/"
MODEL_PEERS: Dict[str, List[str]] = {"f5": [F15_MODELS_PEER], "Raptor": [F15_MODELS_PEER]}
FINAL_STATES = {"ready", "failed", "hash_mismatch", "no_space", "no_route", "deleted",
                "not_needed", "exists_different"}

# Fields a managed row inherits from its template: what the workflow needs to
# run the file. Provenance, validation and defaults stay with the template.
TEMPLATE_FIELDS = ("kind", "family", "base", "services", "workflow", "recommended",
                   "sampling_policy", "frame_range", "qwen_image_modes", "qwen_image_generation",
                   "companion_files", "loader", "recommendation_kind")

_lock = asyncio.Lock()
_monitors: Dict[str, asyncio.Task] = {}


# ------------------------------------------------------------------ storage

def _now() -> int:
    return int(time.time())


def _catalogue_file() -> pathlib.Path:
    import ai_model_catalogue
    return pathlib.Path(ai_model_catalogue.CATALOGUE_FILE)


def _dir() -> pathlib.Path:
    import ai_model_catalogue
    return pathlib.Path(ai_model_catalogue.CATALOGUE_DIR) / "fleet_models"


def load_registry() -> Dict[str, Any]:
    data = lm._read_json(_dir() / "registry.json", {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("version", 1)
    data.setdefault("models", [])
    return data


def save_registry(data: Dict[str, Any]) -> None:
    lm._write_json(_dir() / "registry.json", data)


def _read_catalogue() -> List[Dict[str, Any]]:
    raw = json.loads(_catalogue_file().read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise RuntimeError("model_catalogue.json is not a list")
    return raw


def _write_catalogue(rows: List[Dict[str, Any]], why: str) -> None:
    """Atomic rewrite with a one-off backup; the cache is dropped at once."""
    path = _catalogue_file()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    try:
        backup = path.with_name(f"{path.name}.bak.{stamp}-{why}")
        backup.write_bytes(path.read_bytes())
    except OSError:
        logger.warning("Could not back up the model catalogue", exc_info=True)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(rows, indent=1, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)
    import ai_model_catalogue
    ai_model_catalogue._cache_at = 0.0


def _update_catalogue_row(managed_id: str, fn: Callable[[Optional[Dict[str, Any]], List[Dict[str, Any]]], bool],
                          why: str) -> None:
    """Re-read the catalogue, let `fn` change our row (or the list), write back.

    Always re-read right before writing: the catalogue is hand-edited by other
    people and agents, and only our own row may change here.
    """
    rows = _read_catalogue()
    row = next((r for r in rows if isinstance(r, dict) and r.get("managed_id") == managed_id), None)
    if fn(row, rows):
        _write_catalogue(rows, why)


# ---------------------------------------------------------------- protection

def workflow_files() -> set:
    names = set()
    for path in lm.WORKFLOWS_DIR.glob("*.json"):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        names.update(re.findall(r'"(?:unet_name|ckpt_name)"\s*:\s*"([^"$]+)"', text))
    return names


def is_protected(row: Dict[str, Any]) -> Tuple[bool, str]:
    if row.get("managed_by") != MANAGED_BY:
        return True, "curated core model (hand-maintained catalogue row)"
    if row.get("default_for_families") or row.get("default_for_services") or row.get("qwen_image_default"):
        return True, "a default model"
    return False, ""


# ------------------------------------------------------------ resolving

def _pick_model_file(files: List[Dict[str, Any]], prefer: str = "") -> Dict[str, Any]:
    kinds = ("Model", "Diffusion Model", "Pruned Model")
    usable = [f for f in files if str(f.get("type") or "Model") in kinds
              and (str(f.get("name") or "").lower().endswith((".safetensors", ".gguf"))
                   or str((f.get("metadata") or {}).get("format") or "").lower() in ("safetensor", "gguf"))]
    if not usable:
        raise lm.ResolveError("no_safetensors", "This version has no .safetensors / .gguf model file; "
                              "pickled checkpoints are not accepted")
    if prefer:
        chosen = [f for f in usable if str(f.get("id")) == str(prefer)
                  or str(f.get("name")) == prefer
                  or str((f.get("metadata") or {}).get("fp") or "").lower() == prefer.lower()]
        if chosen:
            return chosen[0]
    primary = [f for f in usable if f.get("primary")]
    return (primary or usable)[0]


def template_for(family: str) -> Optional[Dict[str, Any]]:
    """The curated usable checkpoint whose workflow a new file of `family` uses."""
    import ai_model_catalogue
    rows = [r for r in ai_model_catalogue.raw_entries()
            if r.get("kind") == "checkpoint" and r.get("usable") and r.get("family") == family
            and r.get("managed_by") != MANAGED_BY and r.get("workflow")]
    if not rows:
        return None
    defaults = [r for r in rows if family in (r.get("default_for_families") or [])
                or r.get("qwen_image_default")]
    return (defaults or rows)[0]


async def resolve_model(url: str, base_hint: str = "", prefer_file: str = "") -> Dict[str, Any]:
    source = lm.parse_source_url(url)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        if source["kind"] == "civitai":
            version_id = source.get("version_id")
            if not version_id:
                model = await lm._civitai_json(client, f"/models/{source['model_id']}")
                versions = model.get("modelVersions") or []
                if not versions:
                    raise lm.ResolveError("not_found", "That model has no published version")
                version_id = versions[0]["id"]
            version = await lm._civitai_json(client, f"/model-versions/{version_id}")
            info = version.get("model") or {}
            kind = str(info.get("type") or "").upper()
            if kind in ("LORA", "LOCON", "LYCORIS", "DORA"):
                raise lm.ResolveError("is_a_lora", "That is a LoRA: add it on the LoRA tab")
            if kind and kind != "CHECKPOINT":
                raise lm.ResolveError("not_a_model", f"That is a {kind.title()}, not a checkpoint")
            chosen = _pick_model_file(version.get("files") or [], prefer_file)
            sha = str((chosen.get("hashes") or {}).get("SHA256") or "").lower()
            if not re.fullmatch(r"[0-9a-f]{64}", sha):
                raise lm.ResolveError("no_hash", "Civitai publishes no SHA-256 for this file")
            base = str(version.get("baseModel") or "")
            previews = [img for img in version.get("images") or []
                        if str(img.get("type") or "image") == "image"
                        and int(img.get("nsfwLevel") or 0) <= 1 and img.get("url")]
            record = {
                "id": f"civitai-{version['id']}-{chosen.get('id')}",
                "file": lm.safe_file_name(chosen.get("name")),
                "source_file_name": str(chosen.get("name") or ""),
                "sha256": sha,
                "size_bytes": int(float(chosen.get("sizeKB") or 0) * 1024),
                "precision": str((chosen.get("metadata") or {}).get("fp") or ""),
                "base": base,
                "title": str(info.get("name") or ""),
                "version": str(version.get("name") or ""),
                "page": f"https://civitai.com/models/{version.get('modelId')}?modelVersionId={version['id']}",
                "nsfw": bool(info.get("nsfw")) or int(version.get("nsfwLevel") or 0) > 2,
                "preview_source": re.sub(r"/original=true/", "/width=450/", str(previews[0]["url"])) if previews else "",
                "other_files": [{"id": f.get("id"), "name": f.get("name"),
                                 "size_bytes": int(float(f.get("sizeKB") or 0) * 1024),
                                 "precision": (f.get("metadata") or {}).get("fp")}
                                for f in version.get("files") or [] if f is not chosen],
                "source": {"kind": "civitai", "model_id": version.get("modelId"),
                           "version_id": version["id"], "file_id": chosen.get("id"),
                           "download_url": str(chosen.get("downloadUrl") or "")},
            }
        else:
            record = await lm.resolve_hf(client, source, base_hint)
            record = {k: record[k] for k in ("file", "source_file_name", "sha256", "size_bytes",
                                             "title", "version", "page", "source")}
            record.update(id=f"hf-{record['sha256'][:16]}", base=base_hint, nsfw=False,
                          preview_source="", precision="", other_files=[])
    family = lm.family_for_base(base_hint or record["base"]) if (base_hint or record["base"]) else ""
    if not family and record["base"]:
        family = lm.family_for_base(record["base"])
    record["family"] = family
    if record["size_bytes"] > MAX_MODEL_BYTES:
        raise lm.ResolveError("too_large", "That file is larger than 60 GB")
    template = template_for(family) if family else None
    record["template_id"] = str((template or {}).get("id") or "")
    record["template_title"] = str((template or {}).get("title") or "")
    record["workflow"] = str((template or {}).get("workflow") or "")
    record["target_boxes"] = sorted(target_boxes(template)) if template else []
    return record


# ---------------------------------------------------------------- boxes

def target_boxes(template: Optional[Dict[str, Any]]) -> set:
    """Boxes that run the template's workflow and that the VPS can reach."""
    if not template:
        return set()
    configured = lm.boxes()
    candidates = set(template.get("validated_workers") or configured)
    runners = lm.workflow_boxes(str(template.get("workflow") or ""))
    if runners is not None and runners:
        candidates &= runners | set(template.get("validated_workers") or [])
    return {b for b in candidates if b in configured and b not in EXCLUDED_BOXES}


def _ssh_route(box: str) -> Optional[Dict[str, Any]]:
    cfg = lm.boxes().get(box) or {}
    return cfg if cfg.get("ssh_port") else None


async def run_ps(box: str, script: str, timeout: float = 60.0) -> Tuple[int, str]:
    """Run a PowerShell script on a box (scp + -File: no quoting games, no length cap)."""
    cfg = _ssh_route(box)
    if not cfg or not lm.FARM_SSH_KEY.is_file():
        return -1, "no ssh route"
    # cmd.exe (the Windows OpenSSH shell) caps a command line at 8191
    # characters and Windows sshd does not pass stdin EOF to PowerShell, so
    # the script is copied to the user's home with scp and run with -File.
    body = ("\ufeff$ProgressPreference='SilentlyContinue'\n" + script).encode("utf-8")
    name = f"autorig-fm-{os.getpid()}-{int(time.time() * 1000) % 10**9}.ps1"
    known = lm._path("known_hosts")
    common = ["-i", str(lm.FARM_SSH_KEY), "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
              "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={known}"]
    target = f"{cfg.get('ssh_user') or 'user'}@{lm.FARM_GATEWAY}"
    local = _dir() / "tmp" / name
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(body)
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "scp", *common, "-P", str(int(cfg["ssh_port"])), str(local), f"{target}:{name}",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _out, err = await asyncio.wait_for(proc.communicate(), timeout=40)
        if proc.returncode:
            return -1, "scp failed: " + err.decode("utf-8", "replace")[-200:]
        proc = await asyncio.create_subprocess_exec(
            "ssh", *common, "-p", str(int(cfg["ssh_port"])), target,
            f"powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File {name} & del {name}",
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        text = out.decode("utf-8", "replace")
        if proc.returncode:
            text += "\n" + err.decode("utf-8", "replace")[-300:]
        return proc.returncode or 0, text
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return -1, "ssh timed out"
    except Exception as exc:
        return -1, f"{type(exc).__name__}: {exc}"
    finally:
        local.unlink(missing_ok=True)


def _json_line(text: str) -> Dict[str, Any]:
    for line in reversed(text.splitlines()):
        line = line.strip().lstrip("\ufeff")
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except ValueError:
                continue
    return {}


def _ps_str(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


PS_COMMON = r"""
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$Home_ = 'C:\ProgramData\AutoRig\model-dl'
New-Item -ItemType Directory -Force $Home_ | Out-Null
$Root = ''
foreach ($c in 'D:\ComfyUI_windows_portable', 'C:\AI\ComfyUI_windows_portable', 'C:\ComfyUI_windows_portable') {
    if (Test-Path -LiteralPath ($c + '\ComfyUI\main.py') -ErrorAction SilentlyContinue) { $Root = $c; break }
}
function Out-Json($o) { Write-Output ($o | ConvertTo-Json -Compress -Depth 4) }
"""

JOB_BODY = r"""
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
$St = Join-Path $Home_ ($Sha + '.json')
function St($state, $err) {
    $o = @{ state = $state; error = [string]$err; file = $File; pid = $PID;
            updated = [int64](([datetime]::UtcNow - [datetime]'1970-01-01').TotalSeconds) }
    Set-Content -Path $St -Value ($o | ConvertTo-Json -Compress) -Encoding utf8
}
if (-not $Root) { St 'failed' 'ComfyUI root not found'; exit 1 }
$DestDir = Join-Path $Root ('ComfyUI\models\' + $Sub)
$Tmp = Join-Path $Root 'autorig_model_tmp'
New-Item -ItemType Directory -Force $DestDir, $Tmp | Out-Null
$Dest = Join-Path $DestDir $File
$Part = Join-Path $Tmp ($Sha + '.part')
if (Test-Path -LiteralPath $Dest) {
    St 'verifying' ''
    $h = (Get-FileHash -Algorithm SHA256 -LiteralPath $Dest).Hash.ToLower()
    if ($h -eq $Sha) { St 'ready' 'already on the box' } else { St 'exists_different' ('a different ' + $File + ' is already here') }
    exit 0
}
$drive = (Get-Item $DestDir).PSDrive.Name
$have = 0; if (Test-Path -LiteralPath $Part) { $have = (Get-Item -LiteralPath $Part).Length }
if ((Get-PSDrive $drive).Free -lt ($Size - $have + 10GB)) { St 'no_space' ('less than size + 10 GB free on ' + $drive + ':'); exit 1 }
$curl = Join-Path $env:SystemRoot 'System32\curl.exe'
$ok = $false
foreach ($u in $Urls) {
    if (-not $u) { continue }
    St 'downloading' ''
    & $curl -sS -f -L -C - --connect-timeout 10 --retry 3 --retry-delay 5 --speed-time 180 --speed-limit 20480 -o $Part $u 2>&1 | Out-Null
    if ((Test-Path -LiteralPath $Part) -and (Get-Item -LiteralPath $Part).Length -eq $Size) { $ok = $true; break }
    if ((Test-Path -LiteralPath $Part) -and (Get-Item -LiteralPath $Part).Length -gt $Size) { Remove-Item -Force -LiteralPath $Part }
}
if (-not $ok) { St 'failed' 'download failed (all sources)'; exit 1 }
St 'verifying' ''
$h = (Get-FileHash -Algorithm SHA256 -LiteralPath $Part).Hash.ToLower()
if ($h -ne $Sha) { Remove-Item -Force -LiteralPath $Part; St 'hash_mismatch' ('downloaded file hashes to ' + $h.Substring(0, 12)); exit 1 }
Move-Item -Force -LiteralPath $Part -Destination $Dest
St 'ready' ''
Remove-Item -Force -LiteralPath $MyInvocation.MyCommand.Path -ErrorAction SilentlyContinue
"""


def _vars(entry: Dict[str, Any]) -> str:
    return (f"$Sha = {_ps_str(entry['sha256'])}\n$File = {_ps_str(entry['file'])}\n"
            f"$Sub = {_ps_str(MODEL_SUBDIR)}\n$Size = [int64]{int(entry['size_bytes'])}\n")


async def start_box(entry: Dict[str, Any], box: str, source_url: str) -> Dict[str, Any]:
    """Write the job on the box and start it detached; returns the first state."""
    urls = [peer + urllib.parse.quote(entry["file"]) for peer in MODEL_PEERS.get(box, [])]
    if source_url:
        urls.append(source_url)
    job = (PS_COMMON + _vars(entry) + "$Urls = @(" + ", ".join(_ps_str(u) for u in urls) + ")\n"
           + JOB_BODY)
    job_b64 = base64.b64encode(job.encode("utf-8")).decode("ascii")
    launcher = PS_COMMON + _vars(entry) + f"""
$St = Join-Path $Home_ ($Sha + '.json')
if (Test-Path $St) {{
    $s = Get-Content $St -Raw | ConvertFrom-Json
    if ($s.state -in 'downloading', 'verifying' -and (Get-Process -Id $s.pid -ErrorAction SilentlyContinue)) {{
        Out-Json @{{ started = $false; state = $s.state; note = 'already running' }}; exit 0 }}
}}
$Job = Join-Path $Home_ ($Sha + '.ps1')
[IO.File]::WriteAllBytes($Job, [Convert]::FromBase64String('{job_b64}'))
Set-Content -Path $St -Value (@{{ state = 'queued'; error = ''; file = $File; pid = 0; updated = 0 }} | ConvertTo-Json -Compress) -Encoding utf8
$cmd = 'powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File "' + $Job + '"'
$r = ([wmiclass]'Win32_Process').Create($cmd)
Out-Json @{{ started = ($r.ReturnValue -eq 0); pid = $r.ProcessId; state = 'queued' }}
"""
    code, out = await run_ps(box, launcher, timeout=90)
    result = _json_line(out)
    if code != 0 or not result:
        return {"state": "failed", "error": f"could not start the download: {out.strip()[-200:] or code}"}
    if not result.get("started") and result.get("note") != "already running":
        return {"state": "failed", "error": "the box refused to start the download job"}
    return {"state": result.get("state") or "queued", "error": ""}


async def poll_box(entry: Dict[str, Any], box: str) -> Dict[str, Any]:
    script = PS_COMMON + _vars(entry) + r"""
$St = Join-Path $Home_ ($Sha + '.json')
$o = @{ state = 'unknown'; error = ''; bytes = 0; alive = $false; free = 0 }
if (Test-Path $St) { $s = Get-Content $St -Raw | ConvertFrom-Json; $o.state = $s.state; $o.error = $s.error
    if ($s.pid) { $o.alive = [bool](Get-Process -Id $s.pid -ErrorAction SilentlyContinue) } }
if ($Root) {
    $Part = Join-Path $Root ('autorig_model_tmp\' + $Sha + '.part')
    $Dest = Join-Path $Root ('ComfyUI\models\' + $Sub + '\' + $File)
    if (Test-Path -LiteralPath $Part) { $o.bytes = (Get-Item -LiteralPath $Part).Length }
    elseif (Test-Path -LiteralPath $Dest) { $o.bytes = (Get-Item -LiteralPath $Dest).Length; $o.on_disk = $true }
    $o.free = (Get-PSDrive (Split-Path $Root -Qualifier).TrimEnd(':')).Free
}
Out-Json $o
"""
    code, out = await run_ps(box, script, timeout=60)
    result = _json_line(out)
    if code != 0 or not result:
        return {"state": "unreachable", "error": out.strip()[-160:]}
    state = str(result.get("state") or "unknown")
    if state in ("downloading", "verifying", "queued") and not result.get("alive") and state != "queued":
        state, result["error"] = "failed", "the download job stopped (box restarted?)"
    return {"state": state, "error": str(result.get("error") or ""),
            "bytes": int(result.get("bytes") or 0), "free_bytes": int(result.get("free") or 0)}


async def delete_on_box(entry: Dict[str, Any], box: str) -> Dict[str, Any]:
    script = PS_COMMON + _vars(entry) + r"""
$o = @{ deleted = $false; error = '' }
# Stop our own download job for this hash (never anything else).
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine.Contains($Sha) } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-Sleep -Milliseconds 500
if ($Root) {
    $Part = Join-Path $Root ('autorig_model_tmp\' + $Sha + '.part')
    Remove-Item -Force -LiteralPath $Part -ErrorAction SilentlyContinue
    $Dest = Join-Path $Root ('ComfyUI\models\' + $Sub + '\' + $File)
    if (Test-Path -LiteralPath $Dest) {
        if ((Get-Item -LiteralPath $Dest).Length -ne $Size) { $o.error = 'file size differs from the registered one: left in place' }
        else { try { Remove-Item -Force -LiteralPath $Dest -ErrorAction Stop; $o.deleted = $true }
               catch { $o.error = 'in use (ComfyUI holds it?): ' + $_.Exception.Message } }
    } else { $o.deleted = $true; $o.note = 'not on the box' }
}
Remove-Item -Force -LiteralPath (Join-Path $Home_ ($Sha + '.json')), (Join-Path $Home_ ($Sha + '.ps1')) -ErrorAction SilentlyContinue
Out-Json $o
"""
    code, out = await run_ps(box, script, timeout=90)
    result = _json_line(out)
    if code != 0 or not result:
        return {"state": "delete_failed", "error": out.strip()[-160:] or "no answer"}
    if result.get("deleted"):
        return {"state": "deleted", "error": ""}
    return {"state": "delete_failed", "error": str(result.get("error") or "")}


# ------------------------------------------------------------- lifecycle

async def _source_url(entry: Dict[str, Any]) -> str:
    source = entry.get("source") or {}
    if source.get("kind") == "civitai":
        # Presigned CDN link for this one file; the API token stays here.
        lm._presigned.pop("fm-" + entry["id"], None)
        return await lm._presigned_url({"id": "fm-" + entry["id"], "source": source})
    return str(source.get("download_url") or "")


async def _set_box(entry_id: str, box: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    async with _lock:
        data = load_registry()
        entry = next((e for e in data["models"] if e.get("id") == entry_id), None)
        if not entry:
            return None
        current = entry.setdefault("box_states", {}).get(box) or {}
        merged = {**current, **state, "updated_at": _now()}
        if state.get("state") == "ready":
            merged["bytes"] = entry.get("size_bytes") or merged.get("bytes") or 0
        if state.get("state") in ("downloading", "queued") and not current.get("started_at"):
            merged["started_at"] = _now()
        if state.get("state") == "ready" and not current.get("ready_at"):
            merged["ready_at"] = _now()
        entry["box_states"][box] = merged
        save_registry(data)
        return entry


def _ready(entry: Dict[str, Any]) -> List[str]:
    return sorted(b for b, s in (entry.get("box_states") or {}).items() if s.get("state") == "ready")


def _sync_catalogue(entry: Dict[str, Any]) -> None:
    """Keep the catalogue row's usable flag and validated_workers in step."""
    ready = _ready(entry)

    def change(row, _rows):
        if row is None:
            return False
        usable = bool(ready) and not entry.get("hidden")
        reason = ("" if usable else "test entry (hidden)" if entry.get("hidden")
                  else "waiting for the render computers to download it")
        if row.get("validated_workers") == ready and row.get("usable") == usable \
                and row.get("unusable_reason", "") == reason:
            return False
        row["validated_workers"] = ready
        row["usable"] = usable
        row["unusable_reason"] = reason
        return True
    _update_catalogue_row(entry["id"], change, "fleet-models-state")


async def _monitor(entry_id: str) -> None:
    try:
        while True:
            data = load_registry()
            entry = next((e for e in data["models"] if e.get("id") == entry_id), None)
            if not entry or entry.get("state") != "active":
                return
            pending = [b for b, s in (entry.get("box_states") or {}).items()
                       if s.get("state") not in FINAL_STATES]
            if pending:
                results = await asyncio.gather(*(poll_box(entry, b) for b in pending))
                for box, result in zip(pending, results):
                    if result["state"] == "unreachable":
                        await _set_box(entry_id, box, {"error": "box did not answer: " + result["error"]})
                        continue
                    if result["state"] == "unknown":
                        continue
                    entry = await _set_box(entry_id, box, result) or entry
            entry = next((e for e in load_registry()["models"] if e.get("id") == entry_id), entry)
            _sync_catalogue(entry)
            if not [b for b, s in (entry.get("box_states") or {}).items() if s.get("state") not in FINAL_STATES]:
                return
            await asyncio.sleep(POLL_SECONDS)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("fleet model monitor for %s failed", entry_id)
    finally:
        _monitors.pop(entry_id, None)


def ensure_monitors() -> None:
    for entry in load_registry()["models"]:
        if entry.get("state") != "active":
            continue
        if not any(s.get("state") not in FINAL_STATES for s in (entry.get("box_states") or {}).values()):
            continue
        task = _monitors.get(entry["id"])
        if task is None or task.done():
            _monitors[entry["id"]] = asyncio.get_event_loop().create_task(_monitor(entry["id"]))


async def start_downloads(entry_id: str, only: Optional[List[str]] = None) -> None:
    entry = next((e for e in load_registry()["models"] if e.get("id") == entry_id), None)
    if not entry:
        return
    url = await _source_url(entry)
    boxes = [b for b in entry.get("target_boxes") or [] if not only or b in only]
    for box in boxes:
        if not _ssh_route(box):
            await _set_box(entry_id, box, {"state": "no_route", "error": "the VPS has no SSH route to this box"})
            continue
        if not url and not MODEL_PEERS.get(box):
            await _set_box(entry_id, box, {"state": "failed", "error": "no download link (Civitai refused the presign)"})
            continue
        await _set_box(entry_id, box, {"state": "queued", "error": "", "bytes": 0})
        result = await start_box(entry, box, url)
        await _set_box(entry_id, box, result)
    ensure_monitors()


async def add_model(url: str, *, base: str = "", prefer_file: str = "", title: str = "",
                    only_boxes: Optional[List[str]] = None, hidden: bool = False,
                    added_by: str = "") -> Dict[str, Any]:
    record = await resolve_model(url, base, prefer_file)
    if not record["family"]:
        raise lm.ResolveError("unknown_base", f"Base model '{record['base']}' is not one the farm knows")
    template = template_for(record["family"])
    if not template:
        raise lm.ResolveError("no_workflow", f"No workflow on the farm runs {record['base'] or record['family']} "
                              "models: a new architecture needs a workflow first", 409)
    import ai_model_catalogue
    rows = _read_catalogue()
    if any(r.get("file") == record["file"] for r in rows) or record["file"] in workflow_files():
        clash = next((r for r in rows if r.get("file") == record["file"]), None)
        if clash and clash.get("managed_id") and clash.get("sha256") == record["sha256"]:
            data = load_registry()
            entry = next((e for e in data["models"] if e.get("id") == clash["managed_id"]), None)
            if entry:
                return {"success_bool": True, "note_string": "already installed", "model_object": view(entry)}
        raise lm.ResolveError("file_taken", f"A model called {record['file']} is already in the catalogue", 409)
    targets = set(record["target_boxes"])
    if only_boxes:
        targets &= set(only_boxes)
    if not targets:
        raise lm.ResolveError("no_boxes", "No reachable render computer runs that model's workflow", 409)
    entry = {**{k: v for k, v in record.items() if k not in ("target_boxes",)},
             "title": title or record["title"] or record["file"],
             "state": "active", "added_at": _now(), "added_by": added_by, "hidden": bool(hidden),
             "target_boxes": sorted(targets), "box_states": {}}
    entry["preview"] = await _save_preview(entry["id"], record.get("preview_source") or "")
    row = {k: copy.deepcopy(template[k]) for k in TEMPLATE_FIELDS if k in template}
    row.update({
        "id": f"fm-{entry['id']}", "file": entry["file"], "title": entry["title"],
        "version": " ".join(x for x in (entry.get("version"), entry.get("precision")) if x),
        "base": record["base"] or template.get("base"), "family": record["family"],
        "page": entry.get("page") or "", "preview": entry["preview"], "triggers": [],
        "size_mb": round(entry["size_bytes"] / 1048576, 1), "sha256": entry["sha256"],
        "usable": False, "unusable_reason": "test entry (hidden)" if hidden
        else "waiting for the render computers to download it",
        "validated_workers": [], "nsfw": bool(entry.get("nsfw")),
        "recommended_from": f"Same workflow and settings as {template.get('title')} (template)",
        "source_version_id": (entry.get("source") or {}).get("version_id"),
        "managed_by": MANAGED_BY, "managed_id": entry["id"], "template_id": template.get("id"),
        "added_at": time.strftime("%Y-%m-%d", time.gmtime()),
    })
    async with _lock:
        data = load_registry()
        data["models"] = [e for e in data["models"] if e.get("id") != entry["id"]]
        data["models"].append(entry)
        save_registry(data)

        def insert(existing, all_rows):
            if existing is not None:
                all_rows.remove(existing)
            all_rows.append(row)
            return True
        _update_catalogue_row(entry["id"], insert, "fleet-models-add")
    ai_model_catalogue._cache_at = 0.0
    lm._spawn(start_downloads(entry["id"]))
    return {"success_bool": True, "model_object": view(entry)}


async def _save_preview(entry_id: str, url: str) -> str:
    path = await lm._save_preview("fm-" + entry_id, url)
    return path


async def delete_model(entry_id: str) -> Dict[str, Any]:
    data = load_registry()
    entry = next((e for e in data["models"] if e.get("id") == entry_id and e.get("state") != "removed"), None)
    rows = _read_catalogue()
    row = next((r for r in rows if r.get("managed_id") == entry_id or r.get("id") == entry_id), None)
    if entry is None:
        if row is not None:
            protected, why = is_protected(row)
            if protected:
                raise HTTPException(status_code=409, detail={"error_string": "protected", "message_string":
                                    f"{row.get('title') or row.get('file')} is protected: {why}"})
        raise HTTPException(status_code=404, detail={"error_string": "not_found",
                                                     "message_string": "no such managed model"})
    if entry["file"] in workflow_files():
        raise HTTPException(status_code=409, detail={"error_string": "protected", "message_string":
                            "a workflow template names this file"})
    task = _monitors.pop(entry_id, None)
    if task:
        task.cancel()
    # Unregister first: nothing new can be queued on it while the files go.
    _update_catalogue_row(entry_id, lambda r, all_rows: (all_rows.remove(r) or True) if r is not None else False,
                          "fleet-models-delete")
    import ai_model_catalogue
    ai_model_catalogue._cache_at = 0.0
    async with _lock:
        data = load_registry()
        for e in data["models"]:
            if e.get("id") == entry_id:
                e["state"] = "removing"
        save_registry(data)
    boxes = sorted(set(entry.get("target_boxes") or []) | set((entry.get("box_states") or {}).keys()))
    results = await asyncio.gather(*(delete_on_box(entry, b) if _ssh_route(b)
                                     else _const({"state": "deleted", "error": "no route: nothing was sent"})
                                     for b in boxes))
    per_box = dict(zip(boxes, results))
    async with _lock:
        data = load_registry()
        for e in data["models"]:
            if e.get("id") == entry_id:
                for b, r in per_box.items():
                    e.setdefault("box_states", {})[b] = {**r, "updated_at": _now()}
                failed = [b for b, r in per_box.items() if r["state"] != "deleted"]
                e["state"] = "removed" if not failed else "removing"
                e["removed_at"] = _now()
        save_registry(data)
    return {"success_bool": True, "boxes_object": per_box}


async def _const(value):
    return value


# ---------------------------------------------------------------- views

def view(entry: Dict[str, Any]) -> Dict[str, Any]:
    states = {}
    size = int(entry.get("size_bytes") or 0)
    for box in sorted(set(entry.get("target_boxes") or []) | set((entry.get("box_states") or {}).keys())):
        s = dict((entry.get("box_states") or {}).get(box) or {"state": "queued"})
        if size and s.get("bytes"):
            s["percent"] = round(100.0 * min(int(s["bytes"]), size) / size, 1)
        states[box] = s
    return {k: entry.get(k) for k in ("id", "file", "title", "version", "precision", "base", "family",
                                       "page", "preview", "size_bytes", "sha256", "nsfw", "added_at",
                                       "state", "hidden", "template_id", "template_title", "workflow",
                                       "source")} | {"box_states_object": states, "ready_boxes_array": _ready(entry)}


def models_view(admin: bool) -> List[Dict[str, Any]]:
    import ai_model_catalogue
    managed = {e["id"]: e for e in load_registry()["models"] if e.get("state") != "removed"}
    out = []
    for row in ai_model_catalogue.raw_entries():
        if row.get("kind") != "checkpoint":
            continue
        protected, why = is_protected(row)
        runners = lm.workflow_boxes(str(row.get("workflow") or ""))
        item = {"id": row.get("id"), "file": row.get("file"), "title": row.get("title") or row.get("file"),
                "version": row.get("version") or "", "base": row.get("base") or "",
                "family": row.get("family") or "", "services": row.get("services") or [],
                "size_mb": row.get("size_mb"), "usable": bool(row.get("usable")),
                "unusable_reason": row.get("unusable_reason") or "",
                "validated_workers": row.get("validated_workers") or [],
                "runnable_workers": sorted(runners) if runners is not None else None,
                "workflow": row.get("workflow") or "", "page": row.get("page") or "",
                "preview": row.get("preview") or "", "protected": protected, "protected_reason": why,
                "managed": row.get("managed_by") == MANAGED_BY, "managed_id": row.get("managed_id") or "",
                "default": bool(row.get("default_for_families") or row.get("default_for_services")
                                or row.get("qwen_image_default"))}
        entry = managed.pop(str(row.get("managed_id") or ""), None)
        if entry:
            item["manager_object"] = view(entry)
        out.append(item)
    for entry in managed.values():  # removing, or registered but row edited away
        out.append({"id": "fm-" + entry["id"], "file": entry["file"], "title": entry.get("title"),
                    "base": entry.get("base"), "family": entry.get("family"),
                    "size_mb": round((entry.get("size_bytes") or 0) / 1048576, 1), "usable": False,
                    "unusable_reason": "not in the catalogue (" + str(entry.get("state")) + ")",
                    "validated_workers": [], "protected": False, "managed": True,
                    "managed_id": entry["id"], "manager_object": view(entry)})
    return out


# ------------------------------------------------------------------- API

@router.get("/api/ai/fleet-models")
async def api_list(request: Request):
    import ai_queue_admin
    admin = await ai_queue_admin.viewer_is_admin(request)
    ensure_monitors()  # idempotent; also picks up models added from the CLI
    return {"success_bool": True, "admin_bool": admin, "models_array": models_view(admin),
            "excluded_boxes_array": sorted(EXCLUDED_BOXES), "server_time_unix_int": _now()}


class ModelAddBody(BaseModel):
    url: str = Field(..., max_length=600)
    base: str = Field("", max_length=80, description="Base model, required for Hugging Face links")
    file: str = Field("", max_length=200, description="Civitai file id, name or precision (fp8/int8); default primary")
    title: str = Field("", max_length=120)
    boxes: Optional[List[str]] = None
    hidden: bool = Field(False, description="Register but never offer it in pickers (tests)")


def build_admin_router(require_admin: Callable[..., Any]) -> APIRouter:
    admin = APIRouter()

    def _raise(exc: lm.ResolveError):
        raise HTTPException(status_code=exc.status, detail={
            "error_string": exc.code, "message_string": exc.message}) from None

    @admin.post("/api/ai/fleet-models/resolve")
    async def api_resolve(body: ModelAddBody, _admin=Depends(require_admin)):
        try:
            record = await resolve_model(body.url, body.base, body.file)
        except lm.ResolveError as exc:
            _raise(exc)
        return {"success_bool": True, "model_object": record}

    @admin.post("/api/ai/fleet-models")
    async def api_add(body: ModelAddBody, user=Depends(require_admin)):
        try:
            return await add_model(body.url, base=body.base, prefer_file=body.file, title=body.title,
                                   only_boxes=body.boxes, hidden=body.hidden,
                                   added_by=str(getattr(user, "email", "") or ""))
        except lm.ResolveError as exc:
            _raise(exc)

    @admin.post("/api/ai/fleet-models/{entry_id}/retry")
    async def api_retry(entry_id: str, box: str = "", _admin=Depends(require_admin)):
        entry = next((e for e in load_registry()["models"]
                      if e.get("id") == entry_id and e.get("state") == "active"), None)
        if not entry:
            raise HTTPException(status_code=404, detail="no such managed model")
        lm._spawn(start_downloads(entry_id, [box] if box else None))
        return {"success_bool": True}

    @admin.delete("/api/ai/fleet-models/{entry_id}")
    async def api_delete(entry_id: str, _admin=Depends(require_admin)):
        return await delete_model(entry_id)

    return admin


@router.on_event("startup")
async def _resume_monitors() -> None:
    try:
        ensure_monitors()
    except Exception:
        logger.exception("Could not resume fleet model monitors")


def _cli(argv: List[str]) -> int:
    """Operator entry point on the VPS (as the autorig user, env of the service):

        python3 -m ai_fleet_models add <url> [--file fp8] [--base ...] [--hidden] [--boxes f15,f5]
        python3 -m ai_fleet_models list
        python3 -m ai_fleet_models delete <id>

    The service's own monitor picks the progress up on its next /api/ai/fleet-models read.
    """
    import argparse
    parser = argparse.ArgumentParser(prog="python3 -m ai_fleet_models")
    sub = parser.add_subparsers(dest="cmd", required=True)
    add = sub.add_parser("add")
    add.add_argument("url")
    add.add_argument("--file", default="")
    add.add_argument("--base", default="")
    add.add_argument("--title", default="")
    add.add_argument("--boxes", default="")
    add.add_argument("--hidden", action="store_true")
    sub.add_parser("list")
    rm = sub.add_parser("delete")
    rm.add_argument("id")
    again = sub.add_parser("retry")
    again.add_argument("id")
    again.add_argument("--box", default="")
    args = parser.parse_args(argv)

    async def run() -> Any:
        if args.cmd == "retry":
            await start_downloads(args.id, [args.box] if args.box else None)
            for task in list(_monitors.values()):
                task.cancel()
            return view(next(e for e in load_registry()["models"] if e["id"] == args.id))
        if args.cmd == "add":
            spawned: List[Any] = []
            original = lm._spawn
            lm._spawn = spawned.append  # run the box launches here, not detached
            try:
                result = await add_model(args.url, base=args.base, prefer_file=args.file, title=args.title,
                                         only_boxes=[b for b in args.boxes.split(",") if b] or None,
                                         hidden=args.hidden, added_by="cli")
            finally:
                lm._spawn = original
            for coro in spawned:
                await coro
            for task in list(_monitors.values()):
                task.cancel()
            entry = next(e for e in load_registry()["models"] if e["id"] == result["model_object"]["id"])
            return view(entry)
        if args.cmd == "delete":
            return await delete_model(args.id)
        return [view(e) for e in load_registry()["models"]]

    try:
        result = asyncio.run(run())
    except lm.ResolveError as exc:
        print(json.dumps({"error": exc.code, "message": exc.message}))
        return 1
    except HTTPException as exc:
        print(json.dumps({"error": exc.status_code, "detail": exc.detail}, default=str))
        return 1
    print(json.dumps(result, indent=1, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(_cli(sys.argv[1:]))
