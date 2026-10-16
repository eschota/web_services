#!/usr/bin/env python3
"""AutoRig datasets: a store that is never wiped (owner 2026-10-16: «у нас вечно теряются сорс изображения от наших
графов лора, нужно какое-то надежное хранилище для датасетов, которое не будет постоянно стираться, и может сделать
сохранение картинок в JPG, прозрачности всё равно нет, сэкономим дофига»).

Files live in /srv/autorig/data/datasets/<set>/ (outside the retention roots of media_retention.py / disk_keeper.py,
and the path segment «datasets» is in their PROTECT list too). nginx serves them at
https://autorig.online/datasets/files/<set>/<name> (like /renderfin/render/, no listing) and proxies
/datasets/api/ to this service on 127.0.0.1:8286.

    POST /keep   {set, url | path, name?, meta?}  -> {url, name, sha256, bytes_in, bytes_out, format, reused}
    GET  /list?set=<set>[&offset=&limit=]         -> files of a set (url, sha256 of the stored file, size)
    GET  /sets                                    -> sets with counts
    GET  /stats                                   -> sets, files, GB, the JPG saving
    GET  /health

A picture without a used alpha channel (under 0.1 % of pixels with alpha < 128; PNG, WebP, BMP, TIFF) is stored as JPG quality 95 (4:4:4); a picture whose
alpha is used stays PNG; JPG and everything else (video, audio) is stored as it is. Every file gets a sidecar
<name>.json: origin, sha256 of the source, sizes, time, meta, the PNG text chunks (ComfyUI prompt / workflow).
Keeping the same source twice (same sha256 in the same set) returns the same file. Nothing is ever deleted: there is
no delete call, and a name already taken by other bytes gets a -<sha8> suffix.

Access: a direct call to 127.0.0.1:8286 (no X-Real-IP header) is trusted; through nginx it needs
Authorization: Bearer <DATASETS_TOKEN> from /srv/autorig/secrets/datasets.env.

CLI (as user autorig):  datasets_api.py serve | keep --set S (--url U | --path P) [--name N] [--meta JSON]
                        | import-dir --set S DIR [--glob '*.png'] | stats
"""
import argparse
import fcntl
import hashlib
import io
import json
import os
import pathlib
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = pathlib.Path(os.environ.get("DATASETS_ROOT", "/srv/autorig/data/datasets"))
PUBLIC = os.environ.get("DATASETS_PUBLIC", "https://autorig.online/datasets/files/")
PORT = int(os.environ.get("DATASETS_PORT", "8286"))
SECRETS = pathlib.Path("/srv/autorig/secrets/datasets.env")
JPG_QUALITY = int(os.environ.get("DATASETS_JPG_QUALITY", "95"))
MAX_BYTES = 2 * 1024 ** 3
# public URLs of files that live on this box: read from disk, not over https (faster, works while nginx reloads)
LOCAL = {
    "https://autorig.online/renderfin/render/": "/srv/autorig/data/var/renderfin/render/",
    "https://autorig.online/dev/api/scratch/": "/srv/autorig/data/devbot/scratch/",
    "https://autorig.online/nsfw/api/scratch/": "/srv/autorig/data/nsfwbot/scratch/",
    "https://autorig.online/datasets/files/": str(ROOT) + "/",
}
READABLE = ("/srv/autorig/data/", "/var/autorig/", "/tmp/")      # where a «path» may point
CONVERT = {"PNG", "WEBP", "BMP", "TIFF", "PPM"}
# alpha counts as used only when more than ALPHA_MIN_SHARE of the pixels have alpha < ALPHA_CUT: the farm's PNGs come
# RGBA with alpha 254-255 everywhere (owner: «прозрачности всё равно нет»), and those go to JPG
ALPHA_CUT = int(os.environ.get("DATASETS_ALPHA_CUT", "128"))
ALPHA_MIN_SHARE = float(os.environ.get("DATASETS_ALPHA_MIN_SHARE", "0.001"))
SET_RX = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}(/[a-z0-9][a-z0-9_.-]{0,63}){0,3}$")
LOCK = threading.Lock()


def token():
    try:
        for line in SECRETS.read_text().splitlines():
            if line.startswith("DATASETS_TOKEN="):
                return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""


def clean_set(s):
    s = (s or "").strip().strip("/").lower()
    if not SET_RX.match(s) or ".." in s:
        raise ValueError(f"bad set {s!r}: lower-case [a-z0-9_.-] segments, up to 4, e.g. lora/sola")
    return s


def clean_name(n):
    n = pathlib.Path(urllib.parse.unquote(n or "")).name
    n = re.sub(r"\.[A-Za-z0-9]{1,5}$", "", n)
    n = re.sub(r"[^A-Za-z0-9_.-]+", "_", n).strip("._-")[:120]
    return n


def read_source(url=None, path=None):
    if path:
        p = os.path.realpath(path)
        if not p.startswith(READABLE):
            raise ValueError(f"path outside {READABLE}")
        return pathlib.Path(p).read_bytes(), pathlib.Path(p).name
    if not url or not re.match(r"^https?://", url):
        raise ValueError("url (http/https) or path required")
    for prefix, folder in LOCAL.items():
        if url.startswith(prefix):
            rel = urllib.parse.unquote(url[len(prefix):].split("?")[0])
            p = os.path.realpath(folder + rel)
            if p.startswith(folder) and os.path.isfile(p):
                return pathlib.Path(p).read_bytes(), pathlib.Path(p).name
    last = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "autorig-datasets/1"})
            with urllib.request.urlopen(req, timeout=120) as r:
                data = r.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise ValueError("source over 2 GB")
            return data, pathlib.Path(urllib.parse.urlparse(url).path).name
        except ValueError:
            raise
        except urllib.error.HTTPError as error:
            if 400 <= error.code < 500:                # gone (the cleaners took it): no retries
                raise ValueError(f"source answers {error.code}: {url}"[:300])
            last = error
            time.sleep(2 * (attempt + 1))
        except Exception as error:
            last = error
            time.sleep(2 * (attempt + 1))
    raise IOError(f"fetch failed: {last!r}"[:300])


def convert(data):
    """(bytes, ext, format, extra) - JPG for a picture without a used alpha, the source bytes otherwise."""
    try:
        from PIL import Image
    except ImportError:
        return data, None, "raw", {}
    try:
        img = Image.open(io.BytesIO(data))
        fmt = (img.format or "").upper()
        img.load()
    except Exception:
        return data, None, "raw", {}
    extra = {"width": img.width, "height": img.height, "source_format": fmt}
    text = {k: v for k, v in (getattr(img, "text", None) or {}).items() if isinstance(v, str)}
    if text:
        extra["png_text"] = text                     # ComfyUI writes its prompt / workflow here: kept in the sidecar
    if fmt in ("JPEG", "MPO"):
        return data, ".jpg", "jpeg", extra
    if fmt not in CONVERT or getattr(img, "n_frames", 1) > 1:
        return data, None, fmt.lower() or "raw", extra
    alpha = None
    if img.mode in ("RGBA", "LA", "PA") or (img.mode == "P" and "transparency" in img.info):
        rgba = img.convert("RGBA")
        hist = rgba.getchannel("A").histogram()
        clear = sum(hist[:ALPHA_CUT])                # pixels really see-through, not the 254/255 noise of a VAE
        extra["alpha_clear_share"] = round(clear / max(1, img.width * img.height), 5)
        if clear > ALPHA_MIN_SHARE * img.width * img.height:
            alpha = True
        img = rgba
    if alpha:
        return data, ".png" if fmt == "PNG" else None, "png-alpha", extra
    rgb = img.convert("RGB")
    out = io.BytesIO()
    kw = {"quality": JPG_QUALITY, "subsampling": 0, "optimize": True}
    icc = img.info.get("icc_profile")
    if icc:
        kw["icc_profile"] = icc
    rgb.save(out, "JPEG", **kw)
    jpg = out.getvalue()
    if len(jpg) >= len(data) and fmt in ("PNG",):   # a tiny / flat PNG can be smaller than its JPG: keep the PNG
        return data, ".png", "png", extra
    return jpg, ".jpg", "jpeg", extra


def _index_path(folder):
    return folder / ".index.json"


def _load_index(folder):
    try:
        return json.loads(_index_path(folder).read_text())
    except Exception:
        return {}


def _atomic(path, data):
    tmp = path.with_name(f".{path.name}.tmp{os.getpid()}.{threading.get_ident()}")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)


def keep(set_name, url=None, path=None, name=None, meta=None):
    set_name = clean_set(set_name)
    data, src_name = read_source(url, path)
    if not data:
        raise ValueError("empty source")
    sha = hashlib.sha256(data).hexdigest()
    folder = ROOT / set_name
    folder.mkdir(parents=True, exist_ok=True)
    with LOCK, open(folder / ".lock", "a") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        index = _load_index(folder)
        hit = index.get(sha)
        if hit and (folder / hit).exists():
            side = json.loads((folder / (hit + ".json")).read_text()) if (folder / (hit + ".json")).exists() else {}
            if meta or url:                            # a second keep adds its origin / meta, never replaces
                seen = side.setdefault("also", [])
                if len(seen) < 50:
                    seen.append({"origin": url or path, "meta": meta or {}, "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
                    _atomic(folder / (hit + ".json"), json.dumps(side, ensure_ascii=False, indent=1).encode())
            return {"ok": True, "reused": True, "set": set_name, "name": hit, "url": PUBLIC + set_name + "/" + hit,
                    "sha256": sha, "bytes_in": side.get("bytes_in"), "bytes_out": side.get("bytes_out"),
                    "format": side.get("format")}
        out, ext, fmt, extra = convert(data)
        if ext is None:
            ext = (os.path.splitext(src_name)[1].lower() or ".bin")[:6]
        stem = clean_name(name) or clean_name(src_name) or sha[:16]
        fname = stem + ext
        if (folder / fname).exists():                  # the name holds other bytes: never overwrite
            fname = f"{stem}-{sha[:8]}{ext}"
        n = 2
        while (folder / fname).exists():
            fname = f"{stem}-{sha[:8]}-{n}{ext}"
            n += 1
        _atomic(folder / fname, out)
        side = {"name": fname, "set": set_name, "origin": url or path, "source_name": src_name, "sha256": sha,
                "sha256_stored": hashlib.sha256(out).hexdigest(), "bytes_in": len(data), "bytes_out": len(out),
                "format": fmt, "jpg_quality": JPG_QUALITY if fmt == "jpeg" and out is not data else None,
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "meta": meta or {}}
        side.update(extra)
        _atomic(folder / (fname + ".json"), json.dumps(side, ensure_ascii=False, indent=1).encode())
        index[sha] = fname
        _atomic(_index_path(folder), json.dumps(index, indent=0).encode())
        with open(folder / ".ledger.jsonl", "a") as fh:  # append-only history of the set
            fh.write(json.dumps({k: side[k] for k in ("name", "origin", "sha256", "sha256_stored", "bytes_in",
                                                      "bytes_out", "format", "time")}) + "\n")
    return {"ok": True, "reused": False, "set": set_name, "name": fname, "url": PUBLIC + set_name + "/" + fname,
            "sha256": sha, "sha256_stored": side["sha256_stored"], "bytes_in": len(data), "bytes_out": len(out),
            "format": fmt}


def _sets():
    out = []
    if not ROOT.exists():
        return out
    for idx in sorted(ROOT.rglob(".index.json")):
        out.append(str(idx.parent.relative_to(ROOT)))
    return out


def listing(set_name, offset=0, limit=100000):
    set_name = clean_set(set_name)
    folder = ROOT / set_name
    files = []
    for side in sorted(folder.glob("*.json")):
        try:
            s = json.loads(side.read_text())
        except Exception:
            continue
        if not isinstance(s, dict) or "sha256_stored" not in s or not (folder / s.get("name", "")).exists():
            continue
        files.append({"name": s["name"], "url": PUBLIC + set_name + "/" + s["name"], "sha256": s["sha256_stored"],
                      "bytes": s["bytes_out"], "source_sha256": s["sha256"], "origin": s.get("origin"),
                      "time": s.get("time"), "sidecar_url": PUBLIC + set_name + "/" + s["name"] + ".json"})
    return {"ok": True, "set": set_name, "total": len(files), "files": files[offset:offset + limit]}


def stats():
    sets, files, b_in, b_out, conv = {}, 0, 0, 0, 0
    for s in _sets():
        n = bi = bo = 0
        for side in (ROOT / s).glob("*.json"):
            try:
                d = json.loads(side.read_text())
            except Exception:
                continue
            if not isinstance(d, dict) or "bytes_out" not in d:
                continue
            n += 1
            bi += d["bytes_in"]
            bo += d["bytes_out"]
            conv += d.get("format") == "jpeg" and d.get("source_format") not in ("JPEG", "MPO")
        sets[s] = {"files": n, "gb": round(bo / 1e9, 3), "gb_source": round(bi / 1e9, 3)}
        files, b_in, b_out = files + n, b_in + bi, b_out + bo
    return {"ok": True, "sets": len(sets), "files": files, "converted_to_jpg": conv, "gb": round(b_out / 1e9, 3),
            "gb_source": round(b_in / 1e9, 3), "saved_gb": round((b_in - b_out) / 1e9, 3),
            "saved_pct": round(100 * (b_in - b_out) / b_in, 1) if b_in else 0.0, "per_set": sets,
            "root": str(ROOT), "never_deletes": True}


class H(BaseHTTPRequestHandler):
    server_version = "autorig-datasets/1"

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.headers.get("X-Real-IP") or self.client_address[0], fmt % args))

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _allowed(self):
        real = self.headers.get("X-Real-IP")
        if self.client_address[0] in ("127.0.0.1", "::1") and (not real or real in ("127.0.0.1", "::1")):
            return True
        want = token()
        got = (self.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        return bool(want) and got == want

    def _route(self):
        p = urllib.parse.urlparse(self.path)
        path = re.sub(r"^/datasets/api", "", p.path).rstrip("/") or "/"
        return path, {k: v[-1] for k, v in urllib.parse.parse_qs(p.query).items()}

    def do_GET(self):
        path, q = self._route()
        if path == "/health":
            return self._send(200, {"ok": True, "root": str(ROOT)})
        if not self._allowed():
            return self._send(403, {"ok": False, "error_string": "forbidden"})
        try:
            if path == "/stats":
                return self._send(200, stats())
            if path == "/sets":
                return self._send(200, {"ok": True, "sets": _sets()})
            if path == "/list":
                return self._send(200, listing(q.get("set", ""), int(q.get("offset", 0)), int(q.get("limit", 100000))))
        except ValueError as error:
            return self._send(400, {"ok": False, "error_string": str(error)})
        return self._send(404, {"ok": False, "error_string": "not_found"})

    def do_POST(self):
        path, _ = self._route()
        if not self._allowed():
            return self._send(403, {"ok": False, "error_string": "forbidden"})
        if path != "/keep":
            return self._send(404, {"ok": False, "error_string": "not_found"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(min(n, 4 * 1024 ** 2)) or b"{}")
            res = keep(body.get("set"), body.get("url"), body.get("path"), body.get("name"), body.get("meta"))
            return self._send(200, res)
        except (ValueError, json.JSONDecodeError) as error:
            return self._send(400, {"ok": False, "error_string": str(error)[:300]})
        except Exception as error:
            return self._send(502, {"ok": False, "error_string": repr(error)[:300]})


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve")
    k = sub.add_parser("keep")
    k.add_argument("--set", required=True)
    k.add_argument("--url")
    k.add_argument("--path")
    k.add_argument("--name")
    k.add_argument("--meta", default="{}")
    i = sub.add_parser("import-dir")
    i.add_argument("--set", required=True)
    i.add_argument("--glob", default="*")
    i.add_argument("--meta", default="{}")
    i.add_argument("dir")
    sub.add_parser("stats")
    a = ap.parse_args()
    if a.cmd == "serve":
        ROOT.mkdir(parents=True, exist_ok=True)
        ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
    elif a.cmd == "keep":
        print(json.dumps(keep(a.set, a.url, a.path, a.name, json.loads(a.meta)), ensure_ascii=False))
    elif a.cmd == "import-dir":
        meta, tot_in, tot_out, n, reused = json.loads(a.meta), 0, 0, 0, 0
        base = pathlib.Path(a.dir)
        for p in sorted(base.glob(a.glob)):
            if not p.is_file() or p.suffix.lower() == ".json" or p.name.startswith("."):
                continue
            stem = str(p.relative_to(base).with_suffix("")).replace("/", "__")
            r = keep(a.set, path=str(p), name=stem, meta=dict(meta, source_path=str(p)))
            n, reused = n + 1, reused + r["reused"]
            tot_in, tot_out = tot_in + (r["bytes_in"] or 0), tot_out + (r["bytes_out"] or 0)
        print(json.dumps({"set": a.set, "files": n, "reused": reused, "mb_in": round(tot_in / 1e6, 1),
                          "mb_out": round(tot_out / 1e6, 1),
                          "saved_pct": round(100 * (tot_in - tot_out) / tot_in, 1) if tot_in else 0}))
    elif a.cmd == "stats":
        print(json.dumps(stats(), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
