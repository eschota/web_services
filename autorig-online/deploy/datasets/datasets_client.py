"""Client of the AutoRig datasets store (stdlib only, importable from any python on the VPS):

    import sys; sys.path.insert(0, "/srv/autorig/datasets")
    import datasets_client as DS
    url = DS.keep_url("lora/sola", url=render_url, name="face_front_a0", meta={"prompt": ...})

keep() returns the service answer (dict) or raises; keep_url() never raises: on any failure it returns the fallback
(the origin URL by default), so a pipeline keeps working when the store is down.
"""
import json
import sys
import urllib.request

API = "http://127.0.0.1:8286"


def keep(set_name, url=None, path=None, name=None, meta=None, timeout=180):
    body = json.dumps({"set": set_name, "url": url, "path": str(path) if path else None, "name": name,
                       "meta": meta or {}}).encode()
    req = urllib.request.Request(API + "/keep", data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        res = json.loads(r.read())
    if not res.get("ok"):
        raise IOError(res.get("error_string") or "keep failed")
    return res


def keep_url(set_name, url=None, path=None, name=None, meta=None, fallback=None, timeout=180):
    try:
        return keep(set_name, url, path, name, meta, timeout)["url"]
    except Exception as error:
        sys.stderr.write(f"datasets keep failed ({set_name} {url or path}): {error!r}\n"[:400])
        return fallback if fallback is not None else url
