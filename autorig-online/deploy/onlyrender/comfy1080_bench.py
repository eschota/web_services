"""Standard image tests (Z-Image t2i, klein t2i, klein 1-ref and 2-ref, 1024^2) against a local ComfyUI; results appended to OUT/results.jsonl."""
import json, os, shutil, subprocess, sys, threading, time, urllib.request, uuid

BASE = "http://127.0.0.1:8389"
ROOT = r"C:\AI\ComfyUI1080\ComfyUI"
OUT = r"C:\ProgramData\AutoRig\comfy1080\bench"
os.makedirs(OUT, exist_ok=True)
PROMPT = ("A friendly cartoon fox adventurer standing in a sunny forest clearing, full body, "
          "green cloak, leather boots, detailed fur, soft cinematic light")
PROMPT2 = "The fox from image 1 wearing the red jacket from image 2, standing on a city street at dusk, full body"


def call(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read() or b"{}")


def zimage(w=1024, h=1024, seed=42, prefix="zimg", text=None):
    text = text or (PROMPT + f", variation {seed}")
    return {
        "unet": {"class_type": "UNETLoader", "inputs": {"unet_name": "z_image_turbo_fp8_e4m3fn.safetensors", "weight_dtype": "default"}},
        "clip": {"class_type": "CLIPLoader", "inputs": {"clip_name": "qwen_3_4b.safetensors", "type": "lumina2", "device": "default"}},
        "vae": {"class_type": "VAELoader", "inputs": {"vae_name": "ae.safetensors"}},
        "shift": {"class_type": "ModelSamplingAuraFlow", "inputs": {"model": ["unet", 0], "shift": 3}},
        "positive": {"class_type": "CLIPTextEncode", "inputs": {"text": text, "clip": ["clip", 0]}},
        "negative": {"class_type": "ConditioningZeroOut", "inputs": {"conditioning": ["positive", 0]}},
        "latent": {"class_type": "EmptySD3LatentImage", "inputs": {"width": w, "height": h, "batch_size": 1}},
        "sample": {"class_type": "KSampler", "inputs": {"model": ["shift", 0], "positive": ["positive", 0], "negative": ["negative", 0], "latent_image": ["latent", 0], "seed": seed, "steps": 8, "cfg": 1, "sampler_name": "res_multistep", "scheduler": "simple", "denoise": 1}},
        "decode": {"class_type": "VAEDecode", "inputs": {"samples": ["sample", 0], "vae": ["vae", 0]}},
        "save": {"class_type": "SaveImage", "inputs": {"images": ["decode", 0], "filename_prefix": prefix}},
    }


def klein(refs=(), w=1024, h=1024, seed=42, prefix="klein", text=PROMPT):
    wf = {
        "model": {"class_type": "UNETLoader", "inputs": {"unet_name": "flux-2-klein-4b.safetensors", "weight_dtype": "default"}},
        "clip": {"class_type": "CLIPLoader", "inputs": {"clip_name": "qwen_3_4b_fp4_flux2.safetensors", "type": "flux2", "device": "cpu"}},
        "vae": {"class_type": "VAELoader", "inputs": {"vae_name": "flux2-vae.safetensors"}},
        "positive": {"class_type": "CLIPTextEncode", "inputs": {"text": text, "clip": ["clip", 0]}},
        "latent": {"class_type": "EmptyFlux2LatentImage", "inputs": {"width": w, "height": h, "batch_size": 1}},
        "noise": {"class_type": "RandomNoise", "inputs": {"noise_seed": seed}},
        "guider": {"class_type": "BasicGuider", "inputs": {"model": ["model", 0], "conditioning": ["positive", 0]}},
        "sampler": {"class_type": "KSamplerSelect", "inputs": {"sampler_name": "euler"}},
        "sigmas": {"class_type": "Flux2Scheduler", "inputs": {"steps": 4, "width": w, "height": h}},
        "sample": {"class_type": "SamplerCustomAdvanced", "inputs": {"noise": ["noise", 0], "guider": ["guider", 0], "sampler": ["sampler", 0], "sigmas": ["sigmas", 0], "latent_image": ["latent", 0]}},
        "decode": {"class_type": "VAEDecode", "inputs": {"samples": ["sample", 0], "vae": ["vae", 0]}},
        "save": {"class_type": "SaveImage", "inputs": {"images": ["decode", 0], "filename_prefix": prefix}},
    }
    cond = ["positive", 0]
    for i, name in enumerate(refs, 1):
        wf[f"r{i}_img"] = {"class_type": "LoadImage", "inputs": {"image": name}}
        wf[f"r{i}_scale"] = {"class_type": "ImageScaleToTotalPixels", "inputs": {"image": [f"r{i}_img", 0], "upscale_method": "area", "megapixels": 1.0, "resolution_steps": 1}}
        wf[f"r{i}_enc"] = {"class_type": "VAEEncode", "inputs": {"pixels": [f"r{i}_scale", 0], "vae": ["vae", 0]}}
        wf[f"r{i}_ref"] = {"class_type": "ReferenceLatent", "inputs": {"conditioning": cond, "latent": [f"r{i}_enc", 0]}}
        cond = [f"r{i}_ref", 0]
    wf["guider"]["inputs"]["conditioning"] = cond
    return wf


def smi():
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=memory.used,utilization.gpu", "--format=csv,noheader,nounits"],
                             capture_output=True, text=True, timeout=10).stdout.strip().split(",")
        return int(out[0]), int(out[1])
    except Exception:
        return -1, -1


def comfy_rss_gb():
    try:
        import psutil
        best = 0
        for p in psutil.process_iter(["cmdline", "memory_info"]):
            cl = " ".join(p.info["cmdline"] or [])
            if "main.py" in cl and "8389" in cl:
                best = max(best, p.info["memory_info"].rss)
        return best / 1024 ** 3
    except Exception:
        return -1


def run(name, wf):
    peak = {"vram": 0, "util": 0, "rss": 0.0}
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            v, u = smi()
            peak["vram"] = max(peak["vram"], v)
            peak["util"] = max(peak["util"], u)
            peak["rss"] = max(peak["rss"], comfy_rss_gb())
            time.sleep(1)
    t = threading.Thread(target=sampler, daemon=True)
    base_vram, _ = smi()
    t.start()
    t0 = time.time()
    pid = call("/prompt", {"prompt": wf, "client_id": "bench"})["prompt_id"]
    status, entry = "timeout", {}
    while time.time() - t0 < 1800:
        h = call(f"/history/{pid}")
        entry = h.get(pid) or {}
        st = (entry.get("status") or {}).get("status_str")
        if st:
            status = st
            break
        time.sleep(1)
    dt = time.time() - t0
    stop.set(); t.join(3)
    files = []
    for node in (entry.get("outputs") or {}).values():
        for img in node.get("images") or []:
            files.append(img["filename"])
    exec_ms = None
    for m in (entry.get("status") or {}).get("messages") or []:
        if m[0] == "execution_start":
            s = m[1].get("timestamp")
        if m[0] in ("execution_success", "execution_error"):
            e = m[1].get("timestamp")
            try:
                exec_ms = e - s
            except Exception:
                pass
    err = ""
    if status != "success":
        err = json.dumps((entry.get("status") or {}).get("messages"))[-1500:]
    rec = {"name": name, "status": status, "wall_s": round(dt, 1), "exec_s": round(exec_ms / 1000, 1) if exec_ms else None,
           "vram_base_mb": base_vram, "vram_peak_mb": peak["vram"], "gpu_util_peak": peak["util"],
           "comfy_rss_peak_gb": round(peak["rss"], 1), "files": files, "error": err}
    print(json.dumps(rec), flush=True)
    with open(os.path.join(OUT, "results.jsonl"), "a") as fh:
        fh.write(json.dumps(dict(rec, at=time.strftime("%Y-%m-%dT%H:%M:%S"))) + "\n")
    return rec


if __name__ == "__main__":
    which = sys.argv[1:] or ["zimage", "zimage", "zimage", "klein", "klein", "klein2ref", "klein2ref"]
    refs = []
    for i, w in enumerate(which):
        if w == "zimage":
            r = run("zimage_1024", zimage(seed=42 + i, prefix="bench_zimg"))
            if r["files"] and len(refs) < 1:
                refs.append(r["files"][0])
        elif w == "klein":
            r = run("klein_t2i_1024", klein(seed=42 + i, prefix="bench_klein", text=f"A red leather jacket on a mannequin, studio photo, white background, take {i}"))
            if r["files"] and len(refs) < 2:
                refs.append(r["files"][0])
        elif w == "klein1ref":
            f = refs[0]
            shutil.copy(os.path.join(ROOT, "output", f), os.path.join(ROOT, "input", f))
            run("klein_1ref_1024", klein(refs=[f], seed=42 + i, prefix="bench_klein1ref", text=f"Make the fox wear a blue scarf, keep everything else, take {i}"))
        elif w == "klein2ref":
            names = []
            for f in refs[:2]:
                shutil.copy(os.path.join(ROOT, "output", f), os.path.join(ROOT, "input", f))
                names.append(f)
            if len(names) < 2:
                print(json.dumps({"name": "klein_2ref", "status": "skipped", "error": "refs missing"}))
                continue
            run("klein_2ref_1024", klein(refs=names, seed=42 + i, prefix="bench_klein2ref", text=PROMPT2 + f", take {i}"))
