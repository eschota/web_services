"""parallel_tasks check for ai_node.py (fakes from selftest.py, no GPU): 4 SLOW tasks (3 s each)
with parallel_tasks 1 vs 2; one llama-server start; both answers complete."""
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import selftest as st  # noqa: E402


def run(parallel):
    tmp = tempfile.mkdtemp(prefix=f"ainode-par{parallel}-")
    weights, mmproj = os.path.join(tmp, "w.gguf"), os.path.join(tmp, "mm.gguf")
    for p in (weights, mmproj):
        open(p, "wb").write(b"x" * 1024)
    open(os.path.join(tmp, "token.txt"), "w").write(st.TOKEN + "\n")
    smi = os.path.join(tmp, "smi.json")
    json.dump({"total": 11264, "free": 10000}, open(smi, "w"))
    llama_log = os.path.join(tmp, "llama.jsonl")
    port, lport = st.free_port(), st.free_port()
    me = os.path.join(HERE, "selftest.py")
    cfg = {"node_name": "par", "listen_port": port, "token_file": "token.txt", "model_id": st.MODEL_ID,
           "weights": weights, "mmproj": mmproj, "vram_need_mb": 1000, "vram_margin_mb": 100,
           "llama_server": [sys.executable, me, "--fake-llama", "--fake-load-delay", "1.0"],
           "llama_port": lport, "nvidia_smi": [sys.executable, me, "--fake-smi"], "comfy_url": "",
           "keepalive_seconds": 60, "max_tasks": 8, "parallel_tasks": parallel,
           "extra_llama_args": ["-np", str(parallel), "-kvu"], "start_timeout_seconds": 30,
           "log_file": "ai-node.log"}
    path = os.path.join(tmp, "config.json")
    json.dump(cfg, open(path, "w"))
    env = dict(os.environ, FAKE_LLAMA_LOG=llama_log, FAKE_SMI_STATE=smi)
    node = subprocess.Popen([sys.executable, os.path.join(HERE, "ai_node.py"), "--config", path], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    api = st.Client(port)
    try:
        st.wait_until(lambda: api.call("GET", "/healthz", token=None)[0] == 200, 20)
        t0 = time.monotonic()
        ids = []
        for i in range(4):
            code, body = api.call("POST", "/api-converter-glb/text2text", {"prompt": f"SLOW {i}", "max_output_tokens": 8})
            assert code == 202, (code, body)
            ids.append(body["task_id"])
        def all_done():
            states = [api.call("GET", f"/api-converter-glb/ai-vision/status/{t}")[1].get("status") for t in ids]
            return all(s in ("Completed", "Failed") for s in states) and states
        states = st.wait_until(all_done, 60, 0.2)
        took = time.monotonic() - t0
        starts = sum(1 for line in open(llama_log) if '"start"' in line)
        argv = [json.loads(l)["argv"] for l in open(llama_log) if '"start"' in l][0]
        return took, states, starts, argv
    finally:
        node.terminate()
        node.wait(10)


if __name__ == "__main__":
    ok = True
    for parallel in (1, 2):
        took, states, starts, argv = run(parallel)
        good = states and all(s == "Completed" for s in states) and starts == 1
        ok &= bool(good)
        print(f"parallel_tasks={parallel}: 4 SLOW tasks in {took:.1f} s, states={states}, llama starts={starts}, "
              f"-np in argv={'-np' in argv}")
    t1, t2 = run(1)[0], run(2)[0]
    faster = t2 < t1 * 0.7
    print("parallel 2 faster than 1:", faster, f"({t2:.1f} s vs {t1:.1f} s)")
    sys.exit(0 if ok and faster else 1)
