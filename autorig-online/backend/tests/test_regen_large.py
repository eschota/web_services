"""Performance budget: a 300k-vertex dressed mesh must decompose well under a
minute on 4 cores in under 4 GB. Runs in a subprocess so the peak RSS is the
decomposition's own."""

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("scipy")
pytest.importorskip("trimesh")

BACKEND = Path(__file__).resolve().parents[1]
TIME_BUDGET_S = 60.0
MEMORY_BUDGET_MB = 4096.0


def test_regen_300k_vertices_decompose_within_time_and_memory_budget(tmp_path):
    code = textwrap.dedent(
        f"""
        import json, resource, sys, time
        sys.path.insert(0, {str(BACKEND)!r})
        from regen.decompose import decompose
        from regen.synthetic import make_pair, write_pair

        pair = make_pair(seed=13, resolution=3.5, cape=True)
        dressed, body = write_pair(pair, {str(tmp_path)!r})
        start = time.perf_counter()
        result = decompose(dressed, body, {str(tmp_path / "out")!r})
        elapsed = time.perf_counter() - start
        print(json.dumps({{
            "dressed_vertices": int(len(pair.dressed.vertices)),
            "body_vertices": int(len(pair.body.vertices)),
            "elapsed_s": elapsed,
            "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0,
            "timings": result.timings,
            "groups": [[g["name"], g["connection"], len(g["chains"])] for g in result.document["groups"]],
        }}))
        """
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=600)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    report = json.loads(proc.stdout.strip().splitlines()[-1])
    print("regen large case:", json.dumps(report))
    assert report["dressed_vertices"] >= 300_000
    assert report["elapsed_s"] < TIME_BUDGET_S, report
    assert report["peak_rss_mb"] < MEMORY_BUDGET_MB, report
    connections = {name: conn for name, conn, _ in report["groups"]}
    assert "loop" in connections.values() and "open" in connections.values()
