"""Exclusive project lease for sequential LTX and tracking CUDA stages.

A crashed owner leaves the file in place intentionally. Inspect the process
and queue before manually recovering it; never infer safety from file age.
The lease coordinates this pipeline, not unrelated external GPU consumers.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import time
import subprocess

DEFAULT_GPU_LOCK = Path(__file__).resolve().parents[2] / 'work/animal-pilot/gpu.lock'


def require_free_cuda_memory(minimum_mib=14000):
    """Check physical GPU memory before loading another model on the 24GB host."""
    result = subprocess.run(['nvidia-smi', '--query-gpu=memory.free',
        '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=15, check=True)
    free = [int(line.strip()) for line in result.stdout.splitlines() if line.strip()]
    if len(free) != 1 or free[0] < minimum_mib:
        raise RuntimeError(f'CUDA stage needs one GPU with {minimum_mib} MiB free; observed {free}. '
                           'Finish the existing GPU work and release idle model caches first.')
    return free[0]


@contextmanager
def gpu_lease(path, stage):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # 'x' is an atomic exclusive creation on both Windows and POSIX.
    with path.open('x', encoding='utf-8') as file:
        json.dump({'pid': os.getpid(), 'stage': stage, 'started_at': time.time()}, file)
    try:
        yield
    finally:
        path.unlink()
