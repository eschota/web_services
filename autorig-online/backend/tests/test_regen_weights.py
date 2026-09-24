"""regen/weights.py: chain skin weights and its numpy-only contract.

The Blender step loads weights.py by file path inside Blender's Python, which
has numpy but not scipy or trimesh.
"""

import subprocess
import sys
import textwrap
from pathlib import Path

import numpy as np
import pytest

from regen import weights as W

BACKEND = Path(__file__).resolve().parents[1]
WEIGHTS_FILE = BACKEND / "regen" / "weights.py"


def _chain(x, z=0.0, top=1.0, n=5, length=0.8):
    ys = np.linspace(top, top - length, n)
    return np.stack([np.full(n, x), ys, np.full(n, z)], axis=1)


def _check(ids, w, max_influences=4):
    assert ids.dtype == np.int32 and w.dtype == np.float32
    assert ids.shape == w.shape and ids.shape[1] == max_influences
    assert np.all(w >= 0.0)
    assert np.allclose(w.sum(axis=1), 1.0, atol=1e-6)
    assert np.all(ids[w == 0.0] == 0)


def test_api_version_is_pinned():
    assert W.WEIGHTS_API_VERSION == 1


def test_bone_ids_follow_the_flat_layout():
    chains = [_chain(0.0, n=5), _chain(1.0, n=3), _chain(2.0, n=4)]
    ids = W.chain_bone_ids(chains)
    assert [list(i) for i in ids] == [[1, 2, 3, 4], [5, 6], [7, 8, 9]]
    assert W.bone_count(chains) == 10


def test_points_on_a_chain_use_that_chains_bones_and_never_end_joints():
    chains = [_chain(0.0), _chain(1.0)]
    pts = np.array([[0.0, 0.6, 0.0], [1.0, 0.45, 0.0], [0.0, 0.2, 0.0]])
    ids, w = W.compute_chain_weights(pts, chains, attach_blend=0.0)
    _check(ids, w)
    end_free = set(np.concatenate(W.chain_bone_ids(chains)).tolist()) | {0}
    assert set(ids[w > 0].tolist()) <= end_free
    # point 0 sits on chain 0, point 1 on chain 1
    assert set(ids[0][w[0] > 1e-3].tolist()) <= {1, 2, 3, 4}
    assert set(ids[1][w[1] > 1e-3].tolist()) <= {5, 6, 7, 8}
    # the chain tip belongs entirely to the last deforming bone
    assert ids[2][0] == 4 and w[2][0] == pytest.approx(1.0)


def test_weights_blend_between_neighbouring_chains_by_distance():
    chains = [_chain(0.0), _chain(1.0)]
    pts = np.array([[0.5, 0.5, 0.0], [0.25, 0.5, 0.0]])
    ids, w = W.compute_chain_weights(pts, chains, attach_blend=0.0)
    _check(ids, w)

    def share(row, bones):
        return float(sum(w[row][ids[row] == b].sum() for b in bones))

    first = [1, 2, 3, 4]
    second = [5, 6, 7, 8]
    assert share(0, first) == pytest.approx(0.5, abs=1e-6)
    assert share(0, second) == pytest.approx(0.5, abs=1e-6)
    assert share(1, first) == pytest.approx(0.9, abs=1e-6)  # 1/d^2: (1/.25^2)/(1/.25^2 + 1/.75^2)


def test_attach_bone_takes_the_root_and_whatever_hangs_above_it():
    chains = [_chain(0.0, top=1.0)]
    origin = np.array([0.0, 1.0, -0.5])
    pts = np.array(
        [
            [0.0, 1.0, 0.0],  # at the root
            [0.0, 1.0, -0.3],  # between the origin and the root
            [0.0, 0.99, 0.0],  # just below the root: blending
            [0.0, 0.5, 0.0],  # far down the chain
        ]
    )
    ids, w = W.compute_chain_weights(pts, chains, attach_origin=origin, attach_blend=0.25)
    _check(ids, w)
    attach = np.where(ids == 0, w, 0.0).sum(axis=1)
    assert attach[0] == pytest.approx(1.0)
    assert attach[1] == pytest.approx(1.0)
    assert 0.0 < attach[2] < 1.0
    assert attach[3] == pytest.approx(0.0)


def test_max_influences_and_degenerate_inputs():
    rng = np.random.default_rng(0)
    chains = [_chain(x, z) for x, z in rng.uniform(-1, 1, size=(6, 2))]
    pts = rng.uniform(-1.2, 1.2, size=(5000, 3))
    for k in (1, 2, 4, 6):
        ids, w = W.compute_chain_weights(pts, chains, attach_origin=[0, 1.2, 0], max_influences=k)
        _check(ids, w, k)
        assert np.all((w > 0).sum(axis=1) <= k)
    ids, w = W.compute_chain_weights(np.zeros((0, 3)), chains)
    assert ids.shape == (0, 4) and w.shape == (0, 4)
    ids, w = W.compute_chain_weights(pts[:3], [])
    assert np.all(ids == 0) and np.allclose(w[:, 0], 1.0)
    duplicated = [np.array([[0, 1, 0], [0, 1, 0], [0, 0, 0]], dtype=float)]  # zero-length segment
    ids, w = W.compute_chain_weights(pts[:50], duplicated)
    _check(ids, w)
    with pytest.raises(ValueError):
        W.compute_chain_weights(pts[:3], [np.zeros((1, 3))])
    with pytest.raises(ValueError):
        W.compute_chain_weights(pts[:3], chains, max_influences=0)


def test_large_batches_are_chunked_consistently():
    rng = np.random.default_rng(1)
    chains = [_chain(x) for x in (-0.5, 0.0, 0.5)]
    pts = rng.uniform(-1, 1, size=(70_000, 3))  # more than one internal chunk
    ids, w = W.compute_chain_weights(pts, chains, attach_origin=[0, 1, 0])
    ids_tail, w_tail = W.compute_chain_weights(pts[-100:], chains, attach_origin=[0, 1, 0])
    assert np.array_equal(ids[-100:], ids_tail)
    assert np.allclose(w[-100:], w_tail)


BLOCKER = textwrap.dedent(
    """
    import importlib.abc, sys

    class Block(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] in ("scipy", "trimesh", "PIL", "renderfin", "main"):
                raise ImportError("blocked in this test: " + name)
            return None

    sys.meta_path.insert(0, Block())
    """
)


def _run_isolated(body: str) -> subprocess.CompletedProcess:
    code = BLOCKER + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(BACKEND),
    )


def test_weights_module_loads_by_file_path_with_numpy_only():
    proc = _run_isolated(
        f"""
        import importlib.util, sys
        import numpy as np
        spec = importlib.util.spec_from_file_location("regen_weights_isolated", {str(WEIGHTS_FILE)!r})
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        assert mod.WEIGHTS_API_VERSION == 1
        chain = np.array([[0, 1, 0], [0, 0.5, 0], [0, 0, 0]], dtype=float)
        ids, w = mod.compute_chain_weights(np.random.rand(100, 3), [chain, chain + [1, 0, 0]], attach_origin=[0.5, 1.2, 0])
        assert ids.shape == (100, 4) and abs(float(w.sum()) - 100.0) < 1e-3
        for blocked in ("scipy", "trimesh"):
            try:
                __import__(blocked)
            except ImportError:
                pass
            else:
                raise SystemExit(blocked + " was importable: the blocker does not work")
        assert "scipy" not in sys.modules and "trimesh" not in sys.modules
        print("OK")
        """
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "OK" in proc.stdout


def test_weights_module_imports_through_the_package_with_numpy_only():
    proc = _run_isolated(
        f"""
        import sys
        sys.path.insert(0, {str(BACKEND)!r})
        import regen.weights as mod
        import regen
        assert regen.DecompositionError and regen.RegenConfig  # eager, pure-python names
        assert "scipy" not in sys.modules and "trimesh" not in sys.modules
        print("OK", mod.WEIGHTS_API_VERSION)
        """
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "OK 1" in proc.stdout
