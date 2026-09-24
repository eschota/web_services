"""AutoRig Regen geometry core.

Splits an image-to-3D character (the "dressed" GLB) into body / hair / loose
cloth parts using a second reconstruction of the same character as a bald,
skin-tight "base body", and builds bone chains for the parts that move.

Pure Python: numpy + scipy + trimesh (Pillow only to read a hair-mask image).
Nothing here imports the web app (``main``) or ``renderfin``.

    from regen import decompose, DecompositionError
    try:
        result = decompose("dressed.glb", "body.glb", "out/")
    except DecompositionError as exc:
        ...  # fall back to plain rigging

Heavy modules load lazily, so ``import regen.weights`` works in an
environment that has numpy only (the Blender step still loads
``weights.py`` by file path; see that module).
"""

from __future__ import annotations

from typing import Any

from .config import RegenConfig
from .errors import DecompositionError, MeshLoadError

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "DecompositionError",
    "DecompositionResult",
    "HairMask",
    "MeshLoadError",
    "RegenConfig",
    "Similarity",
    "align_body_to_dressed",
    "compute_chain_weights",
    "compute_landmarks",
    "decompose",
]

_LAZY = {
    "decompose": ("decompose", "decompose"),
    "DecompositionResult": ("decompose", "DecompositionResult"),
    "HairMask": ("segment", "HairMask"),
    "Similarity": ("align", "Similarity"),
    "align_body_to_dressed": ("align", "align_body_to_dressed"),
    "compute_landmarks": ("landmarks", "compute_landmarks"),
    "compute_chain_weights": ("weights", "compute_chain_weights"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        import importlib

        module_name, attr = _LAZY[name]
        module = importlib.import_module(f"{__name__}.{module_name}")
        value = getattr(module, attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
