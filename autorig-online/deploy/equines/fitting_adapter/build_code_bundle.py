"""Build the small immutable code-only archive for an actionless farm smoke."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import zipfile
from pathlib import Path


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def python_closure(root: Path, seeds: list[Path]) -> set[Path]:
    """All package-local Python imports reachable from seeds."""
    found: set[Path] = set()
    pending = list(seeds)
    while pending:
        path = pending.pop()
        if path in found:
            continue
        if not path.is_file() or root not in path.parents:
            raise RuntimeError(f"local Python dependency is missing or escapes package: {path}")
        found.add(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.level != 1:
                continue
            names = ([node.module] if node.module else [alias.name for alias in node.names])
            for name in names:
                if not name:
                    continue
                candidate = root.joinpath(*name.split(".")).with_suffix(".py")
                if candidate.is_file() and candidate not in found:
                    pending.append(candidate)
    return found


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--animation-fitting", required=True, type=Path)
    ap.add_argument("--patched-exporter", required=True, type=Path)
    ap.add_argument("--adapter", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    args = ap.parse_args()
    root = args.animation_fitting.resolve()
    original_init = root / "__init__.py"
    # Importing a submodule executes package __init__. The complete tool package
    # eagerly imports optimizer/rig/observations, which are irrelevant to the
    # Blender exporter and may require SciPy absent from Blender's Python.
    # The ZIP therefore carries an explicitly scoped initializer; the canonical
    # source file stays byte-identical outside the ZIP.
    subset_init = b'''"""Exporter-only animation_fitting subset; not the full fitting API."""\n\nfrom .errors import ContractError, DependencyUnavailableError, OptimizationError\n\n__all__ = ["ContractError", "DependencyUnavailableError", "OptimizationError"]\n'''
    ast.parse(subset_init.decode("utf-8"), filename="animation_fitting/__init__.py")
    closure = python_closure(root, [root / "semantic_ltx_reference.py", root / "errors.py"])
    files = {f"animation_fitting/{path.relative_to(root).as_posix()}": path for path in closure}
    files["animation_fitting/render_actionless_bundle.py"] = args.patched_exporter.resolve()
    files["fitting_adapter/prepare_fitting_adapter.py"] = args.adapter.resolve()
    for path in sorted((root / "schemas").glob("*.json")):
        files[f"animation_fitting/schemas/{path.name}"] = path
    for path in sorted((root / "data" / "semantic_ltx_profiles").glob("*.json")):
        files[f"animation_fitting/data/semantic_ltx_profiles/{path.name}"] = path
    missing = [str(path) for path in files.values() if not path.is_file()]
    if missing:
        raise RuntimeError(f"bundle inputs missing: {missing}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        archive.writestr("animation_fitting/__init__.py", subset_init)
        for name, path in sorted(files.items()):
            archive.writestr(name, path.read_bytes())
        rows = [{"path": "animation_fitting/__init__.py", "sha256": sha_bytes(subset_init),
                 "bytes": len(subset_init), "role": "exporter_only_package_initializer"}]
        rows += [{"path": name, "sha256": sha(path), "bytes": path.stat().st_size}
                 for name, path in sorted(files.items())]
        manifest = {"schema": "autorig.equine-fitting-code-bundle/1",
                    "subset": {"role": "actionless_exporter_only_not_full_animation_fitting_api",
                               "canonical_init_sha256": sha(original_init),
                               "subset_init_sha256": sha_bytes(subset_init),
                               "excluded_eager_modules": ["optimizer", "observations", "rig"],
                               "reason": "Blender exporter needs semantic_ltx_reference only; no SciPy dependency"},
                    "files": rows}
        archive.writestr("code-manifest.json", json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"path": str(args.output), "sha256": sha(args.output),
                      "bytes": args.output.stat().st_size}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
