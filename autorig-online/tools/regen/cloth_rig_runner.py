"""Server-side runner for the Blender cloth-rig step of AutoRig Regen.

Plain Python (no ``bpy``, no numpy): builds the Blender command, runs it with
a timeout, and then verifies the artifacts itself. Blender exits 0 even when
its ``--python`` script raises, so the exit code is never trusted: success
means ``error.json`` is absent, ``cloth_rig_report.json`` says ``ok``, the
manifest validates, and the FBX/GLB exist, parse, and contain every bone the
manifest names. Any failure raises :class:`ClothRigError` carrying the tail
of Blender's output.

Use from the backend (either way works)::

    runner = load_module_by_path(".../tools/regen/cloth_rig_runner.py")   # or: import cloth_rig_runner
    result = runner.run_cloth_rig(model, decomposition_dir, out_dir)       # REGEN_BLENDER_BIN or blender_bin=
    result.fbx, result.glb, result.manifest_path, result.report

CLI::

    python cloth_rig_runner.py --model rigged.fbx --decomposition DIR --out OUT [--blender PATH]
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import signal
import struct
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "blender_cloth_rig.py"
ENV_BLENDER = "REGEN_BLENDER_BIN"
ENV_TIMEOUT = "REGEN_CLOTH_RIG_TIMEOUT"
DEFAULT_TIMEOUT_S = 1800.0
LOG_NAME = "cloth_rig_blender.log"
TAIL_LINES = 60
TAIL_BYTES = 12000
MIN_MODEL_BYTES = 1024


def _load_manifest_lib():
    name = "autorig_regen_cloth_manifest"
    path = (HERE / "cloth_manifest.py").resolve()
    existing = sys.modules.get(name)
    if existing is not None and Path(getattr(existing, "__file__", "") or "").resolve() == path:
        return existing
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


manifest_lib = _load_manifest_lib()


class ClothRigError(RuntimeError):
    """The cloth-rig step failed; ``stdout_tail`` holds the end of Blender's output."""

    def __init__(
        self,
        reason: str,
        *,
        returncode: int | None = None,
        stdout_tail: str = "",
        error: Mapping[str, Any] | None = None,
        log_path: Path | None = None,
    ):
        message = reason
        if returncode is not None:
            message += f" (blender exit code {returncode})"
        if stdout_tail:
            message += "\n--- blender output (tail) ---\n" + stdout_tail
        super().__init__(message)
        self.reason = reason
        self.returncode = returncode
        self.stdout_tail = stdout_tail
        self.error = dict(error or {})
        self.log_path = log_path


@dataclass(frozen=True)
class ClothRigResult:
    out_dir: Path
    stem: str
    fbx: Path
    glb: Path
    manifest_path: Path
    manifest_alias_path: Path
    report_path: Path
    log_path: Path
    manifest: dict
    report: dict
    returncode: int
    duration_s: float
    stdout_tail: str
    warnings: list = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "ok": True,
            "stem": self.stem,
            "fbx": str(self.fbx),
            "glb": str(self.glb),
            "manifest": str(self.manifest_path),
            "manifest_alias": str(self.manifest_alias_path),
            "report": str(self.report_path),
            "log": str(self.log_path),
            "returncode": self.returncode,
            "duration_s": self.duration_s,
            "groups": len(self.manifest.get("groups", [])),
            "colliders": len(self.manifest.get("colliders", [])),
            "warnings": list(self.warnings) + list(self.report.get("warnings", [])),
        }


def resolve_blender_bin(explicit: str | os.PathLike | None = None) -> str:
    """``explicit`` > ``$REGEN_BLENDER_BIN`` > ``blender`` on PATH."""
    candidate = str(explicit) if explicit else os.environ.get(ENV_BLENDER, "").strip()
    if candidate:
        path = Path(candidate)
        if path.is_file() and os.access(path, os.X_OK):
            return str(path)
        found = shutil.which(candidate)
        if found:
            return found
        raise ClothRigError(f"Blender binary not found or not executable: {candidate}")
    found = shutil.which("blender")
    if found:
        return found
    raise ClothRigError(f"no Blender binary: pass blender_bin or set {ENV_BLENDER}")


def build_command(
    blender_bin: str,
    model: str | os.PathLike,
    decomposition: str | os.PathLike,
    out_dir: str | os.PathLike,
    *,
    weights_module: str | os.PathLike | None = None,
    stem: str | None = None,
    max_residual: float | None = None,
    script: str | os.PathLike = SCRIPT,
    extra_args: Sequence[str] = (),
) -> list[str]:
    command = [
        str(blender_bin), "--background", "--factory-startup", "-noaudio",
        "--python-exit-code", "1",
        "--python", str(Path(script).resolve()),
        "--",
        "--model", str(Path(model).resolve()),
        "--decomposition", str(Path(decomposition).resolve()),
        "--out", str(Path(out_dir).resolve()),
    ]
    if weights_module:
        command += ["--weights-module", str(Path(weights_module).resolve())]
    if stem:
        command += ["--stem", stem]
    if max_residual is not None:
        command += ["--max-residual", repr(float(max_residual))]
    command += [str(a) for a in extra_args]
    return command


def _tail(path: Path, lines: int = TAIL_LINES, max_bytes: int = TAIL_BYTES) -> str:
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            data = handle.read()
    except OSError:
        return ""
    text = data.decode("utf-8", errors="replace")
    return "\n".join(text.splitlines()[-lines:])


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _glb_node_names(path: Path) -> tuple[set[str], int]:
    """Node names and skin count of a GLB; raises ValueError when it is not one."""
    with open(path, "rb") as handle:
        header = handle.read(12)
        if len(header) != 12:
            raise ValueError("truncated GLB header")
        magic, version, length = struct.unpack("<4sII", header)
        if magic != b"glTF" or version != 2:
            raise ValueError(f"not a glTF 2 binary (magic {magic!r}, version {version})")
        if length != path.stat().st_size:
            raise ValueError(f"GLB length field {length} != file size {path.stat().st_size}")
        chunk_header = handle.read(8)
        chunk_len, chunk_type = struct.unpack("<I4s", chunk_header)
        if chunk_type != b"JSON":
            raise ValueError("first GLB chunk is not JSON")
        doc = json.loads(handle.read(chunk_len).decode("utf-8"))
    names = {node.get("name", "") for node in doc.get("nodes", [])}
    return names, len(doc.get("skins", []))


def _stale_paths(paths: Mapping[str, Path]) -> list[Path]:
    out = []
    for path in paths.values():
        out += [path, path.with_name(path.name + ".failed")]
    return out


def verify_artifacts(
    out_dir: str | os.PathLike,
    stem: str,
    *,
    returncode: int | None = None,
    stdout_tail: str = "",
    log_path: Path | None = None,
) -> tuple[dict, dict]:
    """Check every artifact of a run; return (manifest, report) or raise ClothRigError."""
    paths = manifest_lib.artifact_paths(out_dir, stem)

    def fail(reason: str, error: Mapping[str, Any] | None = None) -> ClothRigError:
        return ClothRigError(reason, returncode=returncode, stdout_tail=stdout_tail, error=error, log_path=log_path)

    if paths["error"].exists():
        try:
            error = _read_json(paths["error"])
        except (OSError, ValueError):
            error = {"error": "unreadable error.json"}
        raise fail(f"cloth rig failed at {error.get('stage', '?')}: {error.get('error', 'unknown error')}", error)
    if not paths["report"].is_file():
        raise fail("Blender produced no cloth_rig_report.json (the script did not run to completion)")
    try:
        report = _read_json(paths["report"])
    except (OSError, ValueError) as exc:
        raise fail(f"cloth_rig_report.json is unreadable: {exc}") from exc
    if report.get("ok") is not True:
        raise fail(f"cloth rig report is not ok: {report.get('error')}", report.get("error"))
    self_check = report.get("self_check") or {}
    if not self_check.get("skipped"):
        bad = [k for k in ("fbx", "glb") if not (self_check.get(k) or {}).get("ok")]
        if bad:
            raise fail(f"Blender self-check did not pass for {bad}")
    for key in ("manifest", "manifest_alias"):
        if not paths[key].is_file():
            raise fail(f"missing {paths[key].name}")
    try:
        manifest = _read_json(paths["manifest"])
        alias = _read_json(paths["manifest_alias"])
    except (OSError, ValueError) as exc:
        raise fail(f"manifest is not valid JSON: {exc}") from exc
    if alias != manifest:
        raise fail(f"{paths['manifest_alias'].name} differs from {paths['manifest'].name}")
    problems = manifest_lib.validate_manifest(manifest)
    if problems:
        raise fail("manifest does not satisfy the cloth manifest v1 contract: " + "; ".join(problems[:10]))
    bones = manifest_lib.manifest_bone_names(manifest)
    for key in ("fbx", "glb"):
        path = paths[key]
        if not path.is_file() or path.stat().st_size < MIN_MODEL_BYTES:
            raise fail(f"missing or empty {path.name}")
    try:
        nodes, skins = _glb_node_names(paths["glb"])
    except (OSError, ValueError, struct.error) as exc:
        raise fail(f"{paths['glb'].name} is not a valid GLB: {exc}") from exc
    if not skins:
        raise fail(f"{paths['glb'].name} has no skin")
    missing = sorted(b for b in bones if b not in nodes)
    if missing:
        raise fail(f"{paths['glb'].name} lacks manifest bones {missing[:10]}")
    fbx_bytes = paths["fbx"].read_bytes()
    binary = fbx_bytes.startswith(b"Kaydara FBX Binary")
    if not (binary or fbx_bytes.lstrip().startswith(b";")):
        raise fail(f"{paths['fbx'].name} is not an FBX file")
    # Binary FBX names a node "<name>\x00\x01<class>"; matching the separator
    # keeps "skirt_00_1" from being satisfied by "skirt_00_10".
    suffix = b"\x00\x01Model" if binary else b""
    missing = sorted(b for b in bones if b.encode("utf-8") + suffix not in fbx_bytes)
    if missing:
        raise fail(f"{paths['fbx'].name} lacks manifest bones {missing[:10]}")
    return manifest, report


def _kill(proc: subprocess.Popen) -> None:
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass


def run_cloth_rig(
    model: str | os.PathLike,
    decomposition: str | os.PathLike,
    out_dir: str | os.PathLike,
    *,
    blender_bin: str | os.PathLike | None = None,
    weights_module: str | os.PathLike | None = None,
    stem: str | None = None,
    timeout: float | None = None,
    max_residual: float | None = None,
    env: Mapping[str, str] | None = None,
) -> ClothRigResult:
    """Run Blender on one model and return the verified artifacts."""
    model = Path(model)
    decomposition = Path(decomposition)
    out = Path(out_dir)
    if not model.is_file():
        raise ClothRigError(f"model not found: {model}")
    if not (decomposition / "decomposition.json").is_file():
        raise ClothRigError(f"decomposition.json not found in {decomposition}")
    if not SCRIPT.is_file():
        raise ClothRigError(f"Blender script missing: {SCRIPT}")
    binary = resolve_blender_bin(blender_bin)
    if timeout is None:
        timeout = float(os.environ.get(ENV_TIMEOUT) or DEFAULT_TIMEOUT_S)
    out.mkdir(parents=True, exist_ok=True)
    stem = manifest_lib.resolve_stem(model, stem)
    paths = manifest_lib.artifact_paths(out, stem)
    for path in _stale_paths(paths):  # a previous run's artifacts must not pass as this run's
        if path.exists():
            path.unlink()
    command = build_command(binary, model, decomposition, out, weights_module=weights_module,
                            stem=stem, max_residual=max_residual)
    run_env = dict(os.environ if env is None else env)
    run_env.setdefault("PYTHONNOUSERSITE", "1")
    log_path = out / LOG_NAME
    started = time.monotonic()
    with open(log_path, "wb") as log:
        log.write(("$ " + " ".join(command) + "\n").encode("utf-8"))
        log.flush()
        try:
            proc = subprocess.Popen(
                command, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                env=run_env, start_new_session=(os.name == "posix"),
            )
        except OSError as exc:
            raise ClothRigError(f"cannot start Blender ({binary}): {exc}", log_path=log_path) from exc
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _kill(proc)
            raise ClothRigError(f"Blender timed out after {timeout:.0f}s", stdout_tail=_tail(log_path),
                                log_path=log_path) from None
    duration = round(time.monotonic() - started, 3)
    tail = _tail(log_path)
    manifest, report = verify_artifacts(out, stem, returncode=returncode, stdout_tail=tail, log_path=log_path)
    warnings = []
    if returncode != 0:
        warnings.append(f"Blender exited with code {returncode} after writing valid artifacts")
    return ClothRigResult(
        out_dir=out, stem=stem, fbx=paths["fbx"], glb=paths["glb"], manifest_path=paths["manifest"],
        manifest_alias_path=paths["manifest_alias"], report_path=paths["report"], log_path=log_path,
        manifest=manifest, report=report, returncode=returncode, duration_s=duration, stdout_tail=tail,
        warnings=warnings,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the AutoRig Regen cloth rig step in Blender.")
    parser.add_argument("--model", required=True)
    parser.add_argument("--decomposition", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--blender", default=None, help=f"Blender binary (default: ${ENV_BLENDER} or PATH)")
    parser.add_argument("--weights-module", default=None)
    parser.add_argument("--stem", default=None)
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument("--max-residual", type=float, default=None)
    args = parser.parse_args(argv)
    try:
        result = run_cloth_rig(
            args.model, args.decomposition, args.out, blender_bin=args.blender,
            weights_module=args.weights_module, stem=args.stem, timeout=args.timeout,
            max_residual=args.max_residual,
        )
    except ClothRigError as exc:
        print(json.dumps({"ok": False, "error": exc.reason, "returncode": exc.returncode,
                          "details": exc.error, "log": str(exc.log_path) if exc.log_path else None}, indent=2))
        if exc.stdout_tail:
            print(exc.stdout_tail, file=sys.stderr)
        return 1
    print(json.dumps(result.to_json(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
