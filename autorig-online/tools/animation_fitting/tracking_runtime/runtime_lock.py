from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any

from ..errors import ContractError, DependencyUnavailableError


LOCK_SCHEMA = "autorig-tracking-runtime-lock.v1"


def sha256_file(path: str | Path) -> str:
    source = Path(path).resolve()
    digest = hashlib.sha256()
    with source.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class RepoPin:
    name: str
    url: str
    commit: str
    license: str
    license_sha256: str


@dataclass(frozen=True)
class CheckpointPin:
    name: str
    url: str
    sha256: str
    bytes: int
    license_source_repo: str


@dataclass(frozen=True)
class RuntimeLock:
    path: Path
    repos: dict[str, RepoPin]
    checkpoints: dict[str, CheckpointPin]
    python: dict[str, str]

    def verify_repo(self, name: str, repo: str | Path) -> dict[str, str]:
        if name not in self.repos:
            raise ContractError(f"Runtime lock has no repository pin named {name!r}")
        pin = self.repos[name]
        root = Path(repo).resolve()
        if not (root / ".git").exists():
            raise ContractError(f"Pinned repository is not a Git checkout: {root}")
        try:
            completed = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"],
                text=True,
                capture_output=True,
                check=False,
            )
        except OSError as exc:
            raise DependencyUnavailableError(f"git is required to verify {name}: {exc}") from exc
        if completed.returncode != 0:
            raise ContractError(f"Cannot inspect {name} checkout: {completed.stderr.strip()}")
        actual = completed.stdout.strip().lower()
        if actual != pin.commit:
            raise ContractError(
                f"Pinned repository mismatch for {name}: expected {pin.commit}, got {actual}"
            )
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"],
            text=True,
            capture_output=True,
            check=False,
        )
        if status.returncode != 0:
            raise ContractError(f"Cannot inspect {name} worktree state: {status.stderr.strip()}")
        dirty = [line for line in status.stdout.splitlines() if line.strip()]
        if dirty:
            raise ContractError(
                f"Pinned repository worktree is not clean for {name}: " + "; ".join(dirty[:8])
            )
        license_path = (root / pin.license).resolve()
        try:
            license_path.relative_to(root)
        except ValueError as exc:
            raise ContractError(f"Pinned license escapes repository root for {name}") from exc
        if not license_path.is_file():
            raise ContractError(f"Pinned license is missing for {name}: {license_path}")
        license_sha = sha256_file(license_path)
        if license_sha != pin.license_sha256:
            raise ContractError(
                f"Pinned license mismatch for {name}: expected {pin.license_sha256}, got {license_sha}"
            )
        return {"commit": actual, "license_sha256": license_sha, "url": pin.url}

    def verify_checkpoint(
        self,
        name: str,
        checkpoint: str | Path,
        *,
        license_repo: str | Path,
    ) -> dict[str, Any]:
        if name not in self.checkpoints:
            raise ContractError(f"Runtime lock has no checkpoint pin named {name!r}")
        pin = self.checkpoints[name]
        source = Path(checkpoint).resolve()
        if not source.is_file():
            raise ContractError(f"Pinned checkpoint does not exist: {source}")
        actual_bytes = source.stat().st_size
        if actual_bytes != pin.bytes:
            raise ContractError(
                f"Pinned checkpoint size mismatch for {name}: expected {pin.bytes}, got {actual_bytes}"
            )
        actual_sha = sha256_file(source)
        if actual_sha != pin.sha256:
            raise ContractError(
                f"Pinned checkpoint SHA-256 mismatch for {name}: expected {pin.sha256}, got {actual_sha}"
            )
        license_provenance = self.verify_repo(pin.license_source_repo, license_repo)
        return {
            "sha256": actual_sha,
            "bytes": actual_bytes,
            "url": pin.url,
            "license_source_repo": pin.license_source_repo,
            "license_source_repo_provenance": license_provenance,
            "license_claim": "linked_to_pinned_official_source_repository_not_a_separate_weights_license",
        }


def _object(payload: Any, field: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ContractError(f"{field} must be an object")
    return payload


def load_runtime_lock(path: str | Path | None = None) -> RuntimeLock:
    source = (
        Path(path).resolve()
        if path is not None
        else Path(__file__).with_name("runtime-lock.v1.json").resolve()
    )
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContractError(f"Invalid runtime lock {source}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema") != LOCK_SCHEMA:
        raise ContractError(f"Unsupported runtime lock; expected {LOCK_SCHEMA}")
    repos = {}
    for name, raw_value in _object(payload.get("repos"), "repos").items():
        raw = _object(raw_value, f"repos.{name}")
        try:
            repos[name] = RepoPin(
                name=name,
                url=str(raw["url"]),
                commit=str(raw["commit"]).lower(),
                license=str(raw["license"]),
                license_sha256=str(raw["license_sha256"]).lower(),
            )
        except KeyError as exc:
            raise ContractError(f"Incomplete repository pin {name}: {exc}") from exc
    checkpoints = {}
    for name, raw_value in _object(payload.get("checkpoints"), "checkpoints").items():
        raw = _object(raw_value, f"checkpoints.{name}")
        try:
            checkpoints[name] = CheckpointPin(
                name=name,
                url=str(raw["url"]),
                sha256=str(raw["sha256"]).lower(),
                bytes=int(raw["bytes"]),
                license_source_repo=str(raw["license_source_repo"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractError(f"Incomplete checkpoint pin {name}: {exc}") from exc
    python = {str(k): str(v) for k, v in _object(payload.get("python"), "python").items()}
    if not repos or not checkpoints:
        raise ContractError("Runtime lock must pin repositories and checkpoints")
    unknown_license_sources = {
        pin.license_source_repo for pin in checkpoints.values() if pin.license_source_repo not in repos
    }
    if unknown_license_sources:
        raise ContractError(
            "Checkpoint license_source_repo values are not pinned repositories: "
            + ", ".join(sorted(unknown_license_sources))
        )
    return RuntimeLock(path=source, repos=repos, checkpoints=checkpoints, python=python)
