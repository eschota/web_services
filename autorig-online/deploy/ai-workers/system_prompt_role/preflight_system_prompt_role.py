#!/usr/bin/env python3
"""Fail-closed source preflight for the deployed system-role worker patch."""

from __future__ import annotations

import argparse
import ast
from pathlib import Path


def require(source: str, fragment: str, label: str) -> None:
    if fragment not in source:
        raise RuntimeError(f"missing {label}: {fragment}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    bonsai_path = args.root / "bonsai_adapter.py"
    server_path = args.root / "webserver_converter_glb.py"
    bonsai = bonsai_path.read_text(encoding="utf-8")
    server = server_path.read_text(encoding="utf-8")
    ast.parse(bonsai, filename=str(bonsai_path))
    ast.parse(server, filename=str(server_path))
    require(bonsai, 'system_prompt: str = ""', "optional adapter argument")
    require(bonsai, '"system_prompt_supported": True', "advertised capability")
    require(bonsai, '"system_prompt_models": ["bonsai2-27b"]',
            "verified model allowlist")
    require(bonsai, '"unlimited_output_supported": True', "unlimited output capability")
    require(bonsai, "if value == -1:", "unlimited sentinel validation")
    require(bonsai, '"max_tokens": requested_max_tokens', "exact llama request value")
    require(bonsai, 'usage["requested_max_tokens"] = requested_max_tokens',
            "inference evidence")
    require(bonsai, '"system_prompt_supported": entry.id == "bonsai2-27b"',
            "honest per-model capability")
    require(bonsai, '{"role": "system", "content": clean_system_prompt}', "system role")
    require(bonsai, 'messages.append({"role": "user", "content": content})', "user role")
    require(server, 'system_prompt = str(raw_payload.get("system_prompt") or "").strip()',
            "request parsing")
    require(server, "len(system_prompt) + len(prompt) > BONSAI_MAX_PROMPT_CHARS",
            "combined size guard")
    require(server, "system_prompt=task.system_prompt", "queue forwarding")
    require(server, 'payload.pop("system_prompt", None)', "status redaction")
    print("system-role preflight passed; restart state is intentionally not inferred")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
