#!/usr/bin/env python3
"""Check or apply the optional system-role worker patch without restarting it."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


KNOWN_SOURCE_SHA256 = {
    "bonsai_adapter.py": "481e1ca9be4838b2c3fd920d61a021b8e0635bd1828ef1ec6b1e84d775d783b2",
    "webserver_converter_glb.py": "61b3935d4b09a78e64f415d5f3f8a2e222e6dccdfe3bf7e926913e5dea3ed4c1",
}


def replace_once(source: str, old: str, new: str, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one exact anchor, found {count}")
    return source.replace(old, new, 1)


def patch_bonsai(source: str) -> str:
    if '"system_prompt_supported": True' not in source:
        source = replace_once(source,
            '            "loaded_model": self.loaded_model_id(),\n            "base_url": self.config.base_url,',
            '            "loaded_model": self.loaded_model_id(),\n'
            '            "system_prompt_supported": True,\n'
            '            "base_url": self.config.base_url,',
            "Bonsai status capability")
    if '"system_prompt_models": ["bonsai2-27b"]' not in source:
        source = replace_once(source,
            '            "system_prompt_supported": True,\n            "base_url": self.config.base_url,',
            '            "system_prompt_supported": True,\n'
            '            "system_prompt_models": ["bonsai2-27b"],\n'
            '            "base_url": self.config.base_url,',
            "verified system prompt model list")
    if '"system_prompt_supported": entry.id == "bonsai2-27b"' not in source:
        source = replace_once(source,
            '                    "context_tokens": int(entry.context_tokens),',
            '                    "context_tokens": int(entry.context_tokens),\n'
            '                    "system_prompt_supported": entry.id == "bonsai2-27b",',
            "per-model system prompt capability")
    if 'system_prompt: str = ""' not in source:
        source = replace_once(source,
            "        prompt: str,\n        *,\n        model_id: Optional[str] = None,",
            "        prompt: str,\n        *,\n        system_prompt: str = \"\",\n        model_id: Optional[str] = None,",
            "BonsaiAdapter.run_completion signature")
    if '{"role": "system", "content": clean_system_prompt}' not in source:
        source = replace_once(source,
            "        body = {\n            \"messages\": [{\"role\": \"user\", \"content\": content}],",
            "        messages = []\n"
            "        clean_system_prompt = str(system_prompt or \"\").strip()\n"
            "        if clean_system_prompt:\n"
            "            messages.append({\"role\": \"system\", \"content\": clean_system_prompt})\n"
            "        messages.append({\"role\": \"user\", \"content\": content})\n"
            "        body = {\n            \"messages\": messages,",
            "Bonsai request messages")
    return source


def patch_server(source: str) -> str:
    if "system_prompt=task.system_prompt" in source and 'payload.pop("system_prompt", None)' in source:
        return source
    source = replace_once(source,
        "    MAX_IMAGE_BYTES as BONSAI_MAX_IMAGE_BYTES,\n    BonsaiAdapter,",
        "    MAX_IMAGE_BYTES as BONSAI_MAX_IMAGE_BYTES,\n"
        "    MAX_PROMPT_CHARS as BONSAI_MAX_PROMPT_CHARS,\n    BonsaiAdapter,",
        "server Bonsai imports")
    source = replace_once(source,
        "    prompt: str\n    image_url: str = \"\"",
        "    prompt: str\n    system_prompt: str = \"\"\n    image_url: str = \"\"",
        "AiVisionTask system prompt field")
    source = replace_once(source,
        "        payload = asdict(self)\n        now = time.time()\n        payload[\"elapsed_seconds\"] = (\n            max(0.0, (self.completed_at or now) - self.started_at)\n            if self.started_at\n            else 0.0\n        )\n        return payload\n\n\n# Task types that share the converter queue",
        "        payload = asdict(self)\n        payload.pop(\"system_prompt\", None)\n"
        "        now = time.time()\n        payload[\"elapsed_seconds\"] = (\n"
        "            max(0.0, (self.completed_at or now) - self.started_at)\n"
        "            if self.started_at\n            else 0.0\n        )\n        return payload\n\n\n# Task types that share the converter queue",
        "AiVisionTask status redaction")
    source = replace_once(source,
        "        prompt = validate_prompt(raw_payload.get(\"prompt\"))\n        # The ceiling belongs to the model:",
        "        prompt = validate_prompt(raw_payload.get(\"prompt\"))\n"
        "        system_prompt = str(raw_payload.get(\"system_prompt\") or \"\").strip()\n"
        "        if len(system_prompt) + len(prompt) > BONSAI_MAX_PROMPT_CHARS:\n"
        "            raise BonsaiRequestError(\n"
        "                f\"system_prompt and prompt exceed {BONSAI_MAX_PROMPT_CHARS} characters\"\n"
        "            )\n        # The ceiling belongs to the model:",
        "AI request system prompt validation")
    source = replace_once(source,
        "        mode=mode,\n        prompt=prompt,\n        image_url=image_url,",
        "        mode=mode,\n        prompt=prompt,\n        system_prompt=system_prompt,\n        image_url=image_url,",
        "AiVisionTask construction")
    source = replace_once(source,
        "        task.prompt,\n        model_id=task.model or None,",
        "        task.prompt,\n        system_prompt=task.system_prompt,\n        model_id=task.model or None,",
        "Bonsai completion call")
    return source


PATCHERS = {
    "bonsai_adapter.py": patch_bonsai,
    "webserver_converter_glb.py": patch_server,
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True,
                        help="Directory containing the two worker Python files")
    parser.add_argument("--apply", action="store_true",
                        help="Write patched files atomically; default is check-only")
    parser.add_argument("--allow-anchor-compatible-source", action="store_true",
                        help="Allow an unknown source hash if every exact anchor matches")
    args = parser.parse_args()

    planned = {}
    for name, patcher in PATCHERS.items():
        path = args.root / name
        source = path.read_text(encoding="utf-8")
        candidate = patcher(source)
        if candidate == source:
            print(f"{name}: already patched")
            continue
        digest = sha(path)
        if digest != KNOWN_SOURCE_SHA256[name] and not args.allow_anchor_compatible_source:
            raise RuntimeError(
                f"{name}: source hash {digest} is not the audited {KNOWN_SOURCE_SHA256[name]}"
            )
        planned[path] = candidate
        print(f"{name}: anchors verified; patch ready")

    if args.apply:
        for path, value in planned.items():
            temporary = path.with_suffix(path.suffix + ".system-role.tmp")
            temporary.write_text(value, encoding="utf-8", newline="\n")
            temporary.replace(path)
            print(f"{path.name}: patched; restart still required")
    else:
        print("check-only: no files changed and no process restarted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
