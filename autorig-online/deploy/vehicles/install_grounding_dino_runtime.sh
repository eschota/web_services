#!/bin/sh
set -eu

ROOT=/srv/autorig/data/vehicle-segmentation
MODEL_ROOT=$ROOT/grounding-dino
VENV=$ROOT/venv
MODEL_REVISION=a2bb814dd30d776dcf7e30523b00659f4f141c71
SOURCE_COMMIT=856dde20aee659246248e20734ef9ba5214f5e44

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root" >&2
  exit 2
fi
test -x "$VENV/bin/python"
install -d -o autorig -g autorig -m 0755 "$ROOT/tmp" "$ROOT/cache"
export TMPDIR=$ROOT/tmp
export PIP_CACHE_DIR=$ROOT/cache/pip
export HF_HOME=$ROOT/cache/huggingface
export CUDA_VISIBLE_DEVICES=''

nice -n 10 "$VENV/bin/python" -m pip install --disable-pip-version-check --no-cache-dir \
  transformers==4.46.3 huggingface-hub==0.26.2 tokenizers==0.20.3 safetensors==0.4.5

if [ ! -d "$MODEL_ROOT" ]; then
  install -d -o autorig -g autorig -m 0755 "$MODEL_ROOT"
  MODEL_ROOT="$MODEL_ROOT" MODEL_REVISION="$MODEL_REVISION" SOURCE_COMMIT="$SOURCE_COMMIT" \
    nice -n 10 "$VENV/bin/python" - <<'PY'
import hashlib
import json
import os
import pathlib
import urllib.request
from huggingface_hub import snapshot_download

root = pathlib.Path(os.environ["MODEL_ROOT"])
revision = os.environ["MODEL_REVISION"]
source_commit = os.environ["SOURCE_COMMIT"]
snapshot_download(
    repo_id="IDEA-Research/grounding-dino-tiny", revision=revision,
    local_dir=root, cache_dir=os.environ["HF_HOME"],
    allow_patterns=["model.safetensors", "config.json", "preprocessor_config.json",
                    "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json",
                    "added_tokens.json", "vocab.txt", "README.md"])
urllib.request.urlretrieve(
    f"https://raw.githubusercontent.com/IDEA-Research/GroundingDINO/{source_commit}/LICENSE",
    root / "LICENSE")

def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

receipt = {
    "schema_string": "autorig.vehicle.grounding-dino-runtime/1",
    "model_repository_string": "IDEA-Research/grounding-dino-tiny",
    "model_revision_string": revision,
    "official_source_repository_string": "https://github.com/IDEA-Research/GroundingDINO.git",
    "official_source_commit_string": source_commit,
    "license_string": "Apache-2.0",
    "license_sha256_string": sha(root / "LICENSE"),
    "checkpoint_sha256_string": sha(root / "model.safetensors"),
    "model_config_sha256_string": sha(root / "config.json")}
temp = root / "model-provenance.json.tmp"
temp.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
temp.replace(root / "model-provenance.json")
PY
fi

chown -R autorig:autorig "$MODEL_ROOT" "$ROOT/cache" "$ROOT/tmp"
CUDA_VISIBLE_DEVICES='' OMP_NUM_THREADS=2 MKL_NUM_THREADS=2 \
  nice -n 10 "$VENV/bin/python" -c \
  'import torch, transformers; assert not torch.cuda.is_available(); print(transformers.__version__)'
cat "$MODEL_ROOT/model-provenance.json"
