#!/bin/sh
set -eu

ROOT=/srv/autorig/data/vehicle-segmentation
PYTHON=/usr/bin/python3.11
SOURCE_COMMIT=2b90b9f5ceec907a1c18123530e92e794ad901a4
CHECKPOINT_SHA=7402e0d864fa82708a20fbd15bc84245c2f26dff0eb43a4b5b93452deb34be69
CHECKPOINT_URL=https://dl.fbaipublicfiles.com/segment_anything_2/092824/sam2.1_hiera_tiny.pt
SOURCE_URL=https://github.com/facebookresearch/sam2.git
STAGE="/srv/autorig/data/.vehicle-segmentation.stage.$$"

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root so the immutable runtime can be owned by autorig:autorig" >&2
  exit 2
fi
if [ ! -x "$PYTHON" ]; then
  echo "missing $PYTHON" >&2
  exit 3
fi
if [ -e "$ROOT" ]; then
  "$ROOT/venv/bin/python" -c 'import torch; assert torch.__version__ == "2.5.1+cpu"; assert not torch.cuda.is_available(); import sam2, cv2, numpy'
  printf '%s\n' "runtime already installed and importable: $ROOT"
  exit 0
fi

cleanup() { rm -rf -- "$STAGE"; }
trap cleanup EXIT HUP INT TERM
install -d -m 0755 "$STAGE/checkpoints" "$STAGE/tmp" "$STAGE/pip-cache"
export TMPDIR="$STAGE/tmp"
export PIP_CACHE_DIR="$STAGE/pip-cache"
git clone --filter=blob:none --no-checkout "$SOURCE_URL" "$STAGE/source"
git -C "$STAGE/source" checkout --detach "$SOURCE_COMMIT"
test "$(git -C "$STAGE/source" rev-parse HEAD)" = "$SOURCE_COMMIT"

curl --fail --location --retry 3 --output "$STAGE/checkpoints/sam2.1_hiera_tiny.pt.part" "$CHECKPOINT_URL"
test "$(sha256sum "$STAGE/checkpoints/sam2.1_hiera_tiny.pt.part" | cut -d' ' -f1)" = "$CHECKPOINT_SHA"
mv "$STAGE/checkpoints/sam2.1_hiera_tiny.pt.part" "$STAGE/checkpoints/sam2.1_hiera_tiny.pt"

"$PYTHON" -m venv "$STAGE/venv"
"$STAGE/venv/bin/python" -m pip install --disable-pip-version-check --no-cache-dir \
  setuptools==75.8.0 wheel==0.45.1
"$STAGE/venv/bin/python" -m pip install --disable-pip-version-check --no-cache-dir \
  --index-url https://pypi.org/simple --extra-index-url https://download.pytorch.org/whl/cpu \
  torch==2.5.1+cpu torchvision==0.20.1+cpu
"$STAGE/venv/bin/python" -m pip install --disable-pip-version-check --no-cache-dir \
  numpy==2.2.6 opencv-python-headless==4.10.0.84 pillow==12.3.0 \
  hydra-core==1.3.2 omegaconf==2.3.0 antlr4-python3-runtime==4.9.3 \
  iopath==0.1.10 portalocker==4.4.0 tqdm==4.66.5
SAM2_BUILD_CUDA=0 "$STAGE/venv/bin/python" -m pip wheel --disable-pip-version-check \
  --no-cache-dir --no-deps --no-build-isolation --wheel-dir "$STAGE/wheels" "$STAGE/source"
"$STAGE/venv/bin/python" -m pip install --disable-pip-version-check --no-cache-dir \
  --no-deps "$STAGE"/wheels/SAM_2-1.0-py3-none-any.whl

SAM2_STAGE="$STAGE" CUDA_VISIBLE_DEVICES='' "$STAGE/venv/bin/python" - <<'PY'
import json
import os
import pathlib
import platform
import subprocess
import torch
import torchvision
import cv2
import numpy
import PIL
import hydra
import sam2

assert torch.__version__ == "2.5.1+cpu", torch.__version__
assert torchvision.__version__ == "0.20.1+cpu", torchvision.__version__
assert not torch.cuda.is_available()
root = pathlib.Path(os.environ["SAM2_STAGE"])
receipt = {
    "schema_string": "autorig.vehicle.sam2-runtime-receipt/1",
    "python_string": platform.python_version(),
    "torch_string": torch.__version__,
    "torchvision_string": torchvision.__version__,
    "opencv_string": cv2.__version__,
    "numpy_string": numpy.__version__,
    "pillow_string": PIL.__version__,
    "hydra_string": hydra.__version__,
    "cuda_available_bool": torch.cuda.is_available(),
    "source_commit_string": subprocess.check_output(
        ["git", "-C", str(root / "source"), "rev-parse", "HEAD"], text=True).strip(),
    "checkpoint_sha256_string": subprocess.check_output(
        ["sha256sum", str(root / "checkpoints/sam2.1_hiera_tiny.pt")], text=True).split()[0],
    "pip_freeze_array": subprocess.check_output(
        [str(root / "venv/bin/python"), "-m", "pip", "freeze"], text=True).splitlines(),
}
tmp = root / "runtime-receipt.json.tmp"
tmp.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
tmp.replace(root / "runtime-receipt.json")
PY

chown -R autorig:autorig "$STAGE"
chmod -R a-w "$STAGE/source" "$STAGE/checkpoints"
mv "$STAGE" "$ROOT"
trap - EXIT HUP INT TERM
printf '%s\n' "installed CPU SAM2 runtime: $ROOT"
