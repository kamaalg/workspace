#!/usr/bin/env bash
set -euo pipefail

python -m venv /workspace/.venv
source /workspace/.venv/bin/activate

python -m pip install --upgrade pip wheel
if command -v uv &>/dev/null; then
  pip install -r /workspace/requirements.txt
else
  pip install -r /workspace/requirements.txt
fi

echo "Dev container is ready. Activate venv with: source /workspace/.venv/bin/activate"
