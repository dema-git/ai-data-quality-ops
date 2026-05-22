#!/usr/bin/env bash
set -euo pipefail

echo "[ci-tests] Running tests inside Docker..."

export PYTHONPATH="/app/services/fastapi_app:/app"

python -m pytest -q