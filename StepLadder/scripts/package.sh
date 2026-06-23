#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

rm -f sqlautoscaler.zip
zip -qr sqlautoscaler.zip \
  function_app.py \
  host.json \
  requirements.txt \
  src \
  sql \
  .funcignore \
  -x '*/__pycache__/*' '*.pyc' '.DS_Store'

unzip -l sqlautoscaler.zip
