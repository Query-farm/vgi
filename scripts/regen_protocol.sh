#!/usr/bin/env bash
# Regenerate this native client's Nanoarrow artifacts from the canonical V3 model.
# Pass --check to report drift without writing files. SDK codegen keeps its default
# Arrow C++ backend; this entry point selects only the native extension targets.
set -euo pipefail

vgi_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_root="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"

exec uv run --project "$python_root" python "$python_root/scripts/regen_generated.py" \
    --only vgi --root "vgi=$vgi_root" --native-cpp-backend nanoarrow --require-all "$@"
