#!/usr/bin/env bash
# Host Make targets run this before any `python -m` recipe.
# Python 3.10 has no datetime.UTC, and that is what `make seed` imports.
set -euo pipefail

if ! command -v python >/dev/null 2>&1; then
  cat >&2 <<'EOF'
Python 3.11 or newer is required, and `python` was not found on PATH.
Activate the project venv, or create one with Python 3.11.
Ubuntu 22.04 ships Python 3.10 as python3:
  sudo apt install python3.11 python3.11-venv python3.11-dev postgresql-client
  python3.11 -m venv .venv
  source .venv/bin/activate
EOF
  exit 1
fi

python - <<'PY'
import sys

if sys.version_info >= (3, 11):
    raise SystemExit(0)

version = ".".join(str(part) for part in sys.version_info[:3])
sys.stderr.write(
    f"Python 3.11 or newer is required. This interpreter is Python {version}.\n"
    "The generator imports datetime.UTC, which Python 3.10 does not have.\n"
    "On that interpreter, make seed fails with:\n"
    "  ImportError: cannot import name 'UTC' from 'datetime'\n"
    "Ubuntu 22.04 ships Python 3.10 as python3. Install 3.11 and recreate the venv:\n"
    "  sudo apt install python3.11 python3.11-venv python3.11-dev postgresql-client\n"
    "  python3.11 -m venv .venv\n"
    "  source .venv/bin/activate\n"
)
raise SystemExit(1)
PY
