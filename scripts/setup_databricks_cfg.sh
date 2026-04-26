#!/usr/bin/env bash
set -euo pipefail

# Creates or updates ~/.databrickscfg from environment variables.
# Usage:
#   cp .env.example .env
#   # edit .env with your real host/token/profile
#   source .env
#   bash scripts/setup_databricks_cfg.sh

if [[ -z "${DATABRICKS_HOST:-}" ]]; then
  echo "DATABRICKS_HOST is not set." >&2
  exit 1
fi

if [[ -z "${DATABRICKS_TOKEN:-}" ]]; then
  echo "DATABRICKS_TOKEN is not set." >&2
  exit 1
fi

PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
CFG_PATH="${HOME}/.databrickscfg"

mkdir -p "${HOME}"
touch "${CFG_PATH}"

python3 - <<'PY'
from pathlib import Path
import configparser
import os

cfg_path = Path(os.path.expanduser("~/.databrickscfg"))
profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
host = os.environ["DATABRICKS_HOST"].strip()
token = os.environ["DATABRICKS_TOKEN"].strip()

parser = configparser.RawConfigParser()
parser.read(cfg_path)

if profile.upper() == "DEFAULT":
    parser["DEFAULT"]["host"] = host
    parser["DEFAULT"]["token"] = token
else:
    if not parser.has_section(profile):
        parser.add_section(profile)
    parser.set(profile, "host", host)
    parser.set(profile, "token", token)

with cfg_path.open("w", encoding="utf-8") as f:
    parser.write(f)
PY

chmod 600 "${CFG_PATH}"

echo "Updated ${CFG_PATH} with profile [${PROFILE}]"
echo "Validate with:"
echo "  databricks auth profiles"
echo "  databricks current-user me --profile ${PROFILE}"
