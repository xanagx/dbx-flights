#!/usr/bin/env bash
set -euo pipefail

# Installs Databricks CLI binary and Python helper libraries.
# Run from repository root: bash scripts/install_cli.sh

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required but not installed." >&2
  exit 1
fi

echo "Installing Databricks CLI binary..."
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

echo "Creating local virtual environment..."
python3 -m venv .venv

echo "Installing Python dependencies..."
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

echo "Done. Verify with:"
echo "  databricks --version"
echo "  .venv/bin/python -c 'import pyspark; print(pyspark.__version__)'"
