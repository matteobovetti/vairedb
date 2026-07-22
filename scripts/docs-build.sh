#!/usr/bin/env bash
# Build the VaireDB documentation site into docs/vairedb.io/site/.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCS_DIR="${REPO_ROOT}/docs/vairedb.io"
VENV_DIR="${DOCS_DIR}/.venv"

if [ ! -x "${VENV_DIR}/bin/mkdocs" ]; then
  echo "Setting up docs virtualenv at ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/pip" install --upgrade pip
  "${VENV_DIR}/bin/pip" install -r "${DOCS_DIR}/requirements.txt"
fi

cd "${DOCS_DIR}"
exec "${VENV_DIR}/bin/mkdocs" build --strict "$@"
