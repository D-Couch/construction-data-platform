#!/usr/bin/env bash
# The supplied .git directory is read-only in this workspace; keep repo metadata here instead.
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GIT_DIR="$project_root/.git-data"
export GIT_WORK_TREE="$project_root"
exec git "$@"
