#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
mkdir -p .tools/java .home .cache/ivy .cache/spark-tmp
if [[ ! -x .tools/java/bin/java ]]; then
  curl -L --fail --silent --show-error \
    'https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jre/hotspot/normal/eclipse' \
    -o /tmp/cdp-temurin17.tar.gz
  tar -xzf /tmp/cdp-temurin17.tar.gz -C .tools/java --strip-components=1
fi
python3 -m venv .venv
JAVA_HOME="$project_root/.tools/java" PATH="$project_root/.tools/java/bin:$PATH" \
  HOME="$project_root/.home" SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_LOCAL_DIRS="$project_root/.cache/spark-tmp" \
  .venv/bin/pip install -e '.[dev]'
