#!/bin/sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if command -v mvn >/dev/null 2>&1; then
  exec mvn "-Dmaven.repo.local=$SCRIPT_DIR/.m2/repository" "$@"
fi
echo "Maven is not installed. Install Maven 3.9.16 or run the Windows mvnw.cmd wrapper." >&2
exit 1
