#!/usr/bin/env bash

# Deploy or refresh the Docker playground stack: git pull then compose up.
#
# Intended to run as the deploying user that owns the clone and Docker socket
# (often a dedicated Unix account named "bot"), e.g. from an admin SSH session:
#   su - bot -c '/path/to/pine-lang/scripts/playground-deploy.sh'
# or after cd into the repo as that user:
#   ./scripts/playground-deploy.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"
git pull
docker compose -f playground.docker-compose.yml up -d
