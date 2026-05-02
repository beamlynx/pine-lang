#!/usr/bin/env bash

# Login to the playground and run:
# su -c 'cd ~/beamlynx/pine-lang/ && scripts/playground-deploy.sh' bot # deploy updates

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"
git pull
docker compose -f playground.docker-compose.yml up -d
