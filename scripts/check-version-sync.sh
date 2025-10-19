#!/bin/bash

# Version sync validation script
# Ensures that the version in src/pine/version.clj matches the Docker image version in playground.docker-compose.yml

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Extract version from src/pine/version.clj
VERSION_CLJ_FILE="$PROJECT_ROOT/src/pine/version.clj"
if [ ! -f "$VERSION_CLJ_FILE" ]; then
    echo -e "${RED}Error: $VERSION_CLJ_FILE not found${NC}"
    exit 1
fi

VERSION_CLJ=$(grep -o '"[0-9]\+\.[0-9]\+\.[0-9]\+"' "$VERSION_CLJ_FILE" | tr -d '"')
if [ -z "$VERSION_CLJ" ]; then
    echo -e "${RED}Error: Could not extract version from $VERSION_CLJ_FILE${NC}"
    exit 1
fi

# Extract version from playground.docker-compose.yml
DOCKER_COMPOSE_FILE="$PROJECT_ROOT/playground.docker-compose.yml"
if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
    echo -e "${RED}Error: $DOCKER_COMPOSE_FILE not found${NC}"
    exit 1
fi

VERSION_DOCKER=$(grep -o 'ahmadnazir/pine:[0-9]\+\.[0-9]\+\.[0-9]\+' "$DOCKER_COMPOSE_FILE" | cut -d: -f2)
if [ -z "$VERSION_DOCKER" ]; then
    echo -e "${RED}Error: Could not extract Docker image version from $DOCKER_COMPOSE_FILE${NC}"
    exit 1
fi

# Compare versions
if [ "$VERSION_CLJ" = "$VERSION_DOCKER" ]; then
    echo -e "${GREEN}✓ Version sync check passed: $VERSION_CLJ${NC}"
    exit 0
else
    echo -e "${RED}✗ Version sync check failed!${NC}"
    echo -e "${YELLOW}  Version in $VERSION_CLJ_FILE: $VERSION_CLJ${NC}"
    echo -e "${YELLOW}  Version in $DOCKER_COMPOSE_FILE: $VERSION_DOCKER${NC}"
    echo ""
    echo -e "${YELLOW}To fix this, update one of the files to match the other:${NC}"
    echo -e "  ${YELLOW}1. Update $VERSION_CLJ_FILE: (def version \"$VERSION_DOCKER\")${NC}"
    echo -e "  ${YELLOW}2. Update $DOCKER_COMPOSE_FILE: ahmadnazir/pine:$VERSION_CLJ${NC}"
    exit 1
fi
