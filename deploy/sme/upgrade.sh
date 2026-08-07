#!/usr/bin/env bash
# Zero-downtime upgrade for the KChat Drive single-VM-pool deployment.
#
# Builds the new image, then rolls each gateway replica one at a time.
# Traefik drains in-flight requests to the old container before the
# new one takes over, so clients see no interruption.
#
# Usage:
#   ./upgrade.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$SCRIPT_DIR/docker-compose.production.yml}"

echo "upgrade: building new image ..."
docker compose -f "$COMPOSE_FILE" build gateway-1 gateway-2 worker

echo "upgrade: rolling gateway-1 ..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps --build gateway-1
# Wait for gateway-1 to pass its healthcheck (Traefik handles the
# readiness gate via the Docker provider).
sleep 5

echo "upgrade: rolling gateway-2 ..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps --build gateway-2
sleep 5

echo "upgrade: rolling worker ..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps --build worker

echo "upgrade: done."
docker compose -f "$COMPOSE_FILE" ps
