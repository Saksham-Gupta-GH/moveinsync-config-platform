#!/usr/bin/env bash
# ==============================================================================
# MoveInSync Hierarchical Tenant Configuration Platform
# One-Command Local Startup & Verification Script
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==============================================================================="
echo "   MoveInSync Configuration Platform — One-Command Startup & Verification"
echo "==============================================================================="

# Choose Java 17 if available
if [ -z "${JAVA_HOME:-}" ]; then
    if [ -d "/opt/homebrew/opt/openjdk@17" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
        export PATH="$JAVA_HOME/bin:$PATH"
    fi
fi

echo "[1/3] Running Full Test Suite (24 Tests across Core, Client SDK, Server)..."
./mvnw clean test

echo ""
echo "[2/3] Building Executable Server Jar..."
./mvnw package -DskipTests

echo ""
echo "[3/3] Starting MoveInSync Configuration Server on port 8080..."
echo "  -> REST & SSE API Base:  http://localhost:8080/api/v1/config"
echo "  -> Actuator Health:       http://localhost:8080/actuator/health"
echo "  -> Prometheus Metrics:    http://localhost:8080/api/v1/metrics"
echo ""
echo "Press Ctrl+C to stop the server."
echo "==============================================================================="

java -jar config-server/target/config-server-1.0.0-SNAPSHOT.jar

