#!/bin/bash
set -e
echo "===================================================================="
echo "  Building Airbyte DB Provisioner (Linux / macOS)"
echo "===================================================================="
CGO_ENABLED=0 go build -ldflags="-s -w" -o db-provisioner ./cmd/db-provisioner
chmod +x db-provisioner
echo ""
echo "[SUCCESS] Built successfully: ./db-provisioner"
echo "Run with: ./db-provisioner ui"
