#!/usr/bin/env bash
set -euo pipefail

echo "[smoke] starting stack"
docker compose up -d --build

echo "[smoke] waiting 3s"
sleep 3

echo "[smoke] healthz"
curl -sf http://localhost:8080/healthz >/dev/null || { echo "healthz failed"; exit 1; }

echo "[smoke] trigger admin/run"
curl -s -X POST http://localhost:8080/admin/run | cat

echo "[smoke] last-run"
curl -s http://localhost:8080/admin/last-run | jq . || true

echo "[smoke] ok"


