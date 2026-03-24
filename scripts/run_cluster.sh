#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

CONFIG="${1:-configs/cluster_small.yaml}"
KEY_PATH="${2:?Usage: $0 <config> <ssh-key-path>}"
OUTPUT="${3:-results.json}"

echo "Provisioning cluster..."
python -m src.cli provision "$CONFIG" --key-path "$KEY_PATH"

echo "Running distributed screen..."
python -m src.cli run "$CONFIG" --output "$OUTPUT"

echo "Results in $OUTPUT"
echo "Remember to tear down: python -m src.cli teardown $CONFIG"
