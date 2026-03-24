#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

CONFIG="${1:-configs/default_screen.yaml}"
OUTPUT="${2:-results.json}"

echo "Running local screen with config: $CONFIG"
python -m src.cli run "$CONFIG" --local --output "$OUTPUT"
echo "Done. Results in $OUTPUT"
