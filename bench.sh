#!/bin/bash
set -e

RESULT_DIR="bench_results"
mkdir -p "$RESULT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULT_DIR/$TIMESTAMP.txt"

echo "=== ZeitKert Benchmark ==="
echo "Time: $(date)"
echo "Output: $RESULT_FILE"
echo ""

# Build in release mode
xmake f -m release -y
xmake build bench

# Run all benchmarks, output to both terminal and file
xmake run bench "$@" 2>&1 | tee "$RESULT_FILE"

echo ""
echo "Results saved to $RESULT_FILE"
