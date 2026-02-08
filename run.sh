#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

MODE="${COMPILE_TYPE:-release}"
OUTPUT_DIR="$SCRIPT_DIR/output"
BUILD_BIN="$SCRIPT_DIR/build/linux/x86_64/$MODE"
mkdir -p "$OUTPUT_DIR"

xmake config --mode="$MODE" -y || exit 1

for param in "$@"; do
if [ "${param}" = "help" ]; then
    echo "db      build and run ZeitKert"
    echo "bpm     start buffer pool manager bench"
    echo "file    start file control bench"
    echo "test    start all tests"
elif [ "${param}" = "db" ]; then
    xmake build ZeitKert || exit 1
    cp "$BUILD_BIN/ZeitKert" "$OUTPUT_DIR/ZeitKert"
    "$OUTPUT_DIR/ZeitKert"
elif [ "${param}" = "bpm" ]; then
    xmake build bpm-bench || exit 1
    cp "$BUILD_BIN/bpm-bench" "$OUTPUT_DIR/bpm-bench"
    "$OUTPUT_DIR/bpm-bench"
elif [ "${param}" = "file" ]; then
    mkdir -p "$SCRIPT_DIR/build"
    clang++ -std=c++23 -g -O3 "$SCRIPT_DIR/benchmark/file-control-bench.cpp" -o "$OUTPUT_DIR/file-control-bench" || exit 1
    "$OUTPUT_DIR/file-control-bench"
elif [ "${param}" = "test" ]; then
    xmake build tests || exit 1
    cp "$BUILD_BIN/tests" "$OUTPUT_DIR/tests"
    "$OUTPUT_DIR/tests"
fi
done
