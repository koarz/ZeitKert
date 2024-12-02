#!/bin/bash

for param in "$@"; do
if [ "${param}" = "help" ]; then
    echo "bpm     start buffer pool manager bench"
    echo "file    start file control bench"
    echo "test    start all tests"
elif [ "${param}" = "bpm" ]; then
    xmake build bpm-bench
    xmake run bpm-bench
elif [ "${param}" = "file" ]; then
    clang++ -std=c++23 -g -O3 ./benchmark/file-control-bench.cpp -o ./build/linux/x86_64/debug/file-control-bench
    ./build/linux/x86_64/debug/file-control-bench
elif [ "${param}" = "test" ]; then
    xmake build tests
    xmake run tests
fi
done