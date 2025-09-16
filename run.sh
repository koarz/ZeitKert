#!/bin/bash

for param in "$@"; do
if [ "${param}" = "help" ]; then
    echo "bpm     start buffer pool manager bench"
    echo "file    start file control bench"
    echo "test    start all tests"
elif [ "${param}" = "db" ]; then
    xmake build ZeitKert
    xmake run ZeitKert
elif [ "${param}" = "bpm" ]; then
    xmake build bpm-bench
    xmake run bpm-bench
elif [ "${param}" = "file" ]; then
    mkdir build
    clang++ -std=c++23 -g -O3 ./benchmark/file-control-bench.cpp -o ./build/file-control-bench
    ./build/file-control-bench
elif [ "${param}" = "test" ]; then
    xmake build tests
    xmake run tests
fi
done