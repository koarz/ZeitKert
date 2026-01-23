#!/bin/bash

# ZeitKert Test Runner

set -e

echo "Building test tools..."
xmake build sqltest

echo ""
echo "Running SQL tests..."
echo "===================="

if [ $# -eq 0 ]; then
    # 没有参数时运行所有示例测试
    ./build/linux/x86_64/release/sqltest tools/sqltest/*.sql
else
    # 传入参数时运行指定测试
    ./build/linux/x86_64/release/sqltest "$@"
fi

echo ""
echo "All tests completed!"
