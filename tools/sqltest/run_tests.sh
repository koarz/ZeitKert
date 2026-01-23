#!/bin/bash

# ZeitKert SQL Test Runner

set -e

echo "Building sqltest..."
xmake build sqltest

echo ""
echo "Running SQL tests..."
echo "===================="

# 运行示例测试
./build/linux/x86_64/release/sqltest tools/sqltest/example.sql

echo ""
echo "Done!"
