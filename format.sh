#!/bin/bash

set -euo pipefail

if ! command -v clang-format >/dev/null 2>&1; then
  echo "clang-format not found. Please install clang-format first." >&2
  exit 1
fi

mode="format"
if [ "${1:-}" = "--check" ]; then
  mode="check"
  shift
fi

if [ "$#" -gt 0 ]; then
  if [ "$mode" = "check" ]; then
    clang-format -style=file --dry-run -Werror "$@"
  else
    clang-format -style=file -i "$@"
  fi
  exit 0
fi

mapfile -t files < <(
  git ls-files "*.c" "*.cc" "*.cpp" "*.cxx" "*.h" "*.hh" "*.hpp" "*.hxx" \
    | grep -v '^src/clickhouse/'
)

if [ ${#files[@]} -eq 0 ]; then
  echo "No C/C++ source files found."
  exit 0
fi

if [ "$mode" = "check" ]; then
  clang-format -style=file --dry-run -Werror "${files[@]}"
else
  clang-format -style=file -i "${files[@]}"
fi
