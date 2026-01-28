#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

constexpr auto default_databases_dir = ".ZeitKert";

using frame_id_t = int32_t;
using timestamp_t = uint64_t;
using page_id_t = int32_t;
using ReadLock = std::shared_lock<std::shared_mutex>;
using WriteLock = std::unique_lock<std::shared_mutex>;
using Byte = char;

constexpr page_id_t INVALID_PAGE_ID = -1;
#ifdef TESTS
constexpr uint32_t SSTABLE_SIZE = 8192;
constexpr size_t DEFAULT_PAGE_SIZE = 4096;
constexpr size_t DEFAULT_POOL_SIZE = 128;
constexpr size_t DEFAULT_ROWGROUP_TARGET_SIZE = 64 * 1024;
constexpr size_t DEFAULT_ROWGROUP_ALIGNMENT = 4096;
#else
// per sstable size is 8MB
constexpr uint32_t SSTABLE_SIZE = 8 * 1024 * 1024;
constexpr size_t DEFAULT_PAGE_SIZE = 4096;
constexpr size_t DEFAULT_POOL_SIZE = 4096;
constexpr size_t DEFAULT_ROWGROUP_TARGET_SIZE = 256 * 1024;
constexpr size_t DEFAULT_ROWGROUP_ALIGNMENT = 4096;
#endif
constexpr size_t ZONE_MAP_PREFIX_LEN = 32;
