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
#else
// per sstable size is 4MB
constexpr uint32_t SSTABLE_SIZE = 0x400000;
constexpr size_t DEFAULT_PAGE_SIZE = 32768;
constexpr size_t DEFAULT_POOL_SIZE = 2048;
#endif
