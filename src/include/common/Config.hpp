#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

#ifndef TESTS
#define USELSMENGINE
#endif

constexpr auto default_databases_dir = ".ZeitKert";

using frame_id_t = int32_t;
using timestamp_t = uint64_t;
using page_id_t = int32_t;
using ReadLock = std::shared_lock<std::shared_mutex>;
using WriteLock = std::unique_lock<std::shared_mutex>;
using Byte = std::byte;

constexpr page_id_t INVALID_PAGE_ID = -1;
// per sstable size is 4MB
constexpr uint32_t SSTABLE_SIZE = 0x400000;
#ifdef USELSMENGINE
constexpr size_t DEFAULT_PAGE_SIZE = SSTABLE_SIZE;
constexpr size_t DEFAULT_POOL_SIZE = 16;
#else
constexpr size_t DEFAULT_PAGE_SIZE = 4096;
constexpr size_t DEFAULT_POOL_SIZE = 128;
#endif
