#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

constexpr auto default_databases_dir = ".ZeitgeistDB";

using frame_id_t = int32_t;
using timestamp_t = uint64_t;
using page_id_t = int32_t;
using ReadLock = std::shared_lock<std::shared_mutex>;
using WriteLock = std::unique_lock<std::shared_mutex>;

constexpr page_id_t INVALID_PAGE_ID = -1;
constexpr size_t DEFAULT_PAGE_SIZE = 4096;