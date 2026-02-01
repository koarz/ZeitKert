#include "execution/AbstractExecutor.hpp"

namespace DB {
thread_local std::unordered_map<std::string, ColumnPtr> FilteredDataCache::data;
thread_local bool FilteredDataCache::active = false;
} // namespace DB
