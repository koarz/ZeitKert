#include "storage/lsmtree/MemTable.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <cassert>

namespace DB {
Status MemTable::Put(const Slice &key, const Slice &value) {
  auto add_size = key.Size() + value.Size() + 4;
  approximate_size_.fetch_add(add_size);
  skip_list_.Insert(key, value);
  auto status = wal_.WriteSlice(key, value);
  return status;
}

Status MemTable::Get(const Slice &key, Slice *value) {
  return skip_list_.Get(key, value);
}

void MemTable::RecoverFromWal() {
  Slice key, value;
  while (wal_.ReadFromLogFile(&key, &value)) {
    auto add_size = key.Size() + value.Size() + 4;
    approximate_size_.fetch_add(add_size);
    skip_list_.Insert(key, value);
  }
}

std::string MemTable::Serilize() {
  std::string res;
  auto it = skip_list_.Begin();
  while (it != skip_list_.End()) {
    res += (*it).first.Serilize();
    res += (*it).second.Serilize();
    ++it;
  }
  assert(res.size() <= DEFAULT_PAGE_SIZE);
  return res;
}
} // namespace DB