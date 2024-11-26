#include "storage/lsmtree/WAL.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/Coding.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <memory>
#include <tuple>

namespace DB {
Status WAL::WriteSlice(const Slice &key, const Slice &value) {
  if (!write_log_) {
    return Status::OK();
  }
  if (!fs_.is_open() || !fs_) {
    fs_.close();
    fs_.open(path_, std::ios::binary | std::ios_base::in | std::ios_base::out |
                        std::ios_base::app | std::ios_base::ate);
  }
  auto klen = key.Size();
  auto vlen = value.Size();
  auto total_len = klen + vlen + 8;
  std::string buffer(total_len, 0);
  std::ignore = ParseSliceToEntry(key, value, buffer.data());
  fs_.write(reinterpret_cast<char *>(buffer.data()), total_len);
  if (fs_.bad()) {
    return Status::Error(ErrorCode::IOError, "I/O error when writing page");
  }
  fs_.flush();
  return Status::OK();
}

bool WAL::ReadFromLogFile(Slice *key, Slice *value) {
  int len;
  fs_.read(reinterpret_cast<char *>(&len), 4);
  if (fs_.eof()) {
    fs_.seekp(std::ios::end);
    return false;
  }
  auto buffer = std::make_unique<char[]>(len);
  fs_.read(buffer.get(), len);
  *key = Slice(buffer.get(), len);
  fs_.read(reinterpret_cast<char *>(&len), 4);
  buffer = std::make_unique<char[]>(len);
  fs_.read(buffer.get(), len);
  *value = Slice(buffer.get(), len);
  return true;
}
} // namespace DB