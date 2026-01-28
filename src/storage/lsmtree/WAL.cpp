#include "storage/lsmtree/WAL.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <limits>
#include <memory>

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
  uint32_t klen = key.Size();
  uint32_t vlen = value.Size();
  fs_.write(reinterpret_cast<const char *>(&klen), sizeof(klen));
  fs_.write(reinterpret_cast<const char *>(&vlen), sizeof(vlen));
  if (klen > 0) {
    fs_.write(reinterpret_cast<const char *>(key.GetData()), klen);
  }
  if (vlen > 0) {
    fs_.write(reinterpret_cast<const char *>(value.GetData()), vlen);
  }
  if (fs_.bad()) {
    return Status::Error(ErrorCode::IOError, "I/O error when writing page");
  }
  fs_.flush();
  return Status::OK();
}

bool WAL::ReadFromLogFile(Slice *key, Slice *value) {
  uint32_t klen = 0;
  uint32_t vlen = 0;
  fs_.read(reinterpret_cast<char *>(&klen), sizeof(klen));
  if (fs_.eof()) {
    fs_.seekp(std::ios::end);
    return false;
  }
  fs_.read(reinterpret_cast<char *>(&vlen), sizeof(vlen));
  if (!fs_) {
    return false;
  }
  if (klen > std::numeric_limits<uint16_t>::max() ||
      vlen > std::numeric_limits<uint16_t>::max()) {
    return false;
  }
  auto kbuf = std::make_unique<char[]>(klen);
  if (klen > 0) {
    fs_.read(kbuf.get(), klen);
  }
  auto vbuf = std::make_unique<char[]>(vlen);
  if (vlen > 0) {
    fs_.read(vbuf.get(), vlen);
  }
  *key = Slice(kbuf.get(), static_cast<uint16_t>(klen));
  *value = Slice(vbuf.get(), static_cast<uint16_t>(vlen));
  return true;
}
} // namespace DB
