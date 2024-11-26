#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <cstdint>
#include <cstring>

namespace DB {

inline char *EncodeUInt32(char *data, uint32_t n) {
  std::memcpy(data, &n, 4);
  return data + 4;
}

inline char *DecodeUint32(char *data, uint32_t &n) {
  std::memcpy(&n, data, 4);
  return data + 4;
}

inline Status ParseSliceToEntry(const Slice &key, const Slice &value,
                                char *buffer) {
  auto klen = key.Size();
  auto vlen = value.Size();
  char *p = buffer;
  p = EncodeUInt32(p, klen);
  std::memcpy(p, key.GetData(), klen);
  p += klen;
  p = EncodeUInt32(p, vlen);
  std::memcpy(p, value.GetData(), vlen);
  return Status::OK();
}

// TODO: size use uint16 for SSTable Block
inline Status ParseEntryToSlice(Slice &key, Slice &value, char *buffer) {
  uint32_t klen, vlen;
  char *p = buffer;
  p = DecodeUint32(p, klen);
  key = Slice{p, static_cast<uint16_t>(klen)};
  p += klen;
  p = DecodeUint32(p, klen);
  value = Slice{p, static_cast<uint16_t>(vlen)};
  return Status::OK();
}
} // namespace DB