#include "storage/lsmtree/WAL.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"

namespace DB {
Status WAL::WriteSlice(Slice key, Slice value) {
  return Status::OK();
}

bool WAL::ReadFromLogFile(Slice *key, Slice *value) {
  return false;
}
} // namespace DB