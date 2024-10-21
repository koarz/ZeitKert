#pragma once

namespace DB {
template <typename Tp> struct Instance {
  Tp &GetInstance() { return instance_; }

private:
  static Tp instance_;
};
} // namespace DB