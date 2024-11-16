#pragma once

#include <string>

namespace DB {
struct ValueType {
  using uint = unsigned int;

  enum class Type { Int, Null, String, Double };

  virtual Type GetType() { return type_; }
  virtual uint GetSize() { return size_; }
  virtual bool IsVariableSize() { return false; }
  virtual std::string ToString() = 0;

  ValueType() : type_(ValueType::Type::Null), size_(0) {}
  ValueType(Type type) : type_(type) {}
  ValueType(Type type, uint size) : type_(type), size_(size) {}

private:
  Type type_;
  uint size_{};
};
} // namespace DB