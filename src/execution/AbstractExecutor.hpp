#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"

#include <memory>
#include <string>
#include <unordered_map>

namespace DB {

// 全局过滤数据缓存，用于 Filter 和 ScanColumn 之间共享过滤后的列数据
struct FilteredDataCache {
  static thread_local std::unordered_map<std::string, ColumnPtr> data;
  static thread_local bool active;

  static void Set(const std::string &col_name, ColumnPtr col) {
    data[col_name] = std::move(col);
  }

  static ColumnPtr Get(const std::string &col_name) {
    auto it = data.find(col_name);
    if (it != data.end()) {
      return it->second;
    }
    return nullptr;
  }

  static void Clear() {
    data.clear();
    active = false;
  }

  static void Activate() { active = true; }
  static bool IsActive() { return active; }
};

class AbstractExecutor {
protected:
  SchemaRef schema_;

public:
  AbstractExecutor(SchemaRef schema) : schema_(schema) {}

  virtual ~AbstractExecutor() = default;

  virtual Status Execute() = 0;

  SchemaRef GetSchema() { return schema_; };
};

using AbstractExecutorRef = std::unique_ptr<AbstractExecutor>;
} // namespace DB