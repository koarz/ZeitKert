#pragma once

#include "storage/lsmtree/LSMTree.hpp"
#include "type/ValueType.hpp"

#include <memory>
#include <string>
namespace DB {
struct ColumnMeta {
  std::string name_;
  std::shared_ptr<ValueType> type_;
  std::shared_ptr<LSMTree> lsm_tree_;
};

using ColumnMetaRef = std::shared_ptr<ColumnMeta>;
} // namespace DB