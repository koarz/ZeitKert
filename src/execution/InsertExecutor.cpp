#include "execution/InsertExecutor.hpp"
#include "common/Logger.hpp"
#include "common/Status.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <memory>
#include <vector>

namespace DB {
Status InsertExecutor::Execute() {
  Status status;
  bool first{true};
  auto col_meta = table_meta_->GetColumns();
  auto &begin_row = table_meta_->GetRowNumber();
  uint32_t inserted_row{};

  // 检查是否有 unique key
  int unique_col_idx = -1;
  if (table_meta_->HasUniqueKey()) {
    auto unique_key_name = table_meta_->GetUniqueKeyColumn();
    for (int i = 0; i < col_meta.size(); i++) {
      if (col_meta[i]->name_ == unique_key_name) {
        unique_col_idx = i;
        break;
      }
    }
  }

  for (auto &child : children_) {
    status = child->Execute();
    if (!status.ok()) {
      return status;
    }
    if (child->GetSchema()->GetColumns().size() != col_meta.size()) {
      return Status::Error(ErrorCode::InsertError,
                           "Some tuple size is not match table's column num");
    }
    int i = 0;
    for (auto col : child->GetSchema()->GetColumns()) {
      for (int j = 0; j < col->Size(); j++) {
        Slice key;
        // 如果有 unique key，使用 unique key 列的值作为 key
        if (unique_col_idx >= 0) {
          auto unique_col = child->GetSchema()->GetColumns()[unique_col_idx];
          auto unique_value = unique_col->GetStrElement(j);
          switch (unique_col->GetValueType()->GetType()) {
          case ValueType::Type::Int:
            key = Slice{std::stoi(unique_value)};
            break;
          case ValueType::Type::String: key = Slice{unique_value}; break;
          case ValueType::Type::Double:
            key = Slice{std::stod(unique_value)};
            break;
          case ValueType::Type::Null:
            return Status::Error(ErrorCode::InsertError,
                                 "UNIQUE KEY column cannot be NULL");
          }
        } else {
          // 没有 unique key，使用 row_id
          key = Slice{&begin_row, 4};
        }

        switch (col->GetValueType()->GetType()) {
        case ValueType::Type::Int:
          status = col_meta[i]->lsm_tree_->Insert(
              key, Slice{std::stoi(col->GetStrElement(j))});
          break;
        case ValueType::Type::String:
          status =
              col_meta[i]->lsm_tree_->Insert(key, Slice{col->GetStrElement(j)});
          break;
        case ValueType::Type::Double:
          status = col_meta[i]->lsm_tree_->Insert(
              key, Slice{std::stod(col->GetStrElement(j))});
          break;
        case ValueType::Type::Null:
        }
        if (!status.ok()) {
          return status;
        }
        if (unique_col_idx < 0) {
          begin_row++;
        }
      }
      i++;
      if (unique_col_idx < 0) {
        begin_row -= col->Size();
      }
    }
    inserted_row += child->GetSchema()->GetColumns()[0]->Size();
    if (unique_col_idx < 0) {
      begin_row += child->GetSchema()->GetColumns()[0]->Size();
    }
  }
  auto res = std::make_shared<ColumnVector<int>>();
  res->Insert(inserted_row);
  this->schema_->GetColumns().push_back(std::make_shared<ColumnWithNameType>(
      res, "InsertRows", std::make_shared<Int>()));
  return Status::OK();
}
} // namespace DB