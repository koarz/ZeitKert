#include "execution/InsertExecutor.hpp"
#include "common/Logger.hpp"
#include "common/Status.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <memory>
#include <string>
#include <vector>

namespace DB {
Status InsertExecutor::Execute() {
  Status status;
  if (!lsm_tree_) {
    return Status::Error(ErrorCode::InsertError,
                         "Table storage not initialized");
  }
  auto col_meta = table_meta_->GetColumns();
  auto &row_number = table_meta_->GetRowNumber();
  uint32_t inserted_row = 0;

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
  if (unique_col_idx < 0) {
    return Status::Error(ErrorCode::InsertError, "table has no primary key");
  }

  if (bulk_rows_ > 0) {
    uint32_t base_row = row_number;
    for (size_t row_idx = 0; row_idx < bulk_rows_; row_idx++) {
      uint32_t row_id = base_row + static_cast<uint32_t>(row_idx);
      std::string row_tag = std::to_string(row_id);
      std::string row_buffer;
      row_buffer.reserve(128);
      Slice key;
      for (size_t col_idx = 0; col_idx < col_meta.size(); col_idx++) {
        const auto &meta = col_meta[col_idx];
        auto type = meta->type_->GetType();
        std::string value;
        switch (type) {
        case ValueType::Type::Int: {
          int v = static_cast<int>(row_id + col_idx);
          if (static_cast<int>(col_idx) == unique_col_idx) {
            key = Slice{v};
          }
          value = std::to_string(v);
          break;
        }
        case ValueType::Type::Double: {
          double v =
              static_cast<double>(row_id) + static_cast<double>(col_idx) * 0.01;
          if (static_cast<int>(col_idx) == unique_col_idx) {
            key = Slice{v};
          }
          value = std::to_string(v);
          break;
        }
        case ValueType::Type::String: {
          value = meta->name_ + "_" + row_tag;
          if (static_cast<int>(col_idx) == unique_col_idx) {
            key = Slice{value};
          }
          break;
        }
        case ValueType::Type::Null:
          return Status::Error(ErrorCode::InsertError,
                               "UNIQUE KEY column cannot be NULL");
        }
        RowCodec::AppendValue(row_buffer, type, value);
      }

      status = lsm_tree_->Insert(key, Slice{row_buffer});
      if (!status.ok()) {
        return status;
      }
      inserted_row++;
    }
  } else {
    for (auto &child : children_) {
      status = child->Execute();
      if (!status.ok()) {
        return status;
      }
      if (child->GetSchema()->GetColumns().size() != col_meta.size()) {
        return Status::Error(ErrorCode::InsertError,
                             "Some tuple size is not match table's column num");
      }
      auto &columns = child->GetSchema()->GetColumns();
      size_t row_count = columns.empty() ? 0 : columns[0]->Size();
      for (size_t col_idx = 1; col_idx < columns.size(); col_idx++) {
        if (columns[col_idx]->Size() != row_count) {
          return Status::Error(ErrorCode::InsertError,
                               "Column size mismatch in insert values");
        }
      }

      for (size_t row_idx = 0; row_idx < row_count; row_idx++) {
        Slice key;
        auto unique_col = columns[unique_col_idx];
        auto unique_value = unique_col->GetStrElement(row_idx);
        switch (unique_col->GetValueType()->GetType()) {
        case ValueType::Type::Int: key = Slice{std::stoi(unique_value)}; break;
        case ValueType::Type::String: key = Slice{unique_value}; break;
        case ValueType::Type::Double:
          key = Slice{std::stod(unique_value)};
          break;
        case ValueType::Type::Null:
          return Status::Error(ErrorCode::InsertError,
                               "UNIQUE KEY column cannot be NULL");
        }

        std::string row_buffer;
        row_buffer.reserve(128);
        for (auto &col : columns) {
          RowCodec::AppendValue(row_buffer, col->GetValueType()->GetType(),
                                col->GetStrElement(row_idx));
        }

        status = lsm_tree_->Insert(key, Slice{row_buffer});
        if (!status.ok()) {
          return status;
        }
        inserted_row++;
      }
    }
  }
  row_number += inserted_row;
  auto res = std::make_shared<ColumnVector<int>>();
  res->Insert(inserted_row);
  this->schema_->GetColumns().push_back(std::make_shared<ColumnWithNameType>(
      res, "InsertRows", std::make_shared<Int>()));
  return Status::OK();
}
} // namespace DB
