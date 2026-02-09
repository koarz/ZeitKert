#include "execution/InsertExecutor.hpp"
#include "common/Logger.hpp"
#include "common/Status.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <memory>
#include <string>
#include <thread>
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

    // 确定线程数
    size_t num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 1;
    }
    num_threads = std::min(num_threads, static_cast<size_t>(8));
    if (bulk_rows_ < 10000) {
      num_threads = 1;
    }

    // 多线程并行编码行数据
    std::vector<std::vector<std::pair<Slice, Slice>>> thread_results(
        num_threads);
    std::vector<std::thread> threads;
    size_t rows_per_thread = bulk_rows_ / num_threads;
    size_t remainder = bulk_rows_ % num_threads;

    for (size_t t = 0; t < num_threads; t++) {
      size_t start = t * rows_per_thread + std::min(t, remainder);
      size_t count = rows_per_thread + (t < remainder ? 1 : 0);

      threads.emplace_back([&, t, start, count]() {
        auto &result = thread_results[t];
        result.reserve(count);

        for (size_t i = 0; i < count; i++) {
          uint32_t row_id = base_row + static_cast<uint32_t>(start + i);
          std::string row_buffer;
          row_buffer.reserve(128);
          Slice key;

          for (size_t col_idx = 0; col_idx < col_meta.size(); col_idx++) {
            const auto &meta = col_meta[col_idx];
            auto type = meta->type_->GetType();
            switch (type) {
            case ValueType::Type::Int: {
              int v = static_cast<int>(row_id + col_idx);
              if (static_cast<int>(col_idx) == unique_col_idx) {
                key = Slice{v};
              }
              RowCodec::AppendInt(row_buffer, v);
              break;
            }
            case ValueType::Type::Double: {
              double v = static_cast<double>(row_id) +
                         static_cast<double>(col_idx) * 0.01;
              if (static_cast<int>(col_idx) == unique_col_idx) {
                key = Slice{v};
              }
              RowCodec::AppendDouble(row_buffer, v);
              break;
            }
            case ValueType::Type::String: {
              std::string v = meta->name_ + "_" + std::to_string(row_id);
              if (static_cast<int>(col_idx) == unique_col_idx) {
                key = Slice{v};
              }
              RowCodec::AppendString(row_buffer, v);
              break;
            }
            case ValueType::Type::Null: RowCodec::AppendNull(row_buffer); break;
            }
          }

          result.emplace_back(std::move(key), Slice{std::move(row_buffer)});
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    // 合并所有线程结果
    std::vector<std::pair<Slice, Slice>> all_entries;
    size_t total = 0;
    for (auto &r : thread_results) {
      total += r.size();
    }
    all_entries.reserve(total);
    for (auto &r : thread_results) {
      for (auto &e : r) {
        all_entries.push_back(std::move(e));
      }
    }

    LOG_INFO("BulkInsert: {} rows encoded with {} thread(s), inserting...",
             all_entries.size(), num_threads);

    // 批量插入
    status = lsm_tree_->BatchInsert(all_entries);
    if (!status.ok()) {
      return status;
    }
    inserted_row = static_cast<uint32_t>(all_entries.size());
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
