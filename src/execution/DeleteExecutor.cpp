#include "execution/DeleteExecutor.hpp"
#include "common/Logger.hpp"
#include "common/Status.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundConstant.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "storage/Block.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/Int.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

namespace DB {

Status DeleteExecutor::ScanConditionColumns() {
  std::unordered_map<LSMTree *, std::vector<std::pair<size_t, size_t>>>
      lsm_groups;
  for (size_t i = 0; i < condition_columns_.size(); i++) {
    auto &col_scan = condition_columns_[i];
    lsm_groups[col_scan.lsm_tree.get()].emplace_back(col_scan.column_idx, i);
  }

  for (auto &[lsm_ptr, col_pairs] : lsm_groups) {
    std::vector<size_t> column_indices;
    column_indices.reserve(col_pairs.size());
    for (auto &[col_idx, orig_idx] : col_pairs) {
      column_indices.push_back(col_idx);
    }

    auto &lsm = condition_columns_[col_pairs[0].second].lsm_tree;

    std::vector<ColumnPtr> results;
    auto s = lsm->ScanColumns(column_indices, results);
    if (!s.ok()) {
      return s;
    }

    for (size_t i = 0; i < col_pairs.size(); i++) {
      auto &col_meta = condition_columns_[col_pairs[i].second].column_meta;
      condition_column_data_[col_meta->name_] = results[i];
    }
  }

  return Status::OK();
}

ColumnPtr DeleteExecutor::EvalCondition(const BoundExpressRef &expr) {
  switch (expr->expr_type_) {
  case BoundExpressType::BoundColumnMeta: {
    auto &col_expr = static_cast<BoundColumnMeta &>(*expr);
    auto col_name = col_expr.GetColumnMeta()->name_;
    auto it = condition_column_data_.find(col_name);
    if (it != condition_column_data_.end()) {
      return it->second;
    }
    return nullptr;
  }
  case BoundExpressType::BoundConstant: {
    auto &constant = static_cast<BoundConstant &>(*expr);
    switch (constant.type_->GetType()) {
    case ValueType::Type::Int: {
      auto col = std::make_shared<ColumnVector<int>>();
      col->Insert(constant.value_.i32);
      return col;
    }
    case ValueType::Type::Double: {
      auto col = std::make_shared<ColumnVector<double>>();
      col->Insert(constant.value_.f64);
      return col;
    }
    case ValueType::Type::String: {
      auto col = std::make_shared<ColumnString>();
      col->Insert(std::string(constant.value_.str, constant.size_));
      return col;
    }
    default: return nullptr;
    }
  }
  case BoundExpressType::BoundFunction: {
    auto &func_expr = static_cast<BoundFunction &>(*expr);
    auto func = func_expr.GetFunction();
    auto args = func_expr.GetArguments();

    Block block;
    size_t input_rows = 0;

    for (auto &arg : args) {
      auto arg_col = EvalCondition(arg);
      if (!arg_col) {
        return nullptr;
      }

      std::shared_ptr<ValueType> arg_type;
      if (arg->expr_type_ == BoundExpressType::BoundColumnMeta) {
        arg_type = static_cast<BoundColumnMeta &>(*arg).GetColumnMeta()->type_;
      } else if (arg->expr_type_ == BoundExpressType::BoundConstant) {
        arg_type = static_cast<BoundConstant &>(*arg).type_;
      } else if (arg->expr_type_ == BoundExpressType::BoundFunction) {
        arg_type =
            static_cast<BoundFunction &>(*arg).GetFunction()->GetResultType();
      }

      auto col_with_name =
          std::make_shared<ColumnWithNameType>(arg_col, "", arg_type);
      block.PushColumn(col_with_name);
      input_rows = std::max(input_rows, arg_col->Size());
    }

    std::shared_ptr<ValueType> res_type;
    auto status = func->ResolveResultType(block, res_type);
    if (!status.ok() || !res_type) {
      return nullptr;
    }

    ColumnPtr res_data;
    switch (res_type->GetType()) {
    case ValueType::Type::Int:
      res_data = std::make_shared<ColumnVector<int>>();
      break;
    case ValueType::Type::Double:
      res_data = std::make_shared<ColumnVector<double>>();
      break;
    case ValueType::Type::String:
      res_data = std::make_shared<ColumnString>();
      break;
    default: return nullptr;
    }

    size_t result_idx = block.Size();
    block.PushColumn(
        std::make_shared<ColumnWithNameType>(res_data, "", res_type));

    status = func->ExecuteImpl(block, result_idx, input_rows);
    if (!status.ok()) {
      return nullptr;
    }

    return block.GetColumn(result_idx)->GetColumn();
  }
  default: return nullptr;
  }
}

Status DeleteExecutor::Execute() {
  if (!lsm_tree_) {
    return Status::Error(ErrorCode::InsertError,
                         "Table storage not initialized");
  }

  // 查找主键列索引
  int unique_col_idx = -1;
  auto col_meta = table_meta_->GetColumns();
  if (table_meta_->HasUniqueKey()) {
    auto unique_key_name = table_meta_->GetUniqueKeyColumn();
    for (int i = 0; i < static_cast<int>(col_meta.size()); i++) {
      if (col_meta[i]->name_ == unique_key_name) {
        unique_col_idx = i;
        break;
      }
    }
  }
  if (unique_col_idx < 0) {
    return Status::Error(ErrorCode::InsertError, "table has no primary key");
  }

  // 扫描主键列
  std::vector<size_t> pk_indices{static_cast<size_t>(unique_col_idx)};
  std::vector<ColumnPtr> pk_results;
  auto s = lsm_tree_->ScanColumns(pk_indices, pk_results);
  if (!s.ok()) {
    return s;
  }
  if (pk_results.empty()) {
    // 无数据，无需删除
    auto res = std::make_shared<ColumnVector<int>>();
    res->Insert(0);
    schema_->GetColumns().push_back(std::make_shared<ColumnWithNameType>(
        res, "DeleteRows", std::make_shared<Int>()));
    return Status::OK();
  }

  auto &pk_column = pk_results[0];
  size_t total_rows = pk_column->Size();
  uint32_t deleted_rows = 0;

  if (where_condition_) {
    // 扫描条件列
    s = ScanConditionColumns();
    if (!s.ok()) {
      return s;
    }

    // 求值 WHERE 条件
    auto eval_result = EvalCondition(where_condition_);
    if (!eval_result) {
      return Status::Error(ErrorCode::BindError,
                           "failed to evaluate WHERE condition");
    }
    auto &mask = static_cast<ColumnVector<int> &>(*eval_result);

    // 删除 mask[i] != 0 的行
    auto pk_type = col_meta[unique_col_idx]->type_->GetType();
    for (size_t i = 0; i < total_rows && i < mask.Data().size(); i++) {
      if (mask.Data()[i] != 0) {
        Slice key;
        switch (pk_type) {
        case ValueType::Type::Int: {
          auto &pk_vec = static_cast<ColumnVector<int> &>(*pk_column);
          key = Slice{pk_vec[i]};
          break;
        }
        case ValueType::Type::Double: {
          auto &pk_vec = static_cast<ColumnVector<double> &>(*pk_column);
          key = Slice{pk_vec[i]};
          break;
        }
        case ValueType::Type::String: {
          auto &pk_str = static_cast<ColumnString &>(*pk_column);
          key = Slice{std::string(pk_str[i])};
          break;
        }
        default: continue;
        }
        s = lsm_tree_->Remove(key);
        if (!s.ok()) {
          return s;
        }
        deleted_rows++;
      }
    }
  } else {
    // 无 WHERE 子句：删除所有行
    auto pk_type = col_meta[unique_col_idx]->type_->GetType();
    for (size_t i = 0; i < total_rows; i++) {
      Slice key;
      switch (pk_type) {
      case ValueType::Type::Int: {
        auto &pk_vec = static_cast<ColumnVector<int> &>(*pk_column);
        key = Slice{pk_vec[i]};
        break;
      }
      case ValueType::Type::Double: {
        auto &pk_vec = static_cast<ColumnVector<double> &>(*pk_column);
        key = Slice{pk_vec[i]};
        break;
      }
      case ValueType::Type::String: {
        auto &pk_str = static_cast<ColumnString &>(*pk_column);
        key = Slice{std::string(pk_str[i])};
        break;
      }
      default: continue;
      }
      s = lsm_tree_->Remove(key);
      if (!s.ok()) {
        return s;
      }
      deleted_rows++;
    }
  }

  // 更新行数
  table_meta_->GetRowNumber() -= deleted_rows;

  // 输出删除行数
  auto res = std::make_shared<ColumnVector<int>>();
  res->Insert(static_cast<int>(deleted_rows));
  schema_->GetColumns().push_back(std::make_shared<ColumnWithNameType>(
      res, "DeleteRows", std::make_shared<Int>()));

  return Status::OK();
}

} // namespace DB
