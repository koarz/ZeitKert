#include "execution/FilterExecutor.hpp"
#include "common/Status.hpp"
#include "execution/FunctionExecutor.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundConstant.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "storage/Block.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "storage/lsmtree/LSMTree.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace DB {

// 对 ColumnPtr 按 mask 过滤，只保留 mask[i] != 0 的行
static ColumnPtr FilterColumnData(const ColumnPtr &col,
                                  const std::shared_ptr<ValueType> &type,
                                  const ColumnVector<int> &mask) {
  size_t rows = mask.Data().size();

  switch (type->GetType()) {
  case ValueType::Type::Int: {
    auto &src = static_cast<ColumnVector<int> &>(*col);
    auto dst = std::make_shared<ColumnVector<int>>();
    for (size_t i = 0; i < rows && i < src.Data().size(); ++i) {
      if (mask.Data()[i] != 0) {
        dst->Insert(src[i]);
      }
    }
    return dst;
  }
  case ValueType::Type::Double: {
    auto &src = static_cast<ColumnVector<double> &>(*col);
    auto dst = std::make_shared<ColumnVector<double>>();
    for (size_t i = 0; i < rows && i < src.Data().size(); ++i) {
      if (mask.Data()[i] != 0) {
        dst->Insert(src[i]);
      }
    }
    return dst;
  }
  case ValueType::Type::String: {
    auto &src = static_cast<ColumnString &>(*col);
    auto dst = std::make_shared<ColumnString>();
    for (size_t i = 0; i < rows && i < src.Size(); ++i) {
      if (mask.Data()[i] != 0) {
        dst->Insert(std::string(src[i]));
      }
    }
    return dst;
  }
  default: return col;
  }
}

Status FilterExecutor::ScanConditionColumns() {
  // 按 LSMTree 指针分组 condition_columns_
  std::unordered_map<LSMTree *, std::vector<std::pair<size_t, size_t>>>
      lsm_groups; // lsm -> [(col_idx, index in condition_columns_)]
  for (size_t i = 0; i < condition_columns_.size(); i++) {
    auto &col_scan = condition_columns_[i];
    lsm_groups[col_scan.lsm_tree.get()].emplace_back(col_scan.column_idx, i);
  }

  // 对每组调用 ScanColumns 一次
  for (auto &[lsm_ptr, col_pairs] : lsm_groups) {
    std::vector<size_t> column_indices;
    column_indices.reserve(col_pairs.size());
    for (auto &[col_idx, orig_idx] : col_pairs) {
      column_indices.push_back(col_idx);
    }

    // 找到对应的 shared_ptr（从第一个匹配的 condition_columns_ 取）
    auto &lsm = condition_columns_[col_pairs[0].second].lsm_tree;

    std::vector<ColumnPtr> results;
    auto s = lsm->ScanColumns(column_indices, results);
    if (!s.ok()) {
      return s;
    }

    // 将结果映射回 condition_column_data_
    for (size_t i = 0; i < col_pairs.size(); i++) {
      auto &col_meta = condition_columns_[col_pairs[i].second].column_meta;
      condition_column_data_[col_meta->name_] = results[i];
    }
  }

  return Status::OK();
}

Status FilterExecutor::Execute() {
  if (!condition_) {
    // 没有 WHERE 条件，直接执行子节点
    Status status;
    for (auto &child : children_) {
      status = child->Execute();
      if (!status.ok()) {
        return status;
      }
      for (auto col : child->GetSchema()->GetColumns()) {
        schema_->GetColumns().push_back(col);
      }
    }
    return Status::OK();
  }

  // 1. 扫描所有表列
  auto status = ScanConditionColumns();
  if (!status.ok()) {
    return status;
  }

  // 2. 递归求值 WHERE 条件得到 mask
  auto eval_column = EvalCondition(condition_);
  if (!eval_column) {
    return Status::Error(ErrorCode::BindError,
                         "failed to evaluate WHERE condition");
  }

  // mask 应该是 ColumnVector<int>
  auto &mask = static_cast<ColumnVector<int> &>(*eval_column);

  // 3. 对所有扫描的列应用 mask，并放入全局缓存
  for (auto &col_scan : condition_columns_) {
    auto &col_meta = col_scan.column_meta;
    auto col_name = col_meta->name_;
    auto it = condition_column_data_.find(col_name);
    if (it != condition_column_data_.end()) {
      auto filtered = FilterColumnData(it->second, col_meta->type_, mask);
      FilteredDataCache::Set(col_name, filtered);
    }
  }

  // 4. 激活缓存，让 ScanColumnExecutor 使用过滤后的数据
  FilteredDataCache::Activate();

  // 5. 执行子节点 (Projection)，子节点中的 ScanColumn 会从缓存读取过滤后的数据
  for (auto &child : children_) {
    status = child->Execute();
    if (!status.ok()) {
      FilteredDataCache::Clear();
      return status;
    }
    for (auto col : child->GetSchema()->GetColumns()) {
      schema_->GetColumns().push_back(col);
    }
  }

  // 6. 清理缓存
  FilteredDataCache::Clear();

  return Status::OK();
}

ColumnPtr FilterExecutor::EvalCondition(const BoundExpressRef &expr) {
  switch (expr->expr_type_) {
  case BoundExpressType::BoundColumnMeta: {
    auto &col_expr = static_cast<BoundColumnMeta &>(*expr);
    auto col_name = col_expr.GetColumnMeta()->name_;

    // 在扫描的列中查找
    auto it = condition_column_data_.find(col_name);
    if (it != condition_column_data_.end()) {
      return it->second;
    }

    return nullptr;
  }
  case BoundExpressType::BoundConstant: {
    auto &constant = static_cast<BoundConstant &>(*expr);
    // 创建单元素列
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

      // 推断类型
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

    // 创建结果列
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

} // namespace DB
