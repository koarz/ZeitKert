#include "execution/FilterExecutor.hpp"
#include "common/Status.hpp"
#include "execution/FunctionExecutor.hpp"
#include "function/FunctionComparison.hpp"
#include "function/FunctionLogical.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundConstant.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "storage/Block.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
#include "storage/lsmtree/LSMTree.hpp"
#include "storage/lsmtree/ScanPredicate.hpp"
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

// 翻转比较算子（当常量在左边时）
static FunctionComparison::Operator
FlipOperator(FunctionComparison::Operator op) {
  using Op = FunctionComparison::Operator;
  switch (op) {
  case Op::Less: return Op::Greater;
  case Op::LessOrEquals: return Op::GreaterOrEquals;
  case Op::Greater: return Op::Less;
  case Op::GreaterOrEquals: return Op::LessOrEquals;
  case Op::Equals: return Op::Equals;
  case Op::NotEquals: return Op::NotEquals;
  }
  return op;
}

// 从 BoundExpression 树中提取可下推的简单谓词
// 只处理 AND 连接的 column <op> constant 形式
static void ExtractPredicates(const BoundExpressRef &expr,
                              const std::vector<FilterColumnScan> &columns,
                              std::vector<ScanPredicate> &pushed,
                              bool &all_pushed) {
  if (expr->expr_type_ != BoundExpressType::BoundFunction) {
    all_pushed = false;
    return;
  }

  auto &func_expr = static_cast<BoundFunction &>(*expr);
  auto func = func_expr.GetFunction();
  auto args = func_expr.GetArguments();

  // AND: 递归提取两个子表达式
  auto *logical = dynamic_cast<FunctionLogical *>(func.get());
  if (logical) {
    if (logical->GetOperator() == FunctionLogical::Operator::And) {
      if (args.size() == 2) {
        ExtractPredicates(args[0], columns, pushed, all_pushed);
        ExtractPredicates(args[1], columns, pushed, all_pushed);
      }
      return;
    }
    // OR 或其他逻辑算子：不下推
    all_pushed = false;
    return;
  }

  // 比较算子: column <op> constant
  auto *cmp = dynamic_cast<FunctionComparison *>(func.get());
  if (!cmp || args.size() != 2) {
    all_pushed = false;
    return;
  }

  // 识别 column 和 constant 参数
  BoundColumnMeta *col_arg = nullptr;
  BoundConstant *const_arg = nullptr;
  bool const_on_left = false;

  if (args[0]->expr_type_ == BoundExpressType::BoundColumnMeta &&
      args[1]->expr_type_ == BoundExpressType::BoundConstant) {
    col_arg = static_cast<BoundColumnMeta *>(args[0].get());
    const_arg = static_cast<BoundConstant *>(args[1].get());
  } else if (args[0]->expr_type_ == BoundExpressType::BoundConstant &&
             args[1]->expr_type_ == BoundExpressType::BoundColumnMeta) {
    col_arg = static_cast<BoundColumnMeta *>(args[1].get());
    const_arg = static_cast<BoundConstant *>(args[0].get());
    const_on_left = true;
  } else {
    all_pushed = false;
    return;
  }

  // 在 condition_columns_ 中查找该列的 column_idx
  auto col_name = col_arg->GetColumnMeta()->name_;
  size_t found_col_idx = SIZE_MAX;
  ValueType::Type col_type = ValueType::Type::Null;
  for (const auto &cs : columns) {
    if (cs.column_meta->name_ == col_name) {
      found_col_idx = cs.column_idx;
      col_type = cs.column_meta->type_->GetType();
      break;
    }
  }

  if (found_col_idx == SIZE_MAX) {
    all_pushed = false;
    return;
  }

  auto op = cmp->GetOperator();
  if (const_on_left) {
    op = FlipOperator(op);
  }

  ScanPredicate pred;
  pred.column_idx = found_col_idx;
  pred.column_type = col_type;
  pred.op = op;

  switch (const_arg->type_->GetType()) {
  case ValueType::Type::Int: pred.const_int = const_arg->value_.i32; break;
  case ValueType::Type::Double:
    pred.const_double = const_arg->value_.f64;
    break;
  case ValueType::Type::String:
    pred.const_string = std::string(const_arg->value_.str, const_arg->size_);
    break;
  default: all_pushed = false; return;
  }

  pushed.push_back(std::move(pred));
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

  // 1. 提取可下推谓词
  std::vector<ScanPredicate> pushed_predicates;
  bool all_pushed = true;
  ExtractPredicates(condition_, condition_columns_, pushed_predicates,
                    all_pushed);

  // 2. 带谓词下推的扫描路径
  if (!pushed_predicates.empty()) {
    // 按 LSMTree 指针分组
    std::unordered_map<LSMTree *, std::vector<std::pair<size_t, size_t>>>
        lsm_groups;
    for (size_t i = 0; i < condition_columns_.size(); i++) {
      auto &cs = condition_columns_[i];
      lsm_groups[cs.lsm_tree.get()].emplace_back(cs.column_idx, i);
    }

    for (auto &[lsm_ptr, col_pairs] : lsm_groups) {
      std::vector<size_t> column_indices;
      column_indices.reserve(col_pairs.size());
      for (auto &[col_idx, orig_idx] : col_pairs) {
        column_indices.push_back(col_idx);
      }

      auto &lsm = condition_columns_[col_pairs[0].second].lsm_tree;

      std::vector<ColumnPtr> results;
      bool all_filtered = false;
      auto s = lsm->ScanColumnsWithPredicates(column_indices, results,
                                              pushed_predicates, all_filtered);
      if (!s.ok()) {
        return s;
      }

      // 只有当谓词表达式全部下推且数据全部经过扫描层过滤时才跳过 EvalCondition
      if (!all_filtered) {
        all_pushed = false;
      }

      for (size_t i = 0; i < col_pairs.size(); i++) {
        auto &col_meta = condition_columns_[col_pairs[i].second].column_meta;
        condition_column_data_[col_meta->name_] = results[i];
      }
    }

    if (all_pushed) {
      // 所有谓词都已下推，数据已过滤，直接放入缓存
      for (auto &col_scan : condition_columns_) {
        auto &col_meta = col_scan.column_meta;
        auto it = condition_column_data_.find(col_meta->name_);
        if (it != condition_column_data_.end()) {
          FilteredDataCache::Set(col_meta->name_, it->second);
        }
      }
    } else {
      // 有残留谓词（OR 等），仍需 EvalCondition + mask 过滤
      auto eval_column = EvalCondition(condition_);
      if (!eval_column) {
        return Status::Error(ErrorCode::BindError,
                             "failed to evaluate WHERE condition");
      }
      auto &mask = static_cast<ColumnVector<int> &>(*eval_column);

      for (auto &col_scan : condition_columns_) {
        auto &col_meta = col_scan.column_meta;
        auto it = condition_column_data_.find(col_meta->name_);
        if (it != condition_column_data_.end()) {
          auto filtered = FilterColumnData(it->second, col_meta->type_, mask);
          FilteredDataCache::Set(col_meta->name_, filtered);
        }
      }
    }
  } else {
    // 无可下推谓词，走现有路径
    auto status = ScanConditionColumns();
    if (!status.ok()) {
      return status;
    }

    auto eval_column = EvalCondition(condition_);
    if (!eval_column) {
      return Status::Error(ErrorCode::BindError,
                           "failed to evaluate WHERE condition");
    }
    auto &mask = static_cast<ColumnVector<int> &>(*eval_column);

    for (auto &col_scan : condition_columns_) {
      auto &col_meta = col_scan.column_meta;
      auto col_name = col_meta->name_;
      auto it = condition_column_data_.find(col_name);
      if (it != condition_column_data_.end()) {
        auto filtered = FilterColumnData(it->second, col_meta->type_, mask);
        FilteredDataCache::Set(col_name, filtered);
      }
    }
  }

  // 激活缓存
  FilteredDataCache::Activate();

  // 执行子节点 (Projection)
  Status status;
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

  // 清理缓存
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
