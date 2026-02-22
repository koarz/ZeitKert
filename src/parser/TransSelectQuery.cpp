#include "parser/AST.hpp"
#include "parser/ASTSelectQuery.hpp"
#include "parser/ASTSubquery.hpp"
#include "parser/ASTTableFunction.hpp"
#include "parser/ASTTableNames.hpp"
#include "parser/ASTToken.hpp"
#include "parser/Transform.hpp"
#include "parser/statement/SelectStatement.hpp"

#include "catalog/meta/ColumnMeta.hpp"
#include "common/util/StringUtil.hpp"
#include "type/Int.hpp"

namespace DB {
std::shared_ptr<SelectStatement>
Transform::TransSelectQuery(ASTPtr node, std::string &message,
                            std::shared_ptr<QueryContext> context) {
  auto &select_query = static_cast<SelectQuery &>(*node);
  auto res = std::make_shared<SelectStatement>();

  // 检查 FROM 子句中是否有子查询节点
  // 如果有，递归 transform 内层查询，直接透传（passthrough）
  // 支持任意嵌套深度：select * from (select * from (...))
  for (auto &child : select_query.children_) {
    if (child->GetNodeType() == ASTNodeType::Subquery) {
      auto &subq = static_cast<ASTSubquery &>(*child);
      res->subquery_ = TransSelectQuery(subq.inner_select_, message, context);
      if (!res->subquery_) {
        return nullptr;
      }
      return res;
    }
  }

  // Check for table function in children (e.g., range(1, 100))
  for (auto &child : select_query.children_) {
    if (child->GetNodeType() == ASTNodeType::TableFunction) {
      auto &table_func = static_cast<ASTTableFunction &>(*child);
      std::string func_name = table_func.func_name_;
      StringUtil::ToUpper(func_name);
      if (func_name == "RANGE") {
        // Parse range arguments: range(start, stop[, step])
        auto it = table_func.args_begin_.value();
        auto end = table_func.args_end_.value();

        std::vector<int64_t> args;
        bool negative = false;
        while (it < end) {
          if (it->type == TokenType::Comma) {
            ++it;
            continue;
          }
          if (it->type == TokenType::Minus) {
            negative = true;
            ++it;
            continue;
          }
          if (it->type == TokenType::Number) {
            std::string num_str{it->begin, it->end};
            int64_t val = std::stoll(num_str);
            if (negative) {
              val = -val;
              negative = false;
            }
            args.push_back(val);
          }
          ++it;
        }

        if (args.size() < 2 || args.size() > 3) {
          message = "range() requires 2 or 3 arguments: range(start, stop[, "
                    "step])";
          return nullptr;
        }

        RangeInfo info;
        info.start = args[0];
        info.stop = args[1];
        info.step = args.size() == 3 ? args[2] : 1;

        if (info.step == 0) {
          message = "range() step cannot be zero";
          return nullptr;
        }

        // Create virtual table with single "range" column of type Int
        std::vector<ColumnMetaRef> cols;
        cols.push_back(
            std::make_shared<ColumnMeta>("range", std::make_shared<Int>(), 0));
        auto range_table =
            std::make_shared<TableMeta>("", "range", std::move(cols));
        res->from_.push_back(range_table);
        res->range_info_ = info;
        res->range_table_ = range_table;
      } else {
        message = "unknown table function: " + table_func.func_name_;
        return nullptr;
      }
    }
  }

  // handle from table
  if (select_query.children_.size() > 1) {
    auto &child1 = select_query.children_[1];
    if (child1->GetNodeType() == ASTNodeType::TableNames) {
      auto &tables = static_cast<TableNames &>(*child1);
      for (auto &s : tables.names_) {
        if (context->database_ == nullptr) {
          message = "no database selected, please use 'USE <database>' first";
          return nullptr;
        }
        auto table_meta = context->database_->GetTableMeta(s);
        if (table_meta == nullptr) {
          message = "the table not exist, please check table name";
          return nullptr;
        }
        res->from_.push_back(table_meta);
      }
    }
  }

  auto &node_query = static_cast<ASTToken &>(*select_query.children_[0]);
  std::vector<BoundExpressRef> columns;
  auto it = node_query.Begin();

  // that's one query output column end of 'FROM'
  // we need parse constant or function or colname or table.col
  while (it < node_query.End()) {
    auto col =
        GetColumnExpress(it, node_query.End(), res->from_, columns, message);
    if (!message.empty()) {
      return nullptr;
    }
    if (col) {
      columns.push_back(col);
    }
    ++it;
  }

  res->columns_ = std::move(columns);

  // 处理 WHERE 子句
  // Find WHERE token: it may be at index 2 or 3 depending on whether
  // a TableFunction child was inserted
  for (size_t i = 2; i < select_query.children_.size(); i++) {
    if (select_query.children_[i]->GetNodeType() == ASTNodeType::Token) {
      auto &where_token = static_cast<ASTToken &>(*select_query.children_[i]);
      auto where_it = where_token.Begin();
      std::vector<BoundExpressRef> dummy_columns;
      auto where_expr = ParseExpression(where_it, where_token.End(), res->from_,
                                        dummy_columns, message);
      if (!message.empty()) {
        return nullptr;
      }
      res->where_condition_ = where_expr;
      break;
    }
  }

  return res;
}
} // namespace DB