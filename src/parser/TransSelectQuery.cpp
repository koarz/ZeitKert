#include "parser/AST.hpp"
#include "parser/ASTSelectQuery.hpp"
#include "parser/ASTToken.hpp"
#include "parser/Transform.hpp"
#include "parser/statement/SelectStatement.hpp"

namespace DB {
std::shared_ptr<SelectStatement>
Transform::TransSelectQuery(ASTPtr node, std::string &message,
                            std::shared_ptr<QueryContext> context) {
  auto &select_query = dynamic_cast<SelectQuery &>(*node);

  auto &node_query = dynamic_cast<ASTToken &>(*select_query.children_[0]);
  std::vector<BoundExpressRef> columns;
  auto it = node_query.Begin();
  auto res = std::make_shared<SelectStatement>();

  // that's one query output column end of 'FROM'
  // we need parse constant or function or colname or table.col
  while (it != node_query.End()) {
    auto col = GetColumnExpress(it, node_query.End(), message);
    if (!message.empty()) {
      return nullptr;
    }
    columns.push_back(col);
    ++it;
  }

  res->columns_ = std::move(columns);
  return res;
}
} // namespace DB