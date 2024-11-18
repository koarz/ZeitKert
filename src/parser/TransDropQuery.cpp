#include "parser/ASTDropQuery.hpp"
#include "parser/Transform.hpp"
#include "parser/statement/DropStatement.hpp"

#include <memory>

namespace DB {
std::shared_ptr<DropStatement>
Transform::TransDropQuery(ASTPtr node, std::string &message,
                          std::shared_ptr<QueryContext> context) {
  auto &drop_query = static_cast<DropQuery &>(*node);
  return std::make_shared<DropStatement>(drop_query.GetType(),
                                         drop_query.GetName());
}
} // namespace DB