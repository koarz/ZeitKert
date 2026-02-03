#include "parser/ASTFlushQuery.hpp"
#include "parser/Transform.hpp"

namespace DB {
std::shared_ptr<FlushStatement>
Transform::TransFlushQuery(ASTPtr node, std::string &message,
                           std::shared_ptr<QueryContext> context) {
  auto &flush_query = static_cast<FlushQuery &>(*node);

  return std::make_shared<FlushStatement>(flush_query.GetTableName());
}
} // namespace DB
