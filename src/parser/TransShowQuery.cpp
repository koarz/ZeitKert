#include "parser/ASTShowQuery.hpp"
#include "parser/Transform.hpp"

namespace DB {
std::shared_ptr<ShowStatement>
Transform::TransShowQuery(ASTPtr node, std::string &message,
                          std::shared_ptr<QueryContext> context) {
  auto &show_query = static_cast<ShowQuery &>(*node);

  return std::make_shared<ShowStatement>(show_query.GetShowType());
}
} // namespace DB