#include "parser/ASTUseQuery.hpp"
#include "parser/Transform.hpp"

namespace DB {
std::shared_ptr<UseStatement>
Transform::TransUseQuery(ASTPtr node, std::string &message,
                         std::shared_ptr<QueryContext> context) {
  auto &use_query = dynamic_cast<UseQuery &>(*node);

  auto name = use_query.GetName();

  return std::make_shared<UseStatement>(name);
}
} // namespace DB