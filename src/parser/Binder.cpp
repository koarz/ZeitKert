#include "parser/Binder.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/Transform.hpp"

#include <memory>
#include <string_view>

namespace DB {

Status Binder::Parse(std::string_view query,
                     std::shared_ptr<QueryContext> context,
                     ResultSet &result_set) {

  Tokens tokens(query.begin(), query.end(), 1000);
  TokenIterator iterator(tokens);

  auto status = parser_.Parse(iterator);
  if (!status.ok()) {
    return status;
  }
  std::shared_ptr<SQLStatement> statement;
  switch (parser_.tree_->GetNodeType()) {
  case ASTNodeType::CreateQuery:
    statement = Transform::TransCreateQuery(parser_.tree_);
    break;
  case ASTNodeType::UseQuery:
    statement = Transform::TransUseQuery(parser_.tree_);
    break;
  case ASTNodeType::ShowQuery:
    statement = Transform::TransShowQuery(parser_.tree_);
    break;
  default: break;
  }
  if (statement == nullptr) {
    return Status::Error(ErrorCode::BindError, "Parse tree is invalid");
  }
  statements_.push_back(statement);
  return Status::OK();
}
} // namespace DB