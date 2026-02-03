#include "parser/Binder.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/TokenIterator.hpp"
#include "parser/Transform.hpp"

#include <memory>
#include <string_view>

namespace DB {

Status Binder::Parse(std::string_view query,
                     std::shared_ptr<QueryContext> context,
                     ResultSet &result_set) {
  if (query.size() > 64 * 1024 * 1024) {
    return Status::Error(ErrorCode::SyntaxError,
                         "The query sql size tool big(greater than 64MB)");
  }
  Tokens tokens(query.begin(), query.end(), 64 * 1024 * 1024);
  TokenIterator iterator(tokens);

  auto status = parser_.Parse(iterator);
  if (!status.ok()) {
    return status;
  }
  std::string message;
  switch (parser_.tree_->GetNodeType()) {
  case ASTNodeType::CreateQuery:
    statement_ = Transform::TransCreateQuery(parser_.tree_, message, context);
    break;
  case ASTNodeType::UseQuery:
    statement_ = Transform::TransUseQuery(parser_.tree_, message, context);
    break;
  case ASTNodeType::ShowQuery:
    statement_ = Transform::TransShowQuery(parser_.tree_, message, context);
    break;
  case ASTNodeType::SelectQuery:
    statement_ = Transform::TransSelectQuery(parser_.tree_, message, context);
    break;
  case ASTNodeType::InsertQuery:
    statement_ = Transform::TransInsertQuery(parser_.tree_, message, context);
    break;
  case ASTNodeType::DropQuery:
    statement_ = Transform::TransDropQuery(parser_.tree_, message, context);
    break;
  case ASTNodeType::FlushQuery:
    statement_ = Transform::TransFlushQuery(parser_.tree_, message, context);
    break;
  default:
  }
  if (statement_ == nullptr) {
    return Status::Error(ErrorCode::BindError,
                         "Binder suffer error: " + message);
  }
  return Status::OK();
}
} // namespace DB