#pragma once

#include "parser/AST.hpp"
#include "parser/statement/CreateStmt.hpp"
#include "parser/statement/ShowStmt.hpp"
#include "parser/statement/UseStmt.hpp"

#include <memory>
namespace DB {
struct Transform {
  static std::shared_ptr<CreateStmt> TransCreateQuery(ASTPtr node);

  static std::shared_ptr<UseStmt> TransUseQuery(ASTPtr node);

  static std::shared_ptr<ShowStmt> TransShowQuery(ASTPtr node);
};
} // namespace DB