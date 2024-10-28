#pragma once

#include "common/Context.hpp"
#include "common/EnumClass.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "parser/Binder.hpp"

#include <memory>
#include <string>

namespace DB {

class ZeitgeistDB {
  std::shared_ptr<QueryContext> context_;

  Status HandleCreateStmt();

  Status HandleUseStmt();

  Status HandleShowStmt();

public:
  ZeitgeistDB() : context_(std::make_shared<QueryContext>()) {}

  Status ExecuteQuery(std::string &query, ResultSet &result_set) {
    query.pop_back();

    Binder binder;
    auto status = binder.Parse(query, context_, result_set);
    if (!status.ok()) {
      return status;
    }
    for (auto &stmt : binder.GetStatements()) {
      context_->sql_statement_ = stmt;
      switch (stmt->type) {
      case StatementType::CREATE_STATEMENT:
        status = HandleCreateStmt();
        if (!status.ok()) {
          goto ExecuteEnd;
        }
        continue;
      case StatementType::USE_STATEMENT:
        status = HandleUseStmt();
        if (!status.ok()) {
          goto ExecuteEnd;
        }
        continue;
      case StatementType::SHOW_STATEMENT:
        status = HandleShowStmt();
        if (!status.ok()) {
          goto ExecuteEnd;
        }
        continue;
      case StatementType::INVALID_STATEMENT:
      case StatementType::SELECT_STATEMENT: break;
      }
    }

  ExecuteEnd:
    context_->sql_statement_ = nullptr;

    return status;
  }
};
} // namespace DB