#pragma once

#include "common/Context.hpp"
#include "common/EnumClass.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "execution/ExecutionEngine.hpp"
#include "parser/Binder.hpp"
#include "planner/Planner.hpp"

#include <memory>
#include <string>

namespace DB {

class ZeitKert {
  std::shared_ptr<QueryContext> context_;

  Status HandleCreateStatement();

  Status HandleUseStatement();

  Status HandleShowStatement(ResultSet &result_set);

public:
  ZeitKert() : context_(std::make_shared<QueryContext>()) {}

  Status ExecuteQuery(std::string &query, ResultSet &result_set) {
    query.pop_back();

    Binder binder;
    Planner planner(context_);
    ExecutionEngine executor;

    auto status = binder.Parse(query, context_, result_set);
    if (!status.ok()) {
      return status;
    }

    auto stmt = binder.GetStatement();
    context_->sql_statement_ = stmt;
    switch (stmt->type) {
    case StatementType::CREATE_STATEMENT:
      status = HandleCreateStatement();
      goto ExecuteEnd;
    case StatementType::USE_STATEMENT:
      status = HandleUseStatement();
      goto ExecuteEnd;
    case StatementType::SHOW_STATEMENT:
      status = HandleShowStatement(result_set);
      goto ExecuteEnd;
    case StatementType::INVALID_STATEMENT:
    case StatementType::SELECT_STATEMENT:
    case StatementType::INSERT_STATEMENT: break;
    }

    status = planner.QueryPlan();
    if (!status.ok()) {
      goto ExecuteEnd;
    }
    status = executor.Execute(planner.GetPlan());
    if (!status.ok()) {
      goto ExecuteEnd;
    }
    result_set.schema_ = planner.GetPlan()->GetSchemaRef();
  ExecuteEnd:
    context_->sql_statement_ = nullptr;

    return status;
  }
};
} // namespace DB