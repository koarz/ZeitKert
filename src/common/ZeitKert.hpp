#pragma once

#include "common/Context.hpp"
#include "common/EnumClass.hpp"
#include "common/Logger.hpp"
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

  Status HandleDropStatement();

  Status HandleCreateStatement();

  Status HandleUseStatement();

  Status HandleShowStatement(ResultSet &result_set);

  Status HandleFlushStatement();

public:
  ZeitKert();
  ~ZeitKert();

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
    case StatementType::CreateStatement:
      LOG_INFO("Execute: CREATE statement");
      status = HandleCreateStatement();
      goto ExecuteEnd;
    case StatementType::UseStatement:
      LOG_INFO("Execute: USE statement");
      status = HandleUseStatement();
      goto ExecuteEnd;
    case StatementType::ShowStatement:
      LOG_INFO("Execute: SHOW statement");
      status = HandleShowStatement(result_set);
      goto ExecuteEnd;
    case StatementType::DropStatement:
      LOG_INFO("Execute: DROP statement");
      status = HandleDropStatement();
      goto ExecuteEnd;
    case StatementType::FlushStatement:
      LOG_INFO("Execute: FLUSH statement");
      status = HandleFlushStatement();
      goto ExecuteEnd;
    case StatementType::InvalidStatement:
    case StatementType::SelectStatement:
      LOG_INFO("Execute: SELECT statement");
      break;
    case StatementType::InsertStatement:
      LOG_INFO("Execute: INSERT statement");
      break;
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
