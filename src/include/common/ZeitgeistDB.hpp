#pragma once

#include "common/Context.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "parser/Binder.hpp"

#include <memory>
#include <string>

namespace DB {

class ZeitgeistDB {
  std::shared_ptr<QueryContext> context_;

public:
  ZeitgeistDB() : context_(std::make_shared<QueryContext>()) {}

  Status ExecuteQuery(std::string &query, ResultSet &result_set) {
    query.pop_back();

    Binder binder;
    auto status = binder.Parse(query, context_, result_set);
    if (!status.ok()) {
      return status;
    }
    if (context_->sql_statement_ != nullptr) {
      context_->sql_statement_ = nullptr;
    }
    return status;
  }
};
} // namespace DB