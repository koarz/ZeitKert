#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "parser/SQLStatement.hpp"

namespace DB {
class CreateStatement : public SQLStatement {

public:
  static constexpr const StatementType TYPE = StatementType::CREATE_STATEMENT;

  explicit CreateStatement(CreateType &type, std::string &name,
                           std::vector<std::shared_ptr<ColumnMeta>> &columns)
      : SQLStatement(StatementType::CREATE_STATEMENT), type_(type),
        name_(std::move(name)), columns_(std::move(columns)) {}

  ~CreateStatement() override = default;

  CreateType GetCreateType() { return type_; }

  std::string GetName() { return name_; }

  std::vector<std::shared_ptr<ColumnMeta>> &GetColumns() { return columns_; }

private:
  CreateType type_;
  // table name or database name
  std::string name_;

  std::vector<std::shared_ptr<ColumnMeta>> columns_;
};
} // namespace DB