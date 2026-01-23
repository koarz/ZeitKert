#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "parser/SQLStatement.hpp"

namespace DB {
class CreateStatement : public SQLStatement {

public:
  static constexpr const StatementType TYPE = StatementType::CreateStatement;

  explicit CreateStatement(CreateType &type, std::string &name,
                           std::vector<std::shared_ptr<ColumnMeta>> &columns,
                           std::string unique_key = "")
      : SQLStatement(StatementType::CreateStatement), type_(type),
        name_(std::move(name)), columns_(std::move(columns)),
        unique_key_(std::move(unique_key)) {}

  ~CreateStatement() override = default;

  CreateType GetCreateType() { return type_; }

  std::string GetName() { return name_; }

  std::vector<std::shared_ptr<ColumnMeta>> &GetColumns() { return columns_; }

  std::string GetUniqueKey() { return unique_key_; }

private:
  CreateType type_;
  // table name or database name
  std::string name_;

  std::vector<std::shared_ptr<ColumnMeta>> columns_;
  std::string unique_key_;
};
} // namespace DB