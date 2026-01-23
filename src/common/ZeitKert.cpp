#include "common/ZeitKert.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/statement/CreateStatement.hpp"
#include "parser/statement/DropStatement.hpp"
#include "parser/statement/ShowStatement.hpp"
#include "parser/statement/UseStatement.hpp"

#include <filesystem>
#include <memory>

namespace DB {
Status ZeitKert::HandleDropStatement() {
  auto &drop_statement =
      static_cast<DropStatement &>(*context_->sql_statement_);
  auto name = drop_statement.GetName();

  std::filesystem::path table_path;

  Status status = Status::OK();
  switch (drop_statement.GetType()) {
  case DropType::Table:
    if (context_->database_ == nullptr) {
      return Status::Error(ErrorCode::NotChoiceDatabase,
                           "You have not choice a database");
    }
    table_path = context_->database_->GetPath() / name;
    status = context_->disk_manager_->DropTable(table_path);
    context_->database_->RemoveTable(name);
    break;
  case DropType::Database:
    status = context_->disk_manager_->DropDatabase(name);
    break;
  }
  return status;
}

Status ZeitKert::HandleCreateStatement() {
  auto &create_statement =
      static_cast<CreateStatement &>(*context_->sql_statement_);
  auto name = create_statement.GetName();
  if (create_statement.GetCreateType() == CreateType::Table) {
    if (context_->database_ == nullptr) {
      return Status::Error(ErrorCode::NotChoiceDatabase,
                           "You have not choice a database");
    }
    auto s = context_->database_->CreateTable(
        name, create_statement.GetColumns(), create_statement.GetUniqueKey());
    if (!s.ok()) {
      return s;
    }
    auto table_meta = context_->database_->GetTableMeta(name);

    for (auto &c : table_meta->GetColumns()) {
      auto col_path = context_->database_->GetPath() / name / c->name_;
      std::filesystem::create_directory(col_path);
      c->lsm_tree_ = std::make_shared<LSMTree>(
          col_path, 0, context_->buffer_pool_manager_, c->type_);
    }
    return s;
  } else {
    return context_->disk_manager_->CreateDatabase(name);
  }
}

Status ZeitKert::HandleUseStatement() {
  auto &use_statement = static_cast<UseStatement &>(*context_->sql_statement_);
  auto name = use_statement.GetName();
  auto disk_manager = context_->disk_manager_;
  auto status = disk_manager->OpenDatabase(name);
  if (status.ok()) {
    context_->database_ =
        std::make_shared<Database>(disk_manager->GetPath() / name, disk_manager,
                                   context_->buffer_pool_manager_);
  }
  return status;
}

Status ZeitKert::HandleShowStatement(ResultSet &result_set) {
  auto &show_statement =
      static_cast<ShowStatement &>(*context_->sql_statement_);
  auto show_type = show_statement.GetShowType();
  Status status;
  switch (show_type) {
  case ShowType::Databases:
    status = context_->disk_manager_->ShowDatabase(result_set);
    break;
  case ShowType::Tables:
    if (context_->database_ == nullptr) {
      return Status::Error(ErrorCode::NotChoiceDatabase,
                           "You have not choice a database");
    }
    status = context_->database_->ShowTables(result_set);
    break;
  }
  return status;
}
} // namespace DB