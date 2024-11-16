#include "common/ZeitgeistDB.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/statement/CreateStatement.hpp"
#include "parser/statement/ShowStatement.hpp"
#include "parser/statement/UseStatement.hpp"

#include <memory>

namespace DB {
Status ZeitgeistDB::HandleCreateStatement() {
  auto &create_Statement =
      static_cast<CreateStatement &>(*context_->sql_statement_);
  auto name = create_Statement.GetName();
  if (create_Statement.GetCreateType() == CreateType::TABLE) {
    if (context_->database_ == nullptr) {
      return Status::Error(ErrorCode::NotChoiceDatabase,
                           "You have not choice a database");
    }
    return context_->database_->CreateTable(name,
                                            create_Statement.GetColumns());
  } else {
    return context_->disk_manager_->CreateDatabase(name);
  }
}

Status ZeitgeistDB::HandleUseStatement() {
  auto &use_Statement = static_cast<UseStatement &>(*context_->sql_statement_);
  auto name = use_Statement.GetName();
  auto disk_manager = context_->disk_manager_;
  auto status = disk_manager->OpenDatabase(name);
  if (status.ok()) {
    context_->database_ =
        std::make_shared<Database>(disk_manager->GetPath() / name, disk_manager,
                                   context_->buffer_pool_manager_);
  }
  return status;
}

Status ZeitgeistDB::HandleShowStatement(ResultSet &result_set) {
  auto &show_Statement =
      static_cast<ShowStatement &>(*context_->sql_statement_);
  auto show_type = show_Statement.GetShowType();
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