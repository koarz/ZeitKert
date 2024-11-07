#include "common/ZeitgeistDB.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/statement/CreateStmt.hpp"
#include "parser/statement/ShowStmt.hpp"
#include "parser/statement/UseStmt.hpp"
#include "storage/disk/DiskManager.hpp"

#include <memory>

namespace DB {
Status ZeitgeistDB::HandleCreateStmt() {
  auto &create_stmt = dynamic_cast<CreateStmt &>(*context_->sql_statement_);
  auto name = create_stmt.GetName();
  if (create_stmt.GetCreateType() == CreateType::TABLE) {
    if (context_->database_ == nullptr) {
      return Status::Error(ErrorCode::NotChoiceDatabase,
                           "You have not choice a database");
    }
    return context_->database_->CreateTable(name, create_stmt.GetColumns());
  } else {
    return context_->disk_manager_->CreateDatabase(name);
  }
}

Status ZeitgeistDB::HandleUseStmt() {
  auto &use_stmt = dynamic_cast<UseStmt &>(*context_->sql_statement_);
  auto name = use_stmt.GetName();
  auto disk_manager = context_->disk_manager_;
  auto status = disk_manager->OpenDatabase(name);
  if (status.ok()) {
    context_->database_ = std::make_shared<Database>(
        disk_manager->GetPath() / name, disk_manager);
  }
  return status;
}

Status ZeitgeistDB::HandleShowStmt(ResultSet &result_set) {
  auto &show_stmt = dynamic_cast<ShowStmt &>(*context_->sql_statement_);
  auto show_type = show_stmt.GetShowType();
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