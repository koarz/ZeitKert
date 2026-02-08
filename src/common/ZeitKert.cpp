#include "common/ZeitKert.hpp"
#include "common/EnumClass.hpp"
#include "common/Logger.hpp"
#include "common/Status.hpp"
#include "function/Abs.hpp"
#include "function/FunctionCast.hpp"
#include "function/FunctionCount.hpp"
#include "function/FunctionString.hpp"
#include "function/FunctionSum.hpp"
#include "parser/Checker.hpp"
#include "parser/statement/CreateStatement.hpp"
#include "parser/statement/DropStatement.hpp"
#include "parser/statement/FlushStatement.hpp"
#include "parser/statement/ShowStatement.hpp"
#include "parser/statement/UseStatement.hpp"
#include "storage/lsmtree/LSMTree.hpp"

#include <filesystem>
#include <memory>

namespace DB {

static void RegisterChecker() {
  static bool registered = false;
  if (registered) {
    return;
  }
  registered = true;

  Checker::RegisterKeyWord("CREATE");
  Checker::RegisterKeyWord("DROP");
  Checker::RegisterKeyWord("SHOW");
  Checker::RegisterKeyWord("DATABASE");
  Checker::RegisterKeyWord("DATABASES");
  Checker::RegisterKeyWord("USE");
  Checker::RegisterKeyWord("SELECT");
  Checker::RegisterKeyWord("TABLE");
  Checker::RegisterKeyWord("TABLES");
  Checker::RegisterKeyWord("INSERT");
  Checker::RegisterKeyWord("INTO");
  Checker::RegisterKeyWord("VALUES");
  Checker::RegisterKeyWord("BULK");
  Checker::RegisterKeyWord("FROM");
  Checker::RegisterKeyWord("WHERE");
  Checker::RegisterKeyWord("UNIQUE");
  Checker::RegisterKeyWord("KEY");
  Checker::RegisterKeyWord("FLUSH");

  Checker::RegisterType("INT");
  Checker::RegisterType("STRING");
  Checker::RegisterType("DOUBLE");

  Checker::RegisterFunction("ABS", std::make_shared<FunctionAbs>());
  Checker::RegisterFunction("CAST", std::make_shared<FunctionCast>());
  Checker::RegisterFunction("COUNT", std::make_shared<FunctionCount>());
  Checker::RegisterFunction("SUM", std::make_shared<FunctionSum>());
  Checker::RegisterFunction("TO_UPPER", std::make_shared<FunctionToUpper>());
  Checker::RegisterFunction("TO_LOWER", std::make_shared<FunctionToLower>());
}

ZeitKert::ZeitKert() : context_(std::make_shared<QueryContext>()) {
  Logger::Init("./logs/zeitkert.log");
  RegisterChecker();
}

ZeitKert::~ZeitKert() {
  Logger::Shutdown();
}

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
    context_->lsm_trees_.erase(name);
    status = context_->disk_manager_->DropTable(table_path);
    context_->database_->RemoveTable(name);
    LOG_INFO("DROP TABLE '{}'", name);
    break;
  case DropType::Database:
    // 如果删除的是当前使用的数据库，清空context
    if (context_->database_ != nullptr &&
        context_->database_->GetPath().filename() == name) {
      context_->database_ = nullptr;
    }
    status = context_->disk_manager_->DropDatabase(name);
    LOG_INFO("DROP DATABASE '{}'", name);
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
    LOG_INFO("CREATE TABLE '{}'", name);
    return s;
  } else {
    auto s = context_->disk_manager_->CreateDatabase(name);
    if (s.ok()) {
      LOG_INFO("CREATE DATABASE '{}'", name);
    }
    return s;
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
    LOG_INFO("USE DATABASE '{}'", name);
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
    LOG_INFO("SHOW DATABASES");
    break;
  case ShowType::Tables:
    if (context_->database_ == nullptr) {
      return Status::Error(ErrorCode::NotChoiceDatabase,
                           "You have not choice a database");
    }
    status = context_->database_->ShowTables(result_set);
    LOG_INFO("SHOW TABLES");
    break;
  }
  return status;
}

Status ZeitKert::HandleFlushStatement() {
  auto &flush_statement =
      static_cast<FlushStatement &>(*context_->sql_statement_);
  std::string table_name = flush_statement.GetTableName();

  if (context_->database_ == nullptr) {
    return Status::Error(ErrorCode::NotChoiceDatabase,
                         "You have not choice a database");
  }

  auto table_meta = context_->database_->GetTableMeta(table_name);
  if (table_meta == nullptr) {
    return Status::Error(ErrorCode::NotFound,
                         "Table " + table_name + " not found");
  }

  auto lsm_tree = context_->GetOrCreateLSMTree(table_meta);
  if (lsm_tree == nullptr) {
    return Status::Error(ErrorCode::IOError,
                         "Failed to get LSMTree for table " + table_name);
  }

  auto s = lsm_tree->FlushToSST();
  if (s.ok()) {
    LOG_INFO("FLUSH TABLE '{}'", table_name);
  }
  return s;
}
} // namespace DB
