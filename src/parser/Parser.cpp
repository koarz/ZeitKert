#include "parser/Parser.hpp"
#include "common/DatabaseInstance.hpp"
#include "common/Status.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/Lexer.hpp"
#include "parser/SQLStatement.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"

#include <memory>
#include <string>
#include <string_view>

namespace DB {

Parser::Parser() {
  checker_.RegisterKeyWord("CREATE");
  checker_.RegisterKeyWord("DROP");
  checker_.RegisterKeyWord("SHOW");
  checker_.RegisterKeyWord("DATABASE");
  checker_.RegisterKeyWord("DATABASES");
  checker_.RegisterKeyWord("USE");
  checker_.RegisterKeyWord("SELECT");
  checker_.RegisterKeyWord("TABLE");
  checker_.RegisterKeyWord("INT");
}

Status Parser::Parse(std::string_view query,
                     std::shared_ptr<QueryContext> context,
                     ResultSet &result_set) {
  Lexer lexer(query.begin(), query.end(), 1000);
  auto status = Status::OK();
  auto next_token = lexer.nextToken();
  std::string sv{next_token.begin, next_token.end};

  // Some simple tasks we do directly in Parser
  if (checker_.IsKeyWord(sv)) {
    StringUtil::ToUpper(sv);
    if (sv == "CREATE") {
      status = ParseCreate(lexer, context, result_set);
    }
    if (sv == "DROP") {
      status = ParseDrop(lexer, context, result_set);
    }
    if (sv == "SHOW") {
      status = ParseShow(lexer, context, result_set);
    }
    if (sv == "USE") {
      status = ParseUse(lexer, context, result_set);
    }
  }

  return status;
}

Status Parser::ParseCreate(Lexer &lexer, std::shared_ptr<QueryContext> context,
                           ResultSet &result_set) {
  auto token = lexer.nextToken();
  auto status = Status::OK();
  while (token.type == TokenType::Whitespace) {
    token = lexer.nextToken();
  }
  std::string sv{token.begin, token.end};

  if (checker_.IsKeyWord(sv)) {
    lexer.nextToken();
    if (sv == "DATABASE") {
      token = lexer.nextToken();
      std::string name{token.begin, token.end};
      if (StringUtil::IsAlpha(name)) {
        auto disk_manager = context->disk_manager_;
        status = disk_manager->CreateDatabase(name);
      } else {
        return Status::Error(
            ErrorCode::CreateError,
            "Please use English letters for the database name.");
      }
    }
    if (sv == "TABLE") {
      if (context->database_ == nullptr) {
        return Status::Error(ErrorCode::CreateError,
                             "You have not choose database");
      }
      status = CreateTable(lexer, context);
    }
  }
  return status;
}

Status Parser::CreateTable(Lexer &lexer,
                           std::shared_ptr<QueryContext> context) {
  Token token = lexer.nextToken();
  std::string table_name{token.begin, token.end};
  std::vector<std::shared_ptr<ColumnWithNameType>> columns;
  token = lexer.nextToken();
  if (token.type != TokenType::OpeningRoundBracket) {
    return Status::Error(ErrorCode::SyntaxError, "Your sql have syntax error");
  }
  token = lexer.nextToken();
  do {
    while (token.type == TokenType::Whitespace) {
      token = lexer.nextToken();
    }
    std::string col_name;
    if (token.type == TokenType::BareWord) {
      col_name = std::string{token.begin, token.end};
    }
    token = lexer.nextToken();
    if (token.type != TokenType::Whitespace) {
      return Status::Error(ErrorCode::SyntaxError,
                           "Your sql have syntax error");
    }
    // type
    std::string type;
    token = lexer.nextToken();
    if (token.type == TokenType::BareWord) {
      type = std::string{token.begin, token.end};
      if (!checker_.IsKeyWord(type)) {
        return Status::Error(ErrorCode::SyntaxError,
                             "Your sql have syntax error");
      }
    } else {
      return Status::Error(ErrorCode::SyntaxError,
                           "Your sql have syntax error");
    }
    /*
     *  other information
     */

    ColumnPtr data;
    std::shared_ptr<ValueType> vtype;
    if (type == "INT") {
      data = std::make_shared<ColumnVector<int>>();
      vtype = std::make_shared<Int>();
    }
    auto column = std::make_shared<ColumnWithNameType>(data, col_name, vtype);
    columns.push_back(column);
    token = lexer.nextToken();
    if (token.type == TokenType::ClosingRoundBracket) {
      break;
    }
  } while (true);
  return context->database_->CreateTable(table_name, columns);
}

Status Parser::ParseUse(Lexer &lexer, std::shared_ptr<QueryContext> context,
                        ResultSet &result_set) {
  auto token = lexer.nextToken();
  while (token.type == TokenType::Whitespace) {
    token = lexer.nextToken();
  }
  std::string name{token.begin, token.end};
  if (StringUtil::IsAlpha(name)) {
    auto disk_manager = context->disk_manager_;
    auto status = disk_manager->OpenDatabase(name);
    if (status.ok()) {
      context->database_ = std::make_shared<Database>(
          disk_manager->GetPath() / name, disk_manager);
    }
    return status;
  }
  return Status::Error(ErrorCode::DatabaseNotExists,
                       std::format("The name {} is not correctly.", name));
}

Status Parser::ParseDrop(Lexer &lexer, std::shared_ptr<QueryContext> context,
                         ResultSet &result_set) {
  auto token = lexer.nextToken();
  while (token.type == TokenType::Whitespace) {
    token = lexer.nextToken();
  }
  auto status = Status::OK();
  std::string sv{token.begin, token.end};

  if (checker_.IsKeyWord(sv)) {
    if (sv == "DATABASE") {
      lexer.nextToken();
      token = lexer.nextToken();
      std::string name{token.begin, token.end};
      if (StringUtil::IsAlpha(name)) {
        auto disk_manager = context->disk_manager_;
        status = disk_manager->DropDatabase(name);
      }
    }
  }
  return status;
}

Status Parser::ParseShow(Lexer &lexer, std::shared_ptr<QueryContext> context,
                         ResultSet &result_set) {
  auto token = lexer.nextToken();
  while (token.type == TokenType::Whitespace) {
    token = lexer.nextToken();
  }
  auto status = Status::OK();
  std::string sv{token.begin, token.end};

  if (checker_.IsKeyWord(sv)) {
    if (sv == "DATABASES") {
      auto disk_manager = context->disk_manager_;
      status = disk_manager->ShowDatabase();
    }
  }
  return status;
}
} // namespace DB