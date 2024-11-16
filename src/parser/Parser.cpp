#include "parser/Parser.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/ASTCreateQuery.hpp"
#include "parser/ASTInsertQuery.hpp"
#include "parser/ASTSelectQuery.hpp"
#include "parser/ASTShowQuery.hpp"
#include "parser/ASTTableNames.hpp"
#include "parser/ASTToken.hpp"
#include "parser/ASTUseQuery.hpp"
#include "parser/Checker.hpp"
#include "parser/Lexer.hpp"
#include "parser/TokenIterator.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB {

Status Parser::Parse(TokenIterator &iterator) {
  auto token = *iterator;
  if (token.type != TokenType::BareWord) {
    return Status::Error(ErrorCode::SyntaxError,
                         "Please check your query first token");
  }
  std::string str{token.begin, token.end};
  auto status = Status::OK();

  if (Checker::IsKeyWord(str)) {
    if (str == "CREATE") {
      status = ParseCreate(iterator);
    } else if (str == "USE") {
      status = ParseUse(iterator);
    } else if (str == "SHOW") {
      status = ParseShow(iterator);
    } else if (str == "DROP") {
    } else if (str == "SELECT") {
      status = ParseSelect(iterator);
    } else if (str == "INSERT") {
      status = ParseInsert(iterator);
    }
  } else {
    status = Status::Error(
        ErrorCode::SyntaxError,
        "ZeitKert Just Support CREATE, USE, SHOW, DROP, SELECT Query");
  }
  return status;
}

Status Parser::ParseCreate(TokenIterator &iterator) {
  auto status = Status::OK();
  while ((++iterator)->type != TokenType::BareWord)
    ;
  std::string str{iterator->begin, iterator->end};

  // table name or database name
  while ((++iterator)->type != TokenType::BareWord)
    ;
  if (Checker::IsKeyWord(str)) {
    if (str == "DATABASE") {
      tree_ = std::make_shared<CreateQuery>(
          CreateType::DATABASE, std::string{iterator->begin, iterator->end});
    } else if (str == "TABLE") {
      tree_ = std::make_shared<CreateQuery>(
          CreateType::TABLE, std::string{iterator->begin, iterator->end});
      if ((++iterator)->type != TokenType::OpeningRoundBracket) {
        return Status::Error(ErrorCode::SyntaxError,
                             "Your sql have syntax error");
      }
      std::optional<TokenIterator> begin{++iterator};
      while ((++iterator)->type != TokenType::ClosingRoundBracket)
        ;
      std::optional<TokenIterator> end{iterator};
      tree_->children_.emplace_back(std::make_shared<ASTToken>(begin, end));
    }
  }
  return Status::OK();
}

Status Parser::ParseUse(TokenIterator &iterator) {
  while ((++iterator)->type != TokenType::BareWord)
    ;
  std::string name{iterator->begin, iterator->end};
  if (!(++iterator)->isEnd()) {
    return Status::Error(ErrorCode::SyntaxError,
                         "Please check your database name is correct?");
  }
  tree_ = std::make_shared<UseQuery>(name);
  return Status::OK();
}

Status Parser::ParseShow(TokenIterator &iterator) {
  while ((++iterator)->type != TokenType::BareWord)
    ;
  std::string type{iterator->begin, iterator->end};
  if (!(++iterator)->isEnd()) {
    goto SYNTAXERROR;
  }
  if (Checker::IsKeyWord(type)) {
    if (type == "DATABASES") {
      tree_ = std::make_shared<ShowQuery>(ShowType::Databases);
    } else if (type == "TABLES") {
      tree_ = std::make_shared<ShowQuery>(ShowType::Tables);
    } else {
      goto SYNTAXERROR;
    }
  } else {
    goto SYNTAXERROR;
  }
  return Status::OK();

SYNTAXERROR:
  return Status::Error(ErrorCode::SyntaxError,
                       "Your SQL Query have syntax error");
}

Status Parser::ParseSelect(TokenIterator &iterator) {
  tree_ = std::make_shared<SelectQuery>();
  std::optional<TokenIterator> begin{++iterator};
  bool have_from{};
  while (!(++iterator)->isEnd()) {
    std::string s{iterator->begin, iterator->end};
    if (Checker::IsKeyWord(s)) {
      if (s == "FROM") {
        have_from = true;
        break;
      }
    }
  }
  std::optional<TokenIterator> end{iterator};
  tree_->children_.emplace_back(std::make_shared<ASTToken>(begin, end));
  if (have_from) {
    std::vector<std::string> names;
    while (!(++iterator)->isEnd()) {
      if (iterator->type == TokenType::Comma) {
        continue;
      }
      names.emplace_back(iterator->begin, iterator->end);
    }
    tree_->children_.emplace_back(
        std::make_shared<TableNames>(std::move(names)));
  }
  return Status::OK();
}

Status Parser::ParseDrop(Lexer &lexer, std::shared_ptr<QueryContext> context,
                         ResultSet &result_set) {
  auto token = lexer.nextToken();
  while (token.type == TokenType::Whitespace) {
    token = lexer.nextToken();
  }
  auto status = Status::OK();
  std::string sv{token.begin, token.end};

  if (Checker::IsKeyWord(sv)) {
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

Status Parser::ParseInsert(TokenIterator &iterator) {
  ++iterator;
  std::string s{iterator->begin, iterator->end};
  ++iterator;

  if (Checker::IsKeyWord(s)) {
    if (s == "INTO") {
      s = std::string{iterator->begin, iterator->end};
      tree_ = std::make_shared<InsertQuery>(std::move(s));
      ++iterator;
      s = std::string{iterator->begin, iterator->end};
      if (Checker::IsKeyWord(s)) {
        if (s == "VALUES") {
          std::optional<TokenIterator> begin{++iterator};
          while (!(++iterator)->isEnd())
            ;
          std::optional<TokenIterator> end{iterator};
          // values tuple
          tree_->children_.emplace_back(std::make_shared<ASTToken>(begin, end));
        } else if (s == "SELECT") {
        }
      }
      return Status::OK();
    }
  }

  return Status::Error(ErrorCode::SyntaxError,
                       "INSERT back should INTO keyword");
}

} // namespace DB