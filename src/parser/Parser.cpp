#include "parser/Parser.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/ASTCreateQuery.hpp"
#include "parser/ASTDropQuery.hpp"
#include "parser/ASTFlushQuery.hpp"
#include "parser/ASTInsertQuery.hpp"
#include "parser/ASTSelectQuery.hpp"
#include "parser/ASTShowQuery.hpp"
#include "parser/ASTSubquery.hpp"
#include "parser/ASTTableFunction.hpp"
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
      status = ParseDrop(iterator);
    } else if (str == "SELECT") {
      status = ParseSelect(iterator);
    } else if (str == "INSERT") {
      status = ParseInsert(iterator);
    } else if (str == "FLUSH") {
      status = ParseFlush(iterator);
    }
  } else {
    status = Status::Error(ErrorCode::SyntaxError,
                           "ZeitKert Just Support CREATE, USE, SHOW, DROP, "
                           "SELECT, INSERT, FLUSH Query");
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
          CreateType::Database, std::string{iterator->begin, iterator->end});
    } else if (str == "TABLE") {
      tree_ = std::make_shared<CreateQuery>(
          CreateType::Table, std::string{iterator->begin, iterator->end});
      if ((++iterator)->type != TokenType::OpeningRoundBracket) {
        return Status::Error(ErrorCode::SyntaxError,
                             "Your sql have syntax error");
      }
      std::optional<TokenIterator> begin{++iterator};
      while ((++iterator)->type != TokenType::ClosingRoundBracket)
        ;
      std::optional<TokenIterator> end{iterator};
      tree_->children_.emplace_back(std::make_shared<ASTToken>(begin, end));

      // 解析括号后的 UNIQUE KEY 子句
      auto next = iterator;
      ++next;
      if (!next->isEnd() && next->type == TokenType::BareWord) {
        std::string keyword{next->begin, next->end};
        if (Checker::IsKeyWord(keyword) && keyword == "UNIQUE") {
          auto key_token = next;
          ++key_token;
          if (key_token->type == TokenType::BareWord) {
            std::string key_word{key_token->begin, key_token->end};
            if (Checker::IsKeyWord(key_word) && key_word == "KEY") {
              // 跳到 (
              auto paren = key_token;
              ++paren;
              if (paren->type == TokenType::OpeningRoundBracket) {
                // 跳到列名
                auto col_name = paren;
                ++col_name;
                if (col_name->type == TokenType::BareWord) {
                  // 只保存列名到 children_[1]
                  std::optional<TokenIterator> uk_begin{col_name};
                  std::optional<TokenIterator> uk_end{col_name};
                  tree_->children_.emplace_back(
                      std::make_shared<ASTToken>(uk_begin, uk_end));
                }
              }
            }
          }
        }
      }
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

Status Parser::ParseSelect(TokenIterator &iterator, bool stop_at_paren) {
  tree_ = std::make_shared<SelectQuery>();
  std::optional<TokenIterator> begin{++iterator};
  bool have_from{};

  // 扫描 SELECT 列表，直到遇到 FROM 关键字
  // stop_at_paren=true 时，遇到未匹配的 ')' 也停止（子查询场景）
  int col_depth = 0;
  while (!(++iterator)->isEnd()) {
    if (iterator->type == TokenType::OpeningRoundBracket) {
      ++col_depth;
    } else if (iterator->type == TokenType::ClosingRoundBracket) {
      if (col_depth == 0 && stop_at_paren)
        break;
      --col_depth;
    }
    std::string s{iterator->begin, iterator->end};
    if (col_depth == 0 && Checker::IsKeyWord(s) && s == "FROM") {
      have_from = true;
      break;
    }
  }

  std::optional<TokenIterator> end{iterator};
  tree_->children_.emplace_back(std::make_shared<ASTToken>(begin, end));
  if (have_from) {
    std::vector<std::string> names;
    std::optional<TokenIterator> where_begin;
    std::optional<TokenIterator> where_end;
    bool have_where = false;

    while (!(++iterator)->isEnd()) {
      if (iterator->type == TokenType::Comma) {
        continue;
      }
      // 子查询内部：遇到未匹配的 ')' 时停止（该 ')' 是子查询的关闭括号）
      if (stop_at_paren && iterator->type == TokenType::ClosingRoundBracket) {
        break;
      }
      std::string s{iterator->begin, iterator->end};
      if (Checker::IsKeyWord(s) && s == "WHERE") {
        have_where = true;
        where_begin = ++iterator;
        // 收集 WHERE 后面的 token，在子查询内部要用深度计数避免把关闭括号误判为
        // WHERE 结束
        int where_depth = 0;
        while (!(++iterator)->isEnd()) {
          if (iterator->type == TokenType::OpeningRoundBracket) {
            ++where_depth;
          } else if (iterator->type == TokenType::ClosingRoundBracket) {
            if (where_depth == 0 && stop_at_paren)
              break;
            --where_depth;
          }
        }
        where_end = iterator;
        break;
      }
      // 检测子查询：'(' 后紧跟 SELECT 关键字 → 递归解析
      // 递归调用 ParseSelect(iterator, true)，遇到匹配的 ')' 时自动停止
      // 支持任意层嵌套：select * from (select * from (select 1)) 等
      if (iterator->type == TokenType::OpeningRoundBracket) {
        auto peek = iterator;
        ++peek;
        if (!peek->isEnd() && peek->type == TokenType::BareWord) {
          std::string kw{peek->begin, peek->end};
          StringUtil::ToUpper(kw);
          if (kw == "SELECT") {
            iterator = peek; // 移动到 SELECT token
            auto outer_tree = tree_;
            ParseSelect(iterator, true); // 递归解析子查询，遇到 ')' 停止
            auto inner_tree = tree_;
            tree_ = outer_tree;
            tree_->children_.emplace_back(
                std::make_shared<ASTSubquery>(std::move(inner_tree)));
            // 此时 iterator 停在 ')' 处，continue 后外层循环会 ++iterator
            // 跳过它
            continue;
          }
        }
        continue;
      }
      // detect table function: identifier(args...)
      auto peek = iterator;
      ++peek;
      if (!peek->isEnd() && peek->type == TokenType::OpeningRoundBracket) {
        std::string func_name{iterator->begin, iterator->end};
        iterator = peek; // skip past '('
        std::optional<TokenIterator> args_begin{++iterator};
        // skip to matching ')'
        while (!(iterator)->isEnd() &&
               iterator->type != TokenType::ClosingRoundBracket) {
          ++iterator;
        }
        std::optional<TokenIterator> args_end{iterator};
        tree_->children_.emplace_back(std::make_shared<ASTTableFunction>(
            std::move(func_name), args_begin, args_end));
        continue;
      }
      names.emplace_back(iterator->begin, iterator->end);
    }
    tree_->children_.emplace_back(
        std::make_shared<TableNames>(std::move(names)));

    // 添加 WHERE 子句的 token（如果存在）
    if (have_where && where_begin.has_value()) {
      tree_->children_.emplace_back(
          std::make_shared<ASTToken>(where_begin, where_end));
    }
  }
  return Status::OK();
}

Status Parser::ParseDrop(TokenIterator &iterator) {
  ++iterator;
  std::string sv{iterator->begin, iterator->end};

  if (Checker::IsKeyWord(sv)) {
    ++iterator;
    std::string name{iterator->begin, iterator->end};
    if (sv == "DATABASE") {
      tree_ = std::make_shared<DropQuery>(DropType::Database, std::move(name));
    } else if (sv == "TABLE") {
      tree_ = std::make_shared<DropQuery>(DropType::Table, std::move(name));
    } else {
      return Status::Error(ErrorCode::SyntaxError,
                           "Your query have syntax error");
    }
    return Status::OK();
  }
  return Status::Error(ErrorCode::SyntaxError, "Your query have syntax error");
}

Status Parser::ParseInsert(TokenIterator &iterator) {
  ++iterator;
  std::string s{iterator->begin, iterator->end};
  ++iterator;
  Status status = Status::OK();
  if (Checker::IsKeyWord(s)) {
    if (s == "INTO") {
      std::string name = std::string{iterator->begin, iterator->end};
      ++iterator;
      s = std::string{iterator->begin, iterator->end};
      if (Checker::IsKeyWord(s)) {
        if (s == "VALUES") {
          std::optional<TokenIterator> begin{++iterator};
          while (!(++iterator)->isEnd())
            ;
          std::optional<TokenIterator> end{iterator};
          // values tuple
          tree_ = std::make_shared<InsertQuery>(std::move(name), nullptr);
          tree_->children_.emplace_back(std::make_shared<ASTToken>(begin, end));
        } else if (s == "SELECT") {
          status = ParseSelect(iterator);
          if (!status.ok()) {
            return status;
          }
          tree_ =
              std::make_shared<InsertQuery>(std::move(name), std::move(tree_));
        } else if (s == "BULK") {
          ++iterator;
          if (iterator->type != TokenType::Number) {
            goto SYNTAXERROR;
          }
          std::string count_str{iterator->begin, iterator->end};
          if (!StringUtil::IsInteger(count_str)) {
            goto SYNTAXERROR;
          }
          size_t count = std::stoull(count_str);
          if (!(++iterator)->isEnd()) {
            goto SYNTAXERROR;
          }
          auto insert_query =
              std::make_shared<InsertQuery>(std::move(name), nullptr);
          insert_query->SetBulkRows(count);
          tree_ = std::move(insert_query);
        } else {
          goto SYNTAXERROR;
        }
        return Status::OK();
      }
    }
  }
SYNTAXERROR:
  return Status::Error(ErrorCode::SyntaxError, "your query have syntax error");
}

Status Parser::ParseFlush(TokenIterator &iterator) {
  // FLUSH <table_name>
  ++iterator;
  if (iterator->type != TokenType::BareWord) {
    return Status::Error(ErrorCode::SyntaxError,
                         "Expected table name after FLUSH");
  }
  std::string table_name{iterator->begin, iterator->end};
  if (!(++iterator)->isEnd()) {
    return Status::Error(ErrorCode::SyntaxError,
                         "Unexpected token after table name");
  }
  tree_ = std::make_shared<FlushQuery>(std::move(table_name));
  return Status::OK();
}

} // namespace DB
