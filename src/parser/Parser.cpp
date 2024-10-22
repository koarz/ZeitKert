#include "parser/Parser.hpp"
#include "common/Status.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/SQLStatement.hpp"
#include <memory>
#include <string_view>

namespace DB {

Parser::Parser() {
  checker_.RegisterKeyWord("CREATE");
  checker_.RegisterKeyWord("DATABASE");
}

Status Parser::Parse(std::string_view query,
                     std::shared_ptr<QueryContext> context,
                     ResultSet &result_set) {
  int start{}, end{};
  std::string query_clone{query};
  query_clone.pop_back();
  StringUtil::ToUpper(query_clone);
  end = query_clone.find(' ');
  auto sv = std::string_view{query_clone.begin(), query_clone.begin() + end};

  std::shared_ptr<SQLStatement> stmt;

  auto status = Status::OK();
  if (checker_.IsKeyWord(sv)) {
    if (sv == "CREATE") {
      status = ParseCreate(query_clone.substr(end + 1), context, result_set);
    }
  }

  return status;
}

Status Parser::ParseCreate(std::string &&query,
                           std::shared_ptr<QueryContext> context,
                           ResultSet &result_set) {
  auto end = query.find(' ');
  auto sv = std::string_view{query.begin(), query.begin() + end};

  auto status = Status::OK();
  if (checker_.IsKeyWord(sv)) {
    if (sv == "DATABASE") {
      auto name = query.substr(end + 1);
      if (StringUtil::IsAlpha(name)) {
        auto disk_manager = context->disk_manager_;
        status = disk_manager->CreateDatabase(name);
      }
    }
  }
  return status;
}
} // namespace DB