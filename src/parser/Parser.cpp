#include "parser/Parser.hpp"
#include "common/Status.hpp"
#include "common/util/StringUtil.hpp"
#include "parser/SQLStatement.hpp"
#include <memory>
#include <string>
#include <string_view>

namespace DB {

Parser::Parser() {
  checker_.RegisterKeyWord("CREATE");
  checker_.RegisterKeyWord("DATABASE");
}

Status Parser::Parse(std::string_view query,
                     std::shared_ptr<QueryContext> context,
                     ResultSet &result_set) {
  auto end = query.find(' ');
  auto status = Status::OK();
  std::string sv{query.begin(), query.begin() + end};

  if (checker_.IsKeyWord(sv)) {
    if (sv == "CREATE") {
      status = ParseCreate(query.substr(end + 1), context, result_set);
    }
  }

  return status;
}

Status Parser::ParseCreate(std::string_view query,
                           std::shared_ptr<QueryContext> context,
                           ResultSet &result_set) {
  auto end = query.find(' ');
  auto status = Status::OK();
  std::string sv{query.begin(), query.begin() + end};

  if (checker_.IsKeyWord(sv)) {
    if (sv == "DATABASE") {
      std::string name{query.substr(end + 1)};
      if (StringUtil::IsAlpha(name)) {
        auto disk_manager = context->disk_manager_;
        status = disk_manager->CreateDatabase(name);
      }
    }
  }
  return status;
}
} // namespace DB