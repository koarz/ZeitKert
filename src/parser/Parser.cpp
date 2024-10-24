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
  checker_.RegisterKeyWord("DROP");
  checker_.RegisterKeyWord("SHOW");
  checker_.RegisterKeyWord("DATABASE");
  checker_.RegisterKeyWord("DATABASES");
}

Status Parser::Parse(std::string_view query,
                     std::shared_ptr<QueryContext> context,
                     ResultSet &result_set) {
  auto end = query.find(' ');
  auto status = Status::OK();
  std::string sv{query.begin(), query.begin() + end};

  if (checker_.IsKeyWord(sv)) {
    StringUtil::ToUpper(sv);
    if (sv == "CREATE") {
      status = ParseCreate(query.substr(end + 1), context, result_set);
    } else if (sv == "DROP") {
      status = ParseDrop(query.substr(end + 1), context, result_set);
    } else if (sv == "SHOW") {
      status = ParseShow(query.substr(end + 1), context, result_set);
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
Status Parser::ParseDrop(std::string_view query,
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
        status = disk_manager->DropDatabase(name);
      }
    }
  }
  return status;
}
Status Parser::ParseShow(std::string_view query,
                         std::shared_ptr<QueryContext> context,
                         ResultSet &result_set) {
  auto status = Status::OK();
  std::string sv{query};

  if (checker_.IsKeyWord(sv)) {
    if (sv == "DATABASES") {
      auto disk_manager = context->disk_manager_;
      status = disk_manager->ShowDatabase();
    }
  }
  return status;
}
} // namespace DB