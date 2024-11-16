#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <set>
#include <string>

#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "common/ZeitKert.hpp"
#include "common/util/StringUtil.hpp"
#include "function/Abs.hpp"
#include "function/FunctionString.hpp"
#include "linenoise.h"
#include "parser/Checker.hpp"

void CheckerRegister();

int main(int argc, char *argv[]) {
  CheckerRegister();
  DB::ZeitKert db;
  // We need to make sure that the lifecycle of the data held by slice is long
  // enough, So every time you execute a query, move the query's data around
  // to make sure the data pointer stays the same.
  std::map<int, std::string> history;
  int num{};

  linenoiseHistorySetMaxLen(1024);
  linenoiseSetMultiLine(1);

  std::cout << "Welcome to ZeitKert!\n\n";

  auto prompt = "DB > ";
  while (true) {
    std::string &query = history[num++];
    bool first_line = true;
    while (true) {
      auto line_prompt = first_line ? prompt : ".. > ";
      char *query_c_str = linenoise(line_prompt);
      if (query_c_str == nullptr) {
        return 0;
      }
      query += query_c_str;
      if (DB::StringUtil::EndsWith(query, ";") ||
          DB::StringUtil::StartsWith(query, "\\")) {
        break;
      }
      query += " ";
      linenoiseFree(query_c_str);
      first_line = false;
    }
    if (query == "quit;" || query == "exit;") {
      break;
    }

    linenoiseHistoryAdd(query.c_str());

    DB::ResultSet res;
    const auto start = std::chrono::steady_clock::now();
    if (DB::Status status = db.ExecuteQuery(query, res); !status.ok()) {
      std::cout << status.GetMessage() << "\n";
    }
    const auto end = std::chrono::steady_clock::now();
    if (res.schema_) {
      res.schema_->PrintColumns();
    }
    const std::chrono::duration<double> diff = end - start;
    std::cout << "\nTime : " << std::fixed << std::setprecision(9) << diff
              << "\n\n";
  }

  std::cout << "Bye.\n";
  return 0;
}

void CheckerRegister() {
  using Checker = DB::Checker;
  Checker::RegisterKeyWord("CREATE");
  Checker::RegisterKeyWord("DROP");
  Checker::RegisterKeyWord("SHOW");
  Checker::RegisterKeyWord("DATABASE");
  Checker::RegisterKeyWord("DATABASES");
  Checker::RegisterKeyWord("USE");
  Checker::RegisterKeyWord("SELECT");
  Checker::RegisterKeyWord("TABLE");
  Checker::RegisterKeyWord("TABLES");
  Checker::RegisterKeyWord("SELECT");
  Checker::RegisterKeyWord("INSERT");
  Checker::RegisterKeyWord("INTO");
  Checker::RegisterKeyWord("VALUES");
  Checker::RegisterKeyWord("FROM");

  Checker::RegisterType("INT");
  // Checker::RegisterType("Varchar");
  Checker::RegisterType("STRING");
  Checker::RegisterType("DOUBLE");

  Checker::RegisterFunction("ABS", std::make_shared<DB::FunctionAbs>());
  Checker::RegisterFunction("TO_UPPER",
                            std::make_shared<DB::FunctionToUpper>());
  Checker::RegisterFunction("TO_LOWER",
                            std::make_shared<DB::FunctionToLower>());
}