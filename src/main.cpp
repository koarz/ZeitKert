#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>

#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "common/ZeitgeistDB.hpp"
#include "common/util/StringUtil.hpp"
#include "linenoise.h"

int main(int argc, char *argv[]) {
  DB::ZeitgeistDB db;

  linenoiseHistorySetMaxLen(1024);
  linenoiseSetMultiLine(1);

  std::cout << "Welcome to ZeitgeistDB!\n\n";

  auto prompt = "ZeitgeistDB > ";
  while (true) {
    std::string query;
    bool first_line = true;
    while (true) {
      auto line_prompt = first_line ? prompt : "        ... > ";
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

    DB::ResultSet res;
    const auto start = std::chrono::steady_clock::now();
    if (DB::Status status = db.ExecuteQuery(query, res); !status.ok()) {
      std::cout << status.GetMessage() << std::endl;
    }
    const auto end = std::chrono::steady_clock::now();
    const std::chrono::duration<double> diff = end - start;
    std::cout << "Execute : " << std::fixed << std::setprecision(9) << diff
              << '\n';
  }

  std::cout << "Bye.\n";
  return 0;
}