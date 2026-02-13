#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include "common/Logger.hpp"
#include "common/ResultSet.hpp"
#include "common/Status.hpp"
#include "common/ZeitKert.hpp"
#include "common/util/StringUtil.hpp"
#include "linenoise.h"

static std::string GetHistoryPath() {
  const char *home = std::getenv("HOME");
  if (!home)
    return {};
  auto dir = std::filesystem::path(home) / ".ZeitKert";
  std::filesystem::create_directories(dir);
  return (dir / "history").string();
}

int main(int argc, char *argv[]) {
  DB::ZeitKert db;

  linenoiseHistorySetMaxLen(1024);
  linenoiseSetMultiLine(1);

  auto history_path = GetHistoryPath();
  if (!history_path.empty()) {
    linenoiseHistoryLoad(history_path.c_str());
  }

  std::cout << "Welcome to ZeitKert!\n\n";

  auto prompt = "DB > ";
  while (true) {
    std::string query;
    bool first_line = true;
    while (true) {
      auto line_prompt = first_line ? prompt : ".. > ";
      char *query_c_str = linenoise(line_prompt);
      if (query_c_str == nullptr) {
        return 0;
      }
      query += query_c_str;
      linenoiseFree(query_c_str);
      if (DB::StringUtil::EndsWith(query, ";") ||
          DB::StringUtil::StartsWith(query, "\\")) {
        break;
      }
      query += " ";
      first_line = false;
    }
    if (query == "quit;" || query == "exit;") {
      break;
    }

    linenoiseHistoryAdd(query.c_str());
    if (!history_path.empty()) {
      linenoiseHistorySave(history_path.c_str());
    }

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

  LOG_INFO("ZeitKert shutting down");
  std::cout << "Bye.\n";
  return 0;
}
