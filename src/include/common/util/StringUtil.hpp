#pragma once

#include <algorithm>
#include <cctype>
#include <regex>
#include <string>

namespace DB {
struct StringUtil {

  static bool StartsWith(const std::string &str, const std::string &prefix) {
    return std::equal(prefix.begin(), prefix.end(), str.begin());
  }

  static bool EndsWith(const std::string &str, const std::string &suffix) {
    if (suffix.size() > str.size()) {
      return false;
    }
    return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
  }

  static bool Contains(const std::string &str, const std::string &src) {
    return str.contains(src);
  }

  static bool IsAlpha(const std::string &str) {
    return std::all_of(str.begin(), str.end(), [](const char &c) {
      return static_cast<bool>(isalpha(c));
    });
  }

  static void ToUpper(std::string &str) {
    std::for_each(str.begin(), str.end(), [](char &c) { c = toupper(c); });
  }

  static void ToLower(std::string &str) {
    std::for_each(str.begin(), str.end(), [](char &c) { c = tolower(c); });
  }

  static bool IsInteger(std::string &str) {
    return std::all_of(str.begin(), str.end(),
                       [](char &c) { return c >= '0' && c <= '9'; });
  }

  static bool IsFloat(std::string &str) {
    int point_num{};
    return std::all_of(str.begin(), str.end(),
                       [&point_num](char &c) {
                         return (c >= '0' && c <= '9') ||
                                (c == '.' && ++point_num);
                       }) &&
           point_num == 1;
  }

  static bool ValidName(const std::string &str) {
    std::regex pattern("^[a-zA-Z0-9_]+$");
    return std::regex_match(str, pattern);
  }

  // return 1 if str is table.column
  // return 0 if just column name
  // return -1 if the str have more than 1 '.'
  static int SpliteTableColumn(std::string &str, std::string &table,
                               std::string &column) {
    if (std::count(str.begin(), str.end(), '.') > 1) {
      return -1;
    }
    auto p = str.find('.');
    if (p == std::string::npos) {
      column = str;
      return 0;
    }
    table = str.substr(0, p);
    column = str.substr(p + 1);
    return 1;
  }
};

} // namespace DB