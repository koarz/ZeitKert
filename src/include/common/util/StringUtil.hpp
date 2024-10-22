#pragma once

#include <algorithm>
#include <cctype>
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
};

} // namespace DB