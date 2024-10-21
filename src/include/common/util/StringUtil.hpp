#pragma once

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
};

} // namespace DB