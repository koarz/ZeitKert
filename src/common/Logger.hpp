#pragma once

#include <string>

namespace DB {
class Logger {
public:
  static void Init(const std::string &log_file = "./logs/zeitkert.log");
  static void Shutdown();

  static void Info(const char *file, int line, const std::string &msg);
  static void Warn(const char *file, int line, const std::string &msg);
  static void Error(const char *file, int line, const std::string &msg);
  static void Debug(const char *file, int line, const std::string &msg);
};
} // namespace DB

#include "fmt/format.h"

#define LOG_INFO(...)                                                          \
  DB::Logger::Info(__FILE__, __LINE__, fmt::format(__VA_ARGS__))
#define LOG_WARN(...)                                                          \
  DB::Logger::Warn(__FILE__, __LINE__, fmt::format(__VA_ARGS__))
#define LOG_ERROR(...)                                                         \
  DB::Logger::Error(__FILE__, __LINE__, fmt::format(__VA_ARGS__))
#define LOG_DEBUG(...)                                                         \
  DB::Logger::Debug(__FILE__, __LINE__, fmt::format(__VA_ARGS__))
