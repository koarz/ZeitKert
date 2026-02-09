#include "common/Logger.hpp"

#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

#include <filesystem>
#include <memory>

namespace DB {

static std::shared_ptr<spdlog::logger> g_logger;

void Logger::Init(const std::string &log_file) {
  std::filesystem::path log_path(log_file);
  if (log_path.has_parent_path()) {
    std::filesystem::create_directories(log_path.parent_path());
  }
  auto file_sink =
      std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file, false);
  g_logger = std::make_shared<spdlog::logger>("zeitkert", file_sink);
  // 设置格式：[2026-01-23 13:00:00.000] [INFO] [file:line] message
  g_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
  g_logger->set_level(spdlog::level::info);
  g_logger->flush_on(spdlog::level::info);
  spdlog::set_default_logger(g_logger);
}

void Logger::Shutdown() {
  if (g_logger) {
    g_logger->flush();
    g_logger.reset();
  }
  spdlog::shutdown();
}

void Logger::Info(const char *file, int line, const std::string &msg) {
  if (g_logger) {
    g_logger->info("[{}:{}] {}", file, line, msg);
  }
}

void Logger::Warn(const char *file, int line, const std::string &msg) {
  if (g_logger) {
    g_logger->warn("[{}:{}] {}", file, line, msg);
  }
}

void Logger::Error(const char *file, int line, const std::string &msg) {
  if (g_logger) {
    g_logger->error("[{}:{}] {}", file, line, msg);
  }
}

void Logger::Debug(const char *file, int line, const std::string &msg) {
  if (g_logger) {
    g_logger->debug("[{}:{}] {}", file, line, msg);
  }
}

} // namespace DB
