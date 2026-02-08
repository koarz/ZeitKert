#include "storage/lsmtree/Manifest.hpp"
#include "common/Config.hpp"
#include "fmt/format.h"

#include <cstring>
#include <fstream>
#include <sstream>

namespace DB {

static constexpr const char *kManifestFileName = "MANIFEST";
static constexpr const char *kAddRecord = "ADD";
static constexpr const char *kDelRecord = "DEL";
static constexpr const char *kSnapshotRecord = "SNAPSHOT";

// 将二进制数据编码为 hex 字符串
static std::string EncodeHex(const std::string &data) {
  static const char hex_chars[] = "0123456789abcdef";
  std::string result;
  result.reserve(data.size() * 2);
  for (unsigned char c : data) {
    result.push_back(hex_chars[c >> 4]);
    result.push_back(hex_chars[c & 0xf]);
  }
  return result;
}

// 将 hex 字符串解码为二进制
static std::string DecodeHex(const std::string &hex) {
  std::string result;
  result.reserve(hex.size() / 2);
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    auto hex_to_int = [](char c) -> int {
      if (c >= '0' && c <= '9')
        return c - '0';
      if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
      if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
      return 0;
    };
    result.push_back(
        static_cast<char>((hex_to_int(hex[i]) << 4) | hex_to_int(hex[i + 1])));
  }
  return result;
}

Manifest::Manifest(std::filesystem::path dir)
    : dir_(std::move(dir)), manifest_path_(dir_ / kManifestFileName) {}

Manifest::~Manifest() = default;

Status Manifest::Load(std::vector<LevelMeta> &levels) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!std::filesystem::exists(manifest_path_)) {
    // manifest 文件不存在，从空状态开始
    return Status::OK();
  }

  std::ifstream ifs(manifest_path_);
  if (!ifs.is_open()) {
    return Status::Error(ErrorCode::IOError, "Failed to open MANIFEST file");
  }

  // 先清空所有层
  for (auto &level : levels) {
    level.Clear();
  }

  std::string line;
  bool in_snapshot = false;

  while (std::getline(ifs, line)) {
    if (line.empty())
      continue;

    std::istringstream iss(line);
    std::string type;
    iss >> type;

    if (type == kSnapshotRecord) {
      // 新快照开始，清空所有层
      for (auto &level : levels) {
        level.Clear();
      }
      in_snapshot = true;
      continue;
    }

    if (type == kAddRecord) {
      uint32_t level_num, sstable_id;
      uint64_t file_size;
      std::string min_key_hex, max_key_hex;

      if (!(iss >> level_num >> sstable_id >> file_size >> min_key_hex >>
            max_key_hex)) {
        continue; // 跳过格式错误的行
      }

      if (level_num >= levels.size()) {
        continue;
      }

      LeveledSSTableMeta meta;
      meta.sstable_id = sstable_id;
      meta.level = level_num;
      meta.file_size = file_size;
      meta.min_key = DecodeHex(min_key_hex);
      meta.max_key = DecodeHex(max_key_hex);
      meta.being_compacted = false;

      levels[level_num].AddSSTable(meta);
    } else if (type == kDelRecord) {
      uint32_t level_num, sstable_id;

      if (!(iss >> level_num >> sstable_id)) {
        continue;
      }

      if (level_num >= levels.size()) {
        continue;
      }

      levels[level_num].RemoveSSTable(sstable_id);
    }
  }

  record_count_ = 0;
  return Status::OK();
}

Status Manifest::Save(const std::vector<LevelMeta> &levels) {
  std::lock_guard<std::mutex> lock(mutex_);
  return WriteSnapshot(levels);
}

Status Manifest::AddSSTable(uint32_t level, const LeveledSSTableMeta &meta) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::string record =
      fmt::format("{} {} {} {} {} {}", kAddRecord, level, meta.sstable_id,
                  meta.file_size, EncodeHex(meta.min_key),
                  EncodeHex(meta.max_key));

  return AppendRecord(record);
}

Status Manifest::RemoveSSTable(uint32_t level, uint32_t sstable_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::string record =
      fmt::format("{} {} {}", kDelRecord, level, sstable_id);

  return AppendRecord(record);
}

Status Manifest::AppendRecord(const std::string &record) {
  std::ofstream ofs(manifest_path_, std::ios::app);
  if (!ofs.is_open()) {
    return Status::Error(ErrorCode::IOError,
                         "Failed to open MANIFEST file for append");
  }

  ofs << record << "\n";
  ofs.flush();

  if (!ofs.good()) {
    return Status::Error(ErrorCode::IOError,
                         "Failed to write to MANIFEST file");
  }

  record_count_++;
  return Status::OK();
}

Status Manifest::WriteSnapshot(const std::vector<LevelMeta> &levels) {
  // 先写入临时文件，再原子重命名
  auto temp_path = manifest_path_;
  temp_path += ".tmp";

  {
    std::ofstream ofs(temp_path, std::ios::trunc);
    if (!ofs.is_open()) {
      return Status::Error(ErrorCode::IOError,
                           "Failed to create temporary MANIFEST file");
    }

    // 写入快照头
    ofs << kSnapshotRecord << "\n";

    // 写入所有 SSTable
    for (const auto &level : levels) {
      for (const auto &meta : level.sstables) {
        ofs << fmt::format("{} {} {} {} {} {}", kAddRecord, level.level_num,
                           meta.sstable_id, meta.file_size,
                           EncodeHex(meta.min_key), EncodeHex(meta.max_key))
            << "\n";
      }
    }

    ofs.flush();
    if (!ofs.good()) {
      return Status::Error(ErrorCode::IOError,
                           "Failed to write MANIFEST snapshot");
    }
  }

  // 原子替换旧 manifest
  std::error_code ec;
  std::filesystem::rename(temp_path, manifest_path_, ec);
  if (ec) {
    return Status::Error(ErrorCode::IOError,
                         "Failed to rename MANIFEST file: " + ec.message());
  }

  record_count_ = 0;
  return Status::OK();
}

} // namespace DB
