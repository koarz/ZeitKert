#include "storage/lsmtree/builder/SSTableBuilder.hpp"

#include "common/Config.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/builder/BloomFilterBuilder.hpp"

#include "fmt/format.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace DB {
static constexpr uint32_t kSSTableMagic = 0x5A4B5254; // ZKRT
static constexpr uint16_t kSSTableVersion = 2;

static size_t AlignTo(size_t size, size_t alignment) {
  // 向上对齐到指定字节边界
  if (alignment == 0) {
    return size;
  }
  return (size + alignment - 1) / alignment * alignment;
}

static uint32_t FixedSize(ValueType::Type type) {
  switch (type) {
  case ValueType::Type::Int: return sizeof(int);
  case ValueType::Type::Double: return sizeof(double);
  case ValueType::Type::String:
  case ValueType::Type::Null: return 0;
  }
  return 0;
}

struct ZoneMapBuilder {
  explicit ZoneMapBuilder(ValueType::Type type) : type(type) {}

  ValueType::Type type;
  bool has_value = false;
  int min_int = 0;
  int max_int = 0;
  double min_double = 0.0;
  double max_double = 0.0;
  std::string min_string;
  std::string max_string;

  void Update(const Byte *data, uint32_t len) {
    if (len == 0 || !data) {
      return;
    }
    switch (type) {
    case ValueType::Type::Int: {
      int v = 0;
      std::memcpy(&v, data, sizeof(int));
      if (!has_value) {
        min_int = max_int = v;
        has_value = true;
        return;
      }
      min_int = std::min(min_int, v);
      max_int = std::max(max_int, v);
      return;
    }
    case ValueType::Type::Double: {
      double v = 0.0;
      std::memcpy(&v, data, sizeof(double));
      if (!has_value) {
        min_double = max_double = v;
        has_value = true;
        return;
      }
      min_double = std::min(min_double, v);
      max_double = std::max(max_double, v);
      return;
    }
    case ValueType::Type::String: {
      std::string value(reinterpret_cast<const char *>(data), len);
      if (!has_value) {
        min_string = max_string = std::move(value);
        has_value = true;
        return;
      }
      if (value < min_string) {
        min_string = value;
      }
      if (value > max_string) {
        max_string = std::move(value);
      }
      return;
    }
    case ValueType::Type::Null: return;
    }
  }

  ZoneMap Finish() const {
    ZoneMap zone;
    zone.has_value = has_value;
    switch (type) {
    case ValueType::Type::Int: {
      if (!has_value) {
        return zone;
      }
      zone.min.assign(reinterpret_cast<const char *>(&min_int),
                      sizeof(min_int));
      zone.max.assign(reinterpret_cast<const char *>(&max_int),
                      sizeof(max_int));
      return zone;
    }
    case ValueType::Type::Double: {
      if (!has_value) {
        return zone;
      }
      zone.min.assign(reinterpret_cast<const char *>(&min_double),
                      sizeof(min_double));
      zone.max.assign(reinterpret_cast<const char *>(&max_double),
                      sizeof(max_double));
      return zone;
    }
    case ValueType::Type::String: {
      if (!has_value) {
        return zone;
      }
      auto truncate = [](const std::string &s) {
        // 字符串 zonemap 使用前缀截断
        if (s.size() <= ZONE_MAP_PREFIX_LEN) {
          return s;
        }
        return s.substr(0, ZONE_MAP_PREFIX_LEN);
      };
      zone.min = truncate(min_string);
      zone.max = truncate(max_string);
      return zone;
    }
    case ValueType::Type::Null: return zone;
    }
    return zone;
  }
};

struct ColumnBuilder {
  explicit ColumnBuilder(ValueType::Type type) : type(type), zone(type) {
    if (type == ValueType::Type::String) {
      offsets.push_back(0);
    }
  }

  ValueType::Type type;
  std::string data;
  std::vector<uint32_t> offsets;
  ZoneMapBuilder zone;
  std::vector<uint8_t> null_bitmap;
  bool has_nulls = false;
  uint32_t row_count = 0;

  void Append(const Byte *value, uint32_t len) {
    // Track null bitmap
    size_t byte_idx = row_count / 8;
    if (byte_idx >= null_bitmap.size()) {
      null_bitmap.resize(byte_idx + 1, 0);
    }

    bool is_null = (!value || len == 0);

    if (type == ValueType::Type::String) {
      if (is_null) {
        offsets.push_back(offsets.back());
        null_bitmap[byte_idx] |= (1 << (row_count % 8));
        has_nulls = true;
        row_count++;
        return;
      }
      offsets.push_back(offsets.back() + len);
      data.append(reinterpret_cast<const char *>(value), len);
      zone.Update(value, len);
      row_count++;
      return;
    }

    uint32_t fixed = FixedSize(type);
    if (fixed == 0) {
      row_count++;
      return;
    }
    if (is_null) {
      std::string zeros(fixed, 0);
      data.append(zeros.data(), zeros.size());
      null_bitmap[byte_idx] |= (1 << (row_count % 8));
      has_nulls = true;
      row_count++;
      return;
    }
    data.append(reinterpret_cast<const char *>(value), fixed);
    zone.Update(value, fixed);
    row_count++;
  }
};

class RowGroupBuilder {
  std::vector<std::shared_ptr<ValueType>> column_types_;
  std::vector<ColumnBuilder> columns_;
  std::vector<Slice> keys_;
  uint32_t row_count_{0};
  size_t current_size_{0};
  size_t target_size_;

public:
  RowGroupBuilder(std::vector<std::shared_ptr<ValueType>> column_types,
                  size_t target_size)
      : column_types_(std::move(column_types)), target_size_(target_size) {
    Reset();
  }

  void Reset() {
    columns_.clear();
    columns_.reserve(column_types_.size());
    current_size_ = 0;
    for (auto &type : column_types_) {
      columns_.emplace_back(type->GetType());
      if (type->GetType() == ValueType::Type::String) {
        current_size_ += sizeof(uint32_t);
      }
    }
    keys_.clear();
    row_count_ = 0;
  }

  bool Empty() const { return row_count_ == 0; }

  size_t DataSize() const { return current_size_; }

  uint32_t RowCount() const { return row_count_; }

  bool AddRow(const Slice &key, const Slice &row) {
    // 解码行后计算 RowGroup 的目标大小
    std::vector<std::pair<const Byte *, uint32_t>> values(column_types_.size());
    bool ok =
        RowCodec::DecodeRow(row, column_types_.size(),
                            [&](size_t idx, const Byte *data, uint32_t len) {
                              values[idx] = {data, len};
                            });
    if (!ok) {
      return false;
    }
    size_t size_inc = 0;
    for (size_t i = 0; i < column_types_.size(); i++) {
      if (column_types_[i]->GetType() == ValueType::Type::String) {
        size_inc += sizeof(uint32_t);
        size_inc += values[i].second;
      } else {
        size_inc += FixedSize(column_types_[i]->GetType());
      }
    }
    if (row_count_ > 0 && current_size_ + size_inc > target_size_) {
      return false;
    }
    for (size_t i = 0; i < columns_.size(); i++) {
      columns_[i].Append(values[i].first, values[i].second);
    }
    // key 仅用于主键 Bloom 和 max_key
    keys_.push_back(key);
    row_count_++;
    current_size_ += size_inc;
    return true;
  }

  RowGroupMeta Build(std::string &data) const {
    RowGroupMeta meta;
    meta.row_count = row_count_;
    size_t offset = 0;
    data.clear();
    // PAX 布局按列顺序写入
    for (size_t i = 0; i < columns_.size(); i++) {
      const auto &col = columns_[i];
      ColumnChunkMeta col_meta;
      col_meta.offset = static_cast<uint32_t>(offset);
      col_meta.zone = col.zone.Finish();
      col_meta.has_nulls = col.has_nulls;

      // 如果列有 null，先写入 null bitmap
      if (col.has_nulls) {
        size_t bitmap_size = (row_count_ + 7) / 8;
        data.append(reinterpret_cast<const char *>(col.null_bitmap.data()),
                    bitmap_size);
      }

      size_t bitmap_bytes = col.has_nulls ? (row_count_ + 7) / 8 : 0;
      if (col.type == ValueType::Type::String) {
        for (auto off : col.offsets) {
          data.append(reinterpret_cast<const char *>(&off), sizeof(off));
        }
        data.append(col.data);
        col_meta.size = static_cast<uint32_t>(
            bitmap_bytes + col.offsets.size() * sizeof(uint32_t) +
            col.data.size());
      } else {
        data.append(col.data);
        col_meta.size = static_cast<uint32_t>(bitmap_bytes + col.data.size());
      }
      meta.columns.emplace_back(std::move(col_meta));
      offset += meta.columns.back().size;
    }

    // 写入 key 列数据（固定 4 字节 int 或 8 字节 double，或变长 string）
    if (!keys_.empty()) {
      meta.key_column_offset = static_cast<uint32_t>(offset);
      // 假设所有 key 大小一致（int 或 double），使用第一个 key 的大小
      uint32_t key_size = static_cast<uint32_t>(keys_[0].Size());
      for (auto &k : keys_) {
        data.append(reinterpret_cast<const char *>(k.GetData()), k.Size());
      }
      meta.key_column_size = static_cast<uint32_t>(keys_.size() * key_size);
      offset += meta.key_column_size;

      // Bloom 仅覆盖主键 key
      BloomFilterBuilder bloom_builder(keys_.size());
      for (auto &k : keys_) {
        bloom_builder.AddKey(k);
      }
      meta.bloom = bloom_builder.GetData();
      meta.max_key = keys_.back().ToString();
    }
    return meta;
  }
};

SSTableBuilder::~SSTableBuilder() = default;

SSTableBuilder::SSTableBuilder(
    std::filesystem::path path, uint32_t table_num,
    std::vector<std::shared_ptr<ValueType>> column_types,
    uint16_t primary_key_idx)
    : table_id_(table_num), column_types_(std::move(column_types)),
      primary_key_idx_(primary_key_idx) {
  std::filesystem::create_directory(path);
  path_ = std::move(path / fmt::format("{}.sst", table_num));
  fs_ = std::make_unique<std::ofstream>(
      path_, std::ios::trunc | std::ios::binary | std::ios::out);
  rowgroup_builder_ = std::make_unique<RowGroupBuilder>(
      column_types_, DEFAULT_ROWGROUP_TARGET_SIZE);
}

bool SSTableBuilder::Add(const Slice &key, const Slice &row) {
  if (rowgroup_builder_->AddRow(key, row)) {
    return true;
  }
  auto status = FlushRowGroup();
  if (!status.ok()) {
    return false;
  }
  return rowgroup_builder_->AddRow(key, row);
}

Status SSTableBuilder::FlushRowGroup() {
  if (!fs_ || !(*fs_)) {
    return Status::Error(ErrorCode::FileNotOpen, "SSTable file not open");
  }
  if (!rowgroup_builder_->Empty()) {
    std::string rg_data;
    auto meta = rowgroup_builder_->Build(rg_data);
    // 记录 RowGroup 在文件内的起始偏移
    meta.offset = data_size_;
    auto aligned = AlignTo(rg_data.size(), DEFAULT_ROWGROUP_ALIGNMENT);
    fs_->write(rg_data.data(), rg_data.size());
    if (aligned > rg_data.size()) {
      // 4KB 对齐填充
      std::string padding(aligned - rg_data.size(), 0);
      fs_->write(padding.data(), padding.size());
    }
    data_size_ += static_cast<uint32_t>(aligned);
    rowgroups_.push_back(std::move(meta));
    rowgroup_builder_->Reset();
  }
  return Status::OK();
}

Status SSTableBuilder::Finish() {
  auto status = FlushRowGroup();
  if (!status.ok()) {
    return status;
  }

  std::string meta_blob;
  // RowGroup 元数据连续写入
  for (const auto &rg : rowgroups_) {
    rg.Serialize(column_types_, meta_blob);
  }
  uint32_t meta_offset = data_size_;
  uint32_t meta_size = static_cast<uint32_t>(meta_blob.size());
  if (meta_size > 0) {
    fs_->write(meta_blob.data(), meta_blob.size());
  }

  uint32_t rowgroup_count = static_cast<uint32_t>(rowgroups_.size());
  uint16_t column_count = static_cast<uint16_t>(column_types_.size());
  uint16_t version = kSSTableVersion;
  uint16_t reserved = 0;

  // Footer 固定长度用于反向读取元数据
  fs_->write(reinterpret_cast<const char *>(&meta_offset), sizeof(meta_offset));
  fs_->write(reinterpret_cast<const char *>(&meta_size), sizeof(meta_size));
  fs_->write(reinterpret_cast<const char *>(&rowgroup_count),
             sizeof(rowgroup_count));
  fs_->write(reinterpret_cast<const char *>(&column_count),
             sizeof(column_count));
  fs_->write(reinterpret_cast<const char *>(&primary_key_idx_),
             sizeof(primary_key_idx_));
  fs_->write(reinterpret_cast<const char *>(&version), sizeof(version));
  fs_->write(reinterpret_cast<const char *>(&reserved), sizeof(reserved));
  fs_->write(reinterpret_cast<const char *>(&kSSTableMagic),
             sizeof(kSSTableMagic));
  fs_->flush();
  fs_->close();

  sstable_meta_ = std::make_shared<SSTable>();
  sstable_meta_->sstable_id_ = table_id_;
  sstable_meta_->rowgroup_count_ = rowgroup_count;
  sstable_meta_->column_count_ = column_count;
  sstable_meta_->primary_key_idx_ = primary_key_idx_;
  sstable_meta_->rowgroups_ = rowgroups_;
  sstable_meta_->data_file_ = std::make_shared<MMapFile>(path_);
  return Status::OK();
}
} // namespace DB
