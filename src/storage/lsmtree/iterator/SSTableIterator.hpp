#pragma once

#include "common/Config.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/iterator/Iterator.hpp"
#include "type/ValueType.hpp"

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace DB {
class SSTableIterator : public Iterator {
  Slice key_;
  Slice value_;
  bool valid_{false};
  size_t rowgroup_idx_{0};
  uint32_t row_idx_{0};
  SSTableRef sstable_;
  std::vector<std::shared_ptr<ValueType>> column_types_;
  uint16_t primary_key_idx_{0};
  std::string row_buffer_;

  void LoadCurrent() {
    valid_ = false;
    if (!sstable_ || !sstable_->data_file_ || !sstable_->data_file_->Valid()) {
      return;
    }
    if (rowgroup_idx_ >= sstable_->rowgroups_.size()) {
      return;
    }
    const auto &rg = sstable_->rowgroups_[rowgroup_idx_];
    if (row_idx_ >= rg.row_count) {
      return;
    }
    // 基于 mmap 的 RowGroup 重建当前行
    const Byte *base =
        sstable_->data_file_->Data() + static_cast<size_t>(rg.offset);
    row_buffer_.clear();
    for (size_t col_idx = 0; col_idx < rg.columns.size(); col_idx++) {
      const auto &col = rg.columns[col_idx];
      auto type = column_types_[col_idx]->GetType();
      if (type == ValueType::Type::String) {
        const Byte *offsets_base = base + col.offset;
        uint32_t start = 0;
        uint32_t end = 0;
        std::memcpy(&start, offsets_base + row_idx_ * sizeof(uint32_t),
                    sizeof(uint32_t));
        std::memcpy(&end, offsets_base + (row_idx_ + 1) * sizeof(uint32_t),
                    sizeof(uint32_t));
        const Byte *data_base =
            offsets_base + (rg.row_count + 1) * sizeof(uint32_t);
        uint32_t len = end - start;
        row_buffer_.append(reinterpret_cast<const char *>(&len), sizeof(len));
        if (len > 0) {
          row_buffer_.append(reinterpret_cast<const char *>(data_base + start),
                             len);
        }
        if (col_idx == primary_key_idx_) {
          key_ = Slice{const_cast<Byte *>(data_base + start),
                       static_cast<uint16_t>(len)};
        }
      } else {
        uint32_t len = 0;
        if (type == ValueType::Type::Int) {
          len = sizeof(int);
        } else if (type == ValueType::Type::Double) {
          len = sizeof(double);
        }
        const Byte *data_ptr =
            base + col.offset + row_idx_ * static_cast<size_t>(len);
        row_buffer_.append(reinterpret_cast<const char *>(&len), sizeof(len));
        if (len > 0) {
          row_buffer_.append(reinterpret_cast<const char *>(data_ptr), len);
        }
        if (col_idx == primary_key_idx_) {
          key_ =
              Slice{const_cast<Byte *>(data_ptr), static_cast<uint16_t>(len)};
        }
      }
    }
    value_ = Slice{row_buffer_};
    valid_ = true;
  }

public:
  SSTableIterator(SSTableRef sstable,
                  std::vector<std::shared_ptr<ValueType>> column_types)
      : sstable_(std::move(sstable)), column_types_(std::move(column_types)) {
    if (sstable_) {
      primary_key_idx_ = sstable_->primary_key_idx_;
    }
    if (primary_key_idx_ >= column_types_.size()) {
      primary_key_idx_ = 0;
    }
    LoadCurrent();
  }

  SSTableIterator(SSTableIterator &&other) {
    key_ = std::move(other.key_);
    value_ = std::move(other.value_);
    valid_ = other.valid_;
    rowgroup_idx_ = other.rowgroup_idx_;
    row_idx_ = other.row_idx_;
    sstable_ = std::move(other.sstable_);
    column_types_ = std::move(other.column_types_);
    primary_key_idx_ = other.primary_key_idx_;
    row_buffer_ = std::move(other.row_buffer_);

    other.valid_ = false;
  }

  SSTableIterator &operator=(SSTableIterator &&other) {
    key_ = std::move(other.key_);
    value_ = std::move(other.value_);
    valid_ = other.valid_;
    rowgroup_idx_ = other.rowgroup_idx_;
    row_idx_ = other.row_idx_;
    sstable_ = std::move(other.sstable_);
    column_types_ = std::move(other.column_types_);
    primary_key_idx_ = other.primary_key_idx_;
    row_buffer_ = std::move(other.row_buffer_);

    other.valid_ = false;
    return *this;
  }

  ~SSTableIterator() override = default;

  void Next() override {
    if (!valid_) {
      return;
    }
    row_idx_++;
    if (rowgroup_idx_ < sstable_->rowgroups_.size() &&
        row_idx_ >= sstable_->rowgroups_[rowgroup_idx_].row_count) {
      rowgroup_idx_++;
      row_idx_ = 0;
    }
    LoadCurrent();
  }

  bool Valid() override { return valid_; }

  Slice &GetKey() override { return key_; }

  Slice &GetValue() override { return value_; }
};
} // namespace DB
