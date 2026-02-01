#pragma once

#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/Arena.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/WAL.hpp"
#include "type/ValueType.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace DB {

/**
 * VectorizedMemTable - 键值分离 + 主键类型特化的向量化 MemTable
 *
 * 设计理念：
 * 1. 键值分离：key 和 value 分开存储，排序时不访问 value 数据
 * 2. 整数主键特化：int 主键直接存值，排序时 0 内存访问
 * 3. 缓存友好：Entry 数组连续存储，排序时 swap 定长结构
 *
 * 数据布局：
 *   value_arena_: [value0][value1][value2]...
 *   key_arena_:   [key0][key1][key2]...  (仅字符串主键)
 *   entries_:     [(key/key_offset, value_offset, value_len), ...]
 */
class VectorizedMemTable {
public:
  // 整数主键 Entry - 12B，缓存行可容纳 5 个
  struct IntEntry {
    int key;
    uint32_t value_offset;
    uint32_t value_len;
    uint32_t seq; // 插入序号，保证稳定排序
  };

  // 字符串主键 Entry - 20B
  struct StringEntry {
    uint32_t key_offset;
    uint32_t key_len;
    uint32_t value_offset;
    uint32_t value_len;
    uint32_t seq;
  };

private:
  WAL wal_;
  ValueType::Type key_type_;
  Arena value_arena_;
  Arena key_arena_; // 仅字符串主键使用

  // 根据主键类型使用不同的 Entry 数组
  std::vector<IntEntry> int_entries_;
  std::vector<StringEntry> string_entries_;

  std::atomic_uint32_t approximate_size_{0};
  std::atomic_uint32_t seq_{0}; // 插入序号计数器
  mutable bool sorted_{true};

  // 字符串主键：从 key_arena_ 获取 key
  std::string_view GetStringKeyAt(uint32_t key_offset, uint32_t key_len) const {
    return std::string_view(key_arena_.Data() + key_offset, key_len);
  }

  // 从 value_arena_ 获取 value
  std::string_view GetValueAt(uint32_t value_offset, uint32_t value_len) const {
    return std::string_view(value_arena_.Data() + value_offset, value_len);
  }

  // 整数主键比较器
  struct IntEntryCompare {
    bool operator()(const IntEntry &a, const IntEntry &b) const {
      if (a.key != b.key)
        return a.key < b.key;
      return a.seq < b.seq; // 相同 key 按插入顺序
    }
  };

  // 字符串主键比较器
  struct StringEntryCompare {
    const VectorizedMemTable *table;
    bool operator()(const StringEntry &a, const StringEntry &b) const {
      auto ka = table->GetStringKeyAt(a.key_offset, a.key_len);
      auto kb = table->GetStringKeyAt(b.key_offset, b.key_len);
      int cmp = ka.compare(kb);
      if (cmp != 0)
        return cmp < 0;
      return a.seq < b.seq;
    }
  };

  void EnsureSorted() const {
    if (sorted_)
      return;

    auto *self = const_cast<VectorizedMemTable *>(this);
    switch (key_type_) {
    case ValueType::Type::Int:
      std::sort(self->int_entries_.begin(), self->int_entries_.end(),
                IntEntryCompare{});
      break;
    case ValueType::Type::String:
      std::sort(self->string_entries_.begin(), self->string_entries_.end(),
                StringEntryCompare{this});
      break;
    default: break;
    }
    self->sorted_ = true;
  }

public:
  VectorizedMemTable() : key_type_(ValueType::Type::String) {}

  VectorizedMemTable(std::filesystem::path wal_path, bool write_log,
                     ValueType::Type key_type, bool recover = true)
      : wal_(std::move(wal_path), write_log), key_type_(key_type) {
    if (recover) {
      RecoverFromWal();
    }
  }

  ValueType::Type GetKeyType() const { return key_type_; }

  // 写入 KV 对 - O(1) 追加
  Status Put(const Slice &key, const Slice &value) {
    uint32_t value_len = static_cast<uint32_t>(value.Size());

    // 分配并写入 value
    uint32_t value_offset = static_cast<uint32_t>(value_arena_.CurrentOffset());
    if (value_len > 0) {
      Byte *dest = value_arena_.Allocate(value_len);
      std::memcpy(dest, value.GetData(), value_len);
    }

    uint32_t current_seq = seq_.fetch_add(1, std::memory_order_relaxed);

    switch (key_type_) {
    case ValueType::Type::Int: {
      int int_key = 0;
      if (key.Size() == sizeof(int)) {
        std::memcpy(&int_key, key.GetData(), sizeof(int));
      }
      int_entries_.push_back({int_key, value_offset, value_len, current_seq});
      break;
    }
    case ValueType::Type::String:
    default: {
      uint32_t key_len = static_cast<uint32_t>(key.Size());
      uint32_t key_offset = static_cast<uint32_t>(key_arena_.CurrentOffset());
      if (key_len > 0) {
        Byte *dest = key_arena_.Allocate(key_len);
        std::memcpy(dest, key.GetData(), key_len);
      }
      string_entries_.push_back(
          {key_offset, key_len, value_offset, value_len, current_seq});
      break;
    }
    }

    approximate_size_.fetch_add(key.Size() + value_len,
                                std::memory_order_relaxed);
    sorted_ = false;

    return wal_.WriteSlice(key, value);
  }

  // 点查 - 需要排序后二分查找
  Status Get(const Slice &key, Slice *value) {
    EnsureSorted();

    switch (key_type_) {
    case ValueType::Type::Int: {
      int target_key = 0;
      if (key.Size() == sizeof(int)) {
        std::memcpy(&target_key, key.GetData(), sizeof(int));
      }

      // 二分查找
      auto it =
          std::lower_bound(int_entries_.begin(), int_entries_.end(), target_key,
                           [](const IntEntry &e, int k) { return e.key < k; });

      // 找最后一个匹配的（最新版本）
      const IntEntry *found = nullptr;
      while (it != int_entries_.end() && it->key == target_key) {
        found = &(*it);
        ++it;
      }

      if (!found) {
        return Status::Error(ErrorCode::NotFound, "Key not found");
      }
      if (found->value_len == 0) {
        return Status::Error(ErrorCode::NotFound, "Key was deleted");
      }

      auto value_sv = GetValueAt(found->value_offset, found->value_len);
      *value = Slice{std::string(value_sv)};
      return Status::OK();
    }

    case ValueType::Type::String:
    default: {
      std::string_view target_key(key.GetData(), key.Size());

      auto it = std::lower_bound(
          string_entries_.begin(), string_entries_.end(), target_key,
          [this](const StringEntry &e, std::string_view k) {
            return GetStringKeyAt(e.key_offset, e.key_len) < k;
          });

      const StringEntry *found = nullptr;
      while (it != string_entries_.end() &&
             GetStringKeyAt(it->key_offset, it->key_len) == target_key) {
        found = &(*it);
        ++it;
      }

      if (!found) {
        return Status::Error(ErrorCode::NotFound, "Key not found");
      }
      if (found->value_len == 0) {
        return Status::Error(ErrorCode::NotFound, "Key was deleted");
      }

      auto value_sv = GetValueAt(found->value_offset, found->value_len);
      *value = Slice{std::string(value_sv)};
      return Status::OK();
    }
    }
  }

  void RecoverFromWal() {
    Slice key, value;
    while (wal_.ReadFromLogFile(&key, &value)) {
      uint32_t value_len = static_cast<uint32_t>(value.Size());

      uint32_t value_offset =
          static_cast<uint32_t>(value_arena_.CurrentOffset());
      if (value_len > 0) {
        Byte *dest = value_arena_.Allocate(value_len);
        std::memcpy(dest, value.GetData(), value_len);
      }

      uint32_t current_seq = seq_.fetch_add(1, std::memory_order_relaxed);

      switch (key_type_) {
      case ValueType::Type::Int: {
        int int_key = 0;
        if (key.Size() == sizeof(int)) {
          std::memcpy(&int_key, key.GetData(), sizeof(int));
        }
        int_entries_.push_back({int_key, value_offset, value_len, current_seq});
        break;
      }
      case ValueType::Type::String:
      default: {
        uint32_t key_len = static_cast<uint32_t>(key.Size());
        uint32_t key_offset = static_cast<uint32_t>(key_arena_.CurrentOffset());
        if (key_len > 0) {
          Byte *dest = key_arena_.Allocate(key_len);
          std::memcpy(dest, key.GetData(), key_len);
        }
        string_entries_.push_back(
            {key_offset, key_len, value_offset, value_len, current_seq});
        break;
      }
      }

      approximate_size_.fetch_add(key.Size() + value_len,
                                  std::memory_order_relaxed);
    }
    sorted_ = false;
  }

  // 序列化（需要先排序去重）
  std::string Serilize() {
    EnsureSorted();

    std::string res;
    res.reserve(approximate_size_.load());

    switch (key_type_) {
    case ValueType::Type::Int: {
      int last_key = 0;
      bool first = true;
      std::vector<size_t> deduped_indices;

      // 逆序遍历取每个 key 的最新版本
      for (size_t i = int_entries_.size(); i > 0; --i) {
        const auto &e = int_entries_[i - 1];
        if (first || e.key != last_key) {
          deduped_indices.push_back(i - 1);
          last_key = e.key;
          first = false;
        }
      }

      // 逆序输出恢复正序
      for (size_t i = deduped_indices.size(); i > 0; --i) {
        const auto &e = int_entries_[deduped_indices[i - 1]];
        if (e.value_len == 0)
          continue; // 跳过删除

        uint16_t key_len = sizeof(int);
        res.append(reinterpret_cast<const char *>(&key_len), sizeof(key_len));
        res.append(reinterpret_cast<const char *>(&e.key), sizeof(int));

        uint16_t value_len = static_cast<uint16_t>(e.value_len);
        res.append(reinterpret_cast<const char *>(&value_len),
                   sizeof(value_len));
        res.append(GetValueAt(e.value_offset, e.value_len));
      }
      break;
    }
    case ValueType::Type::String:
    default: {
      std::string_view last_key;
      bool first = true;
      std::vector<size_t> deduped_indices;

      for (size_t i = string_entries_.size(); i > 0; --i) {
        const auto &e = string_entries_[i - 1];
        auto key = GetStringKeyAt(e.key_offset, e.key_len);
        if (first || key != last_key) {
          deduped_indices.push_back(i - 1);
          last_key = key;
          first = false;
        }
      }

      for (size_t i = deduped_indices.size(); i > 0; --i) {
        const auto &e = string_entries_[deduped_indices[i - 1]];
        if (e.value_len == 0)
          continue;

        auto key_sv = GetStringKeyAt(e.key_offset, e.key_len);
        uint16_t key_len = static_cast<uint16_t>(key_sv.size());
        res.append(reinterpret_cast<const char *>(&key_len), sizeof(key_len));
        res.append(key_sv);

        uint16_t value_len = static_cast<uint16_t>(e.value_len);
        res.append(reinterpret_cast<const char *>(&value_len),
                   sizeof(value_len));
        res.append(GetValueAt(e.value_offset, e.value_len));
      }
      break;
    }
    }

    return res;
  }

  void ToImmutable() { wal_.Finish(); }

  std::filesystem::path GetWalPath() const { return wal_.GetPath(); }

  void DeleteWal() { WAL::Remove(wal_.GetPath()); }

  size_t GetApproximateSize() const { return approximate_size_.load(); }

  size_t Count() const {
    switch (key_type_) {
    case ValueType::Type::Int: return int_entries_.size();
    default: return string_entries_.size();
    }
  }

  // 整数主键：直接返回 entries 引用
  const std::vector<IntEntry> &GetIntEntries() const {
    EnsureSorted();
    return int_entries_;
  }

  const std::vector<StringEntry> &GetStringEntries() const {
    EnsureSorted();
    return string_entries_;
  }

  // 直接访问 Arena（零拷贝）
  const Byte *ValueData() const { return value_arena_.Data(); }
  const Byte *KeyData() const { return key_arena_.Data(); }

  // 通过索引获取 value 的裸指针
  bool GetValueRawByIndex(size_t idx, const Byte *&ptr, uint32_t &len) const {
    EnsureSorted();

    switch (key_type_) {
    case ValueType::Type::Int: {
      if (idx >= int_entries_.size())
        return false;
      const auto &e = int_entries_[idx];
      ptr = value_arena_.Data() + e.value_offset;
      len = e.value_len;
      return true;
    }
    default: {
      if (idx >= string_entries_.size())
        return false;
      const auto &e = string_entries_[idx];
      ptr = value_arena_.Data() + e.value_offset;
      len = e.value_len;
      return true;
    }
    }
  }

  class Iterator {
    const VectorizedMemTable *table_;
    size_t idx_;
    size_t end_;
    mutable Slice key_cache_;
    mutable Slice value_cache_;
    mutable bool cache_valid_{false};

    void RebuildCache() const {
      if (idx_ < end_ && !cache_valid_) {
        switch (table_->key_type_) {
        case ValueType::Type::Int: {
          const auto &e = table_->int_entries_[idx_];
          key_cache_ = Slice{e.key};
          auto val = table_->GetValueAt(e.value_offset, e.value_len);
          value_cache_ = Slice{std::string(val)};
          break;
        }
        default: {
          const auto &e = table_->string_entries_[idx_];
          auto key = table_->GetStringKeyAt(e.key_offset, e.key_len);
          key_cache_ = Slice{std::string(key)};
          auto val = table_->GetValueAt(e.value_offset, e.value_len);
          value_cache_ = Slice{std::string(val)};
          break;
        }
        }
        cache_valid_ = true;
      }
    }

  public:
    Iterator(const VectorizedMemTable *table, size_t idx, size_t end)
        : table_(table), idx_(idx), end_(end) {}

    bool Valid() const { return idx_ < end_; }

    void Next() {
      if (Valid()) {
        ++idx_;
        cache_valid_ = false;
      }
    }

    Slice &GetKey() {
      RebuildCache();
      return key_cache_;
    }

    Slice &GetValue() {
      RebuildCache();
      return value_cache_;
    }

    // 零拷贝访问
    std::string_view GetKeyView() const {
      switch (table_->key_type_) {
      case ValueType::Type::Int: {
        // 整数 key 无法返回 string_view，返回空
        return {};
      }
      default: {
        const auto &e = table_->string_entries_[idx_];
        return table_->GetStringKeyAt(e.key_offset, e.key_len);
      }
      }
    }

    std::string_view GetValueView() const {
      switch (table_->key_type_) {
      case ValueType::Type::Int: {
        const auto &e = table_->int_entries_[idx_];
        return table_->GetValueAt(e.value_offset, e.value_len);
      }
      default: {
        const auto &e = table_->string_entries_[idx_];
        return table_->GetValueAt(e.value_offset, e.value_len);
      }
      }
    }

    size_t GetIndex() const { return idx_; }
  };

  Iterator MakeIterator() const {
    EnsureSorted();
    return Iterator(this, 0, Count());
  }
};

using VectorizedMemTableRef = std::unique_ptr<VectorizedMemTable>;

} // namespace DB
