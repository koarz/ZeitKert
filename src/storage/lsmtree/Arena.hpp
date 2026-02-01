#pragma once

#include "common/Config.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>

namespace DB {

/**
 * Arena - 连续内存分配器
 *
 * 设计说明：
 * 为了支持 VectorizedMemTable 的 offset 访问模式，Arena 使用单块连续内存。
 * 当容量不足时，重新分配更大的块并拷贝数据（类似 std::vector）。
 *
 * 初始容量为 64KB，每次扩容 2 倍。
 * MemTable 最大为 64MB (SSTABLE_SIZE)，所以最多扩容 10 次。
 */
class Arena {
  static constexpr size_t kInitialCapacity = 64 * 1024;  // 64KB 初始容量
  static constexpr size_t kGrowthFactor = 2;

  std::unique_ptr<Byte[]> data_;
  size_t capacity_{0};
  size_t size_{0};  // 当前已使用的字节数

  void Grow(size_t min_capacity) {
    size_t new_capacity = capacity_ == 0 ? kInitialCapacity : capacity_;
    while (new_capacity < min_capacity) {
      new_capacity *= kGrowthFactor;
    }

    auto new_data = std::make_unique<Byte[]>(new_capacity);
    if (data_ && size_ > 0) {
      std::memcpy(new_data.get(), data_.get(), size_);
    }
    data_ = std::move(new_data);
    capacity_ = new_capacity;
  }

public:
  Arena() = default;

  Arena(const Arena &) = delete;
  Arena &operator=(const Arena &) = delete;

  Arena(Arena &&other) noexcept
      : data_(std::move(other.data_)), capacity_(other.capacity_),
        size_(other.size_) {
    other.capacity_ = 0;
    other.size_ = 0;
  }

  Arena &operator=(Arena &&other) noexcept {
    if (this != &other) {
      data_ = std::move(other.data_);
      capacity_ = other.capacity_;
      size_ = other.size_;
      other.capacity_ = 0;
      other.size_ = 0;
    }
    return *this;
  }

  ~Arena() = default;

  // 分配指定字节数，返回指向分配内存的指针
  Byte *Allocate(size_t bytes) {
    if (bytes == 0) {
      return nullptr;
    }

    size_t required = size_ + bytes;
    if (required > capacity_) {
      Grow(required);
    }

    Byte *result = data_.get() + size_;
    size_ += bytes;
    return result;
  }

  // 拷贝数据到 Arena 并返回指针
  Byte *CopyIn(const Byte *src, size_t len) {
    if (src == nullptr || len == 0) {
      return nullptr;
    }
    Byte *dest = Allocate(len);
    std::memcpy(dest, src, len);
    return dest;
  }

  // 获取 Arena 数据的基地址（用于 offset 计算）
  const Byte *Data() const { return data_.get(); }

  Byte *Data() { return data_.get(); }

  // 当前已使用的字节数（下一次分配的 offset）
  size_t CurrentOffset() const { return size_; }

  // 已分配的总容量
  size_t Capacity() const { return capacity_; }

  // 内存使用量
  size_t MemoryUsage() const { return size_; }

  // 重置（清空数据但保留容量）
  void Reset() { size_ = 0; }
};

}  // namespace DB
