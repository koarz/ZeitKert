#include "function/FunctionSimdSum.hpp"
#include "storage/Block.hpp"
#include "storage/column/ColumnVector.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"

#include <immintrin.h>

namespace DB {

// AVX2 SIMD 求和 double：4 个累加器 × 4 double = 每次处理 16 个 double
__attribute__((target("avx2")))
static double SimdSumDouble(const double *data, size_t n) {
  size_t i = 0;

  // 4 个独立累加器隐藏 FP add 延迟（Zen3 延迟 3 周期，吞吐 2/周期）
  __m256d acc0 = _mm256_setzero_pd();
  __m256d acc1 = _mm256_setzero_pd();
  __m256d acc2 = _mm256_setzero_pd();
  __m256d acc3 = _mm256_setzero_pd();

  for (; i + 16 <= n; i += 16) {
    acc0 = _mm256_add_pd(acc0, _mm256_loadu_pd(data + i));
    acc1 = _mm256_add_pd(acc1, _mm256_loadu_pd(data + i + 4));
    acc2 = _mm256_add_pd(acc2, _mm256_loadu_pd(data + i + 8));
    acc3 = _mm256_add_pd(acc3, _mm256_loadu_pd(data + i + 12));
  }

  // 合并 4 个累加器
  acc0 = _mm256_add_pd(acc0, acc1);
  acc2 = _mm256_add_pd(acc2, acc3);
  acc0 = _mm256_add_pd(acc0, acc2);

  // 处理剩余的 4 元素块
  for (; i + 4 <= n; i += 4) {
    acc0 = _mm256_add_pd(acc0, _mm256_loadu_pd(data + i));
  }

  // 水平归约 256→标量
  __m128d lo = _mm256_castpd256_pd128(acc0);
  __m128d hi = _mm256_extractf128_pd(acc0, 1);
  lo = _mm_add_pd(lo, hi);
  lo = _mm_add_pd(lo, _mm_shuffle_pd(lo, lo, 1));
  double sum;
  _mm_store_sd(&sum, lo);

  // 尾部标量处理
  for (; i < n; i++) {
    sum += data[i];
  }
  return sum;
}

// AVX2 SIMD 求和 int→double：load 4 int → cvt → add as double
__attribute__((target("avx2")))
static double SimdSumInt(const int *data, size_t n) {
  size_t i = 0;

  __m256d acc0 = _mm256_setzero_pd();
  __m256d acc1 = _mm256_setzero_pd();
  __m256d acc2 = _mm256_setzero_pd();
  __m256d acc3 = _mm256_setzero_pd();

  for (; i + 16 <= n; i += 16) {
    acc0 = _mm256_add_pd(
        acc0, _mm256_cvtepi32_pd(_mm_loadu_si128(
                  reinterpret_cast<const __m128i *>(data + i))));
    acc1 = _mm256_add_pd(
        acc1, _mm256_cvtepi32_pd(_mm_loadu_si128(
                  reinterpret_cast<const __m128i *>(data + i + 4))));
    acc2 = _mm256_add_pd(
        acc2, _mm256_cvtepi32_pd(_mm_loadu_si128(
                  reinterpret_cast<const __m128i *>(data + i + 8))));
    acc3 = _mm256_add_pd(
        acc3, _mm256_cvtepi32_pd(_mm_loadu_si128(
                  reinterpret_cast<const __m128i *>(data + i + 12))));
  }

  acc0 = _mm256_add_pd(acc0, acc1);
  acc2 = _mm256_add_pd(acc2, acc3);
  acc0 = _mm256_add_pd(acc0, acc2);

  for (; i + 4 <= n; i += 4) {
    acc0 = _mm256_add_pd(
        acc0, _mm256_cvtepi32_pd(_mm_loadu_si128(
                  reinterpret_cast<const __m128i *>(data + i))));
  }

  __m128d lo = _mm256_castpd256_pd128(acc0);
  __m128d hi = _mm256_extractf128_pd(acc0, 1);
  lo = _mm_add_pd(lo, hi);
  lo = _mm_add_pd(lo, _mm_shuffle_pd(lo, lo, 1));
  double sum;
  _mm_store_sd(&sum, lo);

  for (; i < n; i++) {
    sum += data[i];
  }
  return sum;
}

Status FunctionSimdSum::ResolveResultType(
    Block &block, std::shared_ptr<ValueType> &result_type) const {
  if (block.Size() != 1) {
    return Status::Error(ErrorCode::BindError,
                         "SIMD_SUM requires exactly 1 argument");
  }
  auto input_type = block.GetColumn(0)->GetValueType()->GetType();
  if (input_type != ValueType::Type::Int &&
      input_type != ValueType::Type::Double) {
    return Status::Error(ErrorCode::BindError,
                         "SIMD_SUM only supports INT or DOUBLE columns");
  }
  result_type = std::make_shared<Double>();
  return Status::OK();
}

Status FunctionSimdSum::ExecuteImpl(Block &block, size_t result_idx,
                                    size_t input_rows_count) const {
  if (result_idx != 1) {
    return Status::Error(ErrorCode::BindError,
                         "SIMD_SUM requires exactly 1 argument");
  }

  auto input_col = block.GetColumn(0);
  auto input_type = input_col->GetValueType()->GetType();
  double sum = 0.0;

  switch (input_type) {
  case ValueType::Type::Int: {
    auto &col = static_cast<ColumnVector<int> &>(*input_col->GetColumn());
    if (col.HasSpans()) {
      // 零拷贝路径：直接从 mmap 指针 SIMD 求和
      for (const auto &span : col.Spans()) {
        sum += SimdSumInt(span.ptr, span.count);
      }
    } else {
      sum = SimdSumInt(col.Data().data(), input_rows_count);
    }
    break;
  }
  case ValueType::Type::Double: {
    auto &col = static_cast<ColumnVector<double> &>(*input_col->GetColumn());
    if (col.HasSpans()) {
      for (const auto &span : col.Spans()) {
        sum += SimdSumDouble(span.ptr, span.count);
      }
    } else {
      sum = SimdSumDouble(col.Data().data(), input_rows_count);
    }
    break;
  }
  default:
    return Status::Error(ErrorCode::BindError,
                         "SIMD_SUM only supports INT or DOUBLE columns");
  }

  auto &res = static_cast<ColumnVector<double> &>(
      *block.GetColumn(result_idx)->GetColumn());
  res.Insert(sum);
  return Status::OK();
}
} // namespace DB
