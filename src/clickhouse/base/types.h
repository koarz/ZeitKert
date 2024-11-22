#pragma once

#include <cstdint>
#include <string>

/// Using char8_t more strict aliasing (https://stackoverflow.com/a/57453713)
using UInt8 = char8_t;

/// Same for using signed _BitInt(8) (there isn't a signed char8_t, which would
/// be more convenient) See https://godbolt.org/z/fafnWEnnf
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbit-int-extension"
using Int8 = int8_t;
#pragma clang diagnostic pop

using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using Float32 = float;
using Float64 = double;

using String = std::string;
