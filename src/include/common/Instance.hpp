#pragma once

#include <memory>
namespace DB {
template <typename Tp>
class Instance : public std::enable_shared_from_this<Tp> {
public:
  std::shared_ptr<Tp> GetInstance() { return this->shared_from_this(); }
};
} // namespace DB