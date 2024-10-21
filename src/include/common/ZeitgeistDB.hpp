#pragma once

#include "common/Instance.hpp"
#include "common/Status.hpp"
#include <iostream>
#include <string>

namespace DB {
class ZeitgeistDB : public Instance<ZeitgeistDB> {
public:
  Status ExecuteQuery(std::string &query) {
    std::cout << query << '\n';
    return Status::OK();
  }
};
} // namespace DB