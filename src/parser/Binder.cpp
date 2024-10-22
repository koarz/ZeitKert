#include "parser/Binder.hpp"
#include "common/Status.hpp"

namespace DB {

Status Binder::Parse(std::string_view query,
                     std::shared_ptr<QueryContext> context,
                     ResultSet &result_set) {

  auto status = parser_.Parse(query, context, result_set);

  // plan

  return status;
}
} // namespace DB