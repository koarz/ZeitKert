#include "execution/TupleExecutor.hpp"
#include "common/Status.hpp"

namespace DB {
Status TupleExecutor::Init() {
  Status status;
  for (auto &child : children_) {
    status = child->Init();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status TupleExecutor::Execute() {
  Status status;
  for (auto &child : children_) {
    status = child->Execute();
    if (!status.ok()) {
      return status;
    }
    for (auto col : child->GetSchema()->GetColumns()) {
      this->schema_->GetColumns().push_back(col);
    }
  }
  return Status::OK();
}
} // namespace DB