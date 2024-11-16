#include "execution/InsertExecutor.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <memory>
#include <vector>

namespace DB {
Status InsertExecutor::Init() {
  Status status;
  for (auto &child : children_) {
    status = child->Init();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status InsertExecutor::Execute() {
  Status status;
  bool first{true};
  auto col_meta = table_meta_->GetColumns();
  auto &begin_row = table_meta_->GetRowNumber();
  uint32_t inserted_row{};
  for (auto &child : children_) {
    status = child->Execute();
    if (!status.ok()) {
      return status;
    }
    if (child->GetSchema()->GetColumns().size() != col_meta.size()) {
      return Status::Error(ErrorCode::InsertError,
                           "Some tuple size is not match table's column num");
    }
    int i = 0;
    for (auto col : child->GetSchema()->GetColumns()) {
      for (int j = 0; j < col->Size(); j++) {
        status = col_meta[i]->lsm_tree_->Insert(Slice{&begin_row, 4},
                                                Slice{col->GetStrElement(j)});
        if (!status.ok()) {
          return status;
        }
        begin_row++;
      }
      i++;
      begin_row -= col->Size();
    }
    inserted_row += child->GetSchema()->GetColumns()[0]->Size();
    begin_row += child->GetSchema()->GetColumns()[0]->Size();
  }
  auto res = std::make_shared<ColumnVector<int>>();
  res->Insert(inserted_row);
  this->schema_->GetColumns().push_back(std::make_shared<ColumnWithNameType>(
      res, "InsertRows", std::make_shared<Int>()));
  return Status::OK();
}
} // namespace DB