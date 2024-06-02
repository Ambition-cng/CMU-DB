//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  insert_finished_ = false;
  // Initialize the child executor
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  // Get the next tuple
  auto status = child_executor_->Next(&child_tuple, rid);

  if (!status && insert_finished_) {
    return false;
  }

  int32_t inserted_rows = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->TableOid());
  auto table_indexes = catalog->GetTableIndexes(table_info->name_);

  while (status) {
    // Insert into table
    auto new_rid =
        table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, child_tuple,
                                        exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());

    // Insert into indexes
    for (auto index_info : table_indexes) {
      index_info->index_->InsertEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *new_rid, exec_ctx_->GetTransaction());
    }

    ++inserted_rows;
    status = child_executor_->Next(&child_tuple, rid);
  }

  insert_finished_ = true;

  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(inserted_rows));
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
