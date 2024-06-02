//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  update_finished_ = false;
  // Initialize the child executor
  child_executor_->Init();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  // Get the next tuple
  auto status = child_executor_->Next(&child_tuple, rid);

  if (!status && update_finished_) {
    return false;
  }

  int32_t update_rows = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->TableOid());
  auto table_indexes = catalog->GetTableIndexes(table_info->name_);

  while (status) {
    // Compute expressions
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    // Delete from table
    table_info->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
    // Insert into table
    auto new_rid =
        table_info->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple,
                                        exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());

    // Update indexes
    for (auto index_info : table_indexes) {
      // Delete from index
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, exec_ctx_->GetTransaction());
      // Insert into index
      index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *new_rid, exec_ctx_->GetTransaction());
    }

    ++update_rows;
    status = child_executor_->Next(&child_tuple, rid);
  }

  update_finished_ = true;

  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(update_rows));
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
