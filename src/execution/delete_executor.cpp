//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  delete_finished_ = false;
  // Initialize the child executor
  child_executor_->Init();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  // Get the next tuple
  auto status = child_executor_->Next(&child_tuple, rid);

  if (!status && delete_finished_) {
    return false;
  }

  int32_t delete_rows = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->TableOid());
  auto table_indexes = catalog->GetTableIndexes(table_info->name_);

  while (status) {
    auto tuple_meta = table_info->table_->GetTupleMeta(*rid);
    // Delete from table
    table_info->table_->UpdateTupleMeta(
        TupleMeta{tuple_meta.insert_txn_id_, exec_ctx_->GetTransaction()->GetTransactionId(), true}, *rid);

    TableWriteRecord write_record = {plan_->TableOid(), *rid, table_info->table_.get()};
    write_record.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(write_record);

    // Delete from  indexes
    for (auto index_info : table_indexes) {
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, exec_ctx_->GetTransaction());
    }

    ++delete_rows;
    status = child_executor_->Next(&child_tuple, rid);
  }

  delete_finished_ = true;

  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(delete_rows));
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub
