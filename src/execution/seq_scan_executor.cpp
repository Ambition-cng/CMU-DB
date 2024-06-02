//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // Construct a table iterator from the TableHeap object
  table_iter_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_->IsEnd()) {
    return false;
  }

  auto tuple_pair = table_iter_->GetTuple();
  while (tuple_pair.first.is_deleted_) {  // jump over deleted tuples
    ++(*table_iter_);

    if (table_iter_->IsEnd()) {
      return false;
    }

    tuple_pair = table_iter_->GetTuple();
  }

  *tuple = tuple_pair.second;
  *rid = table_iter_->GetRID();

  ++(*table_iter_);
  return true;
}

}  // namespace bustub
