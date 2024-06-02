//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // Construct an index iterator from the Index object
  auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(
      exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
  index_iter_ = tree->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_iter_.IsEnd()) {
    return false;
  }

  *rid = (*index_iter_).second;

  // Use these RID to retrieve tuple in the corresponding table.
  auto catalog = exec_ctx_->GetCatalog();
  auto tuple_pair = catalog->GetTable(catalog->GetIndex(plan_->GetIndexOid())->table_name_)->table_->GetTuple(*rid);
  *tuple = tuple_pair.second;

  ++index_iter_;

  return true;
}

}  // namespace bustub
