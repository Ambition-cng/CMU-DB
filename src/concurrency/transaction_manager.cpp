//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto write_set = *(txn->GetWriteSet());

  for (auto iter = write_set.rbegin(); iter != write_set.rend(); ++iter) {
    auto write_record = *iter;
    auto table_heap = write_record.table_heap_;
    if (write_record.wtype_ == WType::INSERT) {
      table_heap->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, write_record.rid_);
    } else {
      auto tuple_meta = table_heap->GetTupleMeta(write_record.rid_);
      table_heap->UpdateTupleMeta(TupleMeta{tuple_meta.insert_txn_id_, INVALID_TXN_ID, false}, write_record.rid_);
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
