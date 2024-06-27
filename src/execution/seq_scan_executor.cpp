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
  table_lock_granted_ = false;

  // Retrieve table lock based on the isolation level of transactions
  txn_ = exec_ctx_->GetTransaction();
  isolation_level_ = txn_->GetIsolationLevel();
  oid_ = plan_->GetTableOid();

  // Acquire a lock on the table to be scanned.
  LockTable();

  // Construct a table iterator from the TableHeap object
  table_iter_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(oid_)->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Check if the iterator has reached the end of the table; unlock table and return false if so.
  if (table_iter_->IsEnd()) {
    UnLockTable();
    return false;
  }

  // Get the current tuple's RID and lock the corresponding row.
  RID tuple_rid = table_iter_->GetRID();
  bool row_lock_granted = LockRow(tuple_rid);

  auto tuple_pair = table_iter_->GetTuple();
  // Skip deleted tuples by iterating until a valid one is found.
  while (tuple_pair.first.is_deleted_) {
    UnLockRow(row_lock_granted, tuple_rid, true);
    ++(*table_iter_);

    // Return false if the iterator reaches the end.
    if (table_iter_->IsEnd()) {
      UnLockTable();
      return false;
    }

    // Lock the new current row and fetch the tuple.
    tuple_rid = table_iter_->GetRID();
    row_lock_granted = LockRow(tuple_rid);
    tuple_pair = table_iter_->GetTuple();
  }

  // Set the output parameters with the found non-deleted tuple and its RID.
  *tuple = tuple_pair.second;
  *rid = tuple_rid;

  // Unlock the row and move the iterator forward.
  UnLockRow(row_lock_granted, tuple_rid);
  ++(*table_iter_);
  return true;
}

// Acquires a lock on the entire table based on transaction operation type and isolation level.
void SeqScanExecutor::LockTable() {
  try {
    bool should_lock = false;
    LockManager::LockMode lock_mode;

    if (exec_ctx_->IsDelete()) {
      // For DELETE operations, acquire an intention exclusive lock if not already locked.
      should_lock = !txn_->IsTableIntentionExclusiveLocked(oid_);
      lock_mode = LockManager::LockMode::INTENTION_EXCLUSIVE;
    } else {
      // For other operations, acquire an intention shared lock under certain conditions.
      should_lock = !txn_->IsTableIntentionSharedLocked(oid_) && !txn_->IsTableIntentionExclusiveLocked(oid_) &&
                    isolation_level_ != IsolationLevel::READ_UNCOMMITTED;
      lock_mode = LockManager::LockMode::INTENTION_SHARED;
    }

    // If the lock needs to be acquired, do so and indicate that the table lock has been granted.
    if (should_lock) {
      table_lock_granted_ = true;
      exec_ctx_->GetLockManager()->LockTable(txn_, lock_mode, oid_);
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("executor fails to acquire a lock");
  }
}

// Releases the lock acquired on the table if the isolation level is READ_COMMITTED.
void SeqScanExecutor::UnLockTable() {
  if (!exec_ctx_->IsDelete() && isolation_level_ == IsolationLevel::READ_COMMITTED && table_lock_granted_) {
    exec_ctx_->GetLockManager()->UnlockTable(txn_, oid_);
    table_lock_granted_ = false;
  }
}

// Acquires a lock on the specified row based on transaction operation type and isolation level.
auto SeqScanExecutor::LockRow(RID rid) -> bool {
  try {
    bool should_lock = false;
    LockManager::LockMode lock_mode;

    if (exec_ctx_->IsDelete()) {
      // For DELETE operations, acquire an exclusive lock on the row if not already locked.
      should_lock = !txn_->IsRowExclusiveLocked(oid_, rid);
      lock_mode = LockManager::LockMode::EXCLUSIVE;
    } else {
      // For other operations, acquire a shared lock on the row under certain conditions.
      should_lock = !txn_->IsRowSharedLocked(oid_, rid) && !txn_->IsRowExclusiveLocked(oid_, rid) &&
                    isolation_level_ != IsolationLevel::READ_UNCOMMITTED;
      lock_mode = LockManager::LockMode::SHARED;
    }

    // If the lock needs to be acquired, do so and return true to indicate success.
    if (should_lock) {
      exec_ctx_->GetLockManager()->LockRow(txn_, lock_mode, oid_, rid);
      return true;
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("executor fails to acquire a lock");
  }

  // Return false if no lock was acquired.
  return false;
}

// Releases the lock acquired on the specified row if the isolation level is READ_COMMITTED or if force is true.
void SeqScanExecutor::UnLockRow(bool row_lock_granted, RID rid, bool force) {
  if (row_lock_granted) {
    if (force) {
      exec_ctx_->GetLockManager()->UnlockRow(txn_, oid_, rid, true);
      return;
    }

    if (!exec_ctx_->IsDelete() && isolation_level_ == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->UnlockRow(txn_, oid_, rid);
    }
  }
}

}  // namespace bustub
