//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  // Fetch table map lock
  std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);

  // Retrieve or create the appropriate lock request queue for table-level locking.
  std::shared_ptr<LockRequestQueue> queue_ptr = table_lock_map_[oid];
  if (queue_ptr == nullptr) {
    // Initialize a new LockRequestQueue
    queue_ptr = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = queue_ptr;
  }

  LockRequestQueue *lock_request_queue = queue_ptr.get();
  std::unique_lock<std::mutex> queue_lock(lock_request_queue->latch_);
  table_map_lock.unlock();  // Release table map lock right after fetching queue lock

  return HandleLockRequest(txn, lock_request_queue, queue_lock, lock_mode, oid, std::nullopt);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // Fetch table map lock
  std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);

  // Retrieve the appropriate lock request queue for table-level locking.
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  LockRequestQueue *lock_request_queue = it->second.get();
  std::lock_guard<std::mutex> queue_lock_guard(lock_request_queue->latch_);
  table_map_lock.unlock();  // Release table map lock right after fetching queue lock

  return ReleaseLockAndCleanUp(txn, oid, std::nullopt, false, lock_request_queue);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode) || !CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    return false;
  }

  // Fetch row map lock
  std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);

  // Retrieve or create the appropriate lock request queue for row-level locking.
  std::shared_ptr<LockRequestQueue> queue_ptr = row_lock_map_[rid];
  if (queue_ptr == nullptr) {
    // Initialize a new LockRequestQueue
    queue_ptr = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = queue_ptr;
  }

  LockRequestQueue *lock_request_queue = queue_ptr.get();
  std::unique_lock<std::mutex> queue_lock(lock_request_queue->latch_);
  row_map_lock.unlock();  // Release row map lock right after fetching queue lock

  return HandleLockRequest(txn, lock_request_queue, queue_lock, lock_mode, oid, rid);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // Fetch row map lock
  std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);

  // Retrieve the appropriate lock request queue for row-level locking.
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  LockRequestQueue *lock_request_queue = it->second.get();
  std::lock_guard<std::mutex> queue_lock_guard(lock_request_queue->latch_);
  row_map_lock.unlock();  // Release row map lock right after fetching queue lock

  return ReleaseLockAndCleanUp(txn, oid, rid, force, lock_request_queue);
}

// Helper template function to delete all lock requests in a lock map.
template <typename LockMapType>
void ClearLockMap(LockMapType &lock_map, std::mutex &lock_map_latch) {
  std::lock_guard<std::mutex> lock_map_guard(lock_map_latch);
  for (auto &lock_request_queue_pair : lock_map) {
    auto &lock_request_queue = lock_request_queue_pair.second;
    if (lock_request_queue != nullptr) {
      std::lock_guard<std::mutex> queue_guard(lock_request_queue->latch_);
      for (auto &lock_request : lock_request_queue->request_queue_) {
        delete lock_request;
      }
      lock_request_queue->request_queue_.clear();
    }
  }
}

void LockManager::UnlockAll() {
  // Clean up table lock map and row lock map
  ClearLockMap(table_lock_map_, table_lock_map_latch_);
  ClearLockMap(row_lock_map_, row_lock_map_latch_);
}

// Adds an edge from t1 to t2 in the waits-for graph if it does not exist already.
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &waits_for_txn_ids = waits_for_[t1];

  // Find the correct position to insert the new edge to keep vector sorted.
  auto iter = std::lower_bound(waits_for_txn_ids.begin(), waits_for_txn_ids.end(), t2);
  if (iter == waits_for_txn_ids.end() || *iter != t2) {
    waits_for_txn_ids.insert(iter, t2);
  }
}

// Removes the edge from t1 to t2 in the waits-for graph.
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &waits_for_txn_ids = waits_for_[t1];

  // Find the edge and remove it if exists.
  auto iter = std::lower_bound(waits_for_txn_ids.begin(), waits_for_txn_ids.end(), t2);
  if (iter != waits_for_txn_ids.end() && *iter == t2) {
    waits_for_txn_ids.erase(iter);
  }
}

// Checks if there's a cycle in the waits-for graph and identifies the youngest transaction ID in the cycle.
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> path;            // Keeps track of the current search path for cycles.
  std::unordered_set<txn_id_t> visited;  // Tracks visited transactions to avoid revisiting.
  std::unordered_set<txn_id_t> on_path;  // Tracks transactions currently being explored in the recursion stack.

  // Sort transaction IDs to maintain a deterministic order when exploring cycles.
  std::vector<txn_id_t> waits_for_txn_ids;
  for (const auto &waits_for_pair : waits_for_) {
    waits_for_txn_ids.push_back(waits_for_pair.first);
  }
  std::sort(waits_for_txn_ids.begin(), waits_for_txn_ids.end());

  for (const auto &waits_for_txn_id : waits_for_txn_ids) {
    if (visited.count(waits_for_txn_id) == 0) {
      txn_id_t abort_txn_id;

      // If a cycle is found, find the position of the abort_txn_id in the path.
      // Identify the youngest transaction ID in the cycle from the point of abort_txn_id.
      if (FindCycle(waits_for_txn_id, path, on_path, visited, &abort_txn_id)) {
        auto iter = std::find(path.begin(), path.end(), abort_txn_id);
        auto youngest_txn_id_iter = std::max_element(iter, path.end());
        *txn_id = *youngest_txn_id_iter;
        return true;
      }
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  std::lock_guard<std::mutex> graph_guard(waits_for_latch_);

  for (const auto &waits_for_pair : waits_for_) {
    auto txn_id = waits_for_pair.first;
    auto waits_for_txn_ids = waits_for_pair.second;

    for (const auto &waits_for_txn_id : waits_for_txn_ids) {
      edges.emplace_back(std::make_pair(txn_id, waits_for_txn_id));
    }
  }

  return edges;
}

// Breaks all edges associated with a transaction involved in a cycle to resolve it.
void LockManager::BreakCycle(txn_id_t victim_txn_id) {
  auto wait_for_txn_ids = waits_for_[victim_txn_id];
  // Remove all outgoing edges from the victim transaction.
  for (const auto &wait_for_txn_id : wait_for_txn_ids) {
    RemoveEdge(victim_txn_id, wait_for_txn_id);
  }

  // Remove all incoming edges to the victim transaction.
  for (const auto &waits_for_pair : waits_for_) {
    txn_id_t txn_id = waits_for_pair.first;
    if (txn_id != victim_txn_id) {
      RemoveEdge(txn_id, victim_txn_id);
    }
  }
}

// Periodically runs cycle detection and resolves any detected cycles.
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    std::unique_lock<std::mutex> waits_for_lock(waits_for_latch_);

    // Construct the waits-for graph.
    BuildWaitsForGraph();

    txn_id_t victim_txn_id;
    while (HasCycle(&victim_txn_id)) {
      // Abort the transaction that is chosen as the victim (youngest).
      auto txn = txn_manager_->GetTransaction(victim_txn_id);
      txn_manager_->Abort(txn);

      // Break the cycle by removing relevant edges in the waits-for graph.
      BreakCycle(victim_txn_id);
    }
  }
}

// Helper template function to traverse lock map and add waits-for edges.
template <typename LockMapType>
void LockManager::AddWaitsForEdges(LockMapType &lock_map, std::mutex &lock_map_latch) {
  std::lock_guard<std::mutex> map_guard(lock_map_latch);
  for (const auto &lock_pair : lock_map) {
    auto &lock_request_queue = lock_pair.second;
    std::lock_guard<std::mutex> queue_lock(lock_request_queue->latch_);

    auto &request_queue = lock_request_queue->request_queue_;
    for (const auto &granted_lock_req : request_queue) {
      if (granted_lock_req->granted_) {
        continue;
      }

      txn_id_t granted_txn_id = granted_lock_req->txn_id_;
      for (const auto &waiting_lock_req : request_queue) {
        if (!waiting_lock_req->granted_) {
          break;
        }

        AddEdge(waiting_lock_req->txn_id_, granted_txn_id);
      }
    }
  }
}

void LockManager::BuildWaitsForGraph() {
  // Destroy previous waits-for graph
  waits_for_.clear();

  // Traverse table lock map and row lock map, then add waits-for edges
  AddWaitsForEdges(table_lock_map_, table_lock_map_latch_);
  AddWaitsForEdges(row_lock_map_, row_lock_map_latch_);
}

// Handle both table and row lock request depending on whether rid is provided or not.
auto LockManager::HandleLockRequest(Transaction *txn, LockRequestQueue *lock_request_queue,
                                    std::unique_lock<std::mutex> &queue_lock, LockMode lock_mode,
                                    const table_oid_t &oid, const std::optional<RID> &rid) -> bool {
  bool row_lock_request = rid.has_value();

  LockRequest *new_lock_request = GetOrCreateLockRequest(txn, lock_mode, oid, rid, lock_request_queue);

  // Wait for the lock to be granted or for the transaction to be aborted.
  WairForLockGranted(new_lock_request, queue_lock, txn, lock_request_queue);

  if (txn->GetState() == TransactionState::ABORTED) {
    CleanUpAbortedTxn(txn->GetTransactionId(), lock_request_queue, new_lock_request);
    return false;
  }

  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  if (row_lock_request) {
    UpgradeLockRow(txn, lock_mode, oid, rid.value());
  } else {
    UpgradeLockTable(txn, lock_mode, oid);
  }
  return true;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->IsTableIntentionSharedLocked(oid)) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (txn->IsTableSharedLocked(oid)) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  } else if (txn->IsTableExclusiveLocked(oid)) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  }

  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
  }

  return true;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->IsRowSharedLocked(oid, rid)) {
    auto shared_row_lock_set_map = txn->GetSharedRowLockSet();
    (*shared_row_lock_set_map)[oid].erase(rid);
  } else if (txn->IsRowExclusiveLocked(oid, rid)) {
    auto exclusive_row_lock_set_map = txn->GetExclusiveRowLockSet();
    (*exclusive_row_lock_set_map)[oid].erase(rid);
  }

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedRowLockSet()->operator[](oid).insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveRowLockSet()->operator[](oid).insert(rid);
      break;
    default:
      AbortTransaction(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      return false;
  }

  return true;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  return (l1 == LockMode::INTENTION_SHARED && l2 != LockMode::EXCLUSIVE) ||
         (l1 != LockMode::EXCLUSIVE && l2 == LockMode::INTENTION_SHARED) ||
         (l1 == LockMode::INTENTION_EXCLUSIVE && l2 == LockMode::INTENTION_EXCLUSIVE) ||
         (l1 == LockMode::SHARED && l2 == LockMode::SHARED);
}

auto LockManager::CheckCompatibility(LockRequestQueue *lock_request_queue, txn_id_t txn_id, LockMode lock_mode)
    -> bool {
  const auto &request_queue = lock_request_queue->request_queue_;

  auto lock_req_iter = std::find_if(request_queue.cbegin(), request_queue.cend(),
                                    [txn_id](const LockRequest *lock_req) { return lock_req->txn_id_ == txn_id; });

  // Check if all granted locks in the queue are compatible with the requested lock mode.
  bool are_all_compatible =
      std::all_of(request_queue.cbegin(), lock_req_iter, [lock_mode, this](const LockRequest *other_req) {
        return other_req->granted_ && AreLocksCompatible(lock_mode, other_req->lock_mode_);
      });

  return are_all_compatible;
}

// Check if this transaction can take the requested lock and return false if it cannot.
auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  auto isolation_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();

  // Throw an exception if the transaction is in the SHRINKING state and either
  // the isolation level is not READ_COMMITTED or the lock mode is not shared.
  if (txn_state != TransactionState::GROWING &&
      (isolation_level != IsolationLevel::READ_COMMITTED ||
       (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED))) {
    AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
  }

  // For READ_UNCOMMITTED isolation level, throw an exception if the lock mode is exclusive,
  // as shared locks are not allowed.
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  // In all other cases, the transaction can take the lock.
  return true;
}

// Fetch or create a new lock request for this transaction.
auto LockManager::GetOrCreateLockRequest(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                         const std::optional<RID> &rid, LockRequestQueue *lock_request_queue)
    -> LockRequest * {
  txn_id_t txn_id = txn->GetTransactionId();
  LockRequest *new_lock_request = nullptr;

  auto &request_queue = lock_request_queue->request_queue_;
  for (auto &lock_req : request_queue) {
    if (lock_req->txn_id_ == txn_id && lock_req->granted_) {
      if (lock_req->lock_mode_ == lock_mode) {
        return lock_req;  // Return immediately as the lock is already held.
      }

      // Handle potential lock upgrade.
      if (CanLockUpgrade(lock_req->lock_mode_, lock_mode)) {
        if (lock_request_queue->upgrading_ == INVALID_TXN_ID) {
          // Release the currently held lock and mark the upgrading flag in the queue
          lock_request_queue->upgrading_ = txn_id;
          if (rid.has_value()) {
            ReleaseLocksForRow(txn, oid, rid.value());
          } else {
            ReleaseLocksForTable(txn, oid);
          }

          // Move upgrading lock request to the beginning of ungranted lock request.
          auto upgrading_iter = std::find_if(request_queue.begin(), request_queue.end(),
                                             [lock_request_queue](const LockRequest *lock_req) {
                                               return lock_req->txn_id_ == lock_request_queue->upgrading_;
                                             });
          auto ungranted_iter = std::find_if(request_queue.begin(), request_queue.end(),
                                             [](const LockRequest *lock_req) { return !lock_req->granted_; });
          if (upgrading_iter != ungranted_iter) {
            if (ungranted_iter != request_queue.end()) {
              request_queue.splice(ungranted_iter, request_queue, upgrading_iter);
            } else {
              request_queue.splice(request_queue.end(), request_queue, upgrading_iter);
            }
          }

          // Reuse previous lock request for upgrading
          new_lock_request = lock_req;
          new_lock_request->lock_mode_ = lock_mode;
          new_lock_request->granted_ = false;

          break;
        }

        // There's already a different transaction upgrading, so abort this one.
        AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
        return nullptr;
      }

      // This transaction cannot upgrade its lock, so abort it.
      AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      return nullptr;
    }
  }

  if (new_lock_request == nullptr) {  // No existing lock found, create a new one.
    if (rid.has_value()) {
      new_lock_request = new LockRequest{txn_id, lock_mode, oid, rid.value()};
    } else {
      new_lock_request = new LockRequest{txn_id, lock_mode, oid};
    }
    lock_request_queue->request_queue_.push_back(new_lock_request);
  }

  return new_lock_request;
}

void LockManager::WairForLockGranted(LockRequest *lock_request, std::unique_lock<std::mutex> &queue_lock,
                                     Transaction *txn, LockRequestQueue *lock_request_queue) {
  lock_request->granted_ = CheckCompatibility(lock_request_queue, txn->GetTransactionId(), lock_request->lock_mode_);
  lock_request_queue->cv_.wait(queue_lock,
                               [&] { return txn->GetState() == TransactionState::ABORTED || lock_request->granted_; });
}

// Iterate through all requests and grant locks where possible.
void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
  for (auto &lock_req : lock_request_queue->request_queue_) {
    if (!lock_req->granted_) {
      bool can_grant = CheckCompatibility(lock_request_queue, lock_req->txn_id_, lock_req->lock_mode_);
      if (can_grant) {
        lock_req->granted_ = true;
      } else {
        // If a lock cannot be granted, notify all and return immediately to prevent starvation.
        lock_request_queue->cv_.notify_all();
        return;
      }
    }
  }
  // Notify all at the end in case any locks were granted.
  lock_request_queue->cv_.notify_all();
}

void LockManager::ReleaseLocksForTable(Transaction *txn, const table_oid_t &oid) {
  if (txn->IsTableIntentionSharedLocked(oid)) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (txn->IsTableSharedLocked(oid)) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  } else if (txn->IsTableExclusiveLocked(oid)) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  }
}

void LockManager::ReleaseLocksForRow(Transaction *txn, const table_oid_t &oid, const RID &rid) {
  if (txn->IsRowSharedLocked(oid, rid)) {
    auto shared_row_lock_set_map = txn->GetSharedRowLockSet();
    (*shared_row_lock_set_map)[oid].erase(rid);
  } else if (txn->IsRowExclusiveLocked(oid, rid)) {
    auto exclusive_row_lock_set_map = txn->GetExclusiveRowLockSet();
    (*exclusive_row_lock_set_map)[oid].erase(rid);
  }
}

void LockManager::UpdateTransactionState(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  auto isolation_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();

  if (txn_state == TransactionState::GROWING) {
    if (lock_mode == LockMode::EXCLUSIVE ||
        (lock_mode == LockMode::SHARED && isolation_level == IsolationLevel::REPEATABLE_READ)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}

auto LockManager::ReleaseLockAndCleanUp(Transaction *txn, const table_oid_t &oid, const std::optional<RID> &rid,
                                        bool force, LockRequestQueue *lock_request_queue) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();

  // Iterate over the lock requests to find the one corresponding to the transaction.
  auto &request_queue = lock_request_queue->request_queue_;
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if ((*iter)->txn_id_ == txn_id) {
      if (!(*iter)->granted_ && lock_request_queue->upgrading_ != txn_id) {
        AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      }

      // Update transaction state and release the lock.
      if (rid.has_value()) {
        ReleaseLocksForRow(txn, oid, rid.value());
        if (!force) {
          UpdateTransactionState(txn, (*iter)->lock_mode_, oid);
        }
      } else {
        if (!CanTxnReleaseTableLock(txn, oid)) {
          AbortTransaction(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
        }

        ReleaseLocksForTable(txn, oid);
        UpdateTransactionState(txn, (*iter)->lock_mode_, oid);
      }

      delete *iter;
      request_queue.erase(iter);

      // Try to grant locks to other transactions waiting in the queue.
      GrantNewLocksIfPossible(lock_request_queue);

      return true;
    }
  }

  AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::CleanUpAbortedTxn(const txn_id_t &txn_id, LockRequestQueue *lock_request_queue,
                                    LockRequest *lock_request) {
  auto &request_queue = lock_request_queue->request_queue_;
  auto iter = std::find_if(request_queue.begin(), request_queue.end(),
                           [lock_request](const LockRequest *req) { return req == lock_request; });
  if (iter != request_queue.end()) {
    delete *iter;
    request_queue.erase(iter);
  }

  if (lock_request_queue->upgrading_ == txn_id) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  GrantNewLocksIfPossible(lock_request_queue);
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  return curr_lock_mode == LockMode::INTENTION_SHARED ||
         (curr_lock_mode == LockMode::SHARED && (requested_lock_mode == LockMode::EXCLUSIVE ||
                                                 requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) ||
         (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
          (requested_lock_mode == LockMode::EXCLUSIVE ||
           requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) ||
         (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && requested_lock_mode == LockMode::EXCLUSIVE);
}

auto LockManager::CanTxnReleaseTableLock(Transaction *txn, const table_oid_t &oid) -> bool {
  auto shared_row_lock_set_map = txn->GetSharedRowLockSet();
  auto exclusive_row_lock_set_map = txn->GetExclusiveRowLockSet();

  return (shared_row_lock_set_map->count(oid) == 0 || (*shared_row_lock_set_map)[oid].empty()) &&
         (exclusive_row_lock_set_map->count(oid) == 0 || (*exclusive_row_lock_set_map)[oid].empty());
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  if (row_lock_mode == LockMode::SHARED) {
    if (txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
        txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return true;
    }
  } else if (row_lock_mode == LockMode::EXCLUSIVE) {
    if (txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
        txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return true;
    }
  } else {
    AbortTransaction(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  AbortTransaction(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
  return false;
}

// Recursive function that performs Depth-First Search to find a cycle in the graph.
auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  // If the transaction is already on the path, a cycle is detected.
  if (on_path.count(source_txn) == 1) {
    *abort_txn_id = source_txn;
    return true;
  }

  // If the transaction has not been visited, explore its edges.
  if (visited.count(source_txn) == 0) {
    visited.insert(source_txn);
    on_path.insert(source_txn);
    path.push_back(source_txn);

    // Explore transactions that the current transaction is waiting for.
    auto &waits_for_txn_ids = waits_for_[source_txn];
    for (const auto &waits_for_txn_id : waits_for_txn_ids) {
      // Recursively look for cycles.
      if (FindCycle(waits_for_txn_id, path, on_path, visited, abort_txn_id)) {
        return true;
      }
    }

    // Backtrack: no cycle found from this transaction, remove it from path and on_path sets.
    on_path.erase(source_txn);
    path.pop_back();
  }

  return false;
}

void LockManager::AbortTransaction(Transaction *txn, AbortReason reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
}

}  // namespace bustub
