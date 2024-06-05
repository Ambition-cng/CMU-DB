//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/** HashJoinKey represents a key in a hash join operation */
struct HashJoinKey {
  /** The group-by values */
  std::vector<Value> join_keys_;

  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent join-key expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.join_keys_.size(); i++) {
      if (join_keys_[i].CompareEquals(other.join_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.join_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an HashJoinKey */
  auto MakeLeftJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an HashJoinKey */
  auto MakeRightJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_executor_->GetOutputSchema()));
    }
    return {keys};
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The child executor from which tuples are obtained for the left side of the join. */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The child executor from which tuples are obtained for the right side of the join. */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** A hash table mapping keys generated from the left or right tuples to vectors of those tuples. */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> left_ht_{};
  std::unordered_map<HashJoinKey, std::vector<Tuple>> right_ht_{};

  /** The current index within the vector of tuples in the left or right hash table that we're iterating over. */
  uint32_t left_ht_value_index_;
  uint32_t right_ht_value_index_;

  /** Hash table iterator of inner table*/
  std::unordered_map<HashJoinKey, std::vector<Tuple>>::const_iterator iter_;

  auto CombineTuples(const Tuple &left_tuple, const Tuple &right_tuple, bool right_null) const -> Tuple;
  auto GetValuesFromTuple(const Tuple &tuple, const Schema &schema, bool is_null) const -> std::vector<Value>;
};

}  // namespace bustub
