//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  /** The child executor from which tuples are obtained for the left side of the join. */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The child executor from which tuples are obtained for the right side of the join. */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** The current tuple from the left child executor that is being processed. */
  Tuple left_tuple_;
  /** The current tuple from the right child executor that is used for matching with the left_tuple_. */
  Tuple right_tuple_;
  /** A flag indicating whether a match was found for the current left_tuple_ when performing a LEFT JOIN. */
  bool left_join_statisfied_;

  /** Indicates whether the inner loop (right side tuples) should continue iterating or reset for a new left tuple. */
  bool inner_loop_status_;
  /** Indicates whether there are more tuples to process in the outer loop (left side tuples). */
  bool outer_loop_status_;

  auto IsJoinConditionSatisfied() const -> bool;
  auto CombineTuples(bool right_null) const -> Tuple;
  auto GetValuesFromTuple(const Tuple &tuple, const Schema &schema, bool is_null) const -> std::vector<Value>;
};

}  // namespace bustub
