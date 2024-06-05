//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  uint32_t index_;

  /** Ascending order-by type corresponding to the large top heap */
  /** Descending order-by type corresponding to the small top heap */
  std::vector<std::pair<Tuple, RID>> top_entries_;

  // Return the result of comparison based on the order type.
  auto CompareFunction(const std::pair<Tuple, RID> &lhs, const std::pair<Tuple, RID> &rhs) const -> bool {
    const auto &schema = child_executor_->GetOutputSchema();
    const auto &order_by_conditions = plan_->GetOrderBy();
    for (const auto &order_by : order_by_conditions) {
      auto order_by_type = order_by.first;
      auto expr = order_by.second;

      auto lhs_value = expr->Evaluate(&lhs.first, schema);
      auto rhs_value = expr->Evaluate(&rhs.first, schema);

      // If the values are equal, continue to the next ordering condition.
      if (lhs_value.CompareEquals(rhs_value) == CmpBool::CmpTrue) {
        continue;
      }

      // Return the result of comparison based on the order type.
      // OrderByType::ASC --- lhs_value < rhs_value
      // OrderByType::DESC --- lhs_value > rhs_value
      return (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT)
                 ? lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue
                 : lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue;
    }
    return false;
  }

  // Adds a new entry to the heap.
  void Push(const std::pair<Tuple, RID> &tuple_pair) {
    top_entries_.push_back(tuple_pair);
    std::push_heap(top_entries_.begin(), top_entries_.end(),
                   [this](const auto &lhs, const auto &rhs) { return CompareFunction(lhs, rhs); });
  }

  // Removes and returns the top entry from the heap.
  auto Pop() -> std::pair<Tuple, RID> {
    std::pop_heap(top_entries_.begin(), top_entries_.end(),
                  [this](const auto &lhs, const auto &rhs) { return CompareFunction(lhs, rhs); });
    std::pair<Tuple, RID> tuple_pair = top_entries_.back();
    top_entries_.pop_back();
    return tuple_pair;
  }
};
}  // namespace bustub
