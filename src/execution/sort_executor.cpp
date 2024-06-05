#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  tuple_pairs_.clear();

  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    tuple_pairs_.emplace_back(tuple, rid);
  }

  const auto &schema = child_executor_->GetOutputSchema();
  const auto &order_by_conditions = plan_->GetOrderBy();

  std::sort(tuple_pairs_.begin(), tuple_pairs_.end(),
            [&](const std::pair<Tuple, RID> &lhs, const std::pair<Tuple, RID> &rhs) {
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
            });

  index_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == tuple_pairs_.size()) {
    return false;
  }

  *tuple = tuple_pairs_[index_].first;
  *rid = tuple_pairs_[index_].second;

  ++index_;

  return true;
}

}  // namespace bustub
