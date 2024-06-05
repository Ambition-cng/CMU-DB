//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // Initialize the child executor
  left_executor_->Init();

  outer_loop_status_ = true;
  inner_loop_status_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid{};
  RID right_rid{};

  // Iterate over tuples from the left table
  while (outer_loop_status_) {
    // When starting a new outer loop or after completing the inner loop, fetch the next tuple from the left table
    if (!inner_loop_status_) {
      outer_loop_status_ = left_executor_->Next(&left_tuple_, &left_rid);
      if (!outer_loop_status_) {
        return false;
      }

      // Prepare the right executor for a new round of iteration over the right table's tuples
      right_executor_->Init();
      inner_loop_status_ = true;
      left_join_statisfied_ = false;
    }

    // Check each tuple in the right table to find any that satisfy the join predicate with the current left tuple
    while (right_executor_->Next(&right_tuple_, &right_rid)) {
      if (IsJoinConditionSatisfied()) {
        *tuple = CombineTuples(false);
        left_join_statisfied_ = true;
        return true;
      }
    }

    inner_loop_status_ = false;

    // For LEFT OUTER JOINs, if the left tuple hasn't joined with any right tuple, output it with nulls for the right
    // side
    if (!left_join_statisfied_ && plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = CombineTuples(true);
      return true;
    }
  }

  return false;
}

// Evaluates the join predicate on the current pair of left and right tuples
auto NestedLoopJoinExecutor::IsJoinConditionSatisfied() const -> bool {
  auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                right_executor_->GetOutputSchema());
  return !value.IsNull() && value.GetAs<bool>();
}

// Combines the values from the left and right tuples into a single tuple with the output schema
auto NestedLoopJoinExecutor::CombineTuples(bool right_null) const -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  auto left_tuple_values = GetValuesFromTuple(left_tuple_, left_executor_->GetOutputSchema(), false);
  auto right_tuple_values = GetValuesFromTuple(right_tuple_, right_executor_->GetOutputSchema(), right_null);
  values.insert(values.end(), left_tuple_values.begin(), left_tuple_values.end());
  values.insert(values.end(), right_tuple_values.begin(), right_tuple_values.end());

  return {values, &GetOutputSchema()};
}

// Retrieves the values for each column in a given tuple according to the specified schema
auto NestedLoopJoinExecutor::GetValuesFromTuple(const Tuple &tuple, const Schema &schema, bool is_null) const
    -> std::vector<Value> {
  std::vector<Value> values{};

  values.reserve(schema.GetColumnCount());
  for (uint32_t index = 0; index < schema.GetColumnCount(); ++index) {
    Value value =
        is_null ? ValueFactory::GetNullValueByType(schema.GetColumn(index).GetType()) : tuple.GetValue(&schema, index);
    values.push_back(value);
  }

  return values;
}

}  // namespace bustub
