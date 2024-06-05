//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  aggregation_finished_ = false;
  aht_.Clear();

  // Initialize the child executor
  child_executor_->Init();

  Tuple child_tuple{};
  RID child_rid{};

  // Get the next tuple
  auto status = child_executor_->Next(&child_tuple, &child_rid);

  while (status) {
    AggregateKey key = MakeAggregateKey(&child_tuple);
    AggregateValue value = MakeAggregateValue(&child_tuple);

    aht_.InsertCombine(key, value);

    status = child_executor_->Next(&child_tuple, &child_rid);
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End() && aggregation_finished_) {
    return false;
  }

  std::vector<Value> values{};
  if (aht_.Begin() == aht_.End()) {       // aggregation operation with null values
    if (!plan_->GetGroupBys().empty()) {  // aggregation operation with null values and group by statement
      return false;
    }

    values = aht_.GenerateInitialAggregateValue().aggregates_;
  } else {
    AggregateKey group_by = aht_iterator_.Key();
    AggregateValue aggregation_columns = aht_iterator_.Val();

    // The output schema consists of the group-by columns followed by the aggregation columns
    values.reserve(GetOutputSchema().GetColumnCount());
    values.insert(values.end(), group_by.group_bys_.begin(), group_by.group_bys_.end());
    values.insert(values.end(), aggregation_columns.aggregates_.begin(), aggregation_columns.aggregates_.end());

    ++aht_iterator_;
  }

  aggregation_finished_ = true;
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
