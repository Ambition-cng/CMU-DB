//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // Initialize the child executor
  left_executor_->Init();
  right_executor_->Init();

  left_ht_.clear();
  right_ht_.clear();

  // Helper lambda to process tuples from a given executor and insert into a hash table.
  auto process_child = [](AbstractExecutor *child_executor, std::unordered_map<HashJoinKey, std::vector<Tuple>> &ht,
                          const std::function<HashJoinKey(const Tuple *)> &make_join_key) {
    Tuple tuple{};
    RID rid{};
    while (child_executor->Next(&tuple, &rid)) {
      HashJoinKey key = make_join_key(&tuple);
      ht[key].push_back(std::move(tuple));
    }
  };

  // Process and build hash tables for the left and right child executors.
  process_child(left_executor_.get(), left_ht_, [this](const Tuple *t) { return MakeLeftJoinKey(t); });
  process_child(right_executor_.get(), right_ht_, [this](const Tuple *t) { return MakeRightJoinKey(t); });

  iter_ = left_ht_.cbegin();

  left_ht_value_index_ = 0;
  right_ht_value_index_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != left_ht_.end()) {
    auto &left_tuples = iter_->second;
    auto right_iter = right_ht_.find(iter_->first);

    if (right_iter != right_ht_.end()) {
      auto &right_tuples = right_iter->second;

      *tuple = CombineTuples(left_tuples[left_ht_value_index_], right_tuples[right_ht_value_index_], false);

      if (++right_ht_value_index_ == right_tuples.size()) {
        right_ht_value_index_ = 0;
        if (++left_ht_value_index_ == left_tuples.size()) {
          left_ht_value_index_ = 0;
          ++iter_;
        }
      }

      return true;
    }

    if (plan_->GetJoinType() == JoinType::LEFT) {
      Tuple right_tuple{};  // Create an empty right tuple for LEFT join
      *tuple = CombineTuples(iter_->second[left_ht_value_index_], right_tuple, true);

      if (++left_ht_value_index_ == left_tuples.size()) {
        left_ht_value_index_ = 0;
        ++iter_;
      }
      return true;
    }

    ++iter_;  // No match found, move to next
  }

  return false;
}

// Combines the values from the left and right tuples into a single tuple with the output schema
auto HashJoinExecutor::CombineTuples(const Tuple &left_tuple, const Tuple &right_tuple, bool right_null) const
    -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  auto left_tuple_values = GetValuesFromTuple(left_tuple, left_executor_->GetOutputSchema(), false);
  auto right_tuple_values = GetValuesFromTuple(right_tuple, right_executor_->GetOutputSchema(), right_null);
  values.insert(values.end(), left_tuple_values.begin(), left_tuple_values.end());
  values.insert(values.end(), right_tuple_values.begin(), right_tuple_values.end());

  return {values, &GetOutputSchema()};
}

// Retrieves the values for each column in a given tuple according to the specified schema
auto HashJoinExecutor::GetValuesFromTuple(const Tuple &tuple, const Schema &schema, bool is_null) const
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
