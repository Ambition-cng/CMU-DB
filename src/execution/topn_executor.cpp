#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  top_entries_.clear();

  Tuple tuple{};
  RID rid{};

  /** Take ascending order-by type as an example:
   *  1. top_entries corresponding to large top heap;
   *  2. when heap size reach to N:
   *    a. new entry is smaller than the top value, pop from heap, push the new entry into leap;
   *    b. new entry is larger than or equal to the top value, ignore it.
   */
  while (child_executor_->Next(&tuple, &rid)) {
    auto tuple_pair = std::make_pair(tuple, rid);
    if (top_entries_.size() == plan_->GetN()) {
      auto top_pair = top_entries_.front();
      if (CompareFunction(tuple_pair, top_pair)) {
        Pop();
        Push(tuple_pair);
      }
    } else {
      Push(tuple_pair);
    }
  }

  std::sort(top_entries_.begin(), top_entries_.end(),
            [this](const auto &lhs, const auto &rhs) { return CompareFunction(lhs, rhs); });
  index_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == top_entries_.size()) {
    return false;
  }

  *tuple = top_entries_[index_].first;
  *rid = top_entries_[index_].second;

  ++index_;

  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
