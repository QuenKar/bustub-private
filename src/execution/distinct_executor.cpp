//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  // generate distinct hash map
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    DistinctKey key;
    key.distincts_.reserve(plan_->OutputSchema()->GetColumnCount());

    for (size_t i = 0; i < key.distincts_.capacity(); i++) {
      key.distincts_.emplace_back(t.GetValue(plan_->OutputSchema(), i));
    }

    if (distinct_map_.count(key) == 0) {
      distinct_map_.insert({key, t});
    }
  }
  // init iter
  distinct_map_iter_ = distinct_map_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (distinct_map_iter_ == distinct_map_.end()) {
    return false;
  }
  *tuple = distinct_map_iter_->second;
  *rid = distinct_map_iter_->second.GetRid();
  ++distinct_map_iter_;
  return true;
}

}  // namespace bustub
