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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      l_executor_(std::move(left_executor)),
      r_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  const Schema *l_schema = plan_->GetLeftPlan()->OutputSchema();
  const Schema *r_schema = plan_->GetRightPlan()->OutputSchema();

  Tuple l_tuple;
  Tuple r_tuple;
  RID l_rid;
  RID r_rid;

  l_executor_->Init();
  try {
    while (l_executor_->Next(&l_tuple, &l_rid)) {
      r_executor_->Init();
      while (r_executor_->Next(&r_tuple, &r_rid)) {
        if (plan_->Predicate() == nullptr ||
            plan_->Predicate()->EvaluateJoin(&l_tuple, l_schema, &r_tuple, r_schema).GetAs<bool>()) {
          std::vector<Value> out;

          for (const auto &c : plan_->OutputSchema()->GetColumns()) {
            out.emplace_back(c.GetExpr()->EvaluateJoin(&l_tuple, l_schema, &r_tuple, r_schema));
          }

          result_.emplace_back(Tuple{out, plan_->OutputSchema()});
        }
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "NestedJoinError:child execute error.");
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (cur_id_ >= result_.size()) {
    return false;
  }

  *tuple = result_.at(cur_id_);
  *rid = result_.at(cur_id_).GetRid();

  cur_id_++;

  return true;
}

}  // namespace bustub
