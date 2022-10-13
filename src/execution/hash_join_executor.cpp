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
      l_executor_(std::move(left_child)),
      r_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  l_executor_->Init();
  r_executor_->Init();
  // schema

  const Schema *l_schema = l_executor_->GetOutputSchema();
  const Schema *r_schema = r_executor_->GetOutputSchema();

  // left table key keep in hash table
  try {
    /* code */
    Tuple tuple;
    RID rid;
    while (l_executor_->Next(&tuple, &rid)) {
      HashJoinKey l_hash_key;
      l_hash_key.column_value_ = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, l_schema);
      if (hash_join_map_.count(l_hash_key) != 0) {
        hash_join_map_[l_hash_key].emplace_back(tuple);
      } else {
        hash_join_map_.insert({l_hash_key, {tuple}});
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "HashJoinError:child executor error.");
  }

  // lookup right table
  try {
    /* code */
    Tuple r_tuple;
    RID r_rid;
    while (r_executor_->Next(&r_tuple, &r_rid)) {
      HashJoinKey r_hash_key;
      r_hash_key.column_value_ = plan_->RightJoinKeyExpression()->Evaluate(&r_tuple, r_schema);
      if (hash_join_map_.count(r_hash_key) != 0) {
        for (auto &l_tuple : hash_join_map_.at(r_hash_key)) {
          std::vector<Value> out;
          for (const auto c : plan_->OutputSchema()->GetColumns()) {
            out.emplace_back(c.GetExpr()->EvaluateJoin(&l_tuple, l_schema, &r_tuple, r_schema));
          }

          result_.emplace_back(Tuple{out, plan_->OutputSchema()});
        }
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "HashJoinError:child executor error.");
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (cur_id_ >= result_.size()) {
    return false;
  }

  *tuple = result_.at(cur_id_);
  *rid = result_.at(cur_id_).GetRid();

  cur_id_++;

  return true;
}

}  // namespace bustub
