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
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // generate SimpleAggregationHashTable
  child_->Init();

  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }

  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  const AggregateKey key_aggregate = aht_iterator_.Key();
  const AggregateValue val_aggregate = aht_iterator_.Val();
  ++aht_iterator_;

  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(key_aggregate.group_bys_, val_aggregate.aggregates_).GetAs<bool>()) {
    std::vector<Value> out;
    for (const auto &c : plan_->OutputSchema()->GetColumns()) {
      out.emplace_back(c.GetExpr()->EvaluateAggregate(key_aggregate.group_bys_, val_aggregate.aggregates_));
    }
    *tuple = Tuple(out, plan_->OutputSchema());
    return true;
  }

  return Next(tuple, rid);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
