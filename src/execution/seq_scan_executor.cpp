//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  tb_hp_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = tb_hp_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == tb_hp_->End()) {
    return false;
  }

  RID old_oid = iter_->GetRid();
  const Schema *schema = plan_->OutputSchema();

  std::vector<Value> res;
  res.reserve(schema->GetColumnCount());

  for (size_t i = 0; i < res.capacity(); ++i) {
    res.emplace_back(schema->GetColumn(i).GetExpr()->Evaluate(
        &(*iter_), &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()))->schema_));
  }

  ++iter_;

  Tuple tmp(res, schema);

  const AbstractExpression *expr = plan_->GetPredicate();
  if (expr == nullptr || expr->Evaluate(&tmp, schema).GetAs<bool>()) {
    *tuple = tmp;
    *rid = old_oid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
