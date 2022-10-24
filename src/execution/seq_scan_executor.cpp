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
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();
  const Schema *out_schema = GetOutputSchema();

  while (iter_ != tb_hp_->End()) {
    Tuple t_tuple = *iter_;
    *rid = iter_->GetRid();
    // add shared lock
    if (lock_mgr && txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsExclusiveLocked(*rid) && !txn->IsSharedLocked(*rid)) {
        if (!lock_mgr->LockShared(txn, *rid)) {
          txn_mgr->Abort(txn);
        }
      }
    }

    std::vector<Value> res;
    for (const auto &col : out_schema->GetColumns()) {
      res.emplace_back(
          col.GetExpr()->Evaluate(&(*iter_), &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()))->schema_));
    }

    *tuple = Tuple(res, out_schema);

    const AbstractExpression *expr = plan_->GetPredicate();
    if (expr == nullptr || expr->Evaluate(tuple, out_schema).GetAs<bool>()) {
      ++iter_;
      if (txn->IsSharedLocked(*rid) && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        lock_mgr->Unlock(txn, *rid);
      }
      return true;
    }
    ++iter_;
  }
  return false;
}

}  // namespace bustub
