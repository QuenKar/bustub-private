//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  tb_info_ = catalog_->GetTable(plan_->TableOid());
  tb_hp_ = tb_info_->table_.get();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();

  Tuple old_tuple;
  RID old_rid;
  while (true) {
    try {
      if (child_executor_->Next(&old_tuple, &old_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteError:child execute error.");
      return false;
    }

    // add lock
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    } else {
      if (!lock_mgr->LockUpgrade(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    }

    // delete
    tb_hp_->MarkDelete(old_rid, exec_ctx_->GetTransaction());

    // update index
    auto idxinfo_arr = catalog_->GetTableIndexes(tb_info_->name_);

    for (auto &idxinfo : idxinfo_arr) {
      idxinfo->index_->DeleteEntry(
          old_tuple.KeyFromTuple(tb_info_->schema_, idxinfo->key_schema_, idxinfo->index_->GetKeyAttrs()), old_rid,
          exec_ctx_->GetTransaction());
      // record the old tuple for rollback
      IndexWriteRecord iw_record(old_rid, tb_info_->oid_, WType::DELETE, old_tuple, old_tuple, idxinfo->index_oid_,
                                 exec_ctx_->GetCatalog());

      txn->GetIndexWriteSet()->emplace_back(iw_record);
    }

    // unlock
    if (lock_mgr && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->Unlock(txn, old_rid);
    }
  }

  return false;
}

}  // namespace bustub
