//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  tb_hp_ = table_info_->table_.get();
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  Tuple old_tuple;
  RID tpl_rid;
  while (true) {
    // get old tuple
    try {
      if (!child_executor_->Next(&old_tuple, &tpl_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "UpdateError:child execute error.");
      return false;
    }
    // add exclusive lock
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    } else {
      if (!lock_mgr->LockUpgrade(txn, *rid)) {
        txn_mgr->Abort(txn);
      }
    }

    // update tuple
    Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
    tb_hp_->UpdateTuple(new_tuple, tpl_rid, exec_ctx_->GetTransaction());

    // update index:
    // delete old tuple index, insert new tuple index
    for (const auto &idx_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      idx_info->index_->DeleteEntry(
          old_tuple.KeyFromTuple(table_info_->schema_, idx_info->key_schema_, idx_info->index_->GetKeyAttrs()), tpl_rid,
          exec_ctx_->GetTransaction());
      idx_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, idx_info->key_schema_, idx_info->index_->GetKeyAttrs()), tpl_rid,
          exec_ctx_->GetTransaction());
      // record the old tuple and new tuple in txn for rollback
      IndexWriteRecord iw_record(tpl_rid, table_info_->oid_, WType::UPDATE, new_tuple, old_tuple, idx_info->index_oid_,
                                 exec_ctx_->GetCatalog());  // WType::UPDATE?

      // will lead to memory unsafe
      // iw_record.old_tuple_ = old_tuple;
      txn->GetIndexWriteSet()->emplace_back(iw_record);
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
