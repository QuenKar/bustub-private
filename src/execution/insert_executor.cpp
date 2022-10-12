//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  tb_info_ = catalog_->GetTable(plan_->TableOid());
  tb_hp_ = tb_info_->table_.get();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // raw insert
  if (plan_->IsRawInsert()) {
    const auto &insert_arr = plan_->RawValues();
    for (size_t i = 0; i < insert_arr.size(); i++) {
      // insert tuple
      Tuple t{insert_arr[i], &tb_info_->schema_};
      RID now_rid;

      tb_hp_->InsertTuple(t, &now_rid, exec_ctx_->GetTransaction());
      // update index
      auto idxinfo_arr = catalog_->GetTableIndexes(tb_info_->name_);
      for (size_t j = 0; j < idxinfo_arr.size(); j++) {
        idxinfo_arr[j]->index_->InsertEntry(
            t.KeyFromTuple(tb_info_->schema_, idxinfo_arr[j]->key_schema_, idxinfo_arr[j]->index_->GetKeyAttrs()),
            now_rid, exec_ctx_->GetTransaction());
      }
    }
    return false;
  }

  // select insert
  std::vector<Tuple> child_tuples;
  child_executor_->Init();

  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      child_tuples.push_back(tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertError:child execute error.");
  }

  for (auto &ct : child_tuples) {
    RID now_rid;
    // insert tuple
    tb_hp_->InsertTuple(ct, &now_rid, exec_ctx_->GetTransaction());
    // update index
    auto idxinfo_arr = catalog_->GetTableIndexes(tb_info_->name_);
    for (size_t j = 0; j < idxinfo_arr.size(); j++) {
      idxinfo_arr[j]->index_->InsertEntry(
          ct.KeyFromTuple(tb_info_->schema_, idxinfo_arr[j]->key_schema_, idxinfo_arr[j]->index_->GetKeyAttrs()),
          now_rid, exec_ctx_->GetTransaction());
    }
  }

  return false;
}

}  // namespace bustub
