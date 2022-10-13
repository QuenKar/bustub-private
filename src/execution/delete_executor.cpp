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
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<std::pair<Tuple, RID>> child_tuples;
  child_executor_->Init();

  try {
    Tuple old_tuple;
    RID old_rid;
    while (child_executor_->Next(&old_tuple, &old_rid)) {
      child_tuples.push_back({old_tuple, old_rid});
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteError:child execute error.");
  }

  // delete tuples
  for (auto &p : child_tuples) {
    // delete
    tb_hp_->MarkDelete(p.second, exec_ctx_->GetTransaction());

    // update index
    auto idxinfo_arr = catalog_->GetTableIndexes(tb_info_->name_);
    for (size_t j = 0; j < idxinfo_arr.size(); j++) {
      idxinfo_arr[j]->index_->DeleteEntry(
          p.first.KeyFromTuple(tb_info_->schema_, idxinfo_arr[j]->key_schema_, idxinfo_arr[j]->index_->GetKeyAttrs()),
          p.second, exec_ctx_->GetTransaction());
    }
  }

  return false;
}

}  // namespace bustub
