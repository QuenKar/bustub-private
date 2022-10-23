//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> l(latch_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  txn->SetState(TransactionState::GROWING);
  auto &lock_queue = lock_table_[rid];
  auto &cond_v = lock_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  lock_queue.request_queue_.emplace_back(LockRequest{txn_id, LockMode::SHARED});
  txn->GetSharedLockSet()->emplace(rid);
  txn_table_[txn_id] = txn;

  bool grant = true;
  bool is_kill = false;
  // wound wait
  for (auto &req : lock_queue.request_queue_) {
    if (req.lock_mode_ == LockMode::EXCLUSIVE) {
      if (req.txn_id_ <= txn_id) {
        grant = false;
      } else {
        txn_table_[req.txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      }
    }
    if (req.txn_id_ == txn_id) {
      req.granted_ = grant;
      break;
    }
  }
  // notify other trans
  if (is_kill) {
    cond_v.notify_all();
  }
  // wait lock
  while (!grant) {
    for (auto &req : lock_queue.request_queue_) {
      // if there is a T using the write lock, break;
      if (req.lock_mode_ == LockMode::EXCLUSIVE && txn_table_[req.txn_id_]->GetState() != TransactionState::ABORTED) {
        break;
      }
      if (req.txn_id_ == txn_id) {
        grant = true;
        req.granted_ = grant;
      }
    }
    if (!grant) {
      cond_v.wait(l);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
    }
  }

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> l(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->IsExclusiveLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  txn->SetState(TransactionState::GROWING);
  auto &lock_queue = lock_table_[rid];
  auto &cond_v = lock_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  lock_queue.request_queue_.emplace_back(LockRequest{txn_id, LockMode::EXCLUSIVE});
  txn->GetSharedLockSet()->emplace(rid);
  txn_table_[txn_id] = txn;

  bool grant = true;
  bool is_kill = false;

  for (auto &req : lock_queue.request_queue_) {
    if (req.txn_id_ == txn_id) {
      req.granted_ = grant;
      break;
    }
    if (req.lock_mode_ == LockMode::EXCLUSIVE) {
      if (req.txn_id_ < txn_id) {
        grant = false;
      } else {
        txn_table_[req.txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      }
    }
  }

  if (is_kill) {
    cond_v.notify_all();
  }

  while (!grant) {
    auto iter = lock_queue.request_queue_.begin();
    while (iter != lock_queue.request_queue_.end() &&
           txn_table_[iter->txn_id_]->GetState() == TransactionState::ABORTED) {
      ++iter;
    }
    if (iter->txn_id_ == txn_id) {
      grant = true;
      iter->granted_ = grant;
    }
    if (!grant) {
      cond_v.wait(l);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
    }
  }

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
