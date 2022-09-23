//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { this->num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

// lru cache 双向链表，左边是刚访问过的frame，右边是最久未访问的frame

//从lru中选出最久未访问的frame，返回frame_id
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mtx_);

  if (lru_list_.empty()) {
    return false;
  }
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  mp_.erase(*frame_id);
  return true;
}

// frame不会被替换
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mtx_);

  auto it = mp_.find(frame_id);
  if (it == mp_.end()) {
    return;
  }

  lru_list_.erase(mp_[frame_id]);
  mp_.erase(frame_id);
}

// frame添加到lru中，将会被替换
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mtx_);
  //若已经存在，就返回
  auto it = mp_.find(frame_id);
  if (it != mp_.end()) {
    return;
  }
  if (lru_list_.size() < num_pages_) {
    lru_list_.push_front(frame_id);
    mp_[frame_id] = lru_list_.begin();
  } else {
    //容器满了
    return;
  }
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
