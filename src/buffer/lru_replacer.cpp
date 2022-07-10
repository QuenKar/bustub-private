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

LRUReplacer::LRUReplacer(size_t num_pages) { this->num_pages = num_pages; }

LRUReplacer::~LRUReplacer() = default;

// lru cache 双向链表，左边是刚访问过的frame，右边是最久未访问的frame

//从lru中选出最久未访问的frame，返回frame_id
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(_mtx);

  if (cache.size() <= 0) return false;
  *frame_id = cache.back();
  cache.pop_back();
  mp.erase(*frame_id);
  return true;
}

// frame不会被替换
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(_mtx);

  auto it = mp.find(frame_id);
  if (it == mp.end()) return;

  cache.erase(mp[frame_id]);
  mp.erase(frame_id);
}

// frame添加到lru中，将会被替换
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(_mtx);
  //若已经存在，就返回
  auto it = mp.find(frame_id);
  if (it != mp.end()) return;
  if (cache.size() < num_pages) {
    cache.push_front(frame_id);
    mp[frame_id] = cache.begin();
  } else {
    //容器满了
    return;
  }
}

size_t LRUReplacer::Size() { return cache.size(); }

}  // namespace bustub
