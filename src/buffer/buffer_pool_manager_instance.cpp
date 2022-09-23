//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  // find the page and write data to disk page.

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t f_id = iter->second;

  Page *p = &pages_[f_id];
  disk_manager_->WritePage(p->GetPageId(), p->GetData());
  p->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  // You can do it!
  for (auto &item : page_table_) {
    frame_id_t f_id = item.second;
    Page *p = &pages_[f_id];
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
    p->is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  Page *p = nullptr;
  frame_id_t f_id = -1;
  if (!free_list_.empty()) {
    f_id = free_list_.front();
    free_list_.pop_front();
    p = &pages_[f_id];
  } else {
    if (replacer_->Victim(&f_id)) {
      p = &pages_[f_id];

      if (p->IsDirty()) {
        disk_manager_->WritePage(p->GetPageId(), p->GetData());
      }

      page_table_.erase(p->GetPageId());
    } else {
      return nullptr;
    }
  }

  p->page_id_ = AllocatePage();
  p->pin_count_ = 1;
  p->is_dirty_ = false;
  p->ResetMemory();

  page_table_[p->page_id_] = f_id;
  replacer_->Pin(f_id);

  *page_id = p->page_id_;

  return p;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  auto iter = page_table_.find(page_id);
  // if find , return it!
  if (iter != page_table_.end()) {
    frame_id_t f_id = iter->second;
    Page *p = &pages_[f_id];
    p->pin_count_++;
    replacer_->Pin(f_id);
    return p;
  }
  // not found
  frame_id_t f_id = -1;
  Page *p = nullptr;
  if (!free_list_.empty()) {
    f_id = free_list_.front();
    free_list_.pop_front();
    p = &pages_[f_id];
  } else {
    if (replacer_->Victim(&f_id)) {
      p = &pages_[f_id];
      if (p->IsDirty()) {
        disk_manager_->WritePage(p->GetPageId(), p->GetData());
      }

      page_table_.erase(p->GetPageId());

    } else {
      //没有找到f_id
      return nullptr;
    }
  }
  // reset state.
  page_table_[page_id] = f_id;

  p->page_id_ = page_id;
  p->pin_count_ = 1;
  p->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, p->GetData());

  replacer_->Pin(f_id);

  return p;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::lock_guard<std::mutex> guard(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t f_id = iter->second;

  Page *p = &pages_[f_id];
  if (p->pin_count_ != 0) {
    return false;
  }
  page_table_.erase(iter);
  DeallocatePage(page_id);

  p->page_id_ = INVALID_PAGE_ID;
  p->ResetMemory();
  p->is_dirty_ = false;

  free_list_.push_back(f_id);

  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t f_id = iter->second;
  Page *p = &pages_[f_id];
  if (p->pin_count_ <= 0) {
    return false;
  }
  p->pin_count_--;
  // notes: 注意这个地方要判断一下，防止覆盖之前的dirty状态:假如之前is_dirty_是true，然后is_dirty是false的情况
  if (is_dirty) {
    p->is_dirty_ = true;
  }

  if (p->pin_count_ <= 0) {
    replacer_->Unpin(f_id);
  }

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
