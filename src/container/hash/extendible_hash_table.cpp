//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t directory_idx = Hash(key) & dir_page->GetGlobalDepthMask();
  return directory_idx;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *ret = nullptr;
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // new a directory page
    page_id_t new_dir_page_id;
    Page *p = buffer_pool_manager_->NewPage(&new_dir_page_id);
    assert(p != nullptr);
    directory_page_id_ = new_dir_page_id;
    ret = reinterpret_cast<HashTableDirectoryPage *>(p->GetData());
    ret->SetPageId(directory_page_id_);
    // new first bucket
    page_id_t new_bkt_page_id;
    p = buffer_pool_manager_->NewPage(&new_bkt_page_id);
    assert(p != nullptr);
    ret->SetBucketPageId(0, new_bkt_page_id);
    // unpin the two pages because write data
    buffer_pool_manager_->UnpinPage(new_dir_page_id, true);
    buffer_pool_manager_->UnpinPage(new_bkt_page_id, true);
  }
  // get page from buffer
  assert(directory_page_id_ != INVALID_PAGE_ID);
  Page *p = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(p != nullptr);
  ret = reinterpret_cast<HashTableDirectoryPage *>(p->GetData());

  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *p = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(p != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(p->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // if (directory_page_id_ == INVALID_PAGE_ID) return false;
  // Get the bucket corresponding to a key.
  HashTableDirectoryPage *dir_p = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_p);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  // bucket->PrintBucket();
  bool flag = bucket->GetValue(key, comparator_, result);
  // unpin page false because no write
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(dir_p->GetPageId(), false);

  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // insert
  HashTableDirectoryPage *dir_p = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_p);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  assert(bucket != nullptr);
  // if not full, insert directly!
  if (!bucket->IsFull()) {
    bool flag = bucket->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(dir_p->GetPageId(), false);  // why false? director page is no change.
    return flag;
  } else {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(dir_p->GetPageId(), false);
    return SplitInsert(transaction, key, value);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_p = FetchDirectoryPage();
  uint32_t old_bkt_dir_idx = KeyToDirectoryIndex(key, dir_p);
  uint32_t old_bkt_depth = dir_p->GetLocalDepth(old_bkt_dir_idx);

  // is there a max limit for bucket number?

  // incr global depth
  if (old_bkt_depth == dir_p->GetGlobalDepth()) {
    dir_p->IncrGlobalDepth();
  }

  dir_p->IncrLocalDepth(old_bkt_dir_idx);
  // keep the data ,then reset the old bucket
  page_id_t old_bkt_page_id = dir_p->GetBucketPageId(old_bkt_dir_idx);
  HASH_TABLE_BUCKET_TYPE *old_bkt_p = FetchBucketPage(old_bkt_page_id);
  uint32_t old_array_size = old_bkt_p->NumReadable();
  MappingType *data = old_bkt_p->GetArrayCopy();
  // reset the bucket
  old_bkt_p->Reset();

  // create and init new image bucket
  page_id_t image_bkt_page_id;
  Page *p = buffer_pool_manager_->NewPage(&image_bkt_page_id);
  assert(p != nullptr);
  HASH_TABLE_BUCKET_TYPE *new_bkt_p = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(p->GetData());
  uint32_t image_bkt_dir_idx = dir_p->GetSplitImageIndex(old_bkt_dir_idx);
  dir_p->SetLocalDepth(image_bkt_dir_idx, dir_p->GetLocalDepth(old_bkt_dir_idx));
  dir_p->SetBucketPageId(image_bkt_dir_idx, image_bkt_page_id);

  // insert the data into the two bucket
  for (size_t i = 0; i < old_array_size; i++) {
    MappingType tmp = data[i];
    uint32_t target_bkt_dir_idx = Hash(tmp.first) & dir_p->GetLocalDepthMask(old_bkt_dir_idx);
    page_id_t target_bkt_page_id = dir_p->GetBucketPageId(target_bkt_dir_idx);

    if (target_bkt_page_id == old_bkt_page_id) {
      old_bkt_p->Insert(tmp.first, tmp.second, comparator_);
    } else {
      new_bkt_p->Insert(tmp.first, tmp.second, comparator_);
    }
  }
  // delete the copy data
  delete[] data;

  // set the same local depth and page
  uint32_t depth = dir_p->GetLocalDepth(old_bkt_dir_idx);
  uint32_t diff = 1 << depth;
  for (uint32_t i = old_bkt_dir_idx; i >= diff; i -= diff) {
    dir_p->SetBucketPageId(i, old_bkt_page_id);
    dir_p->SetLocalDepth(i, depth);
  }
  for (uint32_t i = old_bkt_dir_idx; i < dir_p->Size(); i += diff) {
    dir_p->SetBucketPageId(i, old_bkt_page_id);
    dir_p->SetLocalDepth(i, depth);
  }
  for (uint32_t i = image_bkt_dir_idx; i >= diff; i -= diff) {
    dir_p->SetBucketPageId(i, image_bkt_page_id);
    dir_p->SetLocalDepth(i, depth);
  }
  for (uint32_t i = image_bkt_dir_idx; i < dir_p->Size(); i += diff) {
    dir_p->SetBucketPageId(i, image_bkt_page_id);
    dir_p->SetLocalDepth(i, depth);
  }

  buffer_pool_manager_->UnpinPage(old_bkt_page_id, true);
  buffer_pool_manager_->UnpinPage(image_bkt_page_id, true);
  buffer_pool_manager_->UnpinPage(dir_p->GetPageId(), true);

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // Get the bucket corresponding to a key.
  HashTableDirectoryPage *dir_p = FetchDirectoryPage();
  uint32_t bkt_dir_idx = KeyToDirectoryIndex(key, dir_p);
  page_id_t bucket_page_id = KeyToPageId(key, dir_p);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  assert(bucket != nullptr);

  // remove element and unpin pages
  bool flg = bucket->Remove(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(dir_p->GetPageId(), false);

  // merge
  uint32_t split_bkt_dir_idx = dir_p->GetSplitImageIndex(bkt_dir_idx);
  if ((bucket->IsEmpty()) && (dir_p->GetLocalDepth(bkt_dir_idx) > 0) &&
      (dir_p->GetLocalDepth(bkt_dir_idx) == dir_p->GetLocalDepth(split_bkt_dir_idx))) {
    Merge(transaction, key, value);
  }
  return flg;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // Get the bucket corresponding to a key.
  HashTableDirectoryPage *dir_p = FetchDirectoryPage();
  uint32_t bkt_dir_idx = KeyToDirectoryIndex(key, dir_p);
  uint32_t split_bkt_dir_idx = dir_p->GetSplitImageIndex(bkt_dir_idx);
  // get pages id
  page_id_t bkt_page_id = dir_p->GetBucketPageId(bkt_dir_idx);
  page_id_t split_bkt_page_id = dir_p->GetBucketPageId(split_bkt_dir_idx);
  // remove origin bucket
  buffer_pool_manager_->UnpinPage(bkt_page_id, false);
  buffer_pool_manager_->DeletePage(bkt_page_id);
  // set origin bucket page pointer to new bucket page
  dir_p->SetBucketPageId(bkt_dir_idx, split_bkt_page_id);
  dir_p->DecrLocalDepth(bkt_dir_idx);
  dir_p->DecrLocalDepth(split_bkt_dir_idx);

  for (uint32_t i = 0; i < dir_p->Size(); i++) {
    if (dir_p->GetBucketPageId(i) == bkt_page_id) {
      dir_p->SetBucketPageId(i, split_bkt_page_id);
      dir_p->SetLocalDepth(i, dir_p->GetLocalDepth(bkt_dir_idx));
    }
  }

  while (dir_p->CanShrink()) {
    dir_p->DecrGlobalDepth();
  }
  // unpin directory page
  buffer_pool_manager_->UnpinPage(dir_p->GetPageId(), true);

  return;
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
