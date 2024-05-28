//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // pick the replacement for new page
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = static_cast<frame_id_t>(free_list_.back());
    free_list_.pop_back();
  } else if (replacer_->Evict(&frame_id)) {
    page_table_.erase(pages_[frame_id].GetPageId());

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
  }

  if (frame_id != -1) {
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;

    // reset the memory and metadata for the new page.
    pages_[frame_id].ResetMemory();

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;

    replacer_->RecordAccess(frame_id, AccessType::Unknown);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;

  // search for page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];

    // the requested page has already in the buffer pool, disable eviction and record the access history of the frame.
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  // pick the replacement for the requested page
  if (!free_list_.empty()) {
    frame_id = static_cast<frame_id_t>(free_list_.back());
    free_list_.pop_back();
  } else if (replacer_->Evict(&frame_id)) {
    page_table_.erase(pages_[frame_id].GetPageId());

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
  }

  if (frame_id != -1) {
    page_table_[page_id] = frame_id;

    // read the page from disk by calling disk_manager_->ReadPage(), and replace the old page in the frame.
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;

    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];

    if (pages_[frame_id].GetPinCount() > 0) {
      pages_[frame_id].pin_count_--;
      pages_[frame_id].is_dirty_ = pages_[frame_id].is_dirty_ || is_dirty;

      if (pages_[frame_id].pin_count_ == 0) {
        replacer_->SetEvictable(frame_id, true);
      }

      return true;
    }
  }

  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];

    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;

    return true;
  }

  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    pages_[i].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];

    if (pages_[frame_id].pin_count_ > 0) {
      return false;
    }

    page_table_.erase(page_id);

    // stop tracking the frame in the replacer and add the frame back to the free list, and reset the page's memory
    // and metadata.
    pages_[frame_id].ResetMemory();

    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;

    replacer_->Remove(frame_id);
    free_list_.emplace_back(static_cast<int>(frame_id));
    DeallocatePage(page_id);
  }

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

auto BufferPoolManager::NewPageGuardedWrite(page_id_t *page_id) -> WritePageGuard {
  Page *page = NewPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

}  // namespace bustub
