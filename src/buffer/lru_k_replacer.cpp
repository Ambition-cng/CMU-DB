//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t victim = -1;
  size_t max_backward_k_distance = -1;
  if (curr_size_ > 0) {
    for (auto &node : node_store_) {
      if (node.second.is_evictable_) {
        size_t node_backward_k_distance = node.second.ComputeBackwardKDistance(k_, current_timestamp_);
        if (victim == -1 || max_backward_k_distance < node_backward_k_distance) {
          victim = node.first;
          max_backward_k_distance = node_backward_k_distance;
        }
      }
    }
  }

  if (victim != -1) {
    node_store_.erase(victim);
    curr_size_--;

    *frame_id = victim;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id.");

  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.insert(std::make_pair(frame_id, LRUKNode()));
  }

  current_timestamp_++;
  node_store_[frame_id].history_.push_back(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id.");

  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) != node_store_.end() && node_store_[frame_id].is_evictable_ != set_evictable) {
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }

    node_store_[frame_id].is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id.");

  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) != node_store_.end()) {
    BUSTUB_ASSERT(node_store_[frame_id].is_evictable_, "Removal of a non-evictable frame");

    node_store_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);

  return curr_size_;
}

}  // namespace bustub
