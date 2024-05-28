//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);

  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PairAt(int index) const -> const MappingType & { return array_[index]; }

// Helper method to get value for given key
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                                          const KeyComparator &comparator) const -> bool {
  int pos = SearchPostion(key, comparator);
  if (pos < GetSize() && comparator(key, array_[pos].first) == 0) {
    result->push_back(array_[pos].second);
    return true;
  }

  return false;
}

// Use binary search to find the place to insert a value
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchPostion(const KeyType &key, const KeyComparator &comparator) const -> int {
  int start = 0;
  int end = GetSize() - 1;
  while (start <= end) {
    int mid = start + (end - start) / 2;

    if (comparator(array_[mid].first, key) == 0) {
      return mid;
    }
    if (comparator(array_[mid].first, key) == -1) {
      start = mid + 1;
    } else {
      end = mid - 1;
    }
  }

  return start;
}

// Insert key-value pair into leaf page
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                                const KeyComparator &comparator) -> bool {
  int pos = SearchPostion(key, comparator);
  if (pos < GetSize() && comparator(array_[pos].first, key) == 0) {
    return false;
  }

  // Insert key-value pair into array_
  IncreaseSize(1);
  for (int index = GetSize() - 1; index > pos; --index) {
    array_[index] = array_[index - 1];
  }
  array_[pos] = std::make_pair(key, value);

  return true;
}

// Move keys to split leaf page
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitLeaf(B_PLUS_TREE_LEAF_PAGE_TYPE *split_leaf_page) -> KeyType {
  int page_size = GetMinSize();
  int split_page_size = GetMaxSize() - page_size;
  split_leaf_page->SetSize(split_page_size);
  for (int index = 0; index < split_page_size; ++index) {
    split_leaf_page->array_[index] = array_[page_size + index];
  }
  SetSize(page_size);

  return split_leaf_page->array_[0].first;
}

// Remove key-value pair from leaf page
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveFromLeaf(const KeyType &key, const KeyComparator &comparator) -> bool {
  int pos = SearchPostion(key, comparator);
  if (pos >= GetSize() || comparator(array_[pos].first, key) != 0) {
    return false;
  }

  // Remove key-value pair from array_
  for (int index = pos; index < GetSize() - 1; ++index) {
    array_[index] = array_[index + 1];
  }
  IncreaseSize(-1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::BorrowFromPreviousSibling(B_PLUS_TREE_LEAF_PAGE_TYPE *sibling_page) {
  MappingType pair = sibling_page->PopBack();
  PushFront(pair);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::BorrowFromNextSibling(B_PLUS_TREE_LEAF_PAGE_TYPE *sibling_page) {
  MappingType pair = sibling_page->PopFront();
  PushBack(pair);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushFront(MappingType pair) {
  IncreaseSize(1);
  for (int index = GetSize() - 1; index > 0; --index) {
    array_[index] = array_[index - 1];
  }
  array_[0] = pair;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(MappingType pair) {
  IncreaseSize(1);
  array_[GetSize() - 1] = pair;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() -> MappingType {
  MappingType pair = array_[0];

  for (int index = 0; index < GetSize() - 1; ++index) {
    array_[index] = array_[index + 1];
  }
  IncreaseSize(-1);
  return pair;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() -> MappingType {
  MappingType pair = array_[GetSize() - 1];
  IncreaseSize(-1);
  return pair;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeWithNextSibling(const KeyType &key,
                                                      B_PLUS_TREE_LEAF_PAGE_TYPE *next_sibling_leaf_page) {
  int origin_page_size = GetSize();

  IncreaseSize(next_sibling_leaf_page->GetSize());
  for (int index = origin_page_size; index < GetSize(); ++index) {
    array_[index] = next_sibling_leaf_page->array_[index - origin_page_size];
  }

  next_sibling_leaf_page->SetSize(0);
  next_page_id_ = next_sibling_leaf_page->GetNextPageId();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
