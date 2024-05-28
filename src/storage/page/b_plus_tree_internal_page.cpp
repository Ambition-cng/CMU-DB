//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);

  SetSize(1);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

// Use binary search to find insert postion or next page id
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchPostion(int left, bool search_next_page, const KeyType &key,
                                                   const KeyComparator &comparator) const -> int {
  int start = left;
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

  if (search_next_page) {
    return start - 1;
  }
  return start;
}

// Insert key-value pair into internal page
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertIntoInternal(int start_pos, const KeyType &middle_key,
                                                        const ValueType &page_id, const ValueType &split_page_id,
                                                        const KeyComparator &comparator) {
  int pos = SearchPostion(start_pos, false, middle_key, comparator);

  IncreaseSize(1);
  // insert middle key into array_
  for (int index = GetSize() - 1; index > pos; --index) {
    array_[index] = array_[index - 1];
  }

  if (start_pos != 0) {
    array_[pos - 1].second = page_id;  // only effective when creating a new internal page
  }
  array_[pos] = std::make_pair(middle_key, split_page_id);
}

// Helper method to move keys to split internal page
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveKeys(B_PLUS_TREE_INTERNAL_PAGE_TYPE *split_internal_page, int page_size) {
  int split_page_size = GetMaxSize() - page_size;
  split_internal_page->SetSize(split_page_size);
  for (int index = 0; index < split_page_size; ++index) {
    split_internal_page->array_[index] = array_[page_size + index];
  }
  SetSize(page_size);
}

// Move keys to split internal page and insert key-value pair
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitInternal(B_PLUS_TREE_INTERNAL_PAGE_TYPE *split_internal_page,
                                                   const KeyType &key, const ValueType &page_id,
                                                   const ValueType &split_page_id, const KeyComparator &comparator)
    -> KeyType {
  int page_size = GetMaxSize() / 2;
  if (comparator(key, array_[page_size].first) == -1) {
    MoveKeys(split_internal_page, page_size);
    InsertIntoInternal(1, key, page_id, split_page_id, comparator);
  } else {
    page_size += 1;
    MoveKeys(split_internal_page, page_size);
    split_internal_page->InsertIntoInternal(0, key, page_id, split_page_id, comparator);
  }

  return split_internal_page->array_[0].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveFromInternal(int pos) {
  for (int index = pos; index < GetSize() - 1; ++index) {
    array_[index] = array_[index + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowFromPreviousSibling(B_PLUS_TREE_INTERNAL_PAGE_TYPE *sibling_page) {
  MappingType pair = sibling_page->PopBack();
  PushFront(pair);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowFromNextSibling(B_PLUS_TREE_INTERNAL_PAGE_TYPE *sibling_page) {
  MappingType pair = sibling_page->PopFront();
  PushBack(pair);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushFront(MappingType pair) {
  IncreaseSize(1);
  for (int index = GetSize() - 1; index > 0; --index) {
    array_[index] = array_[index - 1];
  }
  array_[0] = pair;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushBack(MappingType pair) {
  IncreaseSize(1);
  array_[GetSize() - 1] = pair;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFront() -> MappingType {
  MappingType pair = array_[0];

  for (int index = 0; index < GetSize() - 1; ++index) {
    array_[index] = array_[index + 1];
  }
  IncreaseSize(-1);
  return pair;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopBack() -> MappingType {
  MappingType pair = array_[GetSize() - 1];
  IncreaseSize(-1);
  return pair;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeWithNextSibling(KeyType key,
                                                          B_PLUS_TREE_INTERNAL_PAGE_TYPE *next_sibling_internal_page) {
  next_sibling_internal_page->array_[0].first = key;

  int origin_page_size = GetSize();

  IncreaseSize(next_sibling_internal_page->GetSize());
  for (int index = origin_page_size; index < GetSize(); ++index) {
    array_[index] = next_sibling_internal_page->array_[index - origin_page_size];
  }

  next_sibling_internal_page->SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
