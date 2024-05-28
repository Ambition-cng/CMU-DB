/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, ReadPageGuard &&page_guard, int index)
    : index_(index), bpm_(buffer_pool_manager), page_guard_(std::move(page_guard)) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto leaf_page = page_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return (leaf_page->GetNextPageId() == -1 && index_ == leaf_page->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto leaf_page = page_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return leaf_page->PairAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (!IsEnd()) {
    auto leaf_page = page_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    if (index_ == leaf_page->GetSize() - 1 && leaf_page->GetNextPageId() != -1) {
      page_guard_ = std::move(bpm_->FetchPageRead(leaf_page->GetNextPageId()));
      index_ = 0;
    } else {
      ++index_;
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
