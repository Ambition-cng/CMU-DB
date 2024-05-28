#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

// Helper method to find corresponding leaf page and return ReadPageGuard
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageRead(ReadPageGuard &&page_guard, page_id_t root_page_id, const KeyType &key)
    -> ReadPageGuard {
  page_guard = bpm_->FetchPageRead(root_page_id);
  auto cur_page = page_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto internal_page = page_guard.As<InternalPage>();

    page_id_t next_page_id = internal_page->ValueAt(internal_page->SearchPostion(1, true, key, comparator_));

    page_guard = bpm_->FetchPageRead(next_page_id);
    cur_page = page_guard.As<BPlusTreePage>();
  }

  return page_guard;
}

// Helper method to find the first or last leaf page based on `find_first` flag
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindBoundaryLeafPageRead(ReadPageGuard &&page_guard, page_id_t root_page_id, bool find_first)
    -> ReadPageGuard {
  page_guard = bpm_->FetchPageRead(root_page_id);
  auto cur_page = page_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto internal_page = page_guard.As<InternalPage>();

    page_id_t next_page_id = internal_page->ValueAt(0);
    if (!find_first) {
      next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    }

    page_guard = bpm_->FetchPageRead(next_page_id);
    cur_page = page_guard.As<BPlusTreePage>();
  }

  return page_guard;
}

// Helper method to find corresponding leaf page and return WritePageGuard
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageWrite(Context &ctx, page_id_t root_page_id, const KeyType &key, bool is_insert)
    -> WritePageGuard {
  ctx.root_page_id_ = root_page_id;

  WritePageGuard page_guard = bpm_->FetchPageWrite(root_page_id);
  auto cur_page = page_guard.AsMut<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    if ((is_insert && cur_page->GetSize() < cur_page->GetMaxSize()) ||
        (!is_insert && cur_page->GetSize() > std::max(cur_page->GetMinSize(), 2))) {
      ctx.header_page_ = std::nullopt;
      ctx.write_set_.clear();
    }
    auto internal_page = page_guard.AsMut<InternalPage>();

    page_id_t next_page_id = internal_page->ValueAt(internal_page->SearchPostion(1, true, key, comparator_));
    ctx.write_set_.emplace_back(std::move(page_guard));

    page_guard = bpm_->FetchPageWrite(next_page_id);
    cur_page = page_guard.AsMut<BPlusTreePage>();
  }

  if ((is_insert && cur_page->GetSize() < cur_page->GetMaxSize() - 1) ||
      (!is_insert && cur_page->GetSize() > cur_page->GetMinSize())) {
    ctx.header_page_ = std::nullopt;
    ctx.write_set_.clear();
  }

  return page_guard;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // B+ tree is empty
    return false;
  }

  // Find corresponding leaf page
  page_guard = FindLeafPageRead(std::move(page_guard), header_page->root_page_id_, key);
  auto leaf_page = page_guard.As<LeafPage>();
  return leaf_page->GetValue(key, result, comparator_);
}

// Helper method to process split during insertion
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitPage(Context &ctx, const KeyType &middle_key, page_id_t origin_page_id,
                               page_id_t split_page_id) {
  if (ctx.IsRootPage(origin_page_id)) {  // Split root page
    page_id_t root_internal_page_id;
    auto root_internal_page_guard = bpm_->NewPageGuardedWrite(&root_internal_page_id);
    auto root_internal_page = root_internal_page_guard.AsMut<InternalPage>();
    root_internal_page->Init(internal_max_size_);

    root_internal_page->InsertIntoInternal(1, middle_key, origin_page_id, split_page_id, comparator_);

    SetRootPageId(ctx, root_internal_page_id);
    return;
  }

  WritePageGuard internal_page_guard = std::move(ctx.write_set_.back());
  auto cur_internal_page = internal_page_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();

  if (cur_internal_page->GetSize() == cur_internal_page->GetMaxSize()) {  // Split internal page
    page_id_t split_internal_page_id;
    auto split_internal_page_guard = bpm_->NewPageGuardedWrite(&split_internal_page_id);
    auto split_internal_page = split_internal_page_guard.AsMut<InternalPage>();
    split_internal_page->Init(internal_max_size_);

    KeyType internal_middle_key =
        cur_internal_page->SplitInternal(split_internal_page, middle_key, origin_page_id, split_page_id, comparator_);

    SplitPage(ctx, internal_middle_key, internal_page_guard.PageId(), split_internal_page_id);
  } else {
    cur_internal_page->InsertIntoInternal(1, middle_key, origin_page_id, split_page_id, comparator_);
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;

  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();

  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {  // B+ tree is empty
    auto new_root_page_guard = bpm_->NewPageGuardedWrite(&root_page_id);
    auto new_root_page = new_root_page_guard.AsMut<LeafPage>();
    new_root_page->Init(leaf_max_size_);

    new_root_page->InsertIntoLeaf(key, value, comparator_);
    header_page->root_page_id_ = root_page_id;  // Update the page id of the root of this tree
    return true;
  }

  // Find corresponding leaf page
  ctx.header_page_ = std::move(header_guard);
  WritePageGuard page_guard = FindLeafPageWrite(ctx, root_page_id, key, true);
  auto leaf_page = page_guard.AsMut<LeafPage>();

  // Insert key-value pair into leaf page
  if (!leaf_page->InsertIntoLeaf(key, value, comparator_)) {  // attempt to insert duplicate keys
    return false;
  }

  // Check if leaf page is full
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {  // Split leaf page
    page_id_t split_leaf_page_id;

    auto split_leaf_page_guard = bpm_->NewPageGuardedWrite(&split_leaf_page_id);
    auto split_leaf_page = split_leaf_page_guard.AsMut<LeafPage>();
    split_leaf_page->Init(leaf_max_size_);

    // Update next leaf page id
    split_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(split_leaf_page_id);

    // Split leaf page
    KeyType middle_key = leaf_page->SplitLeaf(split_leaf_page);

    // Insert middle key into internal page
    // Recursively check if internal page is full
    SplitPage(ctx, middle_key, page_guard.PageId(), split_leaf_page_id);
  }

  return true;
}

// Helper method to process deletion with root page
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ShrinkRoot(Context &ctx, WritePageGuard &&root_page_guard) {
  auto root_page = root_page_guard.AsMut<BPlusTreePage>();

  if (root_page->IsLeafPage()) {      // Root page is leaf page
    if (root_page->GetSize() == 0) {  // B+ tree is empty
      bpm_->DeletePage(root_page_guard.PageId());
      SetRootPageId(ctx, INVALID_PAGE_ID);
    }
  } else {                            // Root page is internal page
    if (root_page->GetSize() == 1) {  // Set its unique child page as the root page
      auto internal_page = root_page_guard.AsMut<InternalPage>();
      bpm_->DeletePage(root_page_guard.PageId());
      SetRootPageId(ctx, internal_page->ValueAt(0));
    }
  }
}

// Use templete function to reuse code logic of leaf page and internal page
INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
auto BPLUSTREE_TYPE::BorrowFromPreviousSibling(PageType *child_page, PageType *sibling_page, InternalPage *parent_page,
                                               int index) -> bool {
  int min_size = sibling_page->GetMinSize();
  if (!sibling_page->IsLeafPage()) {
    min_size = std::max(min_size, 2);
  }

  if (sibling_page->GetSize() > min_size) {
    child_page->BorrowFromPreviousSibling(sibling_page);
    parent_page->SetKeyAt(index, child_page->KeyAt(0));
    return true;
  }

  return false;
}

// Use templete function to reuse code logic of leaf page and internal page
INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
auto BPLUSTREE_TYPE::BorrowFromNextSibling(PageType *child_page, PageType *sibling_page, InternalPage *parent_page,
                                           int index) -> bool {
  int min_size = sibling_page->GetMinSize();
  if (!sibling_page->IsLeafPage()) {
    min_size = std::max(min_size, 2);
  }

  if (sibling_page->GetSize() > min_size) {
    child_page->BorrowFromNextSibling(sibling_page);
    parent_page->SetKeyAt(index + 1, sibling_page->KeyAt(0));
    return true;
  }

  return false;
}

// Use templete function to reuse code logic of leaf page and internal page
INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
void BPLUSTREE_TYPE::MergeWithSibling(PageType *child_page, PageType *sibling_page, InternalPage *parent_page,
                                      int index) {
  if (index > 0) {  // Merge with previous sibling
    sibling_page->MergeWithNextSibling(parent_page->KeyAt(index), child_page);
    bpm_->DeletePage(parent_page->ValueAt(index));
    parent_page->RemoveFromInternal(index);
  } else {  // Merge with next sibling
    child_page->MergeWithNextSibling(parent_page->KeyAt(index + 1), sibling_page);
    bpm_->DeletePage(parent_page->ValueAt(index + 1));
    parent_page->RemoveFromInternal(index + 1);
  }
}

// Helper method to process redistribution and merge during deletion
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeOrMerge(Context &ctx, const KeyType &key, WritePageGuard &&page_guard) {
  auto child_page = page_guard.AsMut<BPlusTreePage>();

  if (ctx.IsRootPage(page_guard.PageId())) {
    ShrinkRoot(ctx, std::move(page_guard));
    return;
  }

  int min_size = child_page->GetMinSize();
  if (!child_page->IsLeafPage()) {
    min_size = std::max(min_size, 2);
  }

  if (child_page->GetSize() >= min_size) {
    return;
  }

  WritePageGuard parent_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto parent_page = parent_page_guard.AsMut<InternalPage>();

  int index = parent_page->SearchPostion(1, true, key, comparator_);

  // Try to redistribute, borrowing from the sibling
  bool redistributed = false;
  if (child_page->IsLeafPage()) {  // For leaf page
    if (index > 0) {               // Borrow key-value pair from previous sibling
      WritePageGuard sibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
      auto sibling_page = sibling_guard.AsMut<LeafPage>();
      redistributed =
          BorrowFromPreviousSibling<LeafPage>(page_guard.AsMut<LeafPage>(), sibling_page, parent_page, index);
    }

    if (!redistributed && index < parent_page->GetSize() - 1) {  // Borrow key-value pair from next sibling
      WritePageGuard sibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
      auto sibling_page = sibling_guard.AsMut<LeafPage>();
      redistributed = BorrowFromNextSibling<LeafPage>(page_guard.AsMut<LeafPage>(), sibling_page, parent_page, index);
    }
  } else {            // For internal page
    if (index > 0) {  // Borrow key-value pair from previous sibling
      WritePageGuard sibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index - 1));
      auto sibling_page = sibling_guard.AsMut<InternalPage>();
      redistributed =
          BorrowFromPreviousSibling<InternalPage>(page_guard.AsMut<InternalPage>(), sibling_page, parent_page, index);
    }

    if (!redistributed && index < parent_page->GetSize() - 1) {  // Borrow key-value pair from next sibling
      WritePageGuard sibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index + 1));
      auto sibling_page = sibling_guard.AsMut<InternalPage>();
      redistributed =
          BorrowFromNextSibling<InternalPage>(page_guard.AsMut<InternalPage>(), sibling_page, parent_page, index);
    }
  }

  // Check if redistribution is successful
  if (!redistributed) {
    // Redistribution fails, merge target page and the sibling
    int sibling_index = index - 1;
    if (index == 0) {
      sibling_index = index + 1;
    }

    WritePageGuard sibling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(sibling_index));
    if (child_page->IsLeafPage()) {  // For leaf page
      auto sibling_page = sibling_guard.AsMut<LeafPage>();
      MergeWithSibling<LeafPage>(page_guard.AsMut<LeafPage>(), sibling_page, parent_page, index);
    } else {  // For internal page
      auto sibling_page = sibling_guard.AsMut<InternalPage>();
      MergeWithSibling<InternalPage>(page_guard.AsMut<InternalPage>(), sibling_page, parent_page, index);
    }
  }

  // Recursively check if internal page is not half full
  RedistributeOrMerge(ctx, key, std::move(parent_page_guard));
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  Context ctx;

  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();

  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {  // B+ tree is empty
    return;
  }

  // Find corresponding leaf page
  ctx.header_page_ = std::move(header_guard);
  WritePageGuard page_guard = FindLeafPageWrite(ctx, root_page_id, key, false);
  auto leaf_page = page_guard.AsMut<LeafPage>();

  // Remove key-value pair from leaf page
  if (!leaf_page->RemoveFromLeaf(key, comparator_)) {  // Entry doesn't exist
    return;
  }

  // Check if leaf page is not half full
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // Try to redistribute of merge
    RedistributeOrMerge(ctx, key, std::move(page_guard));
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // B+ tree is empty
    return INDEXITERATOR_TYPE();
  }

  // Find first leaf page
  page_guard = FindBoundaryLeafPageRead(std::move(page_guard), header_page->root_page_id_, true);

  return INDEXITERATOR_TYPE(bpm_, std::move(page_guard), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // B+ tree is empty
    return INDEXITERATOR_TYPE();
  }

  // Find corresponding leaf page
  page_guard = FindLeafPageRead(std::move(page_guard), header_page->root_page_id_, key);
  auto leaf_page = page_guard.As<LeafPage>();
  int index = leaf_page->SearchPostion(key, comparator_);

  return INDEXITERATOR_TYPE(bpm_, std::move(page_guard), index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // B+ tree is empty
    return INDEXITERATOR_TYPE();
  }

  // Find last leaf page
  page_guard = FindBoundaryLeafPageRead(std::move(page_guard), header_page->root_page_id_, false);
  auto leaf_page = page_guard.As<LeafPage>();

  return INDEXITERATOR_TYPE(bpm_, std::move(page_guard), leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/**
 * Set page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(Context &ctx, page_id_t root_page_id) {
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = root_page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
