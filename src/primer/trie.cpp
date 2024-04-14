#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> cur_node = root_;

  // find the target node
  for (char ch : key) {
    if (!cur_node || cur_node->children_.find(ch) == cur_node->children_.end()) {
      return nullptr;
    }

    cur_node = cur_node->children_.at(ch);
  }

  if (cur_node && cur_node->is_value_node_) {
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur_node.get());
    // check whether the type of the value is mismatched
    if (value_node) {
      return value_node->value_.get();
    }
  }

  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<TrieNode> new_root = root_ ? std::shared_ptr<TrieNode>(root_->Clone()) : std::make_shared<TrieNode>();
  std::shared_ptr<TrieNode> cur_node = new_root;

  if (key.empty()) {  // key == ""
    auto value_ptr = std::make_shared<T>(std::move(value));
    new_root = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, value_ptr);
  } else {
    // find the parent node of the target node
    for (size_t index = 0; index < key.size() - 1; index++) {
      char ch = key[index];

      std::shared_ptr<TrieNode> next_node;
      if (cur_node->children_.find(ch) == cur_node->children_.end()) {
        next_node = std::make_shared<TrieNode>();
      } else {
        next_node = std::shared_ptr<TrieNode>(cur_node->children_[ch]->Clone());
      }

      cur_node->children_[ch] = next_node;
      cur_node = next_node;
    }

    char last_char = key.back();
    auto value_ptr = std::make_shared<T>(std::move(value));
    if (cur_node->children_.find(last_char) == cur_node->children_.end()) {
      cur_node->children_[last_char] = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
    } else {  // node corresponding to the key already exists, create a new `TrieNodeWithValue`
      cur_node->children_[last_char] =
          std::make_shared<TrieNodeWithValue<T>>(cur_node->children_[last_char]->children_, value_ptr);
    }
  }

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (!root_) {
    return Trie(nullptr);
  }

  std::shared_ptr<TrieNode> new_root = std::shared_ptr<TrieNode>(root_->Clone());
  std::shared_ptr<TrieNode> cur_node = new_root;
  std::stack<std::shared_ptr<TrieNode>> node_stack;  // record the parent node of the target node(need to be removed)

  for (char ch : key) {
    if (!cur_node || cur_node->children_.find(ch) == cur_node->children_.end()) {
      return *this;
    }

    node_stack.push(cur_node);

    std::shared_ptr<TrieNode> next_node = std::shared_ptr<TrieNode>(cur_node->children_[ch]->Clone());
    cur_node->children_[ch] = next_node;
    cur_node = next_node;
  }

  if (cur_node && cur_node->is_value_node_) {  // found node which need to be removed
    if (key.empty()) {                         // key == ""
      new_root = std::make_shared<TrieNode>(new_root->children_);
      if (cur_node->children_.empty()) {  // root node doesn't have children any more, set it to nullptr
        new_root = nullptr;
      }
    } else {  // current node doesn't have children any more, remove it
      if (cur_node->children_.empty()) {
        while (!node_stack.empty()) {  // when removing current node, its parent may also need to be removed
          std::shared_ptr<TrieNode> top_node = node_stack.top();
          // only by deleting the reference of the parent node to the child node can remove operation be truly
          // executed
          top_node->children_.erase(key[node_stack.size() - 1]);

          if (!top_node->children_.empty() || top_node->is_value_node_) {
            break;
          }

          node_stack.pop();
        }

        if (node_stack.empty()) {  // all nodes in trie have been removed, set root to nullptr
          new_root = nullptr;
        }
      } else {  // convert it to `TrieNode`
        std::shared_ptr<TrieNode> top_node = node_stack.top();
        top_node->children_[key.back()] = std::make_shared<TrieNode>(cur_node->children_);
      }
    }
  }

  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
