#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  Trie root_copy;
  root_lock_.lock();
  root_copy = root_;
  root_lock_.unlock();

  // (2) Lookup the value in the trie.
  const T *value_ptr = root_copy.Get<T>(key);

  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  if (value_ptr) {
    return std::optional<ValueGuard<T>>(ValueGuard<T>(root_copy, *value_ptr));
  }

  return std::nullopt;
}

// Rule:
// 1. When someone is modifying the trie, reads can still be performed on the old root.
// 2. When someone is reading, writes can still be performed without waiting for reads.
// 3. Concurrent key-value store should concurrently serve multiple readers and a single writer.
template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // Acquire write_lock to ensure there is only one writer at a time.
  write_lock_.lock();

  // Take the root_lock, get the root, and release the root lock. Just like TrieStore::Get.
  Trie root_copy;
  root_lock_.lock();
  root_copy = root_;
  root_lock_.unlock();

  // do some writing
  root_copy = root_copy.Put(key, std::move(value));

  root_lock_.lock();
  root_ = root_copy;
  root_lock_.unlock();

  write_lock_.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // Acquire write_lock to ensure there is only one writer at a time.
  write_lock_.lock();

  // Take the root_lock, get the root, and release the root lock. Just like TrieStore::Get.
  Trie root_copy;
  root_lock_.lock();
  root_copy = root_;
  root_lock_.unlock();

  // do some writing
  root_copy = root_copy.Remove(key);

  root_lock_.lock();
  root_ = root_copy;
  root_lock_.unlock();

  write_lock_.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
