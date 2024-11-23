#include "storage/Trie.hpp"

namespace DB {

void Trie::Insert(std::string_view source) {
  std::shared_ptr<TrieNode> node = root_;
  for (auto &c : source) {
    auto &next_level = node->next_level_;
    if (auto &&ite =
            std::ranges::find_if(next_level.begin(), next_level.end(),
                                 [&](auto &pair) { return pair.first == c; });

        ite != next_level.end()) {
      node = ite->second;
    } else {
      auto &[_, ptr] = next_level.emplace_back(c, std::make_shared<TrieNode>());
      node = ptr;
    }
  }
  node->valid = true;
}

void Trie::Remove(std::string_view source) {
  auto node = FindNodeHelper(source);
  if (node == nullptr) {
    return;
  }
  node->valid = false;
}

bool Trie::Exist(std::string_view source) {
  auto node = FindNodeHelper(source);
  if (node == nullptr) {
    return false;
  }
  return node->valid;
}

std::shared_ptr<TrieNode> Trie::FindNodeHelper(std::string_view source) {
  std::shared_ptr<TrieNode> node = root_;
  for (auto &c : source) {
    auto &next_level = node->next_level_;
    if (auto &&ite =
            std::ranges::find_if(next_level.begin(), next_level.end(),
                                 [&](auto &pair) { return pair.first == c; });
        ite != next_level.end()) {
      node = ite->second;
    } else {
      return nullptr;
    }
  }
  return node;
}
} // namespace DB