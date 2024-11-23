#pragma once

#include <memory>
#include <string_view>
#include <vector>

namespace DB {

struct TrieNode {
  // A valid value of true means that the current string exists.
  // Note that valid only means that the string from the root node to the
  // current node is valid. For example, if you insert the strings: “koarz”,
  // “ko”, then only z and o are valid.
  bool valid{};
  std::vector<std::pair<char, std::shared_ptr<TrieNode>>> next_level_;
};

class Trie {
  std::shared_ptr<TrieNode> root_;

  std::shared_ptr<TrieNode> FindNodeHelper(std::string_view source);

public:
  Trie() : root_(std::make_shared<TrieNode>()) {}

  void Insert(std::string_view);

  void Remove(std::string_view);

  bool Exist(std::string_view);
};
} // namespace DB