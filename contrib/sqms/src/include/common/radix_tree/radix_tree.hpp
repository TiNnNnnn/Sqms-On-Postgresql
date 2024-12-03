//
// Created by orekilee on 2023/3/31.
//

#ifndef RADIX_RADIXTREE_CPP
#define RADIX_RADIXTREE_CPP

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unordered_set>

using namespace std;

class RadixTreeNode {
public:
  explicit RadixTreeNode(const string &word = "", bool is_end = false)
      : word(word), is_end(is_end) {}

  unordered_set<shared_ptr<RadixTreeNode>> children;
  string word;
  bool is_end;
};

class RadixTree {
public:
  RadixTree() : root(make_shared<RadixTreeNode>()){};

  virtual ~RadixTree() = default;

  RadixTree(const RadixTree &) = delete;
  RadixTree &operator=(const RadixTree &) = delete;

  void insert(const string &str) {
    if (str.empty()) {
      root->is_end = true;
    } else {
      insert_helper(str, root);
    }
  }

  void erase(const string &str) {
    if (str.empty()) {
      root->is_end = false;
    } else {
      erase_helper(str, root);
    }
  }

  bool search(const string &str) {
    if (str.empty()) {
      return root->is_end;
    }
    return search_helper(str, root);
  }

  std::vector<std::string> find_subsets(const string &str){
    
    return std::vector<std::string>();
  }


private:
  shared_ptr<RadixTreeNode> root;

 

  void insert_helper(const string &str, shared_ptr<RadixTreeNode> node) {
    // 如果当前没有子节点，则直接作为新的子节点
    if (node->children.empty()) {
      auto new_node = make_shared<RadixTreeNode>(str, true);
      node->children.insert(new_node);
      return;
    }

    bool is_match = false;
    for (auto current : node->children) {
      int i = 0;
      for (; i < str.size() && i < current->word.size(); i++) {
        if (str[i] != current->word[i]) {
          break;
        }
      }
      if (i != 0) {
        is_match = true;
        // 情况一：当前节点的内容与字符串完全匹配，则直接将该前缀标记为完整
        if (i == str.size() && i == current->word.size()) {
          current->is_end = true;
        } else if (i != current->word.size()) {
          // 如果当前节点的内容是字符串的部分前缀，则进行分裂
          auto new_node = make_shared<RadixTreeNode>(current->word.substr(i),
                                                     current->is_end);

          current->word = current->word.substr(0, i);
          current->is_end = (i == str.size()) ? true : false;
          current->children.swap(new_node->children);
          current->children.insert(new_node);

          if (i != str.size()) {
            auto new_node2 = make_shared<RadixTreeNode>(str.substr(i), true);
            current->children.insert(new_node2);
          }
        } else {
          // 如果当前节点已匹配完，则继续往子节点匹配
          insert_helper(str.substr(i), current);
        }
        if (is_match) {
          return;
        }
      }
    }
    // 如果没有找到，则直接插入
    auto new_node = make_shared<RadixTreeNode>(str, true);
    node->children.insert(new_node);
  }

  shared_ptr<RadixTreeNode> erase_helper(const string &str,
                                         shared_ptr<RadixTreeNode> node) {
    bool is_match = false;
    for (auto current : node->children) {
      int i = 0;
      for (; i < str.size() && i < current->word.size(); i++) {
        if (str[i] != current->word[i]) {
          break;
        }
      }
      if (i != 0) {
        is_match = true;

        // 情况一：当前节点的内容与字符串完全匹配
        if (i == str.size() && i == current->word.size()) {
          // 如果该节点没有子节点，则将该节点删除。否则将is_end标记为false
          if (current->children.empty()) {
            node->children.erase(current);
          } else {
            current->is_end = false;
          }

          // 如果删除了该节点后，父节点仅剩下一个子节点，且父节点不完整，则将两个节点合并
          if (node->children.size() == 1 && !node->is_end && node != root) {
            auto sub_node = *node->children.begin();
            node->children.erase(sub_node);
            node->is_end = sub_node->is_end;
            node->word.append(sub_node->word);
            node->children = sub_node->children;
            return node;
          }
        }
        // 情况二：当前节点是字符串的前缀
        else if (i == current->word.size()) {
          // 继续向下搜索，如果返回值不为空则说明需要合并节点
          auto sub_node = erase_helper(str.substr(i), current);
          if (sub_node && node->children.size() == 1 && !node->is_end &&
              node != root) {
            auto sub_node = *node->children.begin();
            node->children.erase(sub_node);
            node->is_end = sub_node->is_end;
            node->word.append(sub_node->word);
            node->children = sub_node->children;
          }
        }
        // 情况三：字符串是当前节点的前缀，此时必定查询失败
        else {
          break;
        }
      }
      if (is_match) {
        return nullptr;
      }
    }
    return nullptr;
  }

  bool search_helper(const string &str, shared_ptr<RadixTreeNode> node) {
    for (auto current : node->children) {
      int i = 0;
      for (; i < str.size() && i < current->word.size(); i++) {
        if (str[i] != current->word[i]) {
          break;
        }
      }
      if (i != 0) {
        // 情况一：当前节点的内容与字符串完全匹配，根据是否为完整单词判断结果
        if (i == str.size() && i == current->word.size()) {
          return current->is_end;
        }
        // 情况二：当前节点的内容是字符串的前缀
        else if (i == current->word.size()) {
          return search_helper(str.substr(i), current);
        }
        // 情况三：字符串的内容是当前节点的前缀，直接返回错误
        else {
          return false;
        }
      }
    }
    // 没有找到
    return false;
  }
};


#endif // RADIX_RADIXTREE_CPP
