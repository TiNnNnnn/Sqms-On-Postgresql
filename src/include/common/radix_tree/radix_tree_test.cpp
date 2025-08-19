#include "radix_tree.hpp"
#include <vector>

void TestRadixTreeString(){

    RadixTree tree;

    tree.insert("apple");
    tree.insert("apply");
    tree.insert("app");
    tree.insert("pp");
    tree.insert("ss");

    std::cout << std::boolalpha;
    std::cout << "Search for 'app': " << tree.search("app") << std::endl;
    std::cout << "Search for 'apple': " << tree.search("apple") << std::endl;
    std::cout << "Search for 's': " << tree.search("s") << std::endl;

    tree.erase("apply");
    std::cout << "After removing 'apply':" << std::endl;
    std::cout << "Search for 'apply': " << tree.search("apply") << std::endl;
    std::cout << "Search for 'apple': " << tree.search("apple") << std::endl;
}

void TestRadixTreeSubSet(){
    RadixTree tree;

    tree.insert("apply");
    tree.insert("apple");
    tree.insert("app");

    auto subsets = tree.find_subsets("apply");

    std::cout << "Subsets of 'apply':" << std::endl;
    for (const auto& subset : subsets) {
        std::cout << subset << std::endl;
    }
}

int main() {
    TestRadixTreeString();
    TestRadixTreeSubSet();
    return 0;
}