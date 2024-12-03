#include "inverted_index.hpp"

class HistoryQueryIndexNode{
public:
    HistoryQueryIndexNode();
    ~HistoryQueryIndexNode();
    
    std::shared_ptr<HistoryQueryIndex> Child(std::string str){
        return childs_.at(str);
    }
    bool Insert();
    bool Remove();
    bool Search(); 
private:
    std::unordered_map<std::string,std::shared_ptr<HistoryQueryIndex>>childs_;
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
};

class HistoryQueryIndex{
public:
    bool Insert();
    bool Remove();
    bool Search(); 
private:
    std::shared_ptr<HistoryQueryIndexNode> root_;
    int height = 8;
};
