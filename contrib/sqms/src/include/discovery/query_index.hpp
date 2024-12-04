#include "inverted_index.hpp"
#include "collect/format.pb-c.h"

class HistoryQueryIndexNode{
public:
    HistoryQueryIndexNode();
    ~HistoryQueryIndexNode();
    
    std::shared_ptr<HistoryQueryIndex> Child(std::string str){
        return nullptr;
        //return childs_.at(str);
    }
private:
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
    size_t height_ = 8;
};
