#pragma once 
#include "inverted_index.hpp"
#include "collect/format.pb-c.h"

class HistoryQueryIndexNode{
    typedef std::vector<std::string> SET;
public:
    HistoryQueryIndexNode();
    ~HistoryQueryIndexNode();
    
    std::shared_ptr<HistoryQueryIndexNode> Child(size_t l,HistorySlowPlanStat* hsps){

        return nullptr;
        //return childs_.at(str)
    }

    size_t Level(){return level_;}

private:
    bool InsertSet(HistorySlowPlanStat* hsps);
    bool RemoveSet(HistorySlowPlanStat* hsps);
    bool SearchSet(HistorySlowPlanStat* hsps); 
private:
    /*set2node*/
    std::unordered_map<SET,std::shared_ptr<HistoryQueryIndexNode>,SetHasher>childs_;
    /*inverted_idx storage all sets in the node*/
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    /*current node level*/
    size_t level_;
};

class HistoryQueryLevelTree{
public:
    HistoryQueryLevelTree();
    bool Insert();
    bool Remove();
    bool Search(); 
private:
    std::shared_ptr<HistoryQueryIndexNode> root_;
    size_t height_ = 8;
};
