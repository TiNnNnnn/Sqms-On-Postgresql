#pragma once 
#include "inverted_index.hpp"
#include "collect/level_mgr.hpp"
#include "levels.hpp"
#include "collect/format.pb-c.h"

class HistoryQueryIndexNode{
    typedef std::vector<std::string> SET;
public:
    HistoryQueryIndexNode(int l);
    ~HistoryQueryIndexNode();
    
    std::shared_ptr<HistoryQueryIndexNode> Child(size_t l,HistorySlowPlanStat* hsps);
    size_t Level(){return level_;}

private:
    bool InsertSet(HistorySlowPlanStat* hsps);
    bool RemoveSet(HistorySlowPlanStat* hsps);
    bool SearchSet(HistorySlowPlanStat* hsps);
private: 
    // /*set2node*/
    // std::unordered_map<SET,std::shared_ptr<HistoryQueryIndexNode>,SetHasher>childs_;
    // /*inverted_idx storage all sets in the node*/
    // std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;

    /*use context to operate node*/
    LevelStrategyContext* level_strategy_context_;

    /*current node level*/
    size_t level_;
};

class HistoryQueryLevelTree{
public:
    HistoryQueryLevelTree();
    
    bool Insert(LevelManager* level_mgr,int l = 0);
    bool Remove(LevelManager* level_mgr,int l = 0);
    bool Search(LevelManager* level_mgr,int l = 0);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

private:
    std::shared_ptr<HistoryQueryIndexNode> root_;
    size_t height_ = 8;
};
