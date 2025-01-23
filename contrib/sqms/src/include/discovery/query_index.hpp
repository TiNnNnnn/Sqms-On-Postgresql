#pragma once 
#include "inverted_index.hpp"
#include "collect/level_mgr.hpp"
#include "collect/format.pb-c.h"
#include "query_index_node.hpp"

class HistoryQueryLevelTree{
public:
    HistoryQueryLevelTree();
    
    bool Insert(LevelManager* level_mgr,int l = 0);
    bool Remove(LevelManager* level_mgr,int l = 0);
    bool Search(LevelManager* level_mgr,int l = 0);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

    void ShowAllNodeTypes();
    

private:
    std::shared_ptr<HistoryQueryIndexNode> root_;
    size_t height_ = 5;
};
