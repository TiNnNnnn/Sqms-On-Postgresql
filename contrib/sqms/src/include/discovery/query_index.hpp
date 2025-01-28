#pragma once 
#include "inverted_index.hpp"
#include "collect/level_mgr.hpp"
#include "collect/format.pb-c.h"
#include "query_index_node.hpp"

#include <mqueue.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>


/**
 * HistoryQueryLevelTree
 */
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
    size_t height_ = 7;
};

/**
 * QueryIndexManager
 * MARK: unused 
 */
class QueryIndexManager{
public:
    static void RegisterQueryIndex();
    static void Task();
    static void DeRegisterQueryIndex();
};
