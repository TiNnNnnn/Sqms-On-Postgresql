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
#include <atomic>

/**
 * HistoryQueryLevelTree
 */
class HistoryQueryLevelTree{
public:
    HistoryQueryLevelTree(int origin_height = 1);
    
    bool Insert(LevelManager* level_mgr,int l = 0);
    bool Remove(LevelManager* level_mgr,int l = 0);
    bool Search(LevelManager* level_mgr,int l = 0);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

    void ShowAllNodeTypes();

    void SetEffective(int id,bool effective);
    bool CheckEffective(int id);

    int GetNextLid(){return std::atomic_fetch_add(&next_lid_,1);}

private:
    HistoryQueryIndexNode* root_;
    SMConcurrentHashMap<int,bool> id_effective_map_;
    std::atomic<int>next_lid_ = 0;
    size_t height_ = 7;
};

// /**
//  * QueryIndexManager
//  * MARK: unused 
//  */
// class QueryIndexManager{
// public:
//     static void RegisterQueryIndex();
//     static void Task();
//     static void DeRegisterQueryIndex();
// };
