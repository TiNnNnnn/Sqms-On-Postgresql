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
    HistoryQueryLevelTree(LWLock* shmem_lock,int origin_height = 1);
    
    bool Insert(LevelManager* level_mgr,int l = 0);
    bool Remove(LevelManager* level_mgr,int l = 0);
    bool Search(LevelManager* level_mgr,int l = 0);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

    void ShowAllNodeTypes();

private:
    HistoryQueryIndexNode* root_;
    size_t height_ = 7;
    LWLock* shmem_lock_;
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
