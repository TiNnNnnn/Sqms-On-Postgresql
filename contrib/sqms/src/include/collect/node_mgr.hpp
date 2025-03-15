#pragma once
#include<vector>
#include<string>
#include<algorithm>
#include<iostream>
#include<unordered_set>
#include<unordered_map>
#include<set>
#include<map>
#include<assert.h>
#include<memory>
#include "common/bloom_filter/bloom_filter.hpp"
#include "common/thread_pool.hpp"
#include "match_strategy.h"
#include "stat_format.hpp"
#include "level_mgr.hpp"
#include "discovery/query_index.hpp"

extern "C"{
    #include "postgres.h"
    #include "access/parallel.h"
    #include "collect/format.h"
    #include "executor/executor.h"
    #include "executor/instrument.h"
    #include "jit/jit.h"
    #include "utils/guc.h"
    #include "common/config.h" 
	#include "collect/format.pb-c.h"
}

/**
 * NodeStat
 */
class NodeStat{
public:
    
private:
    size_t output_;
    std::unordered_set<size_t>input_map_;
};

/**
 * NodeManager
 */
class NodeManager : public AbstractFormatStrategy{
public:
    NodeManager(HistorySlowPlanStat* hsps,std::shared_ptr<LevelManager> level_mgr,pid_t pid);
    virtual bool Format() override;
    virtual bool PrintPredEquivlences() override;
    
    bool Search();
    std::vector<NodeCollector*>& GetNodeCollectorList(){return node_collector_list_;}
private:
    void ComputeTotalNodes(HistorySlowPlanStat* hsps,std::unordered_map<HistorySlowPlanStat*, NodeCollector*> nodes_collector_map);
    void PlanPartition(HistorySlowPlanStat* hsps);
    bool CancelQuery(pid_t pid);
private:
    int branch_num_;
    HistorySlowPlanStat* hsps_ = nullptr;
    std::shared_ptr<LevelManager> level_mgr_ = nullptr;

    std::vector<NodeCollector*> node_collector_list_;
    std::vector<std::vector<NodeCollector*>>partition_list;
    std::unordered_map<NodeCollector*,int>dependencies_;
    std::shared_ptr<ThreadPool> pool_;
    size_t pool_size_ = 10;

    SqmsLogger* logger_;
    HistoryQueryLevelTree *shared_index_;
    pid_t pid_;
};