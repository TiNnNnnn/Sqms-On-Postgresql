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
class LevelOrderIterator;
class NodeManager : public AbstractFormatStrategy{
public:
    NodeManager(HistorySlowPlanStat* hsps,std::shared_ptr<LevelManager> level_mgr,pid_t pid);
    virtual bool Format() override;
    virtual bool PrintPredEquivlences() override;
    
    bool Search();
    std::vector<NodeCollector*>& GetNodeCollectorList(){return node_collector_list_;}
    NodeExplain* ComputeExplainNodes(NodeCollector* node_collector);
    NodeCollector* GetNodeRoot(){return root_;}
private:
    void ComputeTotalNodes(HistorySlowPlanStat* hsps);
    void PlanPartition(HistorySlowPlanStat* hsps);
    void PlanInit(HistorySlowPlanStat* hsps);
    bool CancelQuery(pid_t pid);
    bool SearchInternal(NodeCollector *node,double total_time,int finish_node_num,LevelOrderIterator* iter);
    
private:
    int branch_num_;
    HistorySlowPlanStat* hsps_ = nullptr;
    NodeCollector* root_;
    std::shared_ptr<LevelManager> level_mgr_ = nullptr;

    std::vector<NodeCollector*> node_collector_list_;
    std::vector<int>node_id_list_;
    std::vector<std::vector<NodeCollector *>> level_collector_;
    std::vector<std::vector<int>> level_collector_ids_;
    std::vector<std::vector<NodeCollector*>>partition_list;
    std::unordered_map<NodeCollector*,int>dependencies_;
    std::shared_ptr<ThreadPool> pool_;
    /*for expalin,first is sub id ,second is candidate id */
    std::vector<std::pair<int,int>> match_list_;
    size_t pool_size_ = 10;

    SqmsLogger* logger_;
    HistoryQueryLevelTree *shared_index_;
    pid_t pid_;
    int node_num_ = 0;
};

class LevelOrderIterator {
    public:
        using NodePtr = NodeCollector*;
    
        LevelOrderIterator(const std::vector<std::vector<NodePtr>>& levels)
            : levels_(levels), level_idx_(0), node_idx_(0) {
            moveToNextValid(); 
        }
    
        bool hasNext() const {
            return level_idx_ < levels_.size();
        }
    
        bool hasPrev() const {
            return !(level_idx_ == 0 && node_idx_ == 0);
        }
    
        NodePtr next() {
            if (!hasNext()) return nullptr;
    
            NodePtr result = levels_[level_idx_][node_idx_++];
            moveToNextValid();
            return result;
        }
    
        NodePtr prev() {
            if (!hasPrev()) return nullptr;
            if (node_idx_ > 0) {
                --node_idx_;
            } else {
                do {
                    if (level_idx_ == 0) return nullptr;
                    --level_idx_;
                } while (level_idx_ > 0 && levels_[level_idx_].empty());
    
                node_idx_ = levels_[level_idx_].size() - 1;
            }
    
            return levels_[level_idx_][node_idx_];
        }

        void begin() {
            level_idx_ = 0;
            node_idx_ = 0;
            moveToNextValid();
        }
    
    private:
        const std::vector<std::vector<NodePtr>>& levels_;
        size_t level_idx_;
        size_t node_idx_;
    
        void moveToNextValid() {
            while (level_idx_ < levels_.size() &&
                   node_idx_ >= levels_[level_idx_].size()) {
                ++level_idx_;
                node_idx_ = 0;
            }
        }
    };
    
    