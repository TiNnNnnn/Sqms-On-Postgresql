#pragma once
#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <list>
#include <shared_mutex>
#include <mutex>
#include <thread>
#include <functional>

#include "inverted_index.hpp"
#include "collect/level_mgr.hpp"
#include "tbb/concurrent_hash_map.h"
#include "sm_level_mgr.hpp"

extern "C" {
    #include "collect/format.pb-c.h"
    #include <pthread.h>
}

class LevelStrategyContext;
class HistoryQueryIndexNode{
public:
    HistoryQueryIndexNode(int l,int total_height);
    ~HistoryQueryIndexNode();
    
    HistoryQueryIndexNode* Child(size_t l,HistorySlowPlanStat* hsps);
    size_t Level(){return level_;}

    bool Insert(LevelManager* level_mgr);
    bool Remove(LevelManager* level_mgr);
    bool Search(LevelManager* level_mgr,int id);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Serach(NodeCollector* node_collector);

private: 
    /*use context to operate node*/
    LevelStrategyContext* level_strategy_context_;
    /*current node level*/
    size_t level_;
};

class LevelStrategy{
public:
    LevelStrategy(size_t total_height)
        :total_height_(total_height){}
    virtual SMString Name() = 0;
    virtual bool Insert(LevelManager* level_mgr) = 0;
    virtual bool Serach(LevelManager* level_mgr,int id) = 0;
    virtual bool Remove(LevelManager* level_mgr) = 0;

    virtual bool Insert(NodeCollector* node_collector) = 0;
    virtual bool Remove(NodeCollector* node_collector) = 0;
    virtual bool Search(NodeCollector* node_collector) = 0;
    
    size_t FindNextInsertLevel(LevelManager* level_mgr, size_t cur_level);
    size_t FindNextInsertLevel(NodeCollector* node_collector, size_t cur_level);

    size_t total_height_;
};

/**
 * LevelOneStrategy: check the set of subplan's node type set,if the sub_plan's 
 * node type is in (join,project,filter,groupby),we will apply more checking,ohterwise,
 * we just use hash to compare,with time going,we will 
 */
class LevelHashStrategy : public LevelStrategy{
public:
    LevelHashStrategy(size_t total_height): LevelStrategy(total_height){}
    SMString Name(){return "PlanHashStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

private:
    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*>set_map_;
};


/**
 * LevelScalingStrategy
 */
class LevelScalingStrategy : public LevelStrategy{
public:
    LevelScalingStrategy(size_t total_height): LevelStrategy(total_height){}
    SMString Name(){return "PlanHashStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

private:
    std::shared_mutex rw_mutex_;
    /* from join_type_score to scaling_list id map */
    SMMap<int,SMUnorderedSet<SMString,SMStringHash>>scaling_idx_;
    SMUnorderedMap<SMString,std::pair<ScalingInfo*,HistoryQueryIndexNode*>,SMStringHash> child_map_;
};

class LevelAggStrategy : public LevelStrategy{
public:
    LevelAggStrategy(size_t total_height)
        :LevelStrategy(total_height){
            inverted_idx_ = (InvertedIndex<PostingList>*) ShmemAlloc(sizeof(InvertedIndex<PostingList>));
            assert(inverted_idx_);
            new (inverted_idx_) InvertedIndex<PostingList>();
        }
    SMString Name(){return "PlanGroupKeyStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr); 

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);
  
private:
    InvertedIndex<PostingList>* inverted_idx_;
    SMConcurrentHashMap<uint32_t, HistoryQueryIndexNode*>child_map_;
};

class LevelSortStrategy : public LevelStrategy{
public:
    LevelSortStrategy(size_t total_height)
        :LevelStrategy(total_height){
            inverted_idx_ = (InvertedIndex<PostingList>*) ShmemAlloc(sizeof(InvertedIndex<PostingList>));
            assert(inverted_idx_);
            new (inverted_idx_) InvertedIndex<PostingList>();
        }
    SMString Name(){return "PlanGroupKeyStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);   

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

private:    
    InvertedIndex<PostingList>* inverted_idx_;
    SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>child_map_;
};

class LevelRangeStrategy : public LevelStrategy{
public:
    LevelRangeStrategy(size_t total_height)
        : LevelStrategy(total_height){
            inverted_idx_ = (InvertedIndex<PostingList>*) ShmemAlloc(sizeof(InvertedIndex<PostingList>));
            assert(inverted_idx_);
            new (inverted_idx_) InvertedIndex<PostingList>();

            range_inverted_idx_ = (RangeInvertedIndex*) ShmemAlloc(sizeof(RangeInvertedIndex));
            assert(range_inverted_idx_);
            new (range_inverted_idx_) RangeInvertedIndex();
        }
    SMString Name(){return "PlanRangePredsStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

private:
    InvertedIndex<PostingList>* inverted_idx_;
    RangeInvertedIndex* range_inverted_idx_;
    SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher> child_map_;
    //SMUnorderedMap<SET,HistoryQueryIndexNode*,SetHasher> child_map_;
    //SMUnorderedMap<SET,SMUnorderedMap<LevelPredEquivlences*,HistoryQueryIndexNode*>,SetHasher> child_map_;
    std::shared_mutex rw_mutex_;
    size_t s_level_;
};

class LevelResidualStrategy : public LevelStrategy{
public:
    LevelResidualStrategy(size_t total_height)
        : LevelStrategy(total_height){
            inverted_idx_ = (InvertedIndex<PostingList>*) ShmemAlloc(sizeof(InvertedIndex<PostingList>));
            assert(inverted_idx_);
            new (inverted_idx_) InvertedIndex<PostingList>();
        }
    SMString Name(){return "PlanRangePredsStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);
private:
    InvertedIndex<PostingList>* inverted_idx_;
    SMUnorderedMap<SET,SMUnorderedMap<LevelPredEquivlences*,std::shared_ptr<HistoryQueryIndexNode>>,SetHasher> child_map_;
    std::shared_mutex rw_mutex_;
    size_t s_level_;
};

/**
 * Leaf Strategy: 
 * - node stat
 * - level stat
 */
class SMLevelPredEquivlences;
class SMPredEquivlence;
class SMLevelManager;
class LeafStrategy : public LevelStrategy{
public:
    LeafStrategy(size_t total_height)
        : LevelStrategy(total_height){}
    SMString Name(){return "LeafStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

private: 
    bool SearchInternal(LevelManager* src_mgr,int h,int id,int dst_id);
    bool SerachRange(LevelManager* src_mgr,int h,int id, std::vector<size_t> &match_lpes_idxs);
    /*old interface*/
    bool SerachAgg(LevelManager* src_mgr,int h,int id);
    bool SerachSort(LevelManager* src_mgr,int h,int id);
    bool SerachResidual(LevelManager* src_mgr,int h,int id);
    
    /*new interface*/
    bool SearchRange(LevelManager* src_mgr,int h,int id, SMLevelPredEquivlences *dst_lpes);
    bool SearchAgg(LevelManager* src_mgr,int h,int id,SMLevelPredEquivlences *dst_lpes);
    bool SearchSort(LevelManager* src_mgr,int h,int id,SMLevelPredEquivlences *dst_lpes);
    bool SerachResidual(LevelManager* src_mgr,int h,int id,SMLevelPredEquivlences *dst_lpes);
    
    bool Match(LevelPredEquivlences* dst_lpes, SMLevelPredEquivlences* lpes);
    bool SuperSet(SMPredEquivlence* dst_pe,PredEquivlence* pe);
private:
    /*for plan level matching*/
    SMLevelManager* level_mgr_;

    /*for node level matching*/
    int output_;
    SMVector<int>inputs_;
    double time_;

    SMVector<SMLevelManager*>historys_;

    std::shared_mutex rw_mutex_;
};

/**
 * DeBugStrategy: only for debug
 */
class DeBugStrategy : public LevelStrategy{
public:
    DeBugStrategy(size_t total_height)
        :LevelStrategy(total_height){}
    SMString Name() {return "DebugStrategy";}
    bool Insert(LevelManager* level_mgr){
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        list_.push_back(1);
        return true;
    }
    bool Serach(LevelManager* level_mgr,int id){
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        if(list_.size())
            std::cout<<"["<<pthread_self()<<"]: "<<list_.back()<<std::endl;
        else{
            return false;
        }
        return true;
    }
    bool Remove(LevelManager* level_mgr){
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        list_.pop_back();
        return true;
    }
    bool Insert(NodeCollector* node_collector){
        return true;
    }
    bool Remove(NodeCollector* node_collector){
        return true;
    }
    bool Search(NodeCollector* node_collector){
        return true;
    }
    
private:
    std::shared_mutex rw_mutex_;
    SMVector<int> list_;
};

/**
 * LevelStrategyContext
 */
class LevelStrategyContext{
public:
    void SetStrategy(int l,int total_height){
        switch(l){
            case 0: {
                strategy_ = (DeBugStrategy*)ShmemAlloc(sizeof(DeBugStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                new (strategy_) DeBugStrategy(total_height);
            }break;
            case 1 :{
                strategy_ = (LevelHashStrategy*)ShmemAlloc(sizeof(LevelHashStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                new (strategy_)LevelHashStrategy(total_height);
            }break;
            case 2 :{
                strategy_ = (LevelScalingStrategy*)ShmemAlloc(sizeof(LevelScalingStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                new (strategy_)LevelScalingStrategy(total_height);
            }break;
            case 3 :{
                strategy_ = (LevelRangeStrategy*)ShmemAlloc(sizeof(LevelRangeStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                new (strategy_)LevelRangeStrategy(total_height);
            }break;
            case 4 :{
                strategy_ = (LevelSortStrategy*)ShmemAlloc(sizeof(LevelSortStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                new (strategy_)LevelSortStrategy(total_height);
            }break;
            case 5 :{
                strategy_ = (LevelAggStrategy*)ShmemAlloc(sizeof(LevelAggStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");                
                new (strategy_)LevelAggStrategy(total_height);
            }break;
            case 6 :{
                strategy_ = (LevelResidualStrategy*)ShmemAlloc(sizeof(LevelResidualStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");    
                new (strategy_)LevelResidualStrategy(total_height);
            }break;
            case 7: {
                strategy_ = (LeafStrategy*)ShmemAlloc(sizeof(LeafStrategy));
                if (!strategy_)
                    elog(ERROR, "ShmemAlloc failed: not enough shared memory");    
                new (strategy_)LeafStrategy(total_height);
            }break;
            default :{
                std::cerr<<"unknon level strategy"<<std::endl;
                exit(-1);
            }
        }
    }

    bool Insert(LevelManager* level_mgr);
    bool Remove(LevelManager* level_mgr);
    bool Search(LevelManager* level_mgr,int id);

    bool Insert(NodeCollector* node_collector);
    bool Remove(NodeCollector* node_collector);
    bool Search(NodeCollector* node_collector);

private:
    LevelStrategy* strategy_;
};


