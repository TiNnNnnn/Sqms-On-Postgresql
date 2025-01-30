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

extern "C" {
    #include "collect/format.pb-c.h"
    #include <pthread.h>
}

class LevelStrategyContext;
class HistoryQueryIndexNode{
    typedef std::vector<std::string> SET;
public:
    HistoryQueryIndexNode(int l,int total_height);
    ~HistoryQueryIndexNode(){};
    
    std::shared_ptr<HistoryQueryIndexNode> Child(size_t l,HistorySlowPlanStat* hsps);
    size_t Level(){return level_;}

    bool Insert(LevelManager* level_mgr);
    bool Remove(LevelManager* level_mgr);
    bool Search(LevelManager* level_mgr,int id);
private: 
    /*use context to operate node*/
    LevelStrategyContext* level_strategy_context_;
    /*current node level*/
    size_t level_;
};

class HistoryQueryIndexNodeContext{
public:
    int id_;
    int mid_;
};

class LevelStrategy{
public:
    LevelStrategy(size_t total_height)
        :total_height_(total_height){}
    virtual std::string Name() = 0;
    virtual bool Insert(LevelManager* level_mgr) = 0;
    virtual bool Serach(LevelManager* level_mgr,int id) = 0;
    virtual bool Remove(LevelManager* level_mgr) = 0;
    size_t FindNextInsertLevel(LevelManager* level_mgr, size_t cur_level);

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
    std::string Name(){return "PlanHashStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);
private:
    SMConcurrentHashMap<std::string,HistoryQueryIndexNode*>set_map_;
};

/**
 * ScalingInfo
 * - join_type_list
 * TODO: 01-23 maybe we can put indexname , cols here to scale  
 */
class ScalingInfo{
public:
    ScalingInfo(std::vector<std::string,SharedMemoryAllocator<std::string>> join_type_list)    
        :join_type_list_(join_type_list){
        join_type_score_ = CalJoinTypeScore(join_type_list_,unique_id_);
    }
    ScalingInfo(std::vector<std::string> join_type_list){
        for(const auto& type : join_type_list){
            join_type_list_.push_back(type);
        }
        join_type_score_ = CalJoinTypeScore(join_type_list_,unique_id_);
    }
    static int CalJoinTypeScore(const SMVector<std::string>& join_type_list,std::string& unique_id);
    bool Match(ScalingInfo* scale_info);
    const std::string& UniqueId(){return unique_id_;}
    int JoinTypeScore(){return join_type_score_;}
private:
    //std::vector<std::string,SharedMemoryAllocator<std::string>> join_type_list_;
    SMVector<std::string> join_type_list_;
    int join_type_score_ = -1;
    std::string unique_id_;
};

/**
 * LevelScalingStrategy
 */
class LevelScalingStrategy : public LevelStrategy{
public:
    LevelScalingStrategy(size_t total_height): LevelStrategy(total_height){}
    std::string Name(){return "PlanHashStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);
private:
    std::shared_mutex rw_mutex_;
    /* from join_type_score to scaling_list id map */
    SMMap<int,SMUnorderedSet<std::string>>scaling_idx_;
    SMUnorderedMap<std::string,std::pair<std::shared_ptr<ScalingInfo>,HistoryQueryIndexNode*>> child_map_;
};

class LevelAggStrategy : public LevelStrategy{
public:
    LevelAggStrategy(size_t total_height)
        :LevelStrategy(total_height){}
    std::string Name(){return "PlanGroupKeyStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);   
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    SMConcurrentHashMap<uint32_t,std::shared_ptr<HistoryQueryIndexNode>>child_map_;
};

class LevelSortStrategy : public LevelStrategy{
public:
    LevelSortStrategy(size_t total_height)
        :LevelStrategy(total_height){}
    std::string Name(){return "PlanGroupKeyStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);   
private:    
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    SMConcurrentHashMap<uint32_t,std::shared_ptr<HistoryQueryIndexNode>>child_map_;
};

class LevelRangeStrategy : public LevelStrategy{
public:
    LevelRangeStrategy(size_t total_height)
        : LevelStrategy(total_height){}
    std::string Name(){return "PlanRangePredsStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    SMUnorderedMap<SET,SMUnorderedMap<LevelPredEquivlences*,std::shared_ptr<HistoryQueryIndexNode>>,SetHasher> child_map_;
    std::shared_mutex rw_mutex_;
    size_t s_level_;
};

class LevelResidualStrategy : public LevelStrategy{
public:
    LevelResidualStrategy(size_t total_height)
        : LevelStrategy(total_height){}
    std::string Name(){return "PlanRangePredsStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    SMUnorderedMap<SET,SMUnorderedMap<LevelPredEquivlences*,std::shared_ptr<HistoryQueryIndexNode>>,SetHasher> child_map_;

    std::shared_mutex rw_mutex_;
    size_t s_level_;
};

/**
 * Leaf Strategy: 
 * - node stat
 * - level stat
 */
class LeafStrategy : public LevelStrategy{
public:
    LeafStrategy(size_t total_height)
        : LevelStrategy(total_height){}
    std::string Name(){return "LeafStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);
private:
    bool SerachAgg(LevelManager* src_mgr,int h,int id);
    bool SerachSort(LevelManager* src_mgr,int h,int id);
    bool SerachRange(LevelManager* src_mgr,int h,int id);
    bool SerachResidual(LevelManager* src_mgr,int h,int id);
private:
    std::shared_ptr<LevelManager> level_mgr_;
    SMVector<std::shared_ptr<LevelManager>>historys_;
};

/**
 * DeBugStrategy: only for debug
 */
class DeBugStrategy : public LevelStrategy{
public:
    DeBugStrategy(size_t total_height)
        :LevelStrategy(total_height){}
    std::string Name() {return "DebugStrategy";}
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
        bool found = true;
        switch(l){
            case 0: {
                //std::cout<<"DeBugStrategy"<<std::endl;
                strategy_ = (DeBugStrategy*)ShmemInitStruct("DeBugStrategy",sizeof(DeBugStrategy),&found);
                if(!found){
                    new (strategy_) DeBugStrategy(total_height);
                }
            }break;
            case 1 :{
                strategy_ =  new LevelHashStrategy(total_height);
            }break;
            case 2 :{
                strategy_ =  new LevelScalingStrategy(total_height);
            }break;
            case 3 :{
                strategy_ = new LevelRangeStrategy(total_height);
            }break;
            case 4 :{
                strategy_ = new LevelSortStrategy(total_height);
            }break;
            case 5 :{
                strategy_ =  new LevelAggStrategy(total_height);
            }break;
            case 6 :{
                strategy_ = new LevelResidualStrategy(total_height);
            }break;
            case 7: {
                strategy_ = new LeafStrategy(total_height);
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

private:
    LevelStrategy* strategy_;
};