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
}

class LevelStrategyContext;
class HistoryQueryIndexNode{
    typedef std::vector<std::string> SET;
public:
    HistoryQueryIndexNode(int l,int total_height);
    ~HistoryQueryIndexNode(){
        free(level_strategy_context_);
    };
    
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
    virtual std::vector<std::string> findChildren() = 0;
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
    std::vector<std::string> findChildren();
private:
    tbb::concurrent_hash_map<std::string,HistoryQueryIndexNode*>set_map_;
};

/**
 * ScalingInfo
 * - join_type_list
 * TODO: 01-23 maybe we can put indexname , cols here to scale  
 */
class ScalingInfo{
public:
    ScalingInfo(std::vector<std::string> join_type_list)    
        :join_type_list_(join_type_list){
        join_type_score_ = CalJoinTypeScore(join_type_list_,unique_id_);
    }
    static int CalJoinTypeScore(const std::vector<std::string>& join_type_list,std::string& unique_id);
    bool Match(ScalingInfo* scale_info);
    const std::string& UniqueId(){return unique_id_;}
    int JoinTypeScore(){return join_type_score_;}
private:
    std::vector<std::string> join_type_list_;
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
    std::vector<std::string> findChildren();
private:
    std::shared_mutex rw_mutex_;
    /* from join_type_score to scaling_list id map */
    std::map<int,std::unordered_set<std::string>> scaling_idx_;
    /* from scaling_list id map to child node*/
    std::unordered_map<std::string,std::pair<std::shared_ptr<ScalingInfo>,HistoryQueryIndexNode*>>child_map_;
};

class LevelAggStrategy : public LevelStrategy{
public:
    LevelAggStrategy(size_t total_height)
        :LevelStrategy(total_height){}
    std::string Name(){return "PlanGroupKeyStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);   
    std::vector<std::string> findChildren();
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    tbb::concurrent_hash_map<SET,std::shared_ptr<HistoryQueryIndexNode>,SetHasher> child_map_;
    
};

class LevelSortStrategy : public LevelStrategy{
public:
    LevelSortStrategy(size_t total_height)
        :LevelStrategy(total_height){}
    std::string Name(){return "PlanGroupKeyStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);   
    std::vector<std::string> findChildren();
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    tbb::concurrent_hash_map<uint32_t,std::shared_ptr<HistoryQueryIndexNode>> child_map_;
};

class LevelRangeStrategy : public LevelStrategy{
public:
    LevelRangeStrategy(size_t total_height)
        : LevelStrategy(total_height){}
    std::string Name(){return "PlanRangePredsStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr,int id);
    bool Remove(LevelManager* level_mgr);
    std::vector<std::string> findChildren();
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    std::unordered_map<SET,std::unordered_map<LevelPredEquivlences *,std::shared_ptr<HistoryQueryIndexNode>>,SetHasher> child_map_;
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
    std::vector<std::string> findChildren();
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    std::unordered_map<SET,std::unordered_map<LevelPredEquivlences *,std::shared_ptr<HistoryQueryIndexNode>>,SetHasher> child_map_;
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

    std::vector<std::string> findChildren(){}
private:
    bool SerachAgg(LevelManager* src_mgr,int h,int id);
    bool SerachSort(LevelManager* src_mgr,int h,int id);
    bool SerachRange(LevelManager* src_mgr,int h,int id);
    bool SerachResidual(LevelManager* src_mgr,int h,int id);
private:
    std::shared_ptr<LevelManager> level_mgr_;
};

/**
 * LevelStrategyContext
 */
class LevelStrategyContext{
public:
    void SetStrategy(int l,int total_height){
        switch(l){
            case 1 :{
                strategy_ =  std::make_shared<LevelHashStrategy>(total_height);
            }break;
            case 2 :{
                strategy_ =  std::make_shared<LevelScalingStrategy>(total_height);
            }break;
            case 3 :{
                strategy_ = std::make_shared<LevelRangeStrategy>(total_height);
            }break;
            case 4 :{
                strategy_ = std::make_shared<LevelSortStrategy>(total_height);
            }break;
            case 5 :{
                strategy_ =  std::make_shared<LevelAggStrategy>(total_height);
            }break;
            case 6 :{
                strategy_ = std::make_shared<LevelResidualStrategy>(total_height);
            }break;
            case 7: {
                strategy_ = std::make_shared<LeafStrategy>(total_height);
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
    std::shared_ptr<LevelStrategy> strategy_;
};