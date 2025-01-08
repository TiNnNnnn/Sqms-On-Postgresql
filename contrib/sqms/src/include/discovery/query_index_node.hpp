#pragma once
#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include "collect/format.pb-c.h"
#include "inverted_index.hpp"
#include <shared_mutex>
#include "collect/level_mgr.hpp"
#include "tbb/concurrent_hash_map.h"
#include <list>

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
    bool Search(LevelManager* level_mgr);
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

class LevelStrategy{
public:
    virtual std::vector<std::string> findChildren() = 0;
    virtual std::string Name() = 0;
    size_t FindNextInsertLevel(LevelManager* level_mgr, size_t cur_level);
};

/**
 * LevelOneStrategy: check the set of subplan's node type set,if the sub_plan's 
 * node type is in (join,project,filter,groupby),we will apply more checking,ohterwise,
 * we just use hash to compare,with time going,we will 
 */
class LevelOneStrategy : public LevelStrategy{
public:
    LevelOneStrategy(size_t total_height)
        :total_height_(total_height){}
    std::string Name(){return "PlanHashStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr);
    bool Remove(LevelManager* level_mgr);
    std::vector<std::string> findChildren();
private:
    tbb::concurrent_hash_map<std::string,HistoryQueryIndexNode*>set_map_;
    size_t total_height_;
};

class LevelTwoStrategy : public LevelStrategy{
public:
    LevelTwoStrategy(size_t total_height)
        :total_height_(total_height){}
    std::string Name(){return "PlanGroupStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr);
    bool Remove(LevelManager* level_mgr);   
    std::vector<std::string> findChildren();
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
    tbb::concurrent_hash_map<SET,std::shared_ptr<HistoryQueryIndexNode>,SetHasher> child_map_;
    size_t total_height_;
};

class LevelThreeStrategy : public LevelStrategy{
public:
    std::string Name(){return "PlanRangePredsStrategy";}
    bool Insert(LevelManager* level_mgr);
    bool Serach(LevelManager* level_mgr);
    bool Remove(LevelManager* level_mgr);
    std::vector<std::string> findChildren();
private:
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
};

/**
 * Leaf Strategy: 
 * - node stat
 * - level stat
 */
class LeafStrategy : public LevelStrategy{
public:
    std::string Name(){return "LeafStrategy";}
    std::vector<std::string> findChildren(){}
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
                strategy_ =  std::make_shared<LevelOneStrategy>(total_height);
            }break;
            case 2 :{
                strategy_ =  std::make_shared<LevelTwoStrategy>(total_height);
            }break;
            case 3 :{
                strategy_ = std::make_shared<LevelThreeStrategy>();
            }break;
            default :{
                std::cerr<<"unknon level strategy"<<std::endl;
                exit(-1);
            }
        }
    }

    void Insert(LevelManager* level_mgr);
    void Remove(LevelManager* level_mgr);
    void Search(LevelManager* level_mgr);

private:
    std::shared_ptr<LevelStrategy> strategy_;
};