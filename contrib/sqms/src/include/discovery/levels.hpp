#pragma once
#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include "collect/format.pb-c.h"
#include "inverted_index.hpp"
#include "tbb/concurrent_hash_map.h"

class HistoryQueryIndexNode;
class LevelStrategy{
public:
    virtual std::vector<std::string> findChildren() = 0;
    virtual std::string Name() = 0;
};

class LevelOneStrategy : public LevelStrategy{
public:
    std::string Name(){return "PlanHashStrategy";}

    std::vector<std::string> findChildren();
private:
    HistorySlowPlanStat* hsps_;
    tbb::concurrent_hash_map<std::vector<std::string>,HistoryQueryIndexNode*> set_map_;
};

class LevelTwoStrategy : public LevelStrategy{
public:
    std::string Name(){return "PlanEqulPredsStrategy";}
    std::vector<std::string> findChildren();
private:
    HistorySlowPlanStat* hsps_;
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
};

class LevelThreeStrategy : public LevelStrategy{
public:
    std::string Name(){return "PlanRangePredsStrategy";}
    std::vector<std::string> findChildren();
private:
    HistorySlowPlanStat* hsps_;
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
};

/**
 * LevelStrategyContext
 */
class LevelStrategyContext{
public:
    void SetStrategy(int l){
        switch(l){
            case 1 :{
                strategy_ =  std::make_shared<LevelOneStrategy>();
            }break;
            case 2 :{
                strategy_ =  std::make_shared<LevelTwoStrategy>();
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

    void Insert();
    void Remove();
    void Search();

private:
    std::shared_ptr<LevelStrategy> strategy_;
};