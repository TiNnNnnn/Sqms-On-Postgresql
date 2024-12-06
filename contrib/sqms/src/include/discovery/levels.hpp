#pragma once
#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include "collect/format.pb-c.h"
#include "inverted_index.hpp"

class LevelStrategy{
public:
    virtual ~LevelStrategy() = default;
    virtual std::vector<std::string> findChildren(HistorySlowPlanStat* hsps) = 0;
    virtual std::string Name() = 0;
};

class LevelStrategyContext{
    void executeStrategy(int l, HistorySlowPlanStat* hsps,std::shared_ptr<InvertedIndex<PostingList>> inverted_idx){
        switch(l){
            case 1 :{
                strategy_ =  std::make_shared<LevelOneStrategy>(hsps);
            }break;
            case 2 :{
                strategy_ =  std::make_shared<LevelOneStrategy>(hsps);
            }break;
        }
        if (strategy_) {
            strategy_->findChildren(hsps);
        } else {
            std::cerr << "No level strategy set!" << std::endl;
        }
    }
private:
    std::shared_ptr<LevelStrategy> strategy_;
};

class LevelOneStrategy : public LevelStrategy{
public:
    LevelOneStrategy(HistorySlowPlanStat* hsps,std::shared_ptr<InvertedIndex<PostingList>> inverted_idx)
        :hsps_(hsps),inverted_idx_(inverted_idx){}
    std::string Name(){return "PlanHashStrategy";}
    std::vector<std::string> findChildren();
private:
    HistorySlowPlanStat* hsps_;
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
};

class LevelTwoStrategy : public LevelStrategy{
public:
    LevelTwoStrategy(HistorySlowPlanStat* hsps,std::shared_ptr<InvertedIndex<PostingList>> inverted_idx)
        :hsps_(hsps),inverted_idx_(inverted_idx){}
    std::string Name(){return "PlanEqulPredsStrategy";}
    std::vector<std::string> findChildren();
private:
    HistorySlowPlanStat* hsps_;
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
};

class LevelThreeStrategy : public LevelStrategy{
public:
    LevelThreeStrategy(HistorySlowPlanStat* hsps,std::shared_ptr<InvertedIndex<PostingList>> inverted_idx)
        :hsps_(hsps),inverted_idx_(inverted_idx){}
    std::string Name(){return "PlanRangePredsStrategy";}
    std::vector<std::string> findChildren();
private:
    HistorySlowPlanStat* hsps_;
    std::shared_ptr<InvertedIndex<PostingList>> inverted_idx_;
};