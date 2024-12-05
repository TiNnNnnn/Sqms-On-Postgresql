#pragma once
#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include "collect/format.pb-c.h"

class LevelStrategy{
public:
    virtual ~LevelStrategy() = default;
    virtual std::vector<int> findChildren(HistorySlowPlanStat* hsps) = 0;
    virtual std::string Name() = 0;
};

class LevelStrategyContext{
    void executeStrategy(int l, HistorySlowPlanStat* hsps){
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
    LevelOneStrategy(HistorySlowPlanStat* hsps){
        hsps_ = hsps;
    }
    std::string Name(){return "PlanHashStrategy";}
    std::vector<int> findChildren(HistorySlowPlanStat* hsps);
private:
    HistorySlowPlanStat* hsps_;
};


class LevelTwoStrategy : public LevelStrategy{
public:
    LevelTwoStrategy(HistorySlowPlanStat* hsps){
        hsps_ = hsps;
    }
    std::string Name(){return "PlanEqulPredsStrategy";}
    std::vector<int> findChildren(HistorySlowPlanStat* hsps);
private:
    HistorySlowPlanStat* hsps_;
};

