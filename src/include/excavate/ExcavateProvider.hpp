#pragma once
#include <iostream>
#include <vector>
#include "collect/format.pb-c.h"

class PlanState;
class AbstractExcavateStrategy {
public:
    virtual ~AbstractExcavateStrategy() = default;
    virtual bool excavate(std::vector<HistorySlowPlanStat*>& list) const = 0;
};

class CostBasedExcavateStrategy: public AbstractExcavateStrategy{
public:
    CostBasedExcavateStrategy(HistorySlowPlanStat* hsps){
        hsps_ = hsps;
    }
    bool excavate(std::vector<HistorySlowPlanStat*>& list) const override;
private:
    HistorySlowPlanStat* hsps_;
};

class ExternalResourceExcavateStrategy : public AbstractExcavateStrategy{
public:
    bool excavate(std::vector<HistorySlowPlanStat*>& list) const override;
};

/*NoProcessedExcavateStrategy: Only For Debugging*/
class NoProcessedExcavateStrategy : public AbstractExcavateStrategy{
public:
    NoProcessedExcavateStrategy(HistorySlowPlanStat* hsps){
        hsps_ = hsps;
    }
    bool excavate(std::vector<HistorySlowPlanStat*>& list) const override;
private:
    HistorySlowPlanStat* hsps_;
};
