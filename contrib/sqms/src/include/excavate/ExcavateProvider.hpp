#include <iostream>
#include <memory>
#include "collect/format.pb-c.h"

class PlanState;
class AbstractExcavateStrategy {
public:
    virtual ~AbstractExcavateStrategy() = default;
    virtual bool excavate() const = 0;
};

class CostBasedExcavateStrategy: public AbstractExcavateStrategy{
public:
    CostBasedExcavateStrategy(HistorySlowPlanStat* hsps){
        hsps_ = hsps;
    }
    bool excavate() const override;
private:
    HistorySlowPlanStat* hsps_;
};

class ExternalResourceExcavateStrategy : public AbstractExcavateStrategy{
public:
    bool excavate() const override;
};
