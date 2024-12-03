#include <iostream>
#include <memory>


class PlanState;
class AbstractExcavateStrategy {
public:
    virtual ~AbstractExcavateStrategy() = default;
    virtual bool excavate() const = 0;
};

class CostBasedExcavateStrategy: public AbstractExcavateStrategy{
public:
    CostBasedExcavateStrategy(PlanState* plan_state){
        plan_state_ = plan_state;
    }
    bool excavate() const override;
private:
    PlanState* plan_state_;
};

class ExternalResourceExcavateStrategy : public AbstractExcavateStrategy{
public:
    bool excavate() const override;
};
