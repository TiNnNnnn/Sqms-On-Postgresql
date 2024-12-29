#pragma once
#include<memory>
#include "ExcavateProvider.hpp"

class ExcavateContext{
public:
    void setStrategy(std::shared_ptr<AbstractExcavateStrategy> strategy) {
        strategy_ = strategy;
    }

    void executeStrategy(std::vector<HistorySlowPlanStat*>& list) const {
        if (strategy_) {
            strategy_->excavate(list);
        } else {
            std::cerr << "No excavate strategy set!" << std::endl;
        }
    }
private:
    std::shared_ptr<AbstractExcavateStrategy> strategy_;
};