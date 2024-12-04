#pragma once
#include<memory>
#include "ExcavateProvider.hpp"

class ExcavateContext{
public:
    void setStrategy(std::shared_ptr<AbstractExcavateStrategy> Strategy) {
        strategy = Strategy;
    }

    void executeStrategy(std::vector<HistorySlowPlanStat*>& list) const {
        if (strategy) {
            strategy->excavate(list);
        } else {
            std::cerr << "No excavate strategy set!" << std::endl;
        }
    }
private:
    std::shared_ptr<AbstractExcavateStrategy> strategy;
};