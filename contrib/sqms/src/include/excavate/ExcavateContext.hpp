#include<memory>
#include "ExcavateProvider.hpp"

class ExcavateContext{
public:
    void setStrategy(std::shared_ptr<AbstractExcavateStrategy> Strategy) {
        strategy = Strategy;
    }

    void executeStrategy() const {
        if (strategy) {
            strategy->excavate();
        } else {
            std::cerr << "No excavate strategy set!" << std::endl;
        }
    }
private:
    std::shared_ptr<AbstractExcavateStrategy> strategy;
};