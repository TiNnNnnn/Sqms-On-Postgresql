#include <functional>
#include <stdexcept>
#include <thread>
#include <map>
#include <memory>
#include "common/counter.hpp"

class CounterStat;
class TimeStat;
class TimeoutException;

class Logger {
public:
    static Logger& get(const std::string& name) {
        static Logger logger;
        return logger;
    }

    void error(const std::string& message) {
        std::cerr << "[ERROR] " << message << std::endl;
    }

    void debug(const std::string& message) {
        std::cerr << "[DEBUG] " << message << std::endl;
    }
};

class RedisProviderApiStats{
public:
    enum Operation
    {
        FetchStats,
        PutStats
    };

    RedisProviderApiStats()
        : fetchTime(1000), putTime(1000),
          putStatRequest(), fetchStatRequest(),
          putStatGenericFailure(), fetchStatGenericFailure(),
          putStatTimeoutFailure(), fetchStatTimeoutFailure(),
          putStatRedisFailure(), fetchStatRedisFailure() {}
    
    TimeStat& getTime(Operation operation) {
        return (operation == Operation::FetchStats) ? fetchTime : putTime;
    }

    CounterStat& getStatRequest(Operation operation) {
        return (operation == Operation::FetchStats) ? fetchStatRequest : putStatRequest;
    }

    CounterStat& getGenericFailureStat(Operation operation) {
        return (operation == Operation::FetchStats) ? fetchStatGenericFailure : putStatGenericFailure;
    }

    CounterStat& getTimeoutFailureStat(Operation operation) {
        return (operation == Operation::FetchStats) ? fetchStatTimeoutFailure : putStatTimeoutFailure;
    }

    CounterStat& getRedisFailureStat(Operation operation) {
        return (operation == Operation::FetchStats) ? fetchStatRedisFailure : putStatRedisFailure;
    }

    template <typename V>
    V execute(std::function<V()> callable, Operation operation) {
        try {
            return wrap(callable, operation)();
        }catch (const std::runtime_error& e) {
            handleException(e, operation, "Redis Exception Error");
        } catch (const std::exception& e) {
            handleException(e, operation, "Generic Error");
        }
        return V();
    }

    template <typename V>
    std::function<V()> wrap(std::function<V()> callable, Operation operation) {
        return [this, callable, operation]() {
            try {
                TimeStat::BlockTimer timer = getTime(operation).time();
                getStatRequest(operation).update(1);
                return callable();
            } catch (const std::exception& e) {
                throw e;
            }
        };
    }
private:
    void handleException(const std::exception& e, Operation operation, const std::string& errorType) {
        if (typeid(e) == typeid(std::runtime_error)) {
            getRedisFailureStat(operation).update(1);
            Logger::get("RedisProviderApiStats").error(errorType + ": " + e.what());
        } else {
            getGenericFailureStat(operation).update(1);
            Logger::get("RedisProviderApiStats").error(errorType + ": " + e.what());
        }
    }
   
private:
    TimeStat fetchTime;
    TimeStat putTime;

    CounterStat putStatRequest;
    CounterStat fetchStatRequest;
    CounterStat putStatGenericFailure;
    CounterStat fetchStatGenericFailure;
    CounterStat putStatTimeoutFailure;
    CounterStat fetchStatTimeoutFailure;
    CounterStat putStatRedisFailure;
    CounterStat fetchStatRedisFailure;

};



