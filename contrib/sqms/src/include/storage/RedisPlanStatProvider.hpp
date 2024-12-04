#include "PlanStatProvider.hpp"
#include "RedisProviderApiStat.hpp"

#include <iostream>
#include <string>
#include <map>
#include <memory>
#include <thread>
#include <vector>
#include <chrono>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <uv.h>
#include <hiredis/adapters/libuv.h>

class RedisSlowPlanStatProvider: public SlowPlanStatProvider{
public:
    RedisSlowPlanStatProvider(const std::string& redis_host, int redis_port,
                                long totalFetchTimeoutMillis, long totalSetTimeMillis, long defaultTTLSeconds);
    ~RedisSlowPlanStatProvider();

    std::string getName();
    void PutStat(std::string hash_plan, HistorySlowPlanStat* hsps);
    bool GetStat(std::string hash_plan);
private:
    redisAsyncContext* redisContext_;
    RedisProviderApiStats redisProviderApiStats_;

    long totalFetchTimeoutMillis_;
    long totalSetTimeMillis_;
    long defaultTTLSeconds_;
};