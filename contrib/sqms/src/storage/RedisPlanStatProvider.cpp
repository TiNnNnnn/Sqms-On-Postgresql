#include "storage/RedisPlanStatProvider.hpp"

void ConnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Redis connection error: %s\n", c->errstr);
    } else {
        printf("Redis connected success\n");
    }
}

void DisconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        std::cerr << "Error: " << c->errstr << std::endl;
    } else {
        std::cout << "Disconnected from Redis" << std::endl;
    }
}

RedisSlowPlanStatProvider::RedisSlowPlanStatProvider(const std::string& redis_host, int redis_port,long totalFetchTimeoutMillis, long totalSetTimeMillis, long defaultTTLSeconds)
        : totalFetchTimeoutMillis_(totalFetchTimeoutMillis),
          totalSetTimeMillis_(totalSetTimeMillis),
          defaultTTLSeconds_(defaultTTLSeconds) {
        /*async connect to redis*/
        redisContext_ = redisAsyncConnect(redis_host.c_str(), redis_port);
        if (redisContext_ == nullptr || redisContext_->err) {
            throw std::runtime_error("Unable to connect to Redis server");
        }
        redisLibuvAttach(redisContext_, uv_default_loop());
        redisAsyncSetConnectCallback(redisContext_, ConnectCallback);
        redisAsyncSetDisconnectCallback(redisContext_, DisconnectCallback);
}

std::string RedisSlowPlanStatProvider::getName(){
    return "redis";
}

void RedisSlowPlanStatProvider::PutStat(std::string hash_plan, HistorySlowPlanStat* hsps){
    redisProviderApiStats_.execute<void>([&](){
        
    }, RedisProviderApiStats::Operation::PutStats);
}





