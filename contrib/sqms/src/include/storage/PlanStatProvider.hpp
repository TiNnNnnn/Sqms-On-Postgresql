#include<string>
#include"collect/format.pb-c.h"

/**
 * interface of slow plan statistic provider
 */
class SlowPlanStatProvider{
public:
    std::string getName();
    void PutStat(std::string hash_plan, HistorySlowPlanStat* hsps);
    bool GetStat(std::string hash_plan);
};