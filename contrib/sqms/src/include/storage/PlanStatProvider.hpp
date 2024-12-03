#include<string>
/**
 * interface of slow plan statistic provider
 */
class SlowPlanStatProvider{
public:
    std::string getName();
    void PutStat();
    bool GetStat();
};