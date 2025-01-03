#pragma once
#include<vector>
#include<string>
#include<algorithm>
#include<iostream>
#include<unordered_set>
#include<unordered_map>
#include<set>
#include<map>
#include<assert.h>
#include<memory>
#include "common/bloom_filter/bloom_filter.hpp"
#include "match_strategy.h"
#include "stat_format.hpp"
#include "level_mgr.hpp"

extern "C"{
    #include "postgres.h"
    #include "access/parallel.h"
    #include "collect/format.h"
    #include "executor/executor.h"
    #include "executor/instrument.h"
    #include "jit/jit.h"
    #include "utils/guc.h"
    #include "common/config.h" 
	#include "collect/format.pb-c.h"
}

/**
 * NodeManager
 */
class NodeManager : public AbstractFormatStrategy{
public:
    NodeManager(HistorySlowPlanStat* hsps,std::shared_ptr<LevelManager> level_mgr)
        : hsps_(hsps),level_mgr_(level_mgr){}
    virtual bool Format() override;

private:
    void ComputeTotalNodes();

private:
    int branch_num_;
    HistorySlowPlanStat* hsps_ = nullptr;
    std::shared_ptr<LevelManager> level_mgr_ = nullptr;  
};