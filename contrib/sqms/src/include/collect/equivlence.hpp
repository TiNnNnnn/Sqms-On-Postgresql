#pragma once
#include<vector>
#include<string>
#include<algorithm>
#include<iostream>
#include "format.pb-c.h"


extern "C"{
    #include "postgres.h"
    #include "access/parallel.h"
    #include "collect/format.h"
    #include "executor/executor.h"
    #include "executor/instrument.h"
    #include "jit/jit.h"
    #include "utils/guc.h"
    #include "common/config.h" 
}

/**
 * Equivlence: 
 */
class PredEquivlenceRange;
class PredEquivlence{
public:    
    bool InsertPredicate();
    bool DeletePredicate();
    
private:
    std::string key;
    std::vector<PredEquivlenceRange*>ranges;
};

/**
 *TODO：should we note the range data type?
 */
class PredEquivlenceRange{
    std::string lower_limit;
    std::string upper_limit;
};

class EquivelenceManager{
public:
    EquivelenceManager(HistorySlowPlanStat* hsps, SlowPlanStat*sps)
        :hsps_(hsps),sps_(sps)
    {}
    void ComputeEquivlenceClass();
    void ComputLevlEquivlenceClass(const std::vector<HistorySlowPlanStat*>& list);

    void ParseExprs(HistorySlowPlanStat* hsps);

    void PredDecompose(PredExpression * root);
private:
    HistorySlowPlanStat* hsps_;
    SlowPlanStat * sps_;
};


