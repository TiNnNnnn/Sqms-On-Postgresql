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

class LevelManager{
public:
    LevelManager(HistorySlowPlanStat* hsps, SlowPlanStat*sps)
        :hsps_(hsps),sps_(sps)
    {}
    void ComputeTotalClass();
private:

    void ComputeLevelClass(const std::vector<HistorySlowPlanStat*>& list);
    void HandleNode(HistorySlowPlanStat* hsps);
    void EquivalenceClassesDecompase(PredExpression* root);
    void RangeConstrainedDecompose(PredExpression * root);

private:
    void ExprLevelCollect(PredExpression * tree,std::vector<std::vector<PredExpression *>> level_collector);

private:
    HistorySlowPlanStat* hsps_; /*plan we need to process to sps_*/
    SlowPlanStat * sps_; /*final output,sps will dircetly storaged*/
    int height_; /*plan height*/
};


