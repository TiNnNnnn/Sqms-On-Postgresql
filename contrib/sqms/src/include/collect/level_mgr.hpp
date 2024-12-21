#pragma once
#include<vector>
#include<string>
#include<algorithm>
#include<iostream>
#include<unordered_set>
#include<set>
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

enum PType{
    /*for pred without range, we just make const = lower_limit*/
    EQUAL = 0,
    RANGE,
};

/**
 *TODO：should we note the range data type? such as int,string,and so on ...
 */
class PredEquivlenceRange{
public:
    PredEquivlenceRange(const std::string&left = LOWER_LIMIT,const std::string&right = UPPER_LIMIT)
        :lower_limit_(left),upper_limit_(right){}
    PType type_;
    std::string lower_limit_ = LOWER_LIMIT;
    std::string upper_limit_ = UPPER_LIMIT;
};
/**
 * LevelPredEquivlences: storage a equivlences. we sort all ranges classes by lower bound to accelate serach
 * example: 
 * 1. set: {A.a,B.b} , ranges: {150,200}
 * 2. set：{C.c}     , ranges: {250,UPPER_LIMIT}
 */
class PredEquivlence{
    struct RangesCompare {
    bool operator()(const PredEquivlenceRange* per1, const PredEquivlenceRange* per2) const {
        if (per1->lower_limit_ != per2->lower_limit_)
            return per1->lower_limit_ < per2->lower_limit_;
        return per1->upper_limit_ < per2->upper_limit_;
    }
    };
public:
    bool Insert(const std::string& s);
    bool Delete(Quals* quals);
    bool UpdateRanges(Quals* quals);
    bool UpdateRanges(PredEquivlence* pe);
    bool Serach(Quals* quals);
    bool Compare(PredEquivlence* range);
    bool Copy(PredEquivlence* pe);
    void ShowPredEquivlence();
private:
    std::unordered_set<std::string> set_;
    std::set<PredEquivlenceRange*,RangesCompare>ranges_;
};

class LevelPredEquivlences{
public:
    bool Insert(PredEquivlence* pe);
    bool Delete(PredEquivlence* quals);
    bool UpdateRanges(Quals* quals);
    bool UpdateRanges(PredEquivlence* pe);
    bool Serach(PredEquivlence* quals);
    bool Compare(PredEquivlence* range);
    bool Copy(PredEquivlence* pe);
    void ShowLevelPredEquivlences();
private:
    std::vector<PredEquivlence*> level_pe_list;
    /**bloomfilter: we use it to check whether a attr is in the total level*/
    
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

    /**
     * Temporary derivation of data container
     * we need reverse them wehen all finish
     * */
    std::vector<LevelPredEquivlences*> pre_equlivlences_;
};


