#pragma once
#include<vector>
#include<string>
#include<algorithm>
#include<iostream>
#include<unordered_set>
#include<unordered_map>
#include<set>
#include<memory>
#include "format.pb-c.h"
#include "common/bloom_filter/bloom_filter.hpp"

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






class AbstractPredNode{
protected:
};

enum PType{
    /*for pred without range, we just make const = lower_limit*/
    EQUAL = 0,
    RANGE,
    LIST
};

/**
 *TODO：should we note the range data type? such as int,string,and so on ...
 */
class PredEquivlence;
class PredEquivlenceRange : public AbstractPredNode{
public:
    PredEquivlenceRange(const std::string&left = LOWER_LIMIT,const std::string&right = UPPER_LIMIT)
        :lower_limit_(left),upper_limit_(right),AbstractPredNode(){}
    
    std::string LowerLimit() const {return lower_limit_;}
    std::string UpperLimit() const {return upper_limit_;}
    std::vector<std::string> const List(){return list_;}
    PType PredType(){return type_;}

    void SetLowerLimit(const std::string & lower_limit){lower_limit_ = lower_limit;}
    void SetUpperLimit(const std::string & upper_limit){upper_limit_ = upper_limit;}
    void SetUpperLimit(const std::vector<std::string>& list){list_ = list;}
    void SetPredType(PType type){type_ = type;}
private:
    PType type_;
    std::string lower_limit_;
    std::string upper_limit_;
    std::vector<std::string> list_;
    std::vector<PredEquivlence*>childs_;
};

/**
 * LevelPredEquivlences: storage a equivlences. we sort all ranges classes by lower bound to accelate serach
 * example: 
 * 1. set: {A.a,B.b} , ranges: {150,200}
 * 2. set：{C.c}     , ranges: {250,UPPER_LIMIT}
 */
class PredEquivlence : public AbstractPredNode{
    struct RangesCompare {
    bool operator()(const PredEquivlenceRange* per1, const PredEquivlenceRange* per2) const {
        if (per1->LowerLimit() != per2->LowerLimit())
            return per1->LowerLimit() < per2->LowerLimit();
        return per1->UpperLimit() < per2->UpperLimit();
    }
    };
public:
    bool Insert(const std::string& s);
    bool Insert(Quals* qual,bool only_left = true);
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

// class LevelPredEquivlences{
// public:
//     LevelPredEquivlences(){
//         //bf_ = std::make_unique<BloomFilter>(30);
//     }
//     bool Insert(Quals* quals,bool only_left = true);
//     bool Insert(PredEquivlence* pe);
//     bool Delete(PredEquivlence* quals);
//     bool UpdateRanges(Quals* quals);
//     bool UpdateRanges(PredEquivlence* pe);
//     bool Serach(PredEquivlence* quals);
//     bool Compare(PredEquivlence* range);
//     bool Copy(PredEquivlence* pe);
//     void ShowLevelPredEquivlences();
// private:
//     std::vector<PredEquivlence*> level_pe_list_;
//     /**bloomfilter: we use it to check whether a attr is in the total level*/
//     //std::unique_ptr<FilterPolicy> bf_;

//     std::unordered_map<std::string,int> level_idx_; 
// };


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


