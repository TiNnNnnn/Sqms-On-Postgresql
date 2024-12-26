#pragma once
#include<vector>
#include<string>
#include<algorithm>
#include<iostream>
#include<unordered_set>
#include<unordered_map>
#include<set>
#include<assert.h>
#include<memory>
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
	#include "collect/format.pb-c.h"
}

enum class AbstractPredNodeType{
    OPERATOR = 0,
    QUALS,
};

/*for pred without range, we just make const = lower_limit*/
enum class PType{
    EQUAL = 0,
    RANGE,
    LIST
};

enum class QualType{
    COMMON,
    SUBQUERY,
};

class AbstractPredNode{
public:
    
    AbstractPredNode(AbstractPredNodeType type)
       :type_(type){}

    AbstractPredNodeType Type(){return type_;}
    virtual AbstractPredNode* GetParent() = 0;
    virtual void SetParent(AbstractPredNode* parent) = 0;

    AbstractPredNodeType type_;
};
class LevelPredEquivlences;
class LevelPredEquivlencesList;
class PredOperatorWrap: public AbstractPredNode{
public:
    PredOperatorWrap(PredOperator__PredOperatorType op_type , AbstractPredNode* parent = nullptr)
        :AbstractPredNode(AbstractPredNodeType::OPERATOR)
        ,op_type_(op_type),parent_(parent){}
    
    AbstractPredNode * Child(size_t idx){
        assert(idx < childs_.size() && idx >=0);
        return childs_[idx];
    }

    size_t ChildSize(){return childs_.size();}
    void SetChildSize(int size){
        childs_.resize(size);
    }
    void AddChild(AbstractPredNode* child){childs_.push_back(child);}
    void ReSetChild(AbstractPredNode* child,int pos){childs_[pos] = child;}

    AbstractPredNode* GetParent() override {return parent_;}
    void SetParent(AbstractPredNode* parent) override {parent_ = parent;}

    PredOperator__PredOperatorType GetOpType(){return op_type_;}

    void SetOrLpesList(LevelPredEquivlencesList* or_lpes_list){or_lpes_list_ = or_lpes_list;}
    LevelPredEquivlencesList* GetOrLpesList(){return or_lpes_list_;}

    void SetAndLpesList(LevelPredEquivlencesList* and_lpes_list){and_lpes_list_ = and_lpes_list;}
    LevelPredEquivlencesList* GetAndLpesList(){return and_lpes_list_;}
    
private:
    PredOperator__PredOperatorType op_type_;
    AbstractPredNode* parent_ = nullptr;
    std::vector<AbstractPredNode*>childs_;

    LevelPredEquivlencesList* or_lpes_list_ = nullptr;
    LevelPredEquivlencesList* and_lpes_list_ = nullptr;
};

class QualsWarp: public AbstractPredNode {
public:
    QualsWarp(Quals* qual,AbstractPredNode* parent = nullptr):AbstractPredNode(AbstractPredNodeType::QUALS)
        ,parent_(parent){
        size_t msg_size = quals__get_packed_size(qual);
        uint8_t *buffer = (uint8_t*)malloc(msg_size);
        if (buffer == NULL) {
            perror("Failed to allocate memory");
            return;
        }
       quals__pack(qual,buffer);
       qual_ = quals__unpack(NULL, msg_size, buffer);
    }

    AbstractPredNode* GetParent(){return parent_;}
    void SetParent(AbstractPredNode* parent){parent_ = parent;}

    Quals* GetQual() {return qual_;}

private:
    Quals* qual_ = nullptr;
    AbstractPredNode* parent_ = nullptr;
};
 
/**
 *TODO：should we note the range data type? such as int,string,and so on ...
 */
class PredEquivlence;
class PredEquivlenceRange{
public:
    PredEquivlenceRange(const std::string&left = LOWER_LIMIT,const std::string&right = UPPER_LIMIT)
        :lower_limit_(left),upper_limit_(right){}
    
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
class PredEquivlence {
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

class LevelPredEquivlences{
public:
    bool Insert(Quals* quals,bool only_left = true,bool is_or = false);
    bool Insert(PredEquivlence* pe,bool only_left = true);
    bool Insert(LevelPredEquivlences* pe,bool only_left = true ,bool is_or = false);

    bool Delete(PredEquivlence* quals);
    bool UpdateRanges(Quals* quals);
    bool UpdateRanges(PredEquivlence* pe);
    bool Serach(PredEquivlence* quals);
    bool Compare(PredEquivlence* range);
    bool Copy(LevelPredEquivlences* pe);
    void ShowLevelPredEquivlences();
    std::vector<PredEquivlence*>& LevelPeList(){return level_pe_list_;};

    std::vector<PredEquivlence*>::iterator begin() { return level_pe_list_.begin(); }
    std::vector<PredEquivlence*>::iterator end() { return level_pe_list_.end(); }
    std::vector<PredEquivlence*>::const_iterator begin() const { return level_pe_list_.cbegin(); }
    std::vector<PredEquivlence*>::const_iterator end() const { return level_pe_list_.cend();}

private:
    std::vector<PredEquivlence*> level_pe_list_;
    std::unordered_map<std::string,int> level_idx_;
};

/**
 * LevelPredEquivlencesList: LevelPredEquivlences in this class is or relation.
 */
class LevelPredEquivlencesList{
public:
    LevelPredEquivlencesList(){};
    bool Insert(LevelPredEquivlences* lpes,bool only_left = true ,bool is_or = false);
    bool Insert(LevelPredEquivlencesList* lpes_list,bool is_or = false);
    size_t Size(){return lpes_list_.size();}

    std::vector<LevelPredEquivlences*>::iterator begin() { return lpes_list_.begin(); }
    std::vector<LevelPredEquivlences*>::iterator end() { return lpes_list_.end(); }
    std::vector<LevelPredEquivlences*>::const_iterator begin() const { return lpes_list_.cbegin(); }
    std::vector<LevelPredEquivlences*>::const_iterator end() const { return lpes_list_.cend();}
private:
    std::vector<LevelPredEquivlences*> lpes_list_;
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
    void ExprLevelCollect(PredExpression * tree,std::vector<std::vector<AbstractPredNode*>>& level_collector);

    //void ConvertExpr(PredExpression * tree, LevelPredEquivlences* lpes_tree);

private:
    HistorySlowPlanStat* hsps_; /*plan we need to process to sps_*/
    SlowPlanStat * sps_; /*final output,sps will dircetly storaged*/
    int height_; /*plan height*/

    int cur_height_;
    //std::vector<LevelPredEquivlences*> total_equivlences_;
    std::vector<LevelPredEquivlencesList*> total_equivlences_;
};


