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

typedef enum class AbstractPredNodeType{
    OPERATOR = 0,
    QUALS,
}AbstractPredNodeType;

/*for pred without range, we just make const = lower_limit*/
enum class PType{
    /**/
    JOIN_EQUAL = 0,
    /**/
    NOT_EQUAL,
    EQUAL,
    RANGE,
    /**/
    LIST,
    /**/
    SUBQUERY,
};

enum class PreProcessLabel{
    PREDICATE = 0,
    SORT,
    AGG,
    TABLE,
    OUTPUT,
};

constexpr std::array<PreProcessLabel, 5> p_labels = {
    PreProcessLabel::PREDICATE,
    PreProcessLabel::SORT,
    PreProcessLabel::AGG,
    PreProcessLabel::TABLE,
    PreProcessLabel::OUTPUT
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
            perror("Failed to allocate memory for quals_wrap");
            exit(-1);
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

    void SetLowerLimit(const std::string & lower_limit){lower_limit_ = lower_limit;}
    void SetUpperLimit(const std::string & upper_limit){upper_limit_ = upper_limit;}
    void SetList(const std::vector<std::string>& list){list_ = list;}

    void SetBoundaryConstraint(std::pair<bool,bool> bc){boundary_constraint_ = bc;}
    std::pair<bool,bool>& GetBoundaryConstraint(){return boundary_constraint_;}

    void SetLowerBoundaryConstraint(bool b){boundary_constraint_.first = b;}
    void SetUpperBoundaryConstraint(bool b){boundary_constraint_.second = b;}
    bool GetLowerBoundaryConstraint(){return boundary_constraint_.first;}
    bool GetUpperBoundaryConstraint(){return boundary_constraint_.second;}
    
    void SetPredType(PType type){type_ = type;}
    PType PredType(){return type_;}

    void Copy(PredEquivlenceRange* new_range);
    bool Serach(PredEquivlenceRange* range);

    void PrintPredEquivlenceRange(int depth = 0);

private:
    PType type_;
    std::string lower_limit_ = LOWER_LIMIT;
    std::string upper_limit_ = UPPER_LIMIT;
    std::pair<bool,bool> boundary_constraint_ = std::make_pair(true,true);
    std::vector<std::string> list_;
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
    PredEquivlence(){}
    PredEquivlence(Quals* qual);

    static bool IsOnlyLeft(Quals* qual);
    static PType QualType(Quals* qual);
    static bool PredVariable(NodeTag node_tag);

    bool Insert(PredEquivlence* pe, bool check_can_merged = true);

    bool Delete(Quals* quals);
    
    bool Serach(Quals* quals);
    bool Serach(PredEquivlence* pe);
    bool Serach(const std::string& attr);

    bool RangesSerach(PredEquivlenceRange* range,std::vector<PredEquivlenceRange*>& merge_pe_list);

    bool Compare(PredEquivlence* range);

    bool Copy(PredEquivlence* pe);

    void ShowPredEquivlence(int depth = 0);
    void ShowPredEquivlenceSets(int depth = 0);

    std::set<std::string>& GetPredSet(){return set_;}
    void SetPredSet(std::set<std::string> set){set_ = set;}

    std::set<PredEquivlenceRange*,RangesCompare>& GetRanges(){return ranges_;}
    void SetRanges(std::set<PredEquivlenceRange*,RangesCompare> ranges){ranges_ = ranges;}

    bool MergePredEquivlenceRanges(const std::vector<PredEquivlenceRange*>& merge_pe_list);
private:
    std::set<std::string> set_;
    std::set<PredEquivlenceRange*,RangesCompare>ranges_;
};

class LevelPredEquivlences{
public:
    bool Insert(Quals* quals,bool is_or = false);
    bool Insert(PredEquivlence* pe);
    bool Insert(LevelPredEquivlences* pe);

    bool Delete(PredEquivlence* quals);
    bool UpdateRanges(Quals* quals);
    bool UpdateRanges(PredEquivlence* pe);

    /*search for insert*/
    bool Serach(Quals* quals,std::vector<PredEquivlence*>& merge_pe_list);
    bool Serach(PredEquivlence* pe, std::vector<PredEquivlence*>& merge_pe_list);

    bool Serach(const std::string& attr,PredEquivlence*& pe);
    
    bool Compare(PredEquivlence* range);
    bool Copy(LevelPredEquivlences* pe);

    bool MergePredEquivlences(const std::vector<PredEquivlence*>& merge_pe_list);

    void ShowLevelPredEquivlences(int depth = 0);

    std::unordered_set<PredEquivlence*>& LevelPeList(){return level_pe_sets_;};

    std::unordered_set<PredEquivlence*>::iterator begin() { return level_pe_sets_.begin(); }
    std::unordered_set<PredEquivlence*>::iterator end() { return level_pe_sets_.end(); }
    std::unordered_set<PredEquivlence*>::const_iterator begin() const { return level_pe_sets_.cbegin(); }
    std::unordered_set<PredEquivlence*>::const_iterator end() const { return level_pe_sets_.cend(); }

private:
    std::unordered_set<PredEquivlence*> level_pe_sets_;

    /**
     * level pred equivlences index:
     * example:
     *    1. A.a --> 0, B.b --> 0,C.c --> 1
     */
    //std::unordered_map<std::string,int> level_idx_;
};

/**
 * LevelPredEquivlencesList: LevelPredEquivlences in this class is or relation.
 */
class LevelPredEquivlencesList{
public:
    LevelPredEquivlencesList(){};
    bool Insert(LevelPredEquivlences* lpes,bool is_or = false);
    bool Insert(LevelPredEquivlencesList* lpes_list,bool is_or = false);

    void Copy(LevelPredEquivlencesList* new_lpes_list);
    size_t Size(){return lpes_list_.size();}

    std::vector<LevelPredEquivlences*>::iterator begin() { return lpes_list_.begin(); }
    std::vector<LevelPredEquivlences*>::iterator end() { return lpes_list_.end(); }
    std::vector<LevelPredEquivlences*>::const_iterator begin() const { return lpes_list_.cbegin(); }
    std::vector<LevelPredEquivlences*>::const_iterator end() const { return lpes_list_.cend();}

    void ShowLevelPredEquivlencesList(int depth);

private:
    std::vector<LevelPredEquivlences*> lpes_list_;
};

/**
 * LevelOutput 
 */
class LevelOutputList{
    typedef std::unordered_set<std::string> USET;
    typedef std::unordered_map<std::string, PredEquivlence*> UMAP;
public:
    LevelOutputList(){};
    void Insert(LevelPredEquivlencesList* lpes_list,HistorySlowPlanStat* hsps);
    void Insert(LevelOutputList* lo_list);
    void ShowLevelOutputList(int depth = 0);

    const std::vector<UMAP>& GetOutput2PeList(){return output2pe_list_;}
    const std::vector<USET>& GetOutputExtendList(){return output_extend_list_;}

private:
    std::vector<UMAP> output2pe_list_;
    std::vector<USET> output_extend_list_;
};

/**
 * NodeAgg
 */
class AggAndSortEquivlence{
public:
    AggAndSortEquivlence(std::string tag)
        :tag_(tag)
    {}
    void Init(HistorySlowPlanStat* hsps,LevelPredEquivlences* lpes = nullptr);
    void ShowAggEquivlence(int depth = 0);
private:
    std::map<std::string, PredEquivlence*>key2pe_;
    std::set<std::string> extends_;
    /*agg true level*/
    int agg_level_;
    std::string tag_;
};
/**
 * LevelAggEquivlnces
 */
class LevelAggAndSortEquivlences{
public:
    void Insert(AggAndSortEquivlence* ae);
    void Insert(LevelAggAndSortEquivlences* level_ae);
    void ShowLevelAggEquivlence(int depth = 0);
private:
    std::vector<AggAndSortEquivlence*> level_agg_sets_;
};

/**
 * LevelAggList
 */
class LevelAggAndSortList{
    typedef std::unordered_set<std::string> USET;
    typedef std::unordered_map<std::string, PredEquivlence*> UMAP;
public:
    LevelAggAndSortList(LevelPredEquivlencesList* lpes_list)
        : lpes_list_(lpes_list)
    {}
    void Insert(HistorySlowPlanStat* hsps, std::string label);
    void Insert(LevelAggAndSortList* la_list);
    void ShowLevelAggAndSortList(int depth = 0);

    int Size(){return level_agg_list_.size();}
    void Copy(LevelAggAndSortList* lal);

    const std::vector<LevelAggAndSortEquivlences*>& GetLevelAggList(){return level_agg_list_;}

private:
    std::vector<LevelAggAndSortEquivlences*> level_agg_list_;
    LevelPredEquivlencesList* lpes_list_ = nullptr;
};


/**
 * LevelTblList:
 * TODO: we need pay attention to the order of the table?
 */
class LevelTblList{
public:
    void Insert(const std::string& tbl_name){tbl_set_.insert(tbl_name);}
    void Insert(LevelTblList* lt_list);
    void ShowLevelTblList(int depth = 0);
    const std::set<std::string>& GetTblSet(){return tbl_set_;}
private:
    std::set<std::string> tbl_set_;
};

/**
 * NodeCollector: supplementary information for hsps node
 */
class NodeCollector{
public:
    /* total equivlences for predicates */
    LevelPredEquivlencesList* node_equivlences_ = nullptr;
    /* total equivlences for outputs */
    LevelOutputList* node_outputs_ = nullptr;
    /* total equivlences for agg keys */
    LevelAggAndSortList* node_aggs_ = nullptr;
    /* total equivlences for sort keys */
    LevelAggAndSortList* node_sorts_ = nullptr;
    /* total sets for tables */
    LevelTblList* node_tbls_ = nullptr;
};

/**
 * LevelManager
 */
class LevelManager : public AbstractFormatStrategy{
public:
    LevelManager(HistorySlowPlanStat* hsps, SlowPlanStat*sps,MatchStrategy ms = C_DEFALUT)
        :hsps_(hsps),sps_(sps),ms_(ms)
    {}

    virtual bool Format() override;

    std::unordered_map<HistorySlowPlanStat*, NodeCollector*>& GetNodeCollector(){return nodes_collector_map_;}
    
    void ComputeTotalClass(); 
    void ShowPredClass(int height,int depth = 0);
    void ShowTotalPredClass(int depth = 0);

private:

    void ComputeLevelClass(const std::vector<HistorySlowPlanStat*>& list);
    void HandleEquivleces(HistorySlowPlanStat* hsps);

    void HandleNode(HistorySlowPlanStat* hsps);
    
    void PredEquivalenceClassesDecompase(PredExpression* root);
    void TblDecompase(HistorySlowPlanStat* hsps);
    void OutputDecompase(HistorySlowPlanStat* hsps);
    void GroupKeyDecompase(HistorySlowPlanStat* hsps);
    void SortKeyDecompase(HistorySlowPlanStat* hsps);
private:
    void ExprLevelCollect(PredExpression * tree,std::vector<std::vector<AbstractPredNode*>>& level_collector);
    bool GetPreProcessed(PreProcessLabel label){return pre_processed_map_[label];}
    void SetPreProcessed(PreProcessLabel label,bool b){pre_processed_map_[label] = b;}
    void ReSetAllPreProcessd();
private:
    HistorySlowPlanStat* hsps_ = nullptr; /*plan we need to process to sps_*/
    HistorySlowPlanStat* cur_hsps_ = nullptr;

    SlowPlanStat * sps_ = nullptr; /*final output,sps will dircetly storaged*/
    int total_height_ = 0; /*plan height*/
    int cur_height_ = 0; /* from bottom to the top,begin height is 0 ... */
    MatchStrategy ms_;
    
    /* total equivlences for predicates */
    std::vector<LevelPredEquivlencesList*> total_equivlences_;
    /* total equivlences for outputs */
    std::vector<LevelOutputList*> total_outputs_;
    /* total equivlences for agg keys */
    std::vector<LevelAggAndSortList*>total_aggs_;
    /* total equivlences for sort keys */
    std::vector<LevelAggAndSortList*>total_sorts_;
    /* total sets for tables */
    std::vector<LevelTblList*>total_tbls_;
    /* pre lvevl processed check map */
    std::unordered_map<PreProcessLabel, bool> pre_processed_map_;

    std::unordered_map<HistorySlowPlanStat*, NodeCollector*> nodes_collector_map_;
};



