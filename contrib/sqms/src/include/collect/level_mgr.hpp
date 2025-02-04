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
    SUBLINK,
    SUBQUERY,
};

/*operator type set*/
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
    std::string PrintPredEquivlenceRange(int depth,std::string tag,SqmsLogger* logger);

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
class LevelManager;
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
    static bool PredSubquery(NodeTag node_tag);

    bool Insert(PredEquivlence* pe, bool check_can_merged,bool pre_merged);

    bool Delete(Quals* quals);
    
    bool Serach(Quals* quals);
    bool Serach(PredEquivlence* pe);
    bool Serach(const std::string& attr);

    bool RangesSerach(PredEquivlenceRange* range,std::vector<PredEquivlenceRange*>& merge_pe_list);

    bool SuperSet(PredEquivlence* pe);

    bool Copy(PredEquivlence* pe);

    void ShowPredEquivlence(int depth = 0);
    std::string ShowPredEquivlence(int depth,std::string tag,SqmsLogger* logger);

    void ShowPredEquivlenceSets(int depth = 0);
    std::string ShowPredEquivlenceSets(int depth,std::string tag,SqmsLogger* logger);

    std::set<std::string>& GetPredSet(){return set_;}
    void SetPredSet(std::set<std::string> set){set_ = set;}

    std::set<PredEquivlenceRange*,RangesCompare>& GetRanges(){return ranges_;}
    void SetRanges(std::set<PredEquivlenceRange*,RangesCompare> ranges){ranges_ = ranges;}

    bool MergePredEquivlenceRanges(const std::vector<PredEquivlenceRange*>& merge_pe_list);

    std::unordered_map<std::string, std::shared_ptr<LevelManager>>& GetSubLinkLevelPeLists(){return sublink_level_pe_lists_;}
    void SetSubLinkLevelPeLists(const std::unordered_map<std::string, std::shared_ptr<LevelManager>>& lists){sublink_level_pe_lists_ = lists;}

    bool EarlyStop(){return early_stop_;}
    void SetEarlyStop(bool early_stop){early_stop_ = early_stop;}

    void SetChild(std::shared_ptr<PredEquivlence> child){child_ = child;}
    std::shared_ptr<PredEquivlence> Child(){return child_;}

private:
    std::string extract_field(const std::string& expression) {
        size_t start_pos = expression.find('(');  
        size_t end_pos = expression.find(')');   
        size_t double_colon_pos = expression.find("::"); 

        if (start_pos != std::string::npos && end_pos != std::string::npos && double_colon_pos != std::string::npos) {
            std::string field = expression.substr(start_pos + 1, end_pos - start_pos - 1);
            return field;
        } else {
            return "";
        }
    }
private:
    std::set<std::string> set_;
    /* common attr ranges */
    std::set<PredEquivlenceRange*,RangesCompare>ranges_;
    /* sublink attr ranges */
    std::unordered_map<std::string, std::shared_ptr<LevelManager>> sublink_level_pe_lists_;
    
    bool early_stop_ = true;
    std::shared_ptr<PredEquivlence> child_ = nullptr;
};

class LevelPredEquivlences{
public:
  
    bool Insert(Quals* quals,bool is_or = false);
    bool Insert(PredEquivlence* pe);
    bool Insert(LevelPredEquivlences* pe,bool pre_merged = false);

    bool Delete(PredEquivlence* quals);
    bool UpdateRanges(Quals* quals);
    bool UpdateRanges(PredEquivlence* pe);

    /*search for insert*/
    bool Serach(Quals* quals,std::vector<PredEquivlence*>& merge_pe_list);
    bool Serach(PredEquivlence* pe, std::vector<PredEquivlence*>& merge_pe_list);
    bool Serach(const std::string& attr,PredEquivlence*& pe);
    
    bool Compare(PredEquivlence* range);
    bool Copy(LevelPredEquivlences* pe);

    bool Match(LevelPredEquivlences* lpes);

    bool MergePredEquivlences(const std::vector<PredEquivlence*>& merge_pe_list,bool pre_merged = false);

    void ShowLevelPredEquivlences(int depth = 0);
    std::string ShowLevelPredEquivlences(int depth,std::string tag, SqmsLogger* logger);

    std::unordered_set<PredEquivlence*>& LevelPeList(){return level_pe_sets_;};

    std::unordered_set<PredEquivlence*>::iterator begin() { return level_pe_sets_.begin(); }
    std::unordered_set<PredEquivlence*>::iterator end() { return level_pe_sets_.end(); }
    std::unordered_set<PredEquivlence*>::const_iterator begin() const { return level_pe_sets_.cbegin(); }
    std::unordered_set<PredEquivlence*>::const_iterator end() const { return level_pe_sets_.cend(); }
    
    bool EarlyStop(){return early_stop_;}
    void SetEarlyStop(bool early_stop){early_stop_ = early_stop;}

    int LpeId(){return lpe_id_;}
    void SetLpeId(int id){lpe_id_ = id;}

    /* this must be used after a LevelPredEquivlencesList not changed*/
    void BuildKey2PeMap();
    const std::unordered_map<std::string,PredEquivlence*>& GetKey2Pe(){return key2pe_;}
    
private:
    std::unordered_set<PredEquivlence*> level_pe_sets_;
     /**
     * a fast map from attr to its pe  
     */
    std::unordered_map<std::string,PredEquivlence*> key2pe_;
    /*mark pe that can't early stop,from current level's pe to p4re level's pe*/
    std::unordered_map<PredEquivlence*,PredEquivlence*>pe2pe_map_;
    bool early_stop_ = true;
    int  lpe_id_;
};

/**
 * LevelPredEquivlencesList: LevelPredEquivlences in this class is or relation.
 */
class LevelPredEquivlencesList{
public:
    bool Insert(LevelPredEquivlences* lpes,bool is_or);
    bool Insert(LevelPredEquivlencesList* lpes_list,bool is_or,bool pre_merge = false);

    void Copy(LevelPredEquivlencesList* new_lpes_list);
    size_t Size(){return lpes_list_.size();}

    std::vector<LevelPredEquivlences*>::iterator begin() { return lpes_list_.begin(); }
    std::vector<LevelPredEquivlences*>::iterator end() { return lpes_list_.end(); }
    std::vector<LevelPredEquivlences*>::const_iterator begin() const { return lpes_list_.cbegin(); }
    std::vector<LevelPredEquivlences*>::const_iterator end() const { return lpes_list_.cend();}

    std::vector<LevelPredEquivlences*>& GetLpesList(){return lpes_list_;}

    void ShowLevelPredEquivlencesList(int depth);
    std::string ShowLevelPredEquivlencesList(int depth, std::string tag, SqmsLogger* logger);
    int DistributeId(){
        int ret = lpes_id_creator_;
        lpes_id_creator_++;
        return ret;
    }

    void SetChildLpesMap(std::unordered_map<size_t,std::vector<size_t>> map){child_lpes_map_ = map;}
    std::unordered_map<size_t,std::vector<size_t>>& GetChildLpesMap(){return child_lpes_map_;}
private:
    std::vector<LevelPredEquivlences*> lpes_list_;
    /**
     * a map from current level lpes to previous level lpes
     * we just use idx as lpes's unique id in one level
     */
    std::unordered_map<size_t,std::vector<size_t>> child_lpes_map_;
    /*local id creator for each level*/
    int lpes_id_creator_ = 0;
};

/**
 * LevelOutput 
 */
class LevelOutputList{
    typedef std::unordered_set<std::string> USET;
    typedef std::unordered_map<std::string, PredEquivlence*> UMAP;
public:
    void Insert(LevelPredEquivlencesList* lpes_list,HistorySlowPlanStat* hsps);
    void Insert(LevelOutputList* lo_list);

    void ShowLevelOutputList(int depth = 0);
    std::string ShowLevelOutputList(int depth,std::string tag,SqmsLogger* logger);

    const std::vector<UMAP>& GetOutput2PeList(){return output2pe_list_;}
    const std::vector<USET>& GetOutputExtendList(){return output_extend_list_;}

    void ReSizeOutput2PeList(size_t sz){output2pe_list_.resize(sz);}
    void ReSizeOutputExtendList(size_t sz){output_extend_list_.resize(sz);}

    UMAP& GetOutput2PeListItem(int pos){return output2pe_list_[pos];}
    USET& GetOutputExtendListItem(int pos){return output_extend_list_[pos];}

    void SetLpeId(int id){lpe_id_list_.push_back(id);}
    int GetLpeId(int pos){return lpe_id_list_[pos];}
private:
    std::vector<int> lpe_id_list_;
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
    std::string ShowAggEquivlence(int depth,std::string tag,SqmsLogger* logger);

    const std::map<std::string, PredEquivlence*>& GetKey2Pe(){return key2pe_;}
    const std::set<std::string>& GetExtends(){return extends_;}
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
    std::string ShowLevelAggEquivlence(int depth,std::string tag, SqmsLogger* logger);

    size_t Size(){return level_agg_sets_.size();}
    const std::vector<AggAndSortEquivlence*>& GetLevelAggSets(){return level_agg_sets_;}

    void SetLpeId(int id){lpe_id_ = id;}
    int GetLpeId(){return lpe_id_;}
private:
    std::vector<AggAndSortEquivlence*> level_agg_sets_;
    int lpe_id_;
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
    std::string ShowLevelAggAndSortList(int depth,std::string tag, SqmsLogger* logger);

    size_t Size(){return level_agg_list_.size();}
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
    std::string ShowLevelTblList(int depth,std::string tag,SqmsLogger* logger);

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
    LevelManager(HistorySlowPlanStat* hsps, SlowPlanStat*sps, SqmsLogger* logger,std::string log_tag = "slow")
        :hsps_(hsps),sps_(sps),logger_(logger),log_tag_(log_tag)
    {}

    virtual bool Format();
    virtual bool PrintPredEquivlences();
    void ComputeTotalClass(); 

    void ShowPredClass(int height,std::string tag,int depth = 0);
    std::string GetPredClassStr(int height,std::string tag,int depth = 0);

    void ShowTotalPredClass(int depth = 0);
    std::string GetTotalPredClassStr(int depth = 0);

    /*for index*/
    const std::vector<std::string>& GetJoinTypeList(){return join_type_list_;}
    HistorySlowPlanStat* GetHsps(){return hsps_;}
    const std::vector<LevelPredEquivlencesList*>& GetTotalEquivlences(){return total_equivlences_;}
    const std::vector<LevelOutputList*>& GetTotalOutput(){return total_outputs_;}
    const std::vector<LevelTblList*>& GetTotalTables(){return total_tbls_;}
    const std::vector<LevelAggAndSortList*>& GetTotalAggs(){return total_aggs_;}
    const std::vector<LevelAggAndSortList*>& GetTotalSorts(){return total_sorts_;}
    const std::vector<LevelPredEquivlencesList*>& GetTotalResidualEquivlences(){return total_residual_equivlences_;}
    std::unordered_map<HistorySlowPlanStat*, NodeCollector*>& GetNodeCollector(){return nodes_collector_map_;}

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
    bool first_pred_check_ = false;

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

    /*plan join_nodes type, we put them in vec as the level sequence from left to right*/
    std::vector<std::string> join_type_list_;

    std::vector<LevelPredEquivlencesList*> total_residual_equivlences_;

    std::string log_tag_;

    SqmsLogger* logger_;
};

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

    void SetOrLpesList(LevelPredEquivlencesList* or_lpes_list){
        if(!or_lpes_list_){
            or_lpes_list_ = new LevelPredEquivlencesList();
        }
        or_lpes_list->Copy(or_lpes_list_);
    }
    LevelPredEquivlencesList* GetOrLpesList(){return or_lpes_list_;}
    void SetAndLpesList(LevelPredEquivlencesList* and_lpes_list){
        if(!and_lpes_list_){
            and_lpes_list_ = new LevelPredEquivlencesList();
        }
        and_lpes_list->Copy(and_lpes_list_);
    }
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