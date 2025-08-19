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
    JOIN_RANGE,
    /**/
    NOT_EQUAL,
    EQUAL,
    RANGE,
    /**/
    LIKE,
    NOT_LIKE,
    LIST,
    /**/
    SUBLINK,
    SUBQUERY,
    PARAM,
    UNKNOWN,
};

enum class VarType{
    INT = 0,
    STRING,
    DOUBLE,
    BOOL,
    DATE,
    UNKNOWN,
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

    void SetSubqueryName(const std::string& name){subquery_name_ = name;}
    const std::string& GetSubqueryName(){return subquery_name_;}

    void SetLowerBoundaryConstraint(bool b){boundary_constraint_.first = b;}
    void SetUpperBoundaryConstraint(bool b){boundary_constraint_.second = b;}
    bool GetLowerBoundaryConstraint(){return boundary_constraint_.first;}
    bool GetUpperBoundaryConstraint(){return boundary_constraint_.second;}
    
    void SetPredType(PType type){type_ = type;}
    PType PredType(){return type_;}

    void SetListOpType(PType type){list_op_type_ = type;}
    PType ListOpType(){return list_op_type_;}

    void SetListUseOr(const std::string& use){list_use_or_ = use;}
    std::string ListUseOr(){return list_use_or_;}

    void SetPredVarType(VarType type){var_type_ = type;}
    VarType PredVarType(){return var_type_;}

    void SetNegation(bool neg){negation_ = neg;}
    bool GetNegation(){return negation_;}

    void Copy(PredEquivlenceRange* new_range);
    bool Serach(PredEquivlenceRange* range);

    void PrintPredEquivlenceRange(int depth = 0);
    std::string PrintPredEquivlenceRange(int depth,std::string tag,SqmsLogger* logger);

    std::set<std::string> GetRightSets(){return right_sets_;}
    void SetRightSets(const std::set<std::string>& set){right_sets_ = set;}
    
    std::string Serialization(){
        std::string str;
        str += std::to_string(int(type_));    
        str += subquery_name_ + lower_limit_ + upper_limit_;
        str += boundary_constraint_.first ? "1":"0" + boundary_constraint_.second ? "1":"0";
        for(const auto& item : list_){
            str += item;
        }
        return str;
    }

    static int LimitCompare(const std::string& left_range,VarType left_type,const std::string& right_range,VarType right_type);
private:
    PType type_;
    VarType var_type_;
    /*type == SUBQRUEY or SUBLINK*/
    std::string subquery_name_;
    bool negation_ = false;
    std::string lower_limit_ = LOWER_LIMIT;
    std::string upper_limit_ = UPPER_LIMIT;
    std::pair<bool,bool> boundary_constraint_ = std::make_pair(true,true);

    std::vector<std::string> list_;
    PType list_op_type_;
    /*ANY or ALL*/
    std::string list_use_or_;
    /*range join*/
    std::set<std::string> right_sets_;
};

/**
 * LevelPredEquivlences: storage a equivlences. we sort all ranges classes by lower bound to accelate serach
 * example: 
 * 1. set: {A.a,B.b} , ranges: {150,200}
 * 2. set：{C.c}     , ranges: {250,UPPER_LIMIT}
 */
class LevelManager;
class PredEquivlence {
    // struct RangesCompare {
    //     bool operator()(const PredEquivlenceRange* per1, const PredEquivlenceRange* per2) const {
    //         //return true;
    //         if (per1->LowerLimit() != per2->LowerLimit())
    //             return per1->LowerLimit() < per2->LowerLimit();
    //         return per1->UpperLimit() < per2->UpperLimit();
    //     }
    // };
public:
    PredEquivlence(){}
    PredEquivlence(Quals* qual,bool is_not = false);

    static bool IsOnlyLeft(Quals* qual);

    static PType QualType(Quals* qual);
    static VarType QualVarType(Quals* qual);

    static bool PredVariable(NodeTag node_tag);
    static bool PredSubquery(NodeTag node_tag);

    bool Insert(PredEquivlence* pe, bool check_can_merged,bool pre_merged,bool early_check);

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
    void SetPredSet(const std::set<std::string>& set){set_ = set;}

    std::set<PredEquivlenceRange*>& GetRanges(){return ranges_;}
    void UpdateRanges(PredEquivlenceRange* range) {ranges_.insert(range);}
    void SetRanges(const std::set<PredEquivlenceRange*>& ranges){ranges_ = ranges;}

    bool MergePredEquivlenceRanges(const std::vector<PredEquivlenceRange*>& merge_pe_list,bool pre_merge = false,bool ealy_check = false);

    std::unordered_map<std::string, std::shared_ptr<LevelManager>>& GetSubLinkLevelPeLists(){return sublink_level_pe_lists_;}
    void SetSubLinkLevelPeLists(const std::unordered_map<std::string, std::shared_ptr<LevelManager>>& lists){sublink_level_pe_lists_ = lists;}

    bool EarlyStop(){return early_stop_;}
    void SetEarlyStop(bool early_stop){early_stop_ = early_stop;}

    //void SetChild(std::shared_ptr<PredEquivlence> child){child_ = child;}
    //std::shared_ptr<PredEquivlence> Child(){return child_;}

    std::string GetPredSetStr();
    std::string Serialization() const;
    void CalSortInfo(){
        std::string lower_limit = UPPER_LIMIT;
        std::string upper_limit = LOWER_LIMIT;
        for(const auto& range: ranges_){
            if(range->PredType() == PType::SUBQUERY){
                has_subquery_ = true;
                subquery_names_.insert(range->GetSubqueryName());
                continue;
            }else if(range->PredType() == PType::JOIN_RANGE){
                continue;
            }
            range_cnt_ ++;
            has_range_ = true;
            auto ret = PredEquivlenceRange::LimitCompare(range->LowerLimit(),range->PredVarType(),lower_limit,range->PredVarType());
            if((lower_limit != UPPER_LIMIT && ret < 0)
                || lower_limit == UPPER_LIMIT 
                || lower_limit == LOWER_LIMIT){
                lower_limit = range->LowerLimit();
            }

            ret = PredEquivlenceRange::LimitCompare(range->UpperLimit(),range->PredVarType(),upper_limit,range->PredVarType());
            if((upper_limit != LOWER_LIMIT && ret)
                || upper_limit == LOWER_LIMIT || upper_limit == UPPER_LIMIT){
                upper_limit = range->UpperLimit();
            }
        }
        lower_limit_ = lower_limit;
        upper_limit_ = upper_limit;
        serialization_ = Serialization();
    }

    std::string LowerLimit(){return lower_limit_;}
    std::string UpperLimit(){return upper_limit_;}
    bool HasSubquery(){return has_subquery_;}
    bool HasRange(){return has_range_;}
    bool JoinRange(){return join_range_;}
    std::set<std::string>& SubqueryNames(){return subquery_names_;}
    int RangeCnt(){return range_cnt_;}
    std::string GetSerialization() const;

    void SetNeedNegative(bool nn){need_negative_ = nn;}
    bool NeedNegative(){return need_negative_;}
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
    std::string extract_param_name(const std::string& subplan_name);
private:
    std::set<std::string> set_;
    /* common attr ranges,here is no need to sort*/
    std::set<PredEquivlenceRange*>ranges_;
    /* sublink attr ranges */
    /**
     * TODO: we should let level_mgr can more easily visit sublink_level_pe_strings_
     * the key is SUBQUERY1,SUBQUERY2 ... and so on
     */
    std::unordered_map<std::string, std::shared_ptr<LevelManager>> sublink_level_pe_lists_;
    
    bool early_stop_ = true;
    //std::shared_ptr<PredEquivlence> child_ = nullptr;

    /*for pe sort*/
    bool has_subquery_ = false;
    bool has_range_ = false;
    bool join_range_ = false;
    std::set<std::string> subquery_names_;
    int range_cnt_ = 0;
    std::string lower_limit_;
    std::string upper_limit_;

    std::string serialization_;
    bool need_negative_ = false;
};

class LevelPredEquivlences{
    struct PeCompare {
        bool operator()(const PredEquivlence* left, const PredEquivlence* right) const {
            return left->GetSerialization() < right->GetSerialization();
        }
    };
public:

    bool Insert(Quals* quals,bool is_or = false);
    bool Insert(PredEquivlence* pe);
    bool Insert(LevelPredEquivlences* pe,bool pre_merged = false,bool early_check = false);

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

    bool MergePredEquivlences(const std::vector<PredEquivlence*>& merge_pe_list,bool pre_merged = false,bool early_check = false);
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

    std::string Serialization() const;
    std::string GetSerialization() const{
        if(serialization_.empty())
            return Serialization();
        return serialization_;
    }
    
private:
    std::unordered_set<PredEquivlence*> level_pe_sets_;
     /**
     * a fast map from attr to its pe  
     */
    std::unordered_map<std::string,PredEquivlence*> key2pe_;

    bool early_stop_ = true;
    int  lpe_id_;

    std::string serialization_;
};

/**
 * LevelPredEquivlencesList: LevelPredEquivlences in this class is or relation.
 */
class LevelPredEquivlencesList{
public:
    bool Insert(LevelPredEquivlences* lpes,bool is_or);
    bool Insert(LevelPredEquivlencesList* lpes_list,int model,bool pre_merge = false,bool early_check = false);

    void Copy(LevelPredEquivlencesList* new_lpes_list);
    size_t Size(){return lpes_list_.size();}

    std::vector<LevelPredEquivlences*>::iterator begin() { return lpes_list_.begin(); }
    std::vector<LevelPredEquivlences*>::iterator end() { return lpes_list_.end(); }
    std::vector<LevelPredEquivlences*>::const_iterator begin() const { return lpes_list_.cbegin(); }
    std::vector<LevelPredEquivlences*>::const_iterator end() const { return lpes_list_.cend();}

    std::vector<LevelPredEquivlences*>& GetLpesList(){return lpes_list_;}
    LevelPredEquivlences* GetLpes(size_t idx){return lpes_list_[idx];}

    void ShowLevelPredEquivlencesList(int depth);
    std::string ShowLevelPredEquivlencesList(int depth, std::string tag, SqmsLogger* logger);
    int DistributeId(){
        int ret = lpes_id_creator_;
        lpes_id_creator_++;
        return ret;
    }

    void SetChildLpesMap(const std::unordered_map<size_t,std::vector<size_t>>& map){child_lpes_map_ = map;}
    std::unordered_map<size_t,std::vector<size_t>>& GetChildLpesMap(){return child_lpes_map_;}

    void SetEarlyStopMap(const std::unordered_map<size_t,std::vector<std::pair<size_t,size_t>>>& map){early_stop_map_ = map;}
    std::unordered_map<size_t,std::vector<std::pair<size_t,size_t>>>& EarlyStopMap(){return early_stop_map_;}

    std::string Serialization() const{
        std::string str;
        for(const auto& item : lpes_list_){
            str += item->GetSerialization();
        }
        return str;
    }
    std::string GetSerialization() const{
        if(serialization_.empty())
            return Serialization();
        return serialization_;
    }
    
private:
    std::vector<LevelPredEquivlences*> lpes_list_;
    /**
     * a map from current level lpes to previous level lpes
     * we just use idx as lpes's unique id in one level
     */
    std::unordered_map<size_t,std::vector<size_t>> child_lpes_map_;
    /**
     * a map from current level lpes to low level's lpes,
     * lpe_idx --> (height,lpe_idx)
     */
    std::unordered_map<size_t,std::vector<std::pair<size_t,size_t>>> early_stop_map_;
    /*local id creator for each level*/
    int lpes_id_creator_ = 0;

    std::string serialization_;
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

    const std::vector<int>& GetLpeIdList(){return lpe_id_list_;}

    /**
     * TODO: mot implement yet.
     */
    std::string Serialization() const{
        std::string str;
        return str;
    }

    std::string GetSerialization() const{
        if(serialization_.empty())
            return Serialization();
        return serialization_;
    }
    
private:
    std::vector<int> lpe_id_list_;
    std::vector<UMAP> output2pe_list_;
    std::vector<USET> output_extend_list_;

    std::string serialization_;
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

    std::string Serialization() const{
        std::string str;
        for(const auto& item: extends_){
            str+=item;
        }
        return str;
    }

    std::string GetSerialization() const{
        if(serialization_.empty())
            return Serialization();
        return serialization_;
    }

private:
    std::map<std::string, PredEquivlence*>key2pe_;
    std::set<std::string> extends_;
    /*agg true level*/
    int agg_level_;
    std::string tag_;
    std::string serialization_;
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

    std::string Serialization() const{
        std::string str;
        for(const auto& item: level_agg_sets_){
            str+=item->GetSerialization();
        }
        return str;
    }

    std::string GetSerialization() const{
        if(serialization_.empty())
            return Serialization();
        return serialization_;
    }

private:
    std::vector<AggAndSortEquivlence*> level_agg_sets_;
    int lpe_id_;
    std::string serialization_;
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
    LevelPredEquivlencesList* GetLpesList(){return lpes_list_;}

    std::string Serialization() const{
        std::string str;
        for(const auto& item: level_agg_list_){
            str+=item->GetSerialization();
        }
        return str;
    }

    std::string GetSerialization() const{
        if(serialization_.empty())
            return Serialization();
        return serialization_;
    }

private:
    std::vector<LevelAggAndSortEquivlences*> level_agg_list_;
    LevelPredEquivlencesList* lpes_list_ = nullptr;
    std::string serialization_;
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

    std::string Serialization() const{
        std::string str;
        for(const auto& item: tbl_set_){
            str+= item;
        }
        return str;
    }

    std::string GetSerialization() const{
        if(serialization_.empty())
            return Serialization();
        return serialization_;
    }
private:
    std::set<std::string> tbl_set_;
    std::string serialization_;
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
    const char * json_sub_plan = nullptr;
    std::vector<std::string> join_type_list;

    /*childs and parent of current node*/
    std::vector<NodeCollector*>childs_;
    NodeCollector* parent_ = nullptr;

    /**node execute info: inputs,output,time*/
    std::vector<int> inputs;
    int output = 0;
    double time = 0;
    const uint8_t* hsps_pack = 0;
    size_t hsps_pack_size = 0;

    /*type name of node*/
    std::string type_name;

    /**match cnt count for single matching process*/
    int match_cnt = 0;
    std::vector<int>output_list_;
    std::vector<int>time_list_;
    std::vector<const uint8_t*> hsps_pack_list_;
    std::vector<int>hsps_pack_size_list_;

    /*if set effictive for match node in shared_index*/
    bool set_effective_ = false;
    /**
     * check_scan_view_decrease_: whether to check scan_view_decrease_
     * scan_view_decrease_：if scan node view has data decrease in scan_index
     * */
    bool check_scan_view_decrease_ = false;
    bool scan_view_decrease_ = false;
    /**
     * Execute time and output for scan node
     * They are different from node time and output, they are used in slow plan match,
     * they will be calulated in slow plan match process.
     */
    double scan_time  = 0;
    int scan_output = 0;
    /*unused variables*/
    size_t child_idx = -1;
    int pe_idx = -1;

    /*for explaining*/
    int node_id = -1;
    int candidate_id = -1;
    int pre_candidate_id = -1;
    HistorySlowPlanStat* match_hsps_ = nullptr;
};

/**
 * LevelManager
 */
class HistoryQueryLevelTree;
class LevelManager : public AbstractFormatStrategy{
public:
    LevelManager(HistorySlowPlanStat* hsps, SlowPlanStat*sps, SqmsLogger* logger,std::string log_tag = "slow")
        :hsps_(hsps),sps_(sps),logger_(logger),log_tag_(log_tag)
    {
        hsps_package_ =  PlanStatFormat::PackHistoryPlanState(hsps,pack_size_);
    }

    virtual bool Format();
    virtual bool PrintPredEquivlences();

    void ComputeTotalClass(); 

    void ShowPredClass(int height,std::string tag,int depth = 0);
    std::string GetPredClassStr(int height,std::string tag,int depth = 0);

    std::string ShowTotalPredClass(int depth = 0);
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
    const std::unordered_map <int,std::vector<LevelManager*>>& GetPredSubQueryMap(){return pred_subquery_map_;}

    std::string Serialization() const;
    std::string GetSerialization() const;
    std::string GetPlanFullJson() const;
    std::string GetPlanCannonicalJson() const;

    void SetHspsPackage(const uint8_t* buffer){ hsps_package_ = buffer;}
    const uint8_t* GetHspsPackage() {return hsps_package_;}

    void SetHspsPackSize(size_t size) {pack_size_ = size;}
    size_t GetHspsPackSize(){return pack_size_;}

    void SetSourceQuery(const char* query){q_str_ = std::string(query);}
    std::string GetSourceQuery(){return q_str_;}

private:
    void ComputeLevelClass(const std::vector<HistorySlowPlanStat*>& list);
    void HandleEquivleces(HistorySlowPlanStat* hsps);

    void HandleNode(HistorySlowPlanStat* hsps);
    
    void PredEquivalenceClassesDecompase(PredExpression* root);
    void TblDecompase(HistorySlowPlanStat* hsps);
    void OutputDecompase(HistorySlowPlanStat* hsps);
    void GroupKeyDecompase(HistorySlowPlanStat* hsps);
    void SortKeyDecompase(HistorySlowPlanStat* hsps);
    void GetSubQueries();

private:
    void ExprLevelCollect(PredExpression * tree,std::vector<std::vector<AbstractPredNode*>>& level_collector);
    bool GetPreProcessed(PreProcessLabel label){return pre_processed_map_[label];}
    void SetPreProcessed(PreProcessLabel label,bool b){pre_processed_map_[label] = b;}
    void ReSetAllPreProcessd();
    bool CancelQuery();
private:
    /*plan we need to process to sps_ (root)*/
    HistorySlowPlanStat* hsps_ = nullptr; 
    /*sub plan we are processing*/
    HistorySlowPlanStat* cur_hsps_ = nullptr;
    bool first_pred_check_ = false;
    /*final output,sps will dircetly storaged*/
    SlowPlanStat * sps_ = nullptr; 
    /*plan height*/
    int total_height_ = 0;
    /* from bottom to the top,begin height is 0 ... */ 
    int cur_height_ = 0; 
    /* hsps after packing */
    const uint8_t* hsps_package_ = nullptr;
    size_t pack_size_ = 0;
    /* origin query */
    std::string q_str_;
    /*match strategy,no used yet...*/
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
    std::vector<PredEquivlence*>residual_predicates_;

    std::unordered_map <int,std::vector<LevelManager*>> pred_subquery_map_;

    SqmsLogger* logger_;
    std::string log_tag_;
    std::string serialization_;
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

    void SetNotLpesList(LevelPredEquivlencesList* not_lpes_list){
        if(!not_lpes_list_){
            not_lpes_list_ = new LevelPredEquivlencesList();
        }
        not_lpes_list->Copy(not_lpes_list_);
    }
    LevelPredEquivlencesList* GetNotLpesList(){return not_lpes_list_;}

    void SetResidualOrLpesList(LevelPredEquivlencesList* residual_or_lpes_list){
        if(!residual_or_lpes_list_){
            residual_or_lpes_list_ = new LevelPredEquivlencesList();
        }
        residual_or_lpes_list->Copy(residual_or_lpes_list_);
    }
    LevelPredEquivlencesList* GetrResidualOrLpesList(){return residual_or_lpes_list_;}

    void SetResidualAndLpesList(LevelPredEquivlencesList* residual_and_lpes_list){
        if(!residual_and_lpes_list_){
            residual_and_lpes_list_ = new LevelPredEquivlencesList();
        }
        residual_and_lpes_list->Copy(residual_and_lpes_list_);
    }
    LevelPredEquivlencesList* GetResidualAndLpesList(){return residual_and_lpes_list_;}

    void SetResidualNotLpesList(LevelPredEquivlencesList* residual_not_lpes_list){
        if(!residual_not_lpes_list_){
            residual_not_lpes_list_ = new LevelPredEquivlencesList();
        }
        residual_not_lpes_list->Copy(not_lpes_list_);
    }
    LevelPredEquivlencesList* GetResidualNotLpesList(){return residual_not_lpes_list_;}
    
private:
    PredOperator__PredOperatorType op_type_;
    AbstractPredNode* parent_ = nullptr;
    std::vector<AbstractPredNode*>childs_;

    LevelPredEquivlencesList* or_lpes_list_ = nullptr;
    LevelPredEquivlencesList* and_lpes_list_ = nullptr;
    LevelPredEquivlencesList* not_lpes_list_ = nullptr;

    LevelPredEquivlencesList* residual_or_lpes_list_ = nullptr;
    LevelPredEquivlencesList* residual_and_lpes_list_ = nullptr;
    LevelPredEquivlencesList* residual_not_lpes_list_ = nullptr;
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