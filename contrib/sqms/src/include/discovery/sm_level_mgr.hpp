#pragma once
#include <string>
#include "common/util.hpp"
#include "collect/level_mgr.hpp"
#include "tbb/concurrent_hash_map.h"

class SMPredEquivlenceRange {
public:
    SMString LowerLimit() const {return lower_limit_;}
    SMString UpperLimit() const {return upper_limit_;}
    SMVector<SMString> const List(){return list_;}
    bool GetLowerBoundaryConstraint(){return boundary_constraint_.first;}
    bool GetUpperBoundaryConstraint(){return boundary_constraint_.second;}
    const SMString& GetSubqueryName(){return subquery_name_;}
    PType PredType(){return type_;}
    VarType PredVarType(){return var_type_;}
    SMString Serialization(){
        SMString str;
        str += SMString(std::to_string(int(type_))) + SMString(std::to_string(int(list_op_type_)));    
        str += subquery_name_ + lower_limit_ + upper_limit_ + list_use_or_;
        str += boundary_constraint_.first ? "1":"0" + boundary_constraint_.second ? "1":"0";
        for(const auto& item : list_){
            str += item;
        }
        return str;
    } 
    SMString GetSerialization(){
        if(serialization_.empty()){
            return Serialization();
        }
        return serialization_; 
    }
    void Copy(PredEquivlenceRange* per);
    static int LimitCompare(const SMString& left_range,VarType left_type,const SMString& right_range,VarType right_type);
    
    PType ListOpType(){return list_op_type_;}
    SMString ListUseOr(){return list_use_or_;}

    private:
    PType type_;
    VarType var_type_;
    SMString subquery_name_;
    SMString lower_limit_ = LOWER_LIMIT;
    SMString upper_limit_ = UPPER_LIMIT;
    std::pair<bool,bool> boundary_constraint_ = std::make_pair(true,true);
    SMVector<SMString> list_;
    PType list_op_type_;
	SMString list_use_or_;
    SMString serialization_;
};

class SMLevelManager;
class SMPredEquivlence {
    // struct SMRangesCompare {
    //     bool operator()(const SMPredEquivlenceRange* per1, const SMPredEquivlenceRange* per2) const {
    //         return true;
    //     }
    // };
public:
    SMSet<SMString>& GetPredSet(){return set_;}
    SMSet<SMPredEquivlenceRange*>& GetRanges(){return ranges_;}
    SMUnorderedMap<SMString, SMLevelManager*,SMStringHash>& GetSubLinkLevelPeLists(){return sublink_level_pe_lists_;}
    bool EarlyStop(){return early_stop_;}
    //SMPredEquivlence* Child(){return child_;}
    void Copy(PredEquivlence* pe);
    SMString Serialization();
    SMString GetSerialization();
    const bool HasSubquery() const {return has_subquery_;}
    const bool HasRange() const {return has_range_;}
    const SMSet<SMString> SubqueryNames() const {return subquery_names_;}
    const int RangeCnt() const {return range_cnt_;}
    const SMString LowerLimit() const {return lower_limit_;}
    const SMString UpperLimit() const {return upper_limit_;}
private:
    SMSet<SMString> set_;
    /* common attr ranges */
    SMSet<SMPredEquivlenceRange*>ranges_;
    /* sublink attr ranges */
    SMUnorderedMap<SMString, SMLevelManager*,SMStringHash> sublink_level_pe_lists_;
    
    bool early_stop_ = true;
    //SMPredEquivlence* child_ = nullptr;
    SMString serialization_;

    /*for pe sort*/
    bool has_subquery_ = false;
    bool has_range_ = false;
    SMSet<SMString> subquery_names_;
    int range_cnt_;
    SMString lower_limit_;
    SMString upper_limit_;
};

class SMLevelPredEquivlences{
public:
    SMUnorderedSet<SMPredEquivlence*>& LevelPeList(){return level_pe_sets_;};
    SMUnorderedSet<SMPredEquivlence*>::iterator begin() { return level_pe_sets_.begin(); }
    SMUnorderedSet<SMPredEquivlence*>::iterator end() { return level_pe_sets_.end(); }
    SMUnorderedSet<SMPredEquivlence*>::const_iterator begin() const { return level_pe_sets_.cbegin(); }
    SMUnorderedSet<SMPredEquivlence*>::const_iterator end() const { return level_pe_sets_.cend(); }
    bool EarlyStop(){return early_stop_;}
    int LpeId(){return lpe_id_;}
    const SMUnorderedMap<SMString,SMPredEquivlence*,SMStringHash>& GetKey2Pe(){return key2pe_;}
    void Copy(LevelPredEquivlences* lpes){
        for(const auto& pe : lpes->LevelPeList()){
            assert(pe);
            SMPredEquivlence* sm_pe = (SMPredEquivlence*)ShmemAlloc(sizeof(SMPredEquivlence));
            assert(sm_pe);
            new (sm_pe) SMPredEquivlence();
            sm_pe->Copy(pe);
            level_pe_sets_.insert(sm_pe);
        }
        for(const auto& item : lpes->GetKey2Pe()){
            SMPredEquivlence* sm_pe = (SMPredEquivlence*)ShmemAlloc(sizeof(SMPredEquivlence));
            assert(sm_pe);
            assert(item.second);
            new (sm_pe) SMPredEquivlence();
            sm_pe->Copy(item.second);
            key2pe_[SMString(item.first)] = sm_pe; 
        }
        early_stop_ = lpes->EarlyStop();
        lpe_id_ = lpes->LpeId();
    }
private:
    SMUnorderedSet<SMPredEquivlence*> level_pe_sets_;
    SMUnorderedMap<SMString,SMPredEquivlence*,SMStringHash> key2pe_;
    //SMUnorderedMap<SMPredEquivlence*,SMPredEquivlence*>pe2pe_map_;
    bool early_stop_ = true;
    int  lpe_id_;
};

class SMLevelPredEquivlencesList{
public:
    SMVector<SMLevelPredEquivlences*>& GetLpesList(){return lpes_list_;}
    SMUnorderedMap<size_t,SMVector<size_t>>& GetChildLpesMap(){return child_lpes_map_;}

    SMVector<SMLevelPredEquivlences*>::iterator begin() { return lpes_list_.begin(); }
    SMVector<SMLevelPredEquivlences*>::iterator end() { return lpes_list_.end(); }
    SMVector<SMLevelPredEquivlences*>::const_iterator begin() const { return lpes_list_.cbegin(); }
    SMVector<SMLevelPredEquivlences*>::const_iterator end() const { return lpes_list_.cend();}

    void Copy(LevelPredEquivlencesList* lpe_list){
        for(const auto& lpe : lpe_list->GetLpesList()){
            SMLevelPredEquivlences* sm_lpe = (SMLevelPredEquivlences*)ShmemAlloc(sizeof(SMLevelPredEquivlences));
            assert(sm_lpe);
            new (sm_lpe) SMLevelPredEquivlences();
            sm_lpe->Copy(lpe);
            lpes_list_.push_back(sm_lpe);
        }
        for(const auto& item : lpe_list->GetChildLpesMap()){
            SMVector<size_t> child_id_list;
            for(const auto& id :  item.second){
                child_id_list.push_back(id);
            }
            child_lpes_map_[item.first] = child_id_list;
        }
    }
private:
    SMVector<SMLevelPredEquivlences*> lpes_list_;
    SMUnorderedMap<size_t,SMVector<size_t>> child_lpes_map_;
};

class SMLevelOutputList{
public:
    typedef SMUnorderedSet<SMString,SMStringHash> USET;
    typedef SMUnorderedMap<SMString, SMPredEquivlence*,SMStringHash> UMAP;
    const SMVector<UMAP>& GetOutput2PeList(){return output2pe_list_;}
    const SMVector<USET>& GetOutputExtendList(){return output_extend_list_;}
    UMAP& GetOutput2PeListItem(int pos){return output2pe_list_[pos];}
    USET& GetOutputExtendListItem(int pos){return output_extend_list_[pos];}
    int GetLpeId(int pos){return lpe_id_list_[pos];}
    void Copy(LevelOutputList* lo_list){
        for(const auto& map : lo_list->GetOutput2PeList()){
            UMAP sm_umap;
            for(const auto& item : map){
                SMPredEquivlence* sm_pe = nullptr;
                if(item.second){
                    sm_pe = (SMPredEquivlence*)ShmemAlloc(sizeof(SMPredEquivlence));
                    assert(sm_pe);
                    new (sm_pe) SMPredEquivlence();
                    sm_pe->Copy(item.second);
                }
                sm_umap[SMString(item.first)] = sm_pe;
            }
            output2pe_list_.push_back(sm_umap);
        }
        for(const auto& set : lo_list->GetOutputExtendList()){
            USET sm_uset;
            for(const auto& item : set){
                sm_uset.insert(SMString(item));
            }
            output_extend_list_.push_back(sm_uset);
        }
        for(const auto& id : lo_list->GetLpeIdList()){
            lpe_id_list_.push_back(id);
        }
    }
private:
    SMVector<int> lpe_id_list_;
    SMVector<UMAP> output2pe_list_;
    SMVector<USET> output_extend_list_;
};

class SMLevelTblList{
public:
    const SMSet<SMString>& GetTblSet(){return tbl_set_;}
    void Copy(LevelTblList* lt_list){
        for(const auto& tb : lt_list->GetTblSet()){
            tbl_set_.insert(SMString(tb));
        }
    }
private:
    SMSet<SMString> tbl_set_;
};


class SMAggAndSortEquivlence{
public:
    const SMMap<SMString, SMPredEquivlence*>& GetKey2Pe(){return key2pe_;}
    const SMSet<SMString>& GetExtends(){return extends_;}
    void Copy(AggAndSortEquivlence* aas_eq){
        for(const auto& item : aas_eq->GetKey2Pe()){
            SMPredEquivlence* sm_pe = nullptr;
            if(item.second){
                sm_pe = (SMPredEquivlence*)ShmemAlloc(sizeof(SMPredEquivlence));
                assert(sm_pe);
                new (sm_pe) SMPredEquivlence();
                sm_pe->Copy(item.second);
            }
            key2pe_[SMString(item.first)] = sm_pe;
        }
        for(const auto& item : aas_eq->GetExtends()){
            extends_.insert(SMString(item));
        }
    }
private:
    SMMap<SMString, SMPredEquivlence*>key2pe_;
    SMSet<SMString> extends_;
    int agg_level_;
    std::string tag_;
};

class SMLevelAggAndSortEquivlences{
public:
    size_t Size(){return level_agg_sets_.size();}
    const SMVector<SMAggAndSortEquivlence*>& GetLevelAggSets(){return level_agg_sets_;}
    int GetLpeId(){return lpe_id_;}
    void Copy(LevelAggAndSortEquivlences* laas_eq){
        for(const auto& eq : laas_eq->GetLevelAggSets()){
            SMAggAndSortEquivlence* sm_aas_eq = (SMAggAndSortEquivlence*)ShmemAlloc(sizeof(SMAggAndSortEquivlence));
            assert(sm_aas_eq);
            new (sm_aas_eq) SMAggAndSortEquivlence();
            sm_aas_eq->Copy(eq);
            level_agg_sets_.push_back(sm_aas_eq);
        }
        lpe_id_ = laas_eq->GetLpeId();
    }
private:
    SMVector<SMAggAndSortEquivlence*> level_agg_sets_;
    int lpe_id_;
};

class SMLevelAggAndSortList{
public:
    size_t Size(){return level_agg_list_.size();}
    const SMVector<SMLevelAggAndSortEquivlences*>& GetLevelAggList(){return level_agg_list_;}
    void Copy(LevelAggAndSortList* laas_list){
        for(const auto& laas : laas_list->GetLevelAggList()){
            SMLevelAggAndSortEquivlences* sm_laas_eq = (SMLevelAggAndSortEquivlences*)ShmemAlloc(sizeof(SMLevelAggAndSortEquivlences));
            assert(sm_laas_eq);
            new (sm_laas_eq) SMLevelAggAndSortEquivlences();
            sm_laas_eq->Copy(laas);
            level_agg_list_.push_back(sm_laas_eq);
        }
        lpes_list_ = (SMLevelPredEquivlencesList*)ShmemAlloc(sizeof(SMLevelPredEquivlencesList));
        assert(lpes_list_);
        new (lpes_list_) SMLevelPredEquivlencesList();
        lpes_list_->Copy(laas_list->GetLpesList());
    }
private:
    SMVector<SMLevelAggAndSortEquivlences*> level_agg_list_;
    SMLevelPredEquivlencesList* lpes_list_ = nullptr;
};

class SMLevelManager{
public:
    const SMVector<SMLevelPredEquivlencesList*>& GetTotalEquivlences(){return total_equivlences_;}
    const SMVector<SMLevelOutputList*>& GetTotalOutput(){return total_outputs_;}
    const SMVector<SMLevelTblList*>& GetTotalTables(){return total_tbls_;}
    const SMVector<SMLevelAggAndSortList*>& GetTotalAggs(){return total_aggs_;}
    const SMVector<SMLevelAggAndSortList*>& GetTotalSorts(){return total_sorts_;}
    const SMVector<SMLevelPredEquivlencesList*>& GetTotalResidualEquivlences(){return total_residual_equivlences_;}
    const SMVector<SMString>& GetJoinTypeList(){return join_type_list_;}
    const SMString& GetJsonSubPlan();
    const SMString& GetJsonFullSubPlan();
    void Copy(LevelManager* level_mgr){
        for(const auto& pe : level_mgr->GetTotalEquivlences()){
            SMLevelPredEquivlencesList* sm_lpes_list = (SMLevelPredEquivlencesList*)ShmemAlloc(sizeof(SMLevelPredEquivlencesList));
            assert(sm_lpes_list);
            new (sm_lpes_list) SMLevelPredEquivlencesList();
            sm_lpes_list->Copy(pe);
            total_equivlences_.push_back(sm_lpes_list);
        }
        for(const auto& output : level_mgr->GetTotalOutput()){
            SMLevelOutputList* sm_lo_list = (SMLevelOutputList*)ShmemAlloc(sizeof(SMLevelOutputList));
            assert(sm_lo_list);
            new (sm_lo_list) SMLevelOutputList();
            sm_lo_list->Copy(output);
            total_outputs_.push_back(sm_lo_list);
        }
        for(const auto& aas_list : level_mgr->GetTotalAggs()){
            SMLevelAggAndSortList* sm_laas_list = (SMLevelAggAndSortList*)ShmemAlloc(sizeof(SMLevelAggAndSortList));
            assert(sm_laas_list);
            new (sm_laas_list) SMLevelAggAndSortList();
            sm_laas_list->Copy(aas_list);
            total_aggs_.push_back(sm_laas_list);
        }
        for(const auto& aas_list : level_mgr->GetTotalSorts()){
            SMLevelAggAndSortList* sm_laas_list = (SMLevelAggAndSortList*)ShmemAlloc(sizeof(SMLevelAggAndSortList));
            assert(sm_laas_list);
            new (sm_laas_list) SMLevelAggAndSortList();
            sm_laas_list->Copy(aas_list);
            total_sorts_.push_back(sm_laas_list);
        }
        for(const auto& tb_list : level_mgr->GetTotalTables()){
            SMLevelTblList* sm_tb_list = (SMLevelTblList*)ShmemAlloc(sizeof(SMLevelTblList));
            assert(sm_tb_list);
            new (sm_tb_list) SMLevelTblList();
            sm_tb_list->Copy(tb_list);
            total_tbls_.push_back(sm_tb_list);
        }
        for(const auto& pe : level_mgr->GetTotalResidualEquivlences()){
            SMLevelPredEquivlencesList* sm_lpes_list = (SMLevelPredEquivlencesList*)ShmemAlloc(sizeof(SMLevelPredEquivlencesList));
            assert(sm_lpes_list);
            new (sm_lpes_list) SMLevelPredEquivlencesList();
            sm_lpes_list->Copy(pe);
            total_residual_equivlences_.push_back(sm_lpes_list);
        }
        json_sub_plan_ = SMString(level_mgr->GetHsps()->canonical_json_plan);
        json_full_sub_plan_ = SMString(level_mgr->GetPlanFullJson());
        for(const auto& join_type : level_mgr->GetJoinTypeList()){
            join_type_list_.push_back(SMString(join_type));
        }
    }
private:
    /* total equivlences for predicates */
    SMVector<SMLevelPredEquivlencesList*> total_equivlences_;
    /* total equivlences for outputs */
    SMVector<SMLevelOutputList*> total_outputs_;
    /* total equivlences for agg keys */
    SMVector<SMLevelAggAndSortList*>total_aggs_;
    /* total equivlences for sort keys */
    SMVector<SMLevelAggAndSortList*>total_sorts_;
    /* total sets for tables */
    SMVector<SMLevelTblList*>total_tbls_;

    SMVector<SMLevelPredEquivlencesList*> total_residual_equivlences_;
    /*hsps of sub_plan root*/
    SMString json_sub_plan_;
    SMString json_full_sub_plan_;
    SMVector<SMString> join_type_list_;
};
