#include "discovery/sm_level_mgr.hpp"

void SMPredEquivlence::Copy(PredEquivlence* pe){
    assert(pe);
    for(const auto& attr : pe->GetPredSet()){
        set_.insert(SMString(attr));
    } 
    for(const auto& range: pe->GetRanges()){
        SMPredEquivlenceRange* sm_range =  (SMPredEquivlenceRange*)ShmemAlloc(sizeof(SMPredEquivlenceRange));
        assert(sm_range);
        new (sm_range) SMPredEquivlenceRange();
        sm_range->Copy(range);
        ranges_.insert(sm_range);
    }
    for(const auto& item : pe->GetSubLinkLevelPeLists()){
        SMLevelManager* sm_level_mgr = (SMLevelManager*)ShmemAlloc(sizeof(SMLevelManager));
        assert(sm_level_mgr);
        assert(item.second);
        new (sm_level_mgr) SMLevelManager();
        sm_level_mgr->Copy(item.second.get());
        sublink_level_pe_lists_[SMString(item.first)] = sm_level_mgr;
    }
    early_stop_ = pe->EarlyStop();

    SMPredEquivlence* sm_child_pe = nullptr;
    if(pe->Child().get()){
        sm_child_pe = (SMPredEquivlence*)ShmemAlloc(sizeof(SMPredEquivlence));
        assert(sm_child_pe);
        new (sm_child_pe) SMPredEquivlence();
        sm_child_pe->Copy(pe->Child().get());
    }
    child_ = sm_child_pe;

    serialization_ = Serialization();
    has_subquery_ = pe->HasSubquery();
    has_range_ =pe->HasRange();
}