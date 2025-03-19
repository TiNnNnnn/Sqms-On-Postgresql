#include "discovery/inverted_index.hpp"

int ScalingInfo::CalJoinTypeScore(const SMVector<SMString>& join_type_list,SMString& unique_id){
    int score = 0;
    SMString id;
    for(const auto& join_type : join_type_list){  
        if (!std::strcmp(join_type.c_str(),"Semi") || !std::strcmp(join_type.c_str(),"Anti")){
            score += 1;
            id += "1";
        }else if(!std::strcmp(join_type.c_str(),"Inner")){
            score += 2;
            id += "2";
        }else if(!std::strcmp(join_type.c_str(),"Left") || !std::strcmp(join_type.c_str(),"Right")){
            score += 3;
            id += "3";
        }else if(!std::strcmp(join_type.c_str(),"Full")){
            score += 4;
            id += "4";
        }else{
            std::cerr<<"unkonw join type"<<std::endl;
            exit(-1);
        }
    }
    unique_id = id;
    return score;
}

bool ScalingInfo::Match(ScalingInfo* scale_info){
    /*check join_type_list */
    bool matched  = true;
    auto src_id = scale_info->UniqueId();
    assert(src_id.size() == unique_id_.size());
    for(size_t i=0;i<src_id.size();i++){
        if(src_id[i] < unique_id_[i]){
            matched = false;
            break;
        } 
    }
    return matched;
}

void RangePostingList::Insert(SMPredEquivlence* range,int id){
    sets_.insert(range);
    set2id_[range] = id;
}

void RangePostingList::Erase(SMPredEquivlence* range,int id){
    sets_.erase(range);
    auto iter = set2id_.find(range);
    if(iter != set2id_.end())
        set2id_.erase(iter);
}

/*get all ranges which is superset of range,used in query index*/
SMVector<int> RangePostingList::SuperSet(SMPredEquivlence* pe){
    SMVector<int> rets;
    //auto pos = sets_.lower_bound(pe);
    for(auto iter = sets_.begin() ; iter != sets_.end(); ++iter){
        if(SuperSetInternal(*iter,pe)){
            rets.push_back(set2id_[*iter]);
        }
    }
    return rets;
}

/**
* TODO: not implement yet
*/
SMVector<int> RangePostingList::SubSet(SMPredEquivlence* range){
    SMVector<int> rets;
    return rets;
}

void RangePostingList::Insert(PredEquivlence* pe,int id){
    assert(pe);
    /*copy a pe in shared_memory*/
    //LWLockAcquire(shmem_lock_, LW_EXCLUSIVE);
    SMPredEquivlence* sm_pe = (SMPredEquivlence*)ShmemAlloc(sizeof(SMPredEquivlence));
    new (sm_pe) SMPredEquivlence();
    sm_pe->Copy(pe);
    //LWLockRelease(shmem_lock_);
    assert(sm_pe);

    sets_.insert(sm_pe);
    set2id_[sm_pe] = id;
}

void RangePostingList::Erase(PredEquivlence* pe,int id){
}

SMVector<int> RangePostingList::SubSet(PredEquivlence* range){
}

/*check if the pe is the dst_pe's superset*/
bool RangePostingList::SuperSetInternal(SMPredEquivlence* dst_pe, SMPredEquivlence* pe){
    assert(pe);

    /*externel check: subquery names in pe*/
    if(dst_pe->SubqueryNames().size() != pe->SubqueryNames().size()){
        return false;
    } 
    auto dst_iter = dst_pe->SubqueryNames().begin();
    for(const auto& subquery_name : pe->SubqueryNames()){
        if(subquery_name != *dst_iter){
            return false;
        }
        /**
         * MARK: here we compare subquery contents
         */
        if(!SearchSubquery(pe->GetSubLinkLevelPeLists()[subquery_name],dst_pe->GetSubLinkLevelPeLists()[subquery_name])){
            return false;
        }
        dst_iter++;
    }

    if(dst_pe->RangeCnt() > 1){
        return true;
    }
    if(pe->RangeCnt() > 1){
        return false;
    }

    auto ranges = pe->GetRanges();
    for(const auto r :dst_pe->GetRanges()){
        bool match = false;
        if(r->PredType() == PType::EQUAL || r->PredType() == PType::NOT_EQUAL || r->PredType() == PType::RANGE){
            for(const auto& src_r : ranges){
                if(r->PredType() == PType::EQUAL || r->PredType() == PType::NOT_EQUAL || r->PredType() == PType::RANGE){
                    bool super = true;
                    /* check lowlimit */
                    if(r->LowerLimit() == LOWER_LIMIT){
                        if(src_r->LowerLimit() != LOWER_LIMIT){
                            super = false;
                            continue;
                        }
                    }else{
                        if(src_r->LowerLimit() != LOWER_LIMIT){
                            int ret = SMPredEquivlenceRange::LimitCompare(r->LowerLimit(),r->PredVarType(),src_r->LowerLimit(),src_r->PredVarType());
                            if(ret < 0 || (!ret && r->GetLowerBoundaryConstraint() && !src_r->GetLowerBoundaryConstraint())){
                                super = false;
                                continue;        
                            }
                        }			
                    }
    
                    /* check upperlimit */
                    if(r->UpperLimit() == UPPER_LIMIT){
                        if(src_r->UpperLimit() != UPPER_LIMIT){
                            super = false;
                            continue;
                        }
                    }else{
                        if(src_r->UpperLimit() != UPPER_LIMIT){
                            int ret = SMPredEquivlenceRange::LimitCompare(r->UpperLimit(),r->PredVarType(),src_r->UpperLimit(),src_r->PredVarType());
                            if(ret > 0 || (!ret && !r->GetLowerBoundaryConstraint() && src_r->GetLowerBoundaryConstraint())){
                                super = false;
                                continue;        
                            }
                        }			
                    }
    
                    if(super){
                        match = true;
                        break;
                    }
                }else{
                    match = false;
                }
            }
        }else if(r->PredType() == PType::SUBQUERY || r->PredType() == PType::SUBLINK){
            /*not support yet*/
            match = true;
        }else{
            match = false;
        }

        if(!match){
            return false;
        }
    }
    return true;
}

/**
 * check if dst_mgr is the subquery of src_mgr
 */
bool RangePostingList::SearchSubquery(SMLevelManager* src_mgr,SMLevelManager* dst_mgr){
    assert(src_mgr && dst_mgr);
    /*check canonical hash code*/
    if(src_mgr->GetJsonSubPlan() != dst_mgr->GetJsonSubPlan()){
        return false;
    }
    /*check join node*/
    auto src_scaling_info = std::make_shared<ScalingInfo>(src_mgr->GetJoinTypeList());
    auto dst_scaling_info = std::make_shared<ScalingInfo>(dst_mgr->GetJoinTypeList());
    if(!src_scaling_info->Match(dst_scaling_info.get())){
        return false;
    }

    for(int h = src_mgr->GetTotalEquivlences().size()-1; h>=0;--h){
        auto& src_lpes_list = src_mgr->GetTotalEquivlences()[h]->GetLpesList();
        auto& dst_lpes_list = dst_mgr->GetTotalEquivlences()[h]->GetLpesList();
        auto& src_sorts = src_mgr->GetTotalSorts()[h]->GetLevelAggList();
        auto& dst_sorts = dst_mgr->GetTotalSorts()[h]->GetLevelAggList();
        auto& src_aggs = src_mgr->GetTotalAggs()[h]->GetLevelAggList();
        auto& dst_aggs = dst_mgr->GetTotalAggs()[h]->GetLevelAggList();
        
        bool match = false;
        for(size_t src_idx = 0; src_idx < src_lpes_list.size(); ++src_idx){
            for(size_t dst_idx = 0; dst_idx < dst_lpes_list.size(); ++dst_idx){
                /*check range*/
                if(!SearchRange(src_lpes_list[src_idx],dst_lpes_list[dst_idx])){
                    continue;
                }
                /*check sort*/
                if(src_sorts.size() && dst_sorts.size()){
                    if(!SearchSort(src_sorts[src_idx],dst_sorts[dst_idx])){
                        continue;
                    }
                }else if(src_sorts.size() && dst_sorts.empty()) {
                    continue;
                }else if(src_sorts.empty() && dst_sorts.size()){
                    continue;
                }
                /*check agg*/
                if(src_aggs.size() && dst_aggs.size()){
                    if(!SearchAgg(src_aggs[src_idx],dst_aggs[dst_idx])){
                        continue;
                    }
                }else if(src_aggs.size() && dst_aggs.empty()){
                    continue;
                }else if(src_aggs.empty() && dst_aggs.size()){
                    continue;
                }
                if(src_lpes_list[src_idx]->EarlyStop()){
                    return true;
                }
                match = true;
                break;
            }
            if(match){
                break;
            }
        }
        if(!match){
            return false;
        }
    }
    return true;
}

bool RangePostingList::SearchRange(SMLevelPredEquivlences * src_lpes,SMLevelPredEquivlences * dst_lpes){
    if(Match(dst_lpes,src_lpes)){
        return true;
    }
    return false;
}

bool RangePostingList::SearchSort(SMLevelAggAndSortEquivlences* src_sorts,SMLevelAggAndSortEquivlences* dst_sorts){
    SMUnorderedSet<SMAggAndSortEquivlence *>states;
    for(const auto& pe: dst_sorts->GetLevelAggSets()){
            auto extends = pe->GetExtends();
            for(const auto& src_eq: src_sorts->GetLevelAggSets()){
                if(states.find(src_eq)!= states.end()){
                    continue;
                }else{
                    bool matched = true;
                    auto src_extends = src_eq->GetExtends();
                    for(const auto& src_key : src_extends){
                        if(extends.find(SMString(src_key))== extends.end()){
                            matched = false;
                        }
                    }
                    if(matched){
                        states.insert(src_eq);
                    }
                }
            }
    }
    if(states.size() == src_sorts->Size()){
        return true;
    }
    return false;
}

bool RangePostingList::SearchAgg(SMLevelAggAndSortEquivlences* src_agg,SMLevelAggAndSortEquivlences* dst_agg){
    SMUnorderedSet<SMAggAndSortEquivlence *>states;
    for(const auto& pe: dst_agg->GetLevelAggSets()){
            auto extends = pe->GetExtends();
            for(const auto& src_eq: src_agg->GetLevelAggSets()){
                if(states.find(src_eq)!= states.end()){
                    continue;
                }else{
                    bool matched = true;
                    auto src_extends = src_eq->GetExtends();
                    for(const auto& src_key : src_extends){
                        if(extends.find(SMString(src_key))== extends.end()){
                            matched = false;
                        }
                    }
                    if(matched){
                        states.insert(src_eq);
                    }
                }
            }
    }
    if(states.size() == src_agg->Size()){
        return true;
    }
    return false;
}

bool RangePostingList::Match(SMLevelPredEquivlences* dst_lpes, SMLevelPredEquivlences* lpes){
    assert(lpes);
    for(const auto& pe : *lpes){
        for(const auto& attr : pe->GetPredSet()){
            auto key2pe = dst_lpes->GetKey2Pe();
            if(key2pe.find(attr) != key2pe.end()){
                auto dst_pe = key2pe[attr];
                if(SuperSetInternal(dst_pe,pe)){
                    continue;
                }else{
                    return false;
                }
            }
        }
    }
    return true;
}


void RangeInvertedIndex::Insert(LevelPredEquivlences* lpes){
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for(const auto & pe : *lpes){
        /**
         * subquery has same subquery_names but without same contents will not be add.
         */
        auto pe_serialization = SMString(pe->GetSerialization());
        if(set2id_.find(pe_serialization) == set2id_.end()){
            set_cnt_++;
            set2id_[pe_serialization] = set_cnt_;
            id2set_[set_cnt_] = pe_serialization;

            if(pe->GetPredSet().empty()){
                assert(pe->HasSubquery());
                for(const auto& sub_name : pe->SubqueryNames()){
                    inverted_map_[SMString(sub_name)].Insert(pe,set2id_[pe_serialization]);
                    items_cnt_ = inverted_map_.size();
                }
            }

            for(auto attr : pe->GetPredSet()){
                auto sm_attr = SMString(attr);
                inverted_map_[sm_attr].Insert(pe,set2id_[pe_serialization]);
                items_cnt_ = inverted_map_.size();
            }
        }
    }
}

void RangeInvertedIndex::Erase(LevelPredEquivlences* lpes){
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for(const auto& pe : *lpes){
        auto pe_serialization = SMString(pe->GetSerialization());
        if(set2id_.find(pe_serialization) == set2id_.end())return;
        for(const auto& attr : pe->GetPredSet()){
            auto sm_attr = SMString(attr);
            inverted_map_[sm_attr].Erase(pe,set2id_[pe_serialization]);
            items_cnt_ = inverted_map_.size();
        }
    }
}

bool RangeInvertedIndex::Serach(LevelPredEquivlences* lpes){
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);    
    for(const auto& pe : *lpes){
        auto pe_serialization = SMString(pe->GetSerialization());
        if(set2id_.find(pe_serialization) == set2id_.end()){
            return false;
        }
    }
    return true;
}

SMSet<int> RangeInvertedIndex::SuperSets(LevelPredEquivlences* lpes){
    assert(lpes);
    /**
     * MARK: can we escape shmemalloc while just seraching
     */
    //LWLockAcquire(shmem_lock_, LW_EXCLUSIVE);
    SMLevelPredEquivlences* sm_lpes = (SMLevelPredEquivlences*)ShmemAlloc(sizeof(SMLevelPredEquivlences));
    new (sm_lpes) SMLevelPredEquivlences();
    sm_lpes->Copy(lpes);
    //LWLockRelease(shmem_lock_);
    assert(sm_lpes);
    
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    SMSet<int>pe_id_set;
    for(const auto& pe : *sm_lpes){
        if(pe->GetPredSet().empty()){
            assert(pe->HasSubquery());
            for(const auto& sub_name : pe->SubqueryNames()){
                auto ids = inverted_map_[SMString(sub_name)].SuperSet(pe);
                for(const auto& id : ids){
                    pe_id_set.insert(id);
                }
            }
        }
        for(const auto& attr : pe->GetPredSet()){
            auto ids = inverted_map_[SMString(attr)].SuperSet(pe);
            for(const auto & id : ids){
                pe_id_set.insert(id);
            }
        }
    }

    return pe_id_set;
}

SMSet<int> RangeInvertedIndex::SubSets(LevelPredEquivlences* lpes){
    // std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    // SMUnorderedSet<SET,SetHasher> set_list;
    // for(auto item : set){
    //     auto temp_sets = inverted_map_[item].SubSets(set);
    //     for(auto set: temp_sets){
    //         set_list.insert(set);
    //     }                
    // }
    // return set_list;
}

SMSet<int> RangeInvertedIndex::GetLpesIds(LevelPredEquivlences* lpes){
    assert(lpes);
    SMSet<int>ids;
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    for(const auto& pe : *lpes){
        const auto& pe_serialization = SMString(pe->Serialization());
        assert(set2id_.find(pe_serialization) != set2id_.end());
        ids.insert(set2id_[pe_serialization]);
    }
    return ids;
}