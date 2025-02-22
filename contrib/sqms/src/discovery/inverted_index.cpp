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
    auto pos = sets_.lower_bound(pe);
    for(auto iter = pos ; iter != sets_.end(); ++iter){
        if(SuperSetInternal(pe,*iter)){
            rets.push_back(set2id_[*iter]);
        }
    }
    return rets;
}

/**
* TODO: not implement yet
*/
SMVector<int> RangePostingList::SubSet(PredEquivlence* range){
    SMVector<int> rets;
    return rets;
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
         * TODO: maybe we should checek subquery contents here
         */

        dst_iter++;
    }

    if(dst_pe->RangeCnt() > 1){
        return true;
    }
    if(pe->RangeCnt() > 1){
        return false;
    }

    /*here we just compare subquery name,such as SUBQUERY1, SUBQUERY2, we will compare */
    auto ranges = pe->GetRanges();
    for(const auto r :dst_pe->GetRanges()){
        bool match = false;
        if(r->PredType() == PType::EQUAL || r->PredType() == PType::NOT_EQUAL || r->PredType() == PType::RANGE){
            for(const auto& src_r : ranges){
                bool super = true;
                /* check lowlimit */
                if(r->LowerLimit() == LOWER_LIMIT){
                    if(src_r->LowerLimit() != LOWER_LIMIT){
                        super = false;
                        continue;
                    }
                }else{
                    if(src_r->LowerLimit() != LOWER_LIMIT){
                        if(r->LowerLimit() < src_r->LowerLimit()){
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
                        if(r->UpperLimit() > src_r->UpperLimit()){
                            super = false;
                            continue;
                        }
                    }			
                }
                if(super){
                    match = true;
                    break;
                }
            }
        }else{
            /*not support yet*/
            match = true;
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
    if(src_scaling_info->Match(dst_scaling_info.get())){
        return true;
    }
    for(int h = src_mgr->GetTotalEquivlences().size()-1; h>=0;--h){
        auto src_lpes_list = src_mgr->GetTotalEquivlences()[h]->GetLpesList();
        auto dst_lpes_list = dst_mgr->GetTotalEquivlences()[h]->GetLpesList();
        
        for(size_t src_idx = 0; src_idx < src_lpes_list.size(); ++src_idx){
            for(const auto& dst_lpes : dst_lpes_list){
                /*check range*/

                /*check sort*/

                /*check agg*/

                if(src_lpes_list[src_idx]->EarlyStop()){
                    return true;
                }
            }
        }
    }
    return true;
}


void RangeInvertedIndex::Insert(LevelPredEquivlences* lpes){
    SMLevelPredEquivlences* sm_lpes = (SMLevelPredEquivlences*)ShmemAlloc(sizeof(SMLevelPredEquivlences));
    new (sm_lpes) SMLevelPredEquivlences();
    sm_lpes->Copy(lpes);
    assert(sm_lpes);

    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    for(const auto & pe : *sm_lpes){
        const auto& pe_serialization = pe->GetSerialization();
        if(set2id_.find(pe_serialization) == set2id_.end()){
            set_cnt_++;
            set2id_[pe_serialization] = set_cnt_;
            id2set_[set_cnt_] = pe_serialization;

            for(auto attr : pe->GetPredSet()){
                inverted_map_[attr].Insert(pe,set2id_[attr]);
                items_cnt_ = inverted_map_.size();
            }
        }
    }
}

void RangeInvertedIndex::Erase(LevelPredEquivlences* lpes){
    SMLevelPredEquivlences* sm_lpes = (SMLevelPredEquivlences*)ShmemAlloc(sizeof(SMLevelPredEquivlences));
    new (sm_lpes) SMLevelPredEquivlences();
    sm_lpes->Copy(lpes);
    assert(sm_lpes);

    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    for(const auto& pe : *sm_lpes){
        auto pe_serialization = pe->GetSerialization();
        if(set2id_.find(pe_serialization) == set2id_.end())return;
        for(const auto& attr : pe->GetPredSet()){
            inverted_map_[attr].Erase(pe,set2id_[attr]);
            items_cnt_ = inverted_map_.size();
        }
    }
}

bool RangeInvertedIndex::Serach(LevelPredEquivlences* lpes){
    SMLevelPredEquivlences* sm_lpes = (SMLevelPredEquivlences*)ShmemAlloc(sizeof(SMLevelPredEquivlences));
    new (sm_lpes) SMLevelPredEquivlences();
    sm_lpes->Copy(lpes);
    assert(sm_lpes);

    std::unique_lock<std::shared_mutex> lock(rw_mutex_);    
    for(const auto& pe : *sm_lpes){
        auto pe_serialization = pe->GetSerialization();
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
    SMLevelPredEquivlences* sm_lpes = (SMLevelPredEquivlences*)ShmemAlloc(sizeof(SMLevelPredEquivlences));
    new (sm_lpes) SMLevelPredEquivlences();
    sm_lpes->Copy(lpes);
    assert(sm_lpes);
    
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    SMSet<int>pe_id_set;
    for(const auto& pe : *sm_lpes){
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