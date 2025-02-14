#include "discovery/inverted_index.hpp"

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
    /*here we just compare subquery name,such as SUBQUERY1, SUBQUERY2, we will compare */
    auto ranges = pe->GetRanges();
    SMVector<SMString> subquery_names;
    for(const auto r :dst_pe->GetRanges()){
        bool match = false;
        if(r->PredType() == PType::SUBQUERY || r->PredType() == PType::SUBLINK){
                assert(!r->GetSubqueryName().empty());
                subquery_names.push_back(r->GetSubqueryName());
                match = true;
        }else if(r->PredType() == PType::EQUAL || r->PredType() == PType::NOT_EQUAL || r->PredType() == PType::RANGE){
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

    /*externel check: subquery names in pe*/
    size_t sub_src_idx = 0;
    for(const auto& r : ranges){
        if(r->PredType() == PType::SUBQUERY || r->PredType() == PType::SUBLINK){
            if(sub_src_idx >= subquery_names.size()){
                return false;
            }
            if(r->GetSubqueryName() != subquery_names[sub_src_idx]){
                return false;
            }
            /**
            * TODO: here we check the true subquery
            */
            ++sub_src_idx;
        }
    }
    return true;
}

void RangeInvertedIndex::Insert(LevelPredEquivlences* lpes){
    SMLevelPredEquivlences* sm_lpes = (SMLevelPredEquivlences*)ShmemAlloc(sizeof(SMLevelPredEquivlences));
    sm_lpes->Copy(lpes);
    assert(sm_lpes);

    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    for(const auto & pe : *sm_lpes){
        auto pe_serialization = pe->GetSerialization();
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
    for(const auto& pe : *lpes){
        auto pe_serialization = pe->Serialization();
        assert(set2id_.find(pe_serialization) == set2id_.end());
        ids.insert(set2id_[pe_serialization]);
    }
    return ids;
}