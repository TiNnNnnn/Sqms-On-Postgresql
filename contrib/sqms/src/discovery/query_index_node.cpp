#include "discovery/query_index_node.hpp"
#include <unordered_set>
#include <algorithm>
#include <memory>

std::vector<std::string> LevelOneStrategy::findChildren(){
    return std::vector<std::string>();
}

/**
 * LevelOneStrategy::Insert
 */
bool LevelOneStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    auto json_sub_plan = std::string(level_mgr->GetHsps()->canonical_json_plan);

    tbb::concurrent_hash_map<std::string, HistoryQueryIndexNode*>::const_accessor acc;
    if(set_map_.find(acc ,json_sub_plan)){
        auto child = acc->second;
        return child->Insert(level_mgr);
    }else{
        /*create a new child node*/
        size_t next_level = FindNextInsertLevel(level_mgr,1);
        HistoryQueryIndexNode* new_idx_node = new HistoryQueryIndexNode(next_level,total_height_);
        if(!new_idx_node->Insert(level_mgr)){
            return false;
        }
        /*update cur level index*/
        set_map_.insert(std::make_pair(json_sub_plan,new_idx_node));
    }
    return true;
}

/**
 * LevelOneStrategy::Serach
 */
bool LevelOneStrategy::Serach(LevelManager* level_mgr){
    assert(level_mgr);
    auto json_sub_plan = std::string(level_mgr->GetHsps()->canonical_json_plan);

    tbb::concurrent_hash_map<std::string, HistoryQueryIndexNode*>::const_accessor acc;
    if(set_map_.find(acc ,json_sub_plan)){
        auto child = acc->second;
        return child->Search(level_mgr);
    }else{
        return false;
    }
    return false;
}

/**
 * LevelOneStrategy::Remove
 */
bool LevelOneStrategy::Remove(LevelManager* level_mgr){
    return false;
}

/**
 * LevelTwoStrategy::Insert
 */
bool LevelTwoStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);

    assert(level_mgr->GetTotalAggs().size());
    auto top_aggs = level_mgr->GetTotalAggs()[0];
    
    bool child_exist = false;
    std::shared_ptr<HistoryQueryIndexNode> child_node = nullptr;

    std::vector<std::vector<std::string>> all_agg_vecs;
    for(const auto& la_eq : top_aggs->GetLevelAggList()){
        /**
         * transforer set type to vector type to insert into inverted_idx
         * TODO: 01-06 how to reduce about agg or sort node more then one in one plan 
         * yyk: 01-07: 
         */
        std::vector<std::string> agg_vec;
        for(const auto& agg : la_eq->GetLevelAggSets()){
            auto agg_extends = agg->GetExtends();
            for(const auto& expr : agg_extends){
                agg_vec.push_back(expr);
            }
        }

        if(inverted_idx_->Serach(agg_vec)){
            child_exist = true;
            tbb::concurrent_hash_map<SET,std::shared_ptr<HistoryQueryIndexNode>,SetHasher>::const_accessor acc;
            child_map_.find(acc,agg_vec);
            assert(acc->second);
            if(child_node){
                assert(child_node == acc->second);
            }
            child_node = acc->second;
        }else{
            /*collect agg_vec not in child_map*/
            all_agg_vecs.push_back(agg_vec);
        }
    }

    bool first_insert = false;
    if(child_exist){
        for(const auto& agg_vec : all_agg_vecs){
            inverted_idx_->Insert(agg_vec);
            /** avoid insert child_node repetitive */
            if(first_insert){
                if(!child_node->Insert(level_mgr)){
                    return false;
                }
                first_insert = false;
            }
            child_map_.insert(std::make_pair(agg_vec,child_node));
        }
        return true;
    }else{
        size_t next_level = FindNextInsertLevel(level_mgr,2);
        auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
        for(const auto& agg_vec : all_agg_vecs){
            inverted_idx_->Insert(agg_vec);
            if(first_insert){
                if(!child_node->Insert(level_mgr)){
                    return false;
                }
                first_insert = false;
            }
            child_map_.insert(std::make_pair(agg_vec,child_node));
        }
        return true;
    }
    return false;
}

/**
 * LevelTwoStrategy::Serach
 */
bool LevelTwoStrategy::Serach(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetTotalAggs().size());
    /*we current only build index for top level aggs*/
    auto top_aggs = level_mgr->GetTotalAggs()[0];
    /**
     * item in levelagglist is or relation
     * TODO: here may be can use multi thread or coroutines
     **/
    for(const auto& la_eq : top_aggs->GetLevelAggList()){
        std::unordered_map<SET,std::pair<int,bool>,SetHasher> match_maps;
        for(const auto& agg : la_eq->GetLevelAggSets()){
            auto extends = agg->GetExtends();
            for(const auto& expr : extends){
                auto match_sets = inverted_idx_->SuperSets({expr});
                for(const auto& m_set : match_sets){
                    /*attrs in one pe, just counted once*/
                    if(!match_maps[m_set].second){
                        match_maps[m_set].first += 1;
                        match_maps[m_set].second = true;
                    }
                }
            }
            for(auto& m_map: match_maps){
                m_map.second.second =false;
            }
        }
        for(const auto& m_map : match_maps){
            if(m_map.second.first == la_eq->Size()){
                tbb::concurrent_hash_map<SET,std::shared_ptr<HistoryQueryIndexNode>,SetHasher>::const_accessor acc;
                child_map_.find(acc,m_map.first);
                assert(acc->second);
                if(acc->second->Search(level_mgr)){
                    return true;
                }
            }
        }
        match_maps.clear();
    }
    return false;
}

/**
 * LevelTwoStrategy::Remove
 */
bool LevelTwoStrategy::Remove(LevelManager* level_mgr){
    assert(level_mgr);
    return false;
}

/**
 * LevelThreeStrategy::Insert
 */
bool LevelRangeStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetTotalEquivlences().size());
    
    auto top_eqs = level_mgr->GetTotalEquivlences()[0];
    /**we just build index on single attribute*/
    for(const auto& lpes : *top_eqs){
        std::vector<std::string>pe_vecs;
        LevelPredEquivlences * remind_lpes = new LevelPredEquivlences();
        for(const auto& pe : *lpes){
            if(pe->GetPredSet().size() == 1){
                pe_vecs.push_back(*pe->GetPredSet().begin());
            }else{
                remind_lpes->Insert(pe);
            }
        }
        std::sort(pe_vecs.begin(),pe_vecs.end());
        {
            std::shared_lock<std::shared_mutex> lock(rw_mutex_);
            if(inverted_idx_->Serach(pe_vecs)){    
                auto acc = child_map_.find(pe_vecs);
                assert(acc == child_map_.end());

                auto remind_list = acc->second;
                auto iter = remind_list.find(remind_lpes);
                if(iter != remind_list.end()){
                    auto child =  iter->second;
                    return child->Insert(level_mgr);
                }else{
                    size_t next_level = FindNextInsertLevel(level_mgr,3);
                    auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
                    if(!new_idx_node->Insert(level_mgr)){
                        return false;
                    }
                    /*update cur level index*/
                    remind_list[remind_lpes] = new_idx_node;
                }
            }else{
                size_t next_level = FindNextInsertLevel(level_mgr,3);
                auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
                if(!new_idx_node->Insert(level_mgr)){
                    return false;
                }
                std::unordered_map<LevelPredEquivlences *, std::shared_ptr<HistoryQueryIndexNode>> new_remind_list;
                new_remind_list[remind_lpes] = new_idx_node;
                child_map_[pe_vecs] = new_remind_list;
            }
        }
    }
    

    return false;
}

/**
 * LevelThreeStrategy::Serach
 */
bool LevelRangeStrategy::Serach(LevelManager* level_mgr){
    assert(level_mgr);

    return false;
}

/**
 * LevelThreeStrategy::Remove
 */
bool LevelRangeStrategy::Remove(LevelManager* level_mgr){
    assert(level_mgr);
    return false;
}

/**
 * LevelTwoStrategy: check the qual constraint condition,such as "t1.a = t2.b"
 */
std::vector<std::string> LevelTwoStrategy::findChildren(){
    return std::vector<std::string>();
}

/**
 * LevelThreeStrategy: check the range constraint condition
 */
std::vector<std::string> LevelRangeStrategy::findChildren(){
    return std::vector<std::string>();
}

size_t LevelStrategy::FindNextInsertLevel(LevelManager* level_mgr, size_t cur_level){
    return cur_level+1;
}

void LevelStrategyContext::Insert(LevelManager* level_mgr){

}
void LevelStrategyContext::Remove(LevelManager* level_mgr){

}
void LevelStrategyContext::Search(LevelManager* level_mgr){

}