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

    auto top_aggs = level_mgr->GetTotalAggs()[0];
    /**
     * item in levelagglist is or relation
     * TODO: here may be can use multi thread or coroutines
     **/
    for(const auto& la_eq : top_aggs->GetLevelAggList()){
        std::vector<std::string> agg_vec;
        for(const auto& agg : la_eq->GetLevelAggSets()){
            auto agg_extends = agg->GetExtends();
            for(const auto& expr : agg_extends){
                agg_vec.push_back(expr);
            }
        }

        auto match_sets = inverted_idx_->SubSets(agg_vec);
        for(const auto& set:  match_sets){
            tbb::concurrent_hash_map<SET,std::shared_ptr<HistoryQueryIndexNode>,SetHasher>::const_accessor acc;
            child_map_.find(acc,agg_vec);
            assert(acc->second);
            if(acc->second->Search(level_mgr)){
                return true;
            }
        }
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
bool LevelThreeStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetTotalAggs().size());
    
    
    
    return false;
}

/**
 * LevelThreeStrategy::Serach
 */
bool LevelThreeStrategy::Serach(LevelManager* level_mgr){
    assert(level_mgr);

    return false;
}

/**
 * LevelThreeStrategy::Remove
 */
bool LevelThreeStrategy::Remove(LevelManager* level_mgr){
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
std::vector<std::string> LevelThreeStrategy::findChildren(){
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