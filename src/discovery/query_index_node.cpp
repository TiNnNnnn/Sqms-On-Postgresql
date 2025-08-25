#include "discovery/query_index_node.hpp"
#include "discovery/query_index.hpp"
#include "common/util.hpp"
#include <unordered_set>
#include <algorithm>
#include <memory>

/**
 * LevelOneStrategy::Insert
 */
bool LevelHashStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    auto json_sub_plan = SMString(level_mgr->GetHsps()->canonical_json_plan);

    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*,SMStringHash>::const_accessor acc;
    if(set_map_.find(acc ,json_sub_plan)){
        auto child = acc->second;
        return child->Insert(level_mgr);
    }else{
        /*create a new child node*/
        size_t next_level = FindNextInsertLevel(level_mgr,1);
        HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
        if(!new_idx_node){
            elog(ERROR, "ShmemAlloc failed: not enough shared memory");
            exit(-1);
        }
        new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);
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
bool LevelHashStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);
    auto json_sub_plan = SMString(level_mgr->GetHsps()->canonical_json_plan);
    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*,SMStringHash>::const_accessor acc;
    if(set_map_.find(acc ,json_sub_plan)){
        auto child = acc->second;
        return child->Search(level_mgr,-1);
    }else{
        
        return false;
    }
    return false;
}

/**
 * LevelOneStrategy::Remove
 */
bool LevelHashStrategy::Remove(LevelManager* level_mgr){
    return false;
}

bool LevelHashStrategy::Insert(NodeCollector* node_collector){
    assert(node_collector);
    auto json_sub_plan = SMString(node_collector->json_sub_plan);

    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*,SMStringHash>::const_accessor acc;
    if(set_map_.find(acc ,json_sub_plan)){
        auto child = acc->second;
        return child->Insert(node_collector);
    }else{
        /*create a new child node*/
        size_t next_level = FindNextInsertLevel(node_collector,1);
        HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
        if(!new_idx_node){
            elog(ERROR, "ShmemAlloc failed: not enough shared memory");
            exit(-1);
        }
        new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);
        if(!new_idx_node->Insert(node_collector)){
            return false;
        }
        set_map_.insert(std::make_pair(json_sub_plan,new_idx_node));
    }
    return true;
}

bool LevelHashStrategy::Remove(NodeCollector* node_collector){
    return true;
}

bool LevelHashStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);
    auto json_sub_plan = SMString(node_collector->json_sub_plan);
    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*,SMStringHash>::const_accessor acc;
    if(set_map_.find(acc ,json_sub_plan)){
        auto child = acc->second;
        return child->Serach(node_collector);
    }else{
        return false;
    }
    return false;
}

bool LevelHashStrategy::Insert(const std::string& plan){
    return set_map_.insert(std::make_pair(SMString(plan),nullptr));
}

bool LevelHashStrategy::Search(const std::string& plan){
    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*,SMStringHash>::const_accessor acc;
    return set_map_.find(acc,SMString(plan));
}

bool LevelScalingStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetJoinTypeList().size());
    auto new_scaling_info =  (ScalingInfo*) ShmemAlloc (sizeof(ScalingInfo));   
    assert(new_scaling_info);
    new (new_scaling_info) ScalingInfo(level_mgr->GetJoinTypeList());

    bool exist = true;
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        auto iter = scaling_idx_.find(new_scaling_info->JoinTypeScore());
        if(iter != scaling_idx_.end()){
            auto id_pos = iter->second.find(new_scaling_info->UniqueId());
            if(id_pos == iter->second.end()){
                iter->second.insert(new_scaling_info->UniqueId());
                exist = false;
            }
        }else{
            scaling_idx_[new_scaling_info->JoinTypeScore()].insert(new_scaling_info->UniqueId());
            exist = false;
        }

        if(exist){
            return child_map_[new_scaling_info->UniqueId()].second->Insert(level_mgr);
        }else{
            /*create a new child node*/
            size_t next_level = FindNextInsertLevel(level_mgr,2);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);
            
            if(!new_idx_node->Insert(level_mgr)){
                return false;
            }
            child_map_.insert({new_scaling_info->UniqueId(),{new_scaling_info,new_idx_node}});
            return true;
        }
    }
}

bool LevelScalingStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);
    assert(level_mgr->GetJoinTypeList().size());
    auto new_scaling_info = std::make_shared<ScalingInfo>(level_mgr->GetJoinTypeList());
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        auto iter = scaling_idx_.find(new_scaling_info->JoinTypeScore());
        if(iter != scaling_idx_.end()){
            const auto& match_id_list = iter->second;
            for(const auto& id : match_id_list){
                if(child_map_[id].first->Match(new_scaling_info.get())){
                    return child_map_[id].second->Search(level_mgr,-1);
                }
            }
        }
        return false;
    }
}

bool LevelScalingStrategy::Remove(LevelManager* level_mgr){
    return true;
}

bool LevelScalingStrategy::Insert(NodeCollector* node_collector){
    assert(node_collector);
    assert(node_collector->join_type_list.size());
    auto new_scaling_info =  (ScalingInfo*) ShmemAlloc (sizeof(ScalingInfo));   
    assert(new_scaling_info);
    new (new_scaling_info) ScalingInfo(node_collector->join_type_list);

    bool exist = true;
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        auto iter = scaling_idx_.find(new_scaling_info->JoinTypeScore());
        if(iter != scaling_idx_.end()){
            auto id_pos = iter->second.find(new_scaling_info->UniqueId());
            if(id_pos == iter->second.end()){
                iter->second.insert(new_scaling_info->UniqueId());
                exist = false;
            }
        }else{
            scaling_idx_[new_scaling_info->JoinTypeScore()].insert(new_scaling_info->UniqueId());
            exist = false;
        }

        if(exist){
            return child_map_[new_scaling_info->UniqueId()].second->Insert(node_collector);
        }else{
            /*create a new child node*/
            size_t next_level = FindNextInsertLevel(node_collector,2);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);
            
            if(!new_idx_node->Insert(node_collector)){
                return false;
            }
            child_map_.insert({new_scaling_info->UniqueId(),{new_scaling_info,new_idx_node}});
            return true;
        }
    } 
    return true;
}
bool LevelScalingStrategy::Remove(NodeCollector* node_collector){
    return true;
}

bool LevelScalingStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);
    assert(node_collector->join_type_list.size() == 1);

    auto new_scaling_info = std::make_shared<ScalingInfo>(node_collector->join_type_list);
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        auto iter = scaling_idx_.find(new_scaling_info->JoinTypeScore());
        if(iter != scaling_idx_.end()){
            const auto& match_id_list = iter->second;
            for(const auto& id : match_id_list){
                if(child_map_[id].first->Match(new_scaling_info.get())){
                    return child_map_[id].second->Serach(node_collector);
                }
            }
        }else{
            elog(INFO, "search failed in scaling steategy");
        }
        return false;
    }
}

/**
 * LevelAggStrategy::Insert
 */
bool LevelAggStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetTotalAggs().size());
    /*we current only build index for top level aggs*/
    auto top_aggs = level_mgr->GetTotalAggs().back();

    for(const auto& la_eq : top_aggs->GetLevelAggList()){
        SMVector<int> agg_vec_id_list;
        for(const auto& agg : la_eq->GetLevelAggSets()){
            auto agg_extends = agg->GetExtends();
            SET agg_vec;
            for(const auto& expr : agg_extends){
                agg_vec.push_back(SMString(expr));
            }
            std::sort(agg_vec.begin(),agg_vec.end());
            
            if(!inverted_idx_->Serach(agg_vec))
                inverted_idx_->Insert(agg_vec);
            int vec_id = inverted_idx_->GetSetIdBySet(agg_vec);
            assert(vec_id != -1);
            agg_vec_id_list.push_back(vec_id);
        }
        std::sort(agg_vec_id_list.begin(),agg_vec_id_list.end());
        auto hash_value =  hash_array(agg_vec_id_list);
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            return child->Insert(level_mgr);
        }else{
            size_t next_level = FindNextInsertLevel(level_mgr,5);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);

            if(!new_idx_node->Insert(level_mgr)){
                return false;
            }
            child_map_.insert(std::make_pair(hash_value,new_idx_node));
        }
    }
    return true;
}

/**
 * LevelAggStrategy::Serach
 */
bool LevelAggStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);
    assert(level_mgr->GetTotalAggs().size());
    auto top_aggs = level_mgr->GetTotalAggs().back();
    
    LevelAggAndSortEquivlences * la_eq = nullptr;
    if(id != -1){
        la_eq = top_aggs->GetLevelAggList()[id];
    }else{
        assert(top_aggs->Size() == 1);
        la_eq = top_aggs->GetLevelAggList()[0];
    }
    
    SMVector<SMVector<int>> agg_vec_id_list;
    for(const auto& agg : la_eq->GetLevelAggSets()){
        auto agg_extends = agg->GetExtends();
        SET agg_vec;
        for(const auto& expr : agg_extends){
            agg_vec.push_back(SMString(expr));
        }
        std::sort(agg_vec.begin(),agg_vec.end());

        SMVector<int>match_set_id_list;
        auto match_set = inverted_idx_->SuperSets(agg_vec);
        for(const auto& m_set : match_set){
            int set_id = inverted_idx_->GetSetIdBySet(m_set);
            assert(set_id != -1);
            match_set_id_list.push_back(set_id);
        }
        std::sort(match_set_id_list.begin(),match_set_id_list.end());
        agg_vec_id_list.push_back(match_set_id_list);
    }
    SMVector<SMVector<int>> combination;
    SMVector<int> currentCombination(agg_vec_id_list.size());
    generateCombinations(agg_vec_id_list, currentCombination, 0, combination);

    for(const auto& c_id_list : combination){
        auto hash_value = hash_array(c_id_list);
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            if(child->Search(level_mgr,id)){
                return true;
            }
        }
    }
    return false;
}

/**
 * LevelTwoStrategy::Remove
 */
bool LevelAggStrategy::Remove(LevelManager* level_mgr){
    assert(level_mgr);
    return false;
}

bool LevelAggStrategy::Insert(NodeCollector* node_collector){
    assert(node_collector);
    assert(node_collector->node_aggs_);
    /*we current only build index for top level aggs*/
    auto top_aggs = node_collector->node_aggs_;

    for(const auto& la_eq : top_aggs->GetLevelAggList()){
        SMVector<int> agg_vec_id_list;
        for(const auto& agg : la_eq->GetLevelAggSets()){
            auto agg_extends = agg->GetExtends();
            SET agg_vec;
            for(const auto& expr : agg_extends){
                agg_vec.push_back(SMString(expr));
            }
            std::sort(agg_vec.begin(),agg_vec.end());
            
            if(!inverted_idx_->Serach(agg_vec))
                inverted_idx_->Insert(agg_vec);
            int vec_id = inverted_idx_->GetSetIdBySet(agg_vec);
            assert(vec_id != -1);
            agg_vec_id_list.push_back(vec_id);
        }
        std::sort(agg_vec_id_list.begin(),agg_vec_id_list.end());
        auto hash_value =  hash_array(agg_vec_id_list);
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            return child->Insert(node_collector);
        }else{
            size_t next_level = FindNextInsertLevel(node_collector,5);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);

            if(!new_idx_node->Insert(node_collector)){
                return false;
            }
            child_map_.insert(std::make_pair(hash_value,new_idx_node));
        }
    }
    return true;
}

bool LevelAggStrategy::Remove(NodeCollector* node_collector){
    assert(node_collector);
    return true;
}

bool LevelAggStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);
    if(!node_collector->node_aggs_){
        return false;
    }
    auto top_aggs = node_collector->node_aggs_;
    
    int id = node_collector->pe_idx;
    LevelAggAndSortEquivlences * la_eq = nullptr;
    if(id != -1){
        la_eq = top_aggs->GetLevelAggList()[id];
    }else{
        assert(top_aggs->Size() == 1);
        la_eq = top_aggs->GetLevelAggList()[0];
    }
    
    SMVector<SMVector<int>> agg_vec_id_list;
    for(const auto& agg : la_eq->GetLevelAggSets()){
        auto agg_extends = agg->GetExtends();
        SET agg_vec;
        for(const auto& expr : agg_extends){
            agg_vec.push_back(SMString(expr));
        }
        std::sort(agg_vec.begin(),agg_vec.end());

        SMVector<int>match_set_id_list;
        auto match_set = inverted_idx_->SuperSets(agg_vec);
        for(const auto& m_set : match_set){
            int set_id = inverted_idx_->GetSetIdBySet(m_set);
            assert(set_id != -1);
            match_set_id_list.push_back(set_id);
        }
        std::sort(match_set_id_list.begin(),match_set_id_list.end());
        agg_vec_id_list.push_back(match_set_id_list);
    }
    SMVector<SMVector<int>> combination;
    SMVector<int> currentCombination(agg_vec_id_list.size());
    generateCombinations(agg_vec_id_list, currentCombination, 0, combination);

    for(const auto& c_id_list : combination){
        auto hash_value = hash_array(c_id_list);
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            if(child->Serach(node_collector)){
                return true;
            }
        }
    }
    if(combination.empty()){
        elog(INFO, "search failed in agg steategy");
    }
    return false;
}

/**
 * LevelSortStrategy::Insert
 */
bool LevelSortStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetTotalSorts().size());
    /*we current only build index for top level aggs*/
    auto top_sorts = level_mgr->GetTotalSorts().back();

    for(const auto& la_eq : top_sorts->GetLevelAggList()){
        SMVector<int> sort_vec_id_list;
        for(const auto& sort : la_eq->GetLevelAggSets()){
            auto sort_extends = sort->GetExtends();
            SET sort_vec;
            for(const auto& expr : sort_extends){
                sort_vec.push_back(SMString(expr));
            }
            std::sort(sort_vec.begin(),sort_vec.end());
            
            if(!inverted_idx_->Serach(sort_vec))
                inverted_idx_->Insert(sort_vec);
            int vec_id = inverted_idx_->GetSetIdBySet(sort_vec);
            assert(vec_id != -1);
            sort_vec_id_list.push_back(vec_id);
        }
        std::sort(sort_vec_id_list.begin(),sort_vec_id_list.end());
        auto hash_value =  hash_array(sort_vec_id_list);
        
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            return child->Insert(level_mgr);
        }else{
            size_t next_level = FindNextInsertLevel(level_mgr,4);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);            
            
            if(!new_idx_node->Insert(level_mgr)){
                return false;
            }
            child_map_.insert(std::make_pair(hash_value,new_idx_node));
        }

    }
    return true;
}

bool LevelSortStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);
    assert(level_mgr->GetTotalSorts().size());
    auto top_sorts = level_mgr->GetTotalSorts().back();

    LevelAggAndSortEquivlences* la_eq = nullptr;
    if(id != -1){
        la_eq = top_sorts->GetLevelAggList()[id];
    }else{
        assert(top_sorts->Size() == 1);
        la_eq = top_sorts->GetLevelAggList()[0];
    }
    
    
    SMVector<SMVector<int>> sort_vec_id_list;
    for(const auto& sort : la_eq->GetLevelAggSets()){
        auto sort_extends = sort->GetExtends();
        SET sort_vec;
        for(const auto& expr : sort_extends){
            sort_vec.push_back(SMString(expr));
        }
        std::sort(sort_vec.begin(),sort_vec.end());

       SMVector<int>match_set_id_list;
        auto match_set = inverted_idx_->SuperSets(sort_vec);
        for(const auto& m_set : match_set){
            int set_id = inverted_idx_->GetSetIdBySet(m_set);
            assert(set_id != -1);
            match_set_id_list.push_back(set_id);
        }
        std::sort(match_set_id_list.begin(),match_set_id_list.end());
        sort_vec_id_list.push_back(match_set_id_list);
    }
    SMVector<SMVector<int>> combination;
    SMVector<int> currentCombination(sort_vec_id_list.size());
    generateCombinations(sort_vec_id_list, currentCombination, 0, combination);

    for(const auto& c_id_list : combination){
        auto hash_value = hash_array(c_id_list);
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            if(child->Search(level_mgr,id)){
                return true;
            }
        }
    }
    return false;
}

bool LevelSortStrategy::Remove(LevelManager* level_mgr){
    return true;
}  

bool LevelSortStrategy::Insert(NodeCollector* node_collector){
    assert(node_collector);
    assert(node_collector->node_sorts_);
    /*we current only build index for top level aggs*/
    auto top_sorts = node_collector->node_sorts_;

    for(const auto& la_eq : top_sorts->GetLevelAggList()){
        SMVector<int> sort_vec_id_list;
        for(const auto& sort : la_eq->GetLevelAggSets()){
            auto sort_extends = sort->GetExtends();
            SET sort_vec;
            for(const auto& expr : sort_extends){
                sort_vec.push_back(SMString(expr));
            }
            std::sort(sort_vec.begin(),sort_vec.end());
            
            if(!inverted_idx_->Serach(sort_vec))
                inverted_idx_->Insert(sort_vec);
            int vec_id = inverted_idx_->GetSetIdBySet(sort_vec);
            assert(vec_id != -1);
            sort_vec_id_list.push_back(vec_id);
        }
        std::sort(sort_vec_id_list.begin(),sort_vec_id_list.end());
        auto hash_value =  hash_array(sort_vec_id_list);
        
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            return child->Insert(node_collector);
        }else{
            size_t next_level = FindNextInsertLevel(node_collector,4);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_);            
            
            if(!new_idx_node->Insert(node_collector)){
                return false;
            }
            child_map_.insert(std::make_pair(hash_value,new_idx_node));
        }
    }
    return true;
}
bool LevelSortStrategy::Remove(NodeCollector* node_collector){
    return true;
}
bool LevelSortStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);
    if(!node_collector->node_sorts_){
        return false;
    }
    auto top_sorts = node_collector->node_sorts_;

    LevelAggAndSortEquivlences* la_eq = nullptr;
    int id = node_collector->pe_idx;
    if(id != -1){
        la_eq = top_sorts->GetLevelAggList()[id];
    }else{
        assert(top_sorts->Size() == 1);
        la_eq = top_sorts->GetLevelAggList()[0];
    }
    
    
    SMVector<SMVector<int>> sort_vec_id_list;
    for(const auto& sort : la_eq->GetLevelAggSets()){
        auto sort_extends = sort->GetExtends();
        SET sort_vec;
        for(const auto& expr : sort_extends){
            sort_vec.push_back(SMString(expr));
        }
        std::sort(sort_vec.begin(),sort_vec.end());

       SMVector<int>match_set_id_list;
        auto match_set = inverted_idx_->SuperSets(sort_vec);
        for(const auto& m_set : match_set){
            int set_id = inverted_idx_->GetSetIdBySet(m_set);
            assert(set_id != -1);
            match_set_id_list.push_back(set_id);
        }
        std::sort(match_set_id_list.begin(),match_set_id_list.end());
        sort_vec_id_list.push_back(match_set_id_list);
    }
    SMVector<SMVector<int>> combination;
    SMVector<int> currentCombination(sort_vec_id_list.size());
    generateCombinations(sort_vec_id_list, currentCombination, 0, combination);

    for(const auto& c_id_list : combination){
        auto hash_value = hash_array(c_id_list);
        SMConcurrentHashMap<uint32_t,HistoryQueryIndexNode*>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            if(child->Serach(node_collector)){
                return true;
            }
        }
    }

    if(combination.empty()){
        elog(INFO, "search failed in sort steategy");
    }

    return false;
}

/**
 * LevelThreeStrategy::Insert
 */
bool LevelRangeStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetTotalEquivlences().size());

    bool child_exist = false;
    HistoryQueryIndexNode* child_node = nullptr;
    
    auto top_eqs = level_mgr->GetTotalEquivlences().back();

    /*top_eqs has no pes,we just insert a empty SET into child_map*/
    if(!top_eqs->Size()){
        SET empty_id_vecs;
        SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
        bool found = child_map_.find(acc,empty_id_vecs);
        if(found){
            auto child = acc->second;
            return child->Insert(level_mgr);
        }else{
            size_t next_level = FindNextInsertLevel(level_mgr,3);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_); 
            if(!new_idx_node->Insert(level_mgr)){
                return false;
            }
            child_map_.insert({empty_id_vecs,new_idx_node});
        }
        return true;
    }

    for(const auto& lpes : *top_eqs){
        range_inverted_idx_->Insert(lpes);
        auto id_set = range_inverted_idx_->GetLpesIds(lpes);
        
        SET id_vecs;
        for(const auto& id : id_set){
            id_vecs.push_back(SMString(std::to_string(id)));
        }

        if(inverted_idx_->Serach(id_vecs)){
            /**
             * if id_vecs exists in inverted_idx, id_vecs must in
             * child_map
             */
            if(child_exist){
                if(!child_node->Insert(level_mgr)){
                    return false;
                }
            }else{
                SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
                bool found = child_map_.find(acc,id_vecs);
                assert(found);
                auto child = acc->second;
                if(!child->Insert(level_mgr)){
                    return false;
                }
            }
        }else{
            inverted_idx_->Insert(id_vecs);
            if(child_exist){
                child_map_.insert({id_vecs,child_node});
                continue;
            }
            size_t next_level = FindNextInsertLevel(level_mgr,3);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_); 
            if(!new_idx_node->Insert(level_mgr)){
                return false;
            }
            child_map_.insert({id_vecs,new_idx_node});
            child_exist = true;
            child_node = new_idx_node;         
        }
    }
    return true;
}

/**
 * LevelThreeStrategy::Serach
 */
bool LevelRangeStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);

    auto top_eqs = level_mgr->GetTotalEquivlences().back();
    
    /*if top_eqs, it means it can match all set in current node*/
    if(!top_eqs->Size()){
        for(const auto& iter : child_map_){
            if(iter.second->Search(level_mgr,id)){
                return true;
            }
        }
        return false;
    }

    size_t lpes_idx = 0;
    for(const auto& lpes : *top_eqs){
        auto set = range_inverted_idx_->SuperSets(lpes);
        SET id_vec;
        for(const auto& id : set){
            id_vec.push_back(SMString(std::to_string(id)));
        }
        auto sub_set = inverted_idx_->SubSets(id_vec);
        for(const auto& set : sub_set){
            SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
            bool found = child_map_.find(acc,set);
            
            assert(found);
            auto child = acc->second;
            if(child->Search(level_mgr,lpes_idx)){
               return true; 
            }
        }
        ++lpes_idx;
    }
    return false;
}

/**
 * LevelThreeStrategy::Remove
 */
bool LevelRangeStrategy::Remove(LevelManager* level_mgr){
    assert(level_mgr);
    return false;
}

bool LevelRangeStrategy::Insert(NodeCollector* node_collector){
    assert(node_collector);
    assert(node_collector->node_equivlences_);

    bool child_exist = false;
    HistoryQueryIndexNode* child_node = nullptr;
    
    auto top_eqs = node_collector->node_equivlences_;

    /*top_eqs has no pes,we just insert a empty SET into child_map*/
    if(!top_eqs->Size() || (top_eqs->GetLpesList().size() == 0)){
        //std::cout<<"siuuuuu"<<std::endl;
        SET empty_id_vecs;
        SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
        bool found = child_map_.find(acc,empty_id_vecs);
        if(found){
            auto child = acc->second;
            return child->Insert(node_collector);
        }else{
            size_t next_level = FindNextInsertLevel(node_collector,3);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_); 
            if(!new_idx_node->Insert(node_collector)){
                return false;
            }
            child_map_.insert({empty_id_vecs,new_idx_node});
        }
        return true;
    }

    for(const auto& lpes : *top_eqs){
        range_inverted_idx_->Insert(lpes);
        auto id_set = range_inverted_idx_->GetLpesIds(lpes);
        
        SET id_vecs;
        for(const auto& id : id_set){
            id_vecs.push_back(SMString(std::to_string(id)));
        }

        if(inverted_idx_->Serach(id_vecs)){
            /**
             * if id_vecs exists in inverted_idx, id_vecs must in
             * child_map
             */
            if(child_exist){
                if(!child_node->Insert(node_collector)){
                    return false;
                }
            }else{
                SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
                bool found = child_map_.find(acc,id_vecs);
                assert(found);
                auto child = acc->second;
                if(!child->Insert(node_collector)){
                    return false;
                }
            }
        }else{
            inverted_idx_->Insert(id_vecs);
            if(child_exist){
                child_map_.insert({id_vecs,child_node});
                continue;
            }
            size_t next_level = FindNextInsertLevel(node_collector,3);
            HistoryQueryIndexNode* new_idx_node = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
            if(!new_idx_node){
                elog(ERROR, "ShmemAlloc failed: not enough shared memory");
                exit(-1);
            }
            new (new_idx_node) HistoryQueryIndexNode(next_level,total_height_); 
            if(!new_idx_node->Insert(node_collector)){
                return false;
            }
            child_map_.insert({id_vecs,new_idx_node});
            child_exist = true;
            child_node = new_idx_node;         
        }
    }
    return true;
}

bool LevelRangeStrategy::Remove(NodeCollector* node_collector){
    assert(node_collector);
    return false;
}

bool LevelRangeStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);

    auto top_eqs = node_collector->node_equivlences_;

    /*if top_eqs empty, it means it can match all set in current node*/
    if(!top_eqs->Size() || (top_eqs->GetLpesList().size() == 0)){
        for(const auto& iter : child_map_){
            if(iter.second->Serach(node_collector)){
                return true;
            }
        }
        return false;
    }

    size_t lpes_idx = 0;
    for(const auto& lpes : *top_eqs){
        auto set = range_inverted_idx_->SuperSets(lpes);
        SET id_vec;
        for(const auto& id : set){
            id_vec.push_back(SMString(std::to_string(id)));
        }
        auto sub_set = inverted_idx_->SubSets(id_vec);
        for(const auto& set : sub_set){
            SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
            bool found = child_map_.find(acc,set);
            assert(found);
            auto child = acc->second;
            node_collector->pe_idx = lpes_idx;
            if(child->Serach(node_collector)){
               return true; 
            }
        }
        ++lpes_idx;
    }
    return false;
}

bool LevelResidualStrategy::Insert(LevelManager* level_mgr){
    /*here we should check residual pes as text mode, and check subquery in pe*/
    return true;
}

bool LevelResidualStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);
    return true;
}

bool LevelResidualStrategy::Remove(LevelManager* level_mgr){
    assert(level_mgr);
    return true;
}

bool LevelResidualStrategy::Insert(NodeCollector* node_collector){
    return true;
}
bool LevelResidualStrategy::Remove(NodeCollector* node_collector){
    return true;
}
bool LevelResidualStrategy::Search(NodeCollector* node_collector){
    return true;
}

bool LeafStrategy::Insert(LevelManager* level_mgr){
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    if(level_mgr_){
        historys_.insert(historys_.begin(),level_mgr_);
    }

    if(!shared_index_){
        bool found = true;
        shared_index_ = (HistoryQueryLevelTree*)ShmemInitStruct(shared_index_name, sizeof(HistoryQueryLevelTree), &found);
        if(!found || !shared_index_){
            std::cerr<<"shared_index not exist!"<<std::endl;
            exit(-1);
        }
    }

    auto new_level_mgr = (SMLevelManager*)ShmemAlloc(sizeof(SMLevelManager));
    assert(new_level_mgr);
    /*new sm level manager need shmemalloc*/
    new (new_level_mgr) SMLevelManager();
    new_level_mgr->Copy(level_mgr);
    level_mgr_ = new_level_mgr;
    effective_ = true;
    return true;
}

bool LeafStrategy::Serach(LevelManager* level_mgr,int id){
    auto total_lpes_list = level_mgr->GetTotalEquivlences();
    
    std::shared_lock<std::shared_mutex>lock(rw_mutex_);
    if(!effective_){
        return false;
    }

    if(!shared_index_->CheckEffective(level_mgr_->GetLid())){
        SetEffective(false);
        return false;
    }

    /*check lpes from top to down*/
    int h = total_lpes_list.size() -1;
    int lpe_id = id;
    {
        while(h >= 1){
            auto child_lpes_list = total_lpes_list[h]->GetChildLpesMap()[lpe_id];
            if(child_lpes_list.empty()){
                /*cur heighr no predicates*/
                if( SerachAgg(level_mgr,h-1,0)&& SerachSort(level_mgr,h-1,0)&& SerachResidual(level_mgr,h-1,0)){
                    h--;
                    continue;
                }
            }
            size_t i = 0;
            for(;i<child_lpes_list.size();++i){
                auto child_id = child_lpes_list[i];
                std::vector<size_t> match_idxs;
                auto ret = SerachRange(level_mgr,h-1,child_id,match_idxs)
                    && SerachAgg(level_mgr,h-1,child_id)
                    && SerachSort(level_mgr,h-1,child_id)
                    && SerachResidual(level_mgr,h-1,child_id);
                if(ret){
                    h--;
                    break;
                }
            }
            if(i == child_lpes_list.size()){
                return false;
            }
        }
        level_mgr->SetSourceQuery(level_mgr_->GetQueryStr().c_str());
        level_mgr->SetHspsPackage(level_mgr_->GetHspsPackage());
        level_mgr->SetHspsPackSize(level_mgr_->GetHspsPackSize());
        return true;
    }
}


bool LeafStrategy::SearchInternal(LevelManager* src_mgr,int h,int id,int dst_id){
    if(h<0){
        return true;
    }
    auto dst_lpes_list = level_mgr_->GetTotalEquivlences()[h];
    SMLevelPredEquivlences *match_dst_lpes = dst_lpes_list->GetLpesList()[dst_id];

    auto child_lpes_list = src_mgr->GetTotalEquivlences()[h]->GetChildLpesMap()[id];
    
    if(SearchRange(src_mgr,h,id,match_dst_lpes)
        &&SearchAgg(src_mgr,h,id,match_dst_lpes)
        && SearchSort(src_mgr,h,id,match_dst_lpes)
        && SerachResidual(src_mgr,h,id,match_dst_lpes)){
        for(const auto cid : child_lpes_list){
            for(const auto dst_cid : dst_lpes_list->GetChildLpesMap()[dst_id]){
                return SearchInternal(src_mgr,h-1,cid,dst_cid);
            }
        }
    }else{
        return false;
    }
}

bool LeafStrategy::Remove(LevelManager* level_mgr){
    return true;
}

bool LeafStrategy::Insert(NodeCollector* node_collector){
    assert(node_collector);
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    std::string input_str;

    if(!shared_index_){
        bool found = true;
        shared_index_ = (HistoryQueryLevelTree*)ShmemInitStruct(shared_index_name, sizeof(HistoryQueryLevelTree), &found);
        if(!found || !shared_index_){
            std::cerr<<"shared_index not exist!"<<std::endl;
            exit(-1);
        }
    }

    std::pair<int,int> input_pair = {0,0};
    if(!node_collector->inputs.size()){
    }else if(node_collector->inputs.size() == 1){
        input_pair = {node_collector->inputs[0],0}; 
    }else if(node_collector->inputs.size() == 2){
        input_pair = {node_collector->inputs[0],node_collector->inputs[1]};
    }else{
        elog(ERROR, "LeafStrategy::Insert: input size is not 0,1 or 2");
        return false;
    }
    
    NodeInfo* new_node_info = (NodeInfo*)ShmemAlloc(sizeof(NodeInfo));
    assert(new_node_info);
    new (new_node_info) NodeInfo(node_collector->output,
        node_collector->time,node_collector->hsps_pack_size,
        node_collector->hsps_pack,node_collector->lid_);
    history_map_[input_pair] = new_node_info;

    inputs_.resize(0);
    for(const auto& in : node_collector->inputs){
        inputs_.push_back(in);
        input_str += std::to_string(in)+",";
    }
    
    output_ = node_collector->output;
    time_ = node_collector->time;
    
    insert_cnt_.fetch_add(1);
    return true;
}

bool LeafStrategy::Remove(NodeCollector* node_collector){
    return true;
}

bool LeafStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);
    std::shared_lock<std::shared_mutex>lock(rw_mutex_);
    
    search_cnt_.fetch_add(1);
    
    bool found = false;
    node_collector->output = 0;
    for(const auto& his : history_map_){
        if(!node_collector->inputs.size()){
            /*nothing todo*/
        }else if(node_collector->inputs.size() == 1){
            if(node_collector->inputs[0] < his.first.first){
                continue;
            }
        }else if(node_collector->inputs.size() == 2){
            if(node_collector->inputs[0] < his.first.first || node_collector->inputs[1] < his.first.second){
                continue;
            }
        }
        
        if(!his.second->effective_){
            continue;
        }

        found = true;
        /**
         * TODO: Here priorvly match larger output instead of time
         * however, i can't proof this is a good idea,it just seems
         * like not bad while running.
         **/
        if(node_collector->output < his.second->output_){
            node_collector->output = his.second->output_;
            node_collector->time = his.second->time_;
            node_collector->hsps_pack = his.second->pack_ptr_;
            node_collector->hsps_pack_size = his.second->pack_size_;
        }
    }

    if(!found){
        return false;
    }

    if(node_collector->set_effective_){
        /**effective is set for scan node*/
        assert(inputs_.empty());
        assert(history_map_.size() == 1);
        history_map_.begin()->second->effective_ = false;
        shared_index_->SetEffective(node_collector->lid_,false);
        /*here we should retrun false, then it will keeping matching all the index*/
        return false;
    }

    if(node_collector->check_scan_view_decrease_){
        assert(node_collector->inputs.empty());
        assert(!node_collector->set_effective_);
        /**
         * scan has no inputs, just has time and output. 
         * example:
         * history scan: [SeqScan(t1),t1.a < 120] [time: 0.1s] [output: 1000]
         * comming scan: [SeqScan(t1),t1.a < 140] [time: 0.08s] [output: 800]
         * it means this history scan is not effective
         */
        for(const auto& his : history_map_){
           /*0.9 is a magic param,just fot testing...*/
           if(node_collector->output <= his.second->output_ * 0.9){
                node_collector->scan_view_decrease_ = true;
                return true;
            }
        }
        node_collector->scan_view_decrease_ = false;
        return true;
    }

    match_cnt_.fetch_add(1);
    node_collector->match_cnt++;
    if(node_collector->match_cnt < node_search_topk){
        node_collector->output_list_.push_back(node_collector->output);
        node_collector->time_list_.push_back(node_collector->time);
        node_collector->hsps_pack_list_.push_back(node_collector->hsps_pack);
        node_collector->hsps_pack_size_list_.push_back(node_collector->hsps_pack_size);
        return false;
    }
    return true;
}

void LeafStrategy::SetEffective(bool effective){
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    effective_ = effective;
}

/**
 * LevelAggStrategy::Serach: check src_mgr can match dst_mgr 
 */
bool LeafStrategy::SerachAgg(LevelManager* level_mgr,int h,int id){
    auto src_aggs = level_mgr->GetTotalAggs()[h];
    if(!src_aggs->Size()){
        if(!level_mgr_->GetTotalAggs()[h]->Size()){
            return true;
        }
        return false;
    }

    int sort_idx = -1;
    for(size_t i = 0;i< src_aggs->Size();++i){
        if(src_aggs->GetLevelAggList()[i]->GetLpeId() == id){
            sort_idx = i;
            break;
        }
    }
    if(sort_idx == -1){
        return false;
    }
    auto src_ls_eqs = src_aggs->GetLevelAggList()[sort_idx]->GetLevelAggSets();
    std::unordered_set<AggAndSortEquivlence *>states;

    for(const auto ls_pes : level_mgr_->GetTotalAggs()[h]->GetLevelAggList()){
        for(const auto& pe: ls_pes->GetLevelAggSets()){
            auto extends = pe->GetExtends();
            for(const auto& src_eq: src_ls_eqs){
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
        if(states.size() == src_ls_eqs.size()){
            return true;
        }
    }
    return false;
}

bool LeafStrategy::SerachSort(LevelManager* level_mgr,int h,int id){
    auto src_sorts = level_mgr->GetTotalSorts()[h];
    if(!src_sorts->Size()){
        if(!level_mgr_->GetTotalSorts()[h]->Size()){
             return true;
        }
        return false;
    }

    int sort_idx = -1;
    for(size_t i = 0;i< src_sorts->Size();++i){
        if(src_sorts->GetLevelAggList()[i]->GetLpeId() == id){
            sort_idx = i;
            break;
        }
    }
    if(sort_idx == -1){
        return false;
    }
    auto src_ls_eqs = src_sorts->GetLevelAggList()[sort_idx]->GetLevelAggSets();
    std::unordered_set<AggAndSortEquivlence *>states;

    for(const auto ls_pes : level_mgr_->GetTotalSorts()[h]->GetLevelAggList()){
        for(const auto& pe: ls_pes->GetLevelAggSets()){
            auto extends = pe->GetExtends();
            for(const auto& src_eq: src_ls_eqs){
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
        if(states.size() == src_ls_eqs.size()){
            return true;
        }
    }
    return false;
}

bool LeafStrategy::SerachRange(LevelManager* src_mgr,int h,int id, std::vector<size_t> &match_lpes_idxs){
    assert(src_mgr);
    auto src_lpes = src_mgr->GetTotalEquivlences()[h]->GetLpesList()[id];
    auto dst_lpes_list = level_mgr_->GetTotalEquivlences()[h];
    size_t dst_idx = 0;
    for(const auto& dst_lpes : *dst_lpes_list){
        if(Match(src_lpes,dst_lpes)){
            match_lpes_idxs.push_back(dst_idx);
        }
        dst_idx++;
    }
    return match_lpes_idxs.size() > 0;
}

bool LeafStrategy::SearchRange(LevelManager* src_mgr,int h,int id, SMLevelPredEquivlences *dst_lpes){
    auto src_lpes = src_mgr->GetTotalEquivlences()[h]->GetLpesList()[id];
    if(Match(src_lpes,dst_lpes)){
        return true;
    }
    return false;
}

bool LeafStrategy::Match(LevelPredEquivlences* dst_lpes, SMLevelPredEquivlences* lpes){
    assert(lpes);
    for(const auto& pe : *lpes){
        for(const auto& attr : pe->GetPredSet()){
            auto key2pe = dst_lpes->GetKey2Pe();
            if(key2pe.find(std::string(attr)) != key2pe.end()){
                auto dst_pe = key2pe[std::string(attr)];
                if(SuperSet(pe,dst_pe)){
                    continue;
                }else{
                    return false;
                }
            }
        }
    }
    return true;
}

/**
 * PredEquivlence::SuperSet: check if this input pe is a superset of the current pe
 */
bool LeafStrategy::SuperSet(SMPredEquivlence* dst_pe,PredEquivlence* pe){
	assert(pe);
	
	/*if one pe has more then one range,it must be return 0 tuple*/
	if(dst_pe->RangeCnt() >= 2 && !dst_pe->HasSubquery()){
		return true;
	}

	/*here we just compare subquery name,such as SUBQUERY1, SUBQUERY2, we will compare */
	auto ranges = pe->GetRanges();
	std::vector<std::string> subquery_names;
	for(const auto r :dst_pe->GetRanges()){
		bool match = false;
		if(r->PredType() == PType::SUBQUERY || r->PredType() == PType::SUBLINK){
			assert(!r->GetSubqueryName().empty());
			subquery_names.push_back(std::string(r->GetSubqueryName()));
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
						auto ret = PredEquivlenceRange::LimitCompare(std::string(r->LowerLimit()),r->PredVarType(),src_r->LowerLimit(),src_r->PredVarType());
						if(ret<0){
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
						auto ret = PredEquivlenceRange::LimitCompare(std::string(r->UpperLimit()),r->PredVarType(),src_r->UpperLimit(),src_r->PredVarType());
						if(ret>0){
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

bool LeafStrategy::SerachResidual(LevelManager* src_mgr,int h,int id){
    return true;
}

bool LeafStrategy::SearchAgg(LevelManager* src_mgr,int h,int id,SMLevelPredEquivlences *dst_lpes){
    auto src_aggs = src_mgr->GetTotalAggs()[h];
    if(!src_aggs->Size()){
        return true;
    }

    int agg_idx = -1;
    for(size_t i = 0;i< src_aggs->Size();++i){
        if(src_aggs->GetLevelAggList()[i]->GetLpeId() == id){
            agg_idx = i;
            break;
        }
    }
    assert(agg_idx != -1);
    auto src_ls_eqs = src_aggs->GetLevelAggList()[agg_idx]->GetLevelAggSets();
    std::unordered_set<AggAndSortEquivlence *>states;

    auto ls_pes = level_mgr_->GetTotalAggs()[h]->GetLevelAggList()[dst_lpes->LpeId()];
    for(const auto& pe: ls_pes->GetLevelAggSets()){
            auto extends = pe->GetExtends();
            for(const auto& src_eq: src_ls_eqs){
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
    if(states.size() == src_ls_eqs.size()){
            return true;
    }
    return false;
}

bool LeafStrategy::SearchSort(LevelManager* src_mgr,int h,int id,SMLevelPredEquivlences *dst_lpes){
    auto src_sorts = src_mgr->GetTotalSorts()[h];
    if(!src_sorts->Size()){
        return true;
    }

    int sort_idx = -1;
    for(size_t i = 0;i< src_sorts->Size();++i){
        if(src_sorts->GetLevelAggList()[i]->GetLpeId() == id){
            sort_idx = i;
            break;
        }
    }
    assert(sort_idx != -1);
    auto src_ls_eqs = src_sorts->GetLevelAggList()[sort_idx]->GetLevelAggSets();
    std::unordered_set<AggAndSortEquivlence *>states;

    auto ls_pes = level_mgr_->GetTotalSorts()[h]->GetLevelAggList()[dst_lpes->LpeId()];
    for(const auto& pe: ls_pes->GetLevelAggSets()){
        auto extends = pe->GetExtends();
        for(const auto& src_eq: src_ls_eqs){
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
    if(states.size() == src_ls_eqs.size()){
        return true;
    }
    return false;
}

bool LeafStrategy::SerachResidual(LevelManager* src_mgr,int h,int id,SMLevelPredEquivlences *dst_lpes){
    return true;
}

size_t LevelStrategy::FindNextInsertLevel(LevelManager* level_mgr, size_t cur_level){
    assert(cur_level>=1);
    for(size_t h = cur_level+1; h <= total_height_; ){
        if(h == 2){
            if(level_mgr->GetJoinTypeList().size()){
                return h;
            }
        }else if(h == 3){
            // if(level_mgr->GetTotalEquivlences().back()->Size()){
            //     return h;
            // }
            return h;
        }else if(h == 4){
            if(level_mgr->GetTotalSorts().back()->Size()){
                return h;
            }
        }else if(h == 5){
            if(level_mgr->GetTotalAggs().back()->Size()){
                return h;
            }
        }else if(h == 6){
        }else if(h == 7){
            return total_height_;
        }else{
            std::cerr<<"error height"<<std::endl;
            exit(-1);
        }
        ++h;
    }
    return -1;
}

size_t LevelStrategy::FindNextInsertLevel(NodeCollector* node_collector, size_t cur_level){
    assert(cur_level>=1);
    for(size_t h = cur_level+1; h <= total_height_; ){
        if(h == 2){
            if(node_collector->join_type_list.size()){
                return h;
            }
        }else if(h == 3){
            return h;
        }else if(h == 4){
            if(node_collector->type_name == "Sort" && node_collector->node_sorts_ && node_collector->node_sorts_->Size()){
                return h;
            }
        }else if(h == 5){
            if(node_collector->type_name == "Aggregate" && node_collector->node_aggs_ && node_collector->node_aggs_->Size()){
                return h;
            }
        }else if(h == 6){
        }else if(h == 7){
            return h;
        }else{
            std::cerr<<"error height"<<std::endl;
            exit(-1);
        }
        ++h;
    }
    return -1;
}

bool LevelStrategyContext::Insert(LevelManager* level_mgr){
    return strategy_->Insert(level_mgr);
}
bool LevelStrategyContext::Remove(LevelManager* level_mgr){
    return strategy_->Remove(level_mgr);
}
bool LevelStrategyContext::Search(LevelManager* level_mgr,int id){
    return strategy_->Serach(level_mgr,id);
}
bool LevelStrategyContext::Insert(NodeCollector* node_collector){
    return strategy_->Insert(node_collector);
}
bool LevelStrategyContext::Remove(NodeCollector* node_collector){
    return strategy_->Remove(node_collector);
}
bool LevelStrategyContext::Search(NodeCollector* node_collector){
    return strategy_->Search(node_collector);
}