#include "discovery/query_index_node.hpp"
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

    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*>::const_accessor acc;
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
    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*>::const_accessor acc;
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

    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*>::const_accessor acc;
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
        /*update cur level index*/
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
    SMConcurrentHashMap<SMString,HistoryQueryIndexNode*>::const_accessor acc;
    if(set_map_.find(acc ,json_sub_plan)){
        auto child = acc->second;
        return child->Serach(node_collector);
    }else{
        return false;
    }
    
    return false;
}

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
        }
    } 
    return true;
}
bool LevelScalingStrategy::Remove(NodeCollector* node_collector){
    return true;
}
bool LevelScalingStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);
    assert(node_collector->join_type_list.size());

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
        }
        return false;
    }
}

/**
 * LevelTwoStrategy::Insert
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
            //auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
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
            assert(next_level == total_height_);

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
    assert(node_collector->node_aggs_);
    auto top_aggs = node_collector->node_aggs_;
    
    LevelAggAndSortEquivlences * la_eq = nullptr;
    if(node_collector->pe_idx == -1){
        assert(top_aggs->Size() == 1);
        la_eq = top_aggs->GetLevelAggList()[0];
    }else{
        assert(top_aggs->Size() > 1);
        la_eq = top_aggs->GetLevelAggList()[node_collector->pe_idx];
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
            //auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
            
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
            //auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
            
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
    assert(node_collector->node_sorts_);
    auto top_sorts = node_collector->node_sorts_;

    LevelAggAndSortEquivlences * la_eq = nullptr;
    if(node_collector->pe_idx == -1){
        assert(top_sorts->Size() == 1);
        la_eq = top_sorts->GetLevelAggList()[0];
    }else{
        assert(top_sorts->Size() > 1);
        la_eq = top_sorts->GetLevelAggList()[node_collector->pe_idx];
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
            assert(child_exist && child_node);
            SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
            bool found = child_map_.find(acc,id_vecs);
            assert(found);
            auto child = acc->second;
            if(!child->Insert(level_mgr)){
                return false;
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
            assert(child_exist && child_node);
            SMConcurrentHashMap<SET,HistoryQueryIndexNode*,SetHasher>::const_accessor acc;
            bool found = child_map_.find(acc,id_vecs);
            assert(found);
            auto child = acc->second;
            if(!child->Insert(node_collector)){
                return false;
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
    if(level_mgr_){
        historys_.insert(historys_.begin(),level_mgr_);
    }else{
        auto new_level_mgr = (SMLevelManager*)ShmemAlloc(sizeof(SMLevelManager));
        assert(new_level_mgr);
        new (new_level_mgr) SMLevelManager();
        new_level_mgr->Copy(level_mgr);
        level_mgr_ = new_level_mgr;
    }
    return true;
}

bool LeafStrategy::Serach(LevelManager* level_mgr,int id){
    auto total_lpes_list = level_mgr->GetTotalEquivlences();
    /*check lpes from top to down*/
    int h = total_lpes_list.size() -1;
    int lpe_id = id;

    /*lpe_id = -1, it indicates no predicates,just stop sarech*/
    if(lpe_id == -1){
        assert(!total_lpes_list[h]->GetLpesList().size());
        return true;
    }
    
    while(h >= 0){
        auto lpes = total_lpes_list[h]->GetLpesList()[lpe_id];
        if(lpes->EarlyStop()){
            return true;
        }
        auto child_lpes_list = total_lpes_list[h]->GetChildLpesMap()[lpe_id];
        for(const auto& child_id : child_lpes_list){
            auto ret = SerachRange(level_mgr,h-1,child_id) 
                && SerachSort(level_mgr,h-1,child_id) 
                && SerachAgg(level_mgr,h-1,child_id) 
                && SerachResidual(level_mgr,h-1,child_id);
            if(!ret){
                return false;
            }
        }
        --h;
    }
    return false;
}

bool LeafStrategy::Remove(LevelManager* level_mgr){
    return true;
}

bool LeafStrategy::Insert(NodeCollector* node_collector){
    assert(node_collector);
    for(const auto& in : node_collector->inputs){
        inputs_.push_back(in);
    }
    output_ = node_collector->output;
    return true;
}

bool LeafStrategy::Remove(NodeCollector* node_collector){
    return true;
}

bool LeafStrategy::Search(NodeCollector* node_collector){
    assert(node_collector);
    for(const auto& in : inputs_){
        node_collector->inputs.push_back(in);
    }
    node_collector->output = output_;
    return true;
}

/**
 * LevelAggStrategy::Serach: check src_mgr can match dst_mgr 
 */
bool LeafStrategy::SerachAgg(LevelManager* level_mgr,int h,int id){
    auto src_aggs = level_mgr->GetTotalAggs()[h];
    int agg_idx = -1;
    for(size_t i = 0;i< src_aggs->Size();++i){
        if(src_aggs->GetLevelAggList()[i]->GetLpeId() == id){
            agg_idx = i;
            break;
        }
    }
    assert(agg_idx != -1);
    auto src_la_eqs = src_aggs->GetLevelAggList()[agg_idx];
    assert(src_la_eqs->Size() == 1);
    
    auto src_keys = src_la_eqs->GetLevelAggSets()[0]->GetExtends();
    for(const auto& la_eqs : level_mgr_->GetTotalAggs()[h]->GetLevelAggList()){
        assert(la_eqs->Size() == 1);
        auto keys = la_eqs->GetLevelAggSets()[0]->GetExtends();
        bool matched = true;
        for(const auto& src_key :src_keys){
            if(keys.find(SMString(src_key)) == keys.end()){
                matched = false;
            }
        }
        if(matched){
            return true;
        }
    }
    return false;
}

bool LeafStrategy::SerachSort(LevelManager* level_mgr,int h,int id){
    auto src_sorts = level_mgr->GetTotalAggs()[h];
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

bool LeafStrategy::SerachRange(LevelManager* src_mgr,int h,int id){
    assert(src_mgr);
    auto src_lpes = src_mgr->GetTotalEquivlences()[h]->GetLpesList()[id];
    auto dst_lpes_list = level_mgr_->GetTotalEquivlences()[h];
    for(const auto& dst_lpes : *dst_lpes_list){
        if(Match(src_lpes,dst_lpes)){
            return true;
        }
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
                if(SuperSet(dst_pe,pe)){
                    continue;
                }else{
                    return false;
                }
            }
        }
    }
    return true;
}

bool LeafStrategy::SuperSet(PredEquivlence* dst_pe, SMPredEquivlence* pe){
    assert(pe);
    auto range = pe->GetRanges();
    for(const auto r : dst_pe->GetRanges()){
        bool match = false;
        for(const auto src_r : range){
            bool super = true;
            /* check lowlimit */
            if(r->LowerLimit() == LOWER_LIMIT){
                if(src_r->LowerLimit() != LOWER_LIMIT){
                    super = false;
                    continue;
                }
            }else{
                if(src_r->LowerLimit() != LOWER_LIMIT){
                    if(r->LowerLimit() < std::string(src_r->LowerLimit())){
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
                    if(r->UpperLimit() > std::string(src_r->UpperLimit())){
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
        if(!match){
            return false;
        }
    }
    return true;
}

bool LeafStrategy::SerachResidual(LevelManager* src_mgr,int h,int id){
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
            if(level_mgr->GetTotalEquivlences().back()->Size()){
                return h;
            }
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
            return h;
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
            if(node_collector->node_equivlences_){
                return h;
            }
        }else if(h == 4){
            if(node_collector->node_sorts_){
                return h;
            }
        }else if(h == 5){
            if(node_collector->node_aggs_){
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