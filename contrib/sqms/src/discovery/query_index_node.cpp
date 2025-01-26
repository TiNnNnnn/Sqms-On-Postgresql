#include "discovery/query_index_node.hpp"
#include "common/util.hpp"
#include <unordered_set>
#include <algorithm>
#include <memory>

std::vector<std::string> LevelHashStrategy::findChildren(){
    return std::vector<std::string>();
}

/**
 * LevelOneStrategy::Insert
 */
bool LevelHashStrategy::Insert(LevelManager* level_mgr){
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
bool LevelHashStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);
    auto json_sub_plan = std::string(level_mgr->GetHsps()->canonical_json_plan);

    tbb::concurrent_hash_map<std::string, HistoryQueryIndexNode*>::const_accessor acc;
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

int ScalingInfo::CalJoinTypeScore(const std::vector<std::string>& join_type_list,std::string& unique_id){
    int score = 0;
    std::string id;
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
    auto new_scaling_info = std::make_shared<ScalingInfo>(level_mgr->GetJoinTypeList());
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
            HistoryQueryIndexNode* new_idx_node = new HistoryQueryIndexNode(next_level,total_height_);
            if(!new_idx_node->Insert(level_mgr)){
                return false;
            }
            child_map_.insert({new_scaling_info->UniqueId(),{new_scaling_info,new_idx_node}});
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

std::vector<std::string> LevelScalingStrategy::findChildren(){
    return std::vector<std::string>();
}

/**
 * LevelTwoStrategy::Insert
 */
bool LevelAggStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);

    assert(level_mgr->GetTotalAggs().size());
    auto top_aggs = level_mgr->GetTotalAggs().back();
    
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
        size_t next_level = FindNextInsertLevel(level_mgr,5);
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
 * LevelAggStrategy::Serach
 */
bool LevelAggStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);
    assert(level_mgr->GetTotalAggs().size());
    /*we current only build index for top level aggs*/
    auto top_aggs = level_mgr->GetTotalAggs().back();

    int top_idx = -1;
    for(size_t i = 0;i< top_aggs->Size();++i){
        if(top_aggs->GetLevelAggList()[i]->GetLpeId() == id){
            top_idx = i;
            break;
        }
    }
    assert(top_idx != -1);
    auto la_eq = top_aggs->GetLevelAggList()[top_idx];

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
            if(acc->second->Search(level_mgr,id)){
                return true;
            }
        }
    }
    match_maps.clear();
    return false;
}


/**
 * LevelTwoStrategy::Remove
 */
bool LevelAggStrategy::Remove(LevelManager* level_mgr){
    assert(level_mgr);
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
        std::vector<int> sort_vec_id_list;
        for(const auto& sort : la_eq->GetLevelAggSets()){
            auto sort_extends = sort->GetExtends();
            std::vector<std::string> sort_vec;
            for(const auto& expr : sort_extends){
                sort_vec.push_back(expr);
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
        
        tbb::concurrent_hash_map<uint32_t,std::shared_ptr<HistoryQueryIndexNode>>::const_accessor acc;
        if(child_map_.find(acc,hash_value)){
            auto child = acc->second;
            return child->Insert(level_mgr);
        }else{
            size_t next_level = FindNextInsertLevel(level_mgr,4);
            auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
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
    auto la_eq = top_sorts->GetLevelAggList()[id];
    
    std::vector<std::vector<int>> sort_vec_id_list;
    int candidate_size = 1;
    for(const auto& sort : la_eq->GetLevelAggSets()){
        auto sort_extends = sort->GetExtends();
        std::vector<std::string> sort_vec;
        for(const auto& expr : sort_extends){
            sort_vec.push_back(expr);
        }
        std::sort(sort_vec.begin(),sort_vec.end());

        std::vector<int>match_set_id_list;
        auto match_set = inverted_idx_->SuperSets(sort_vec);
        for(const auto& m_set : match_set){
            int set_id = inverted_idx_->GetSetIdBySet(m_set);
            assert(set_id != -1);
            match_set_id_list.push_back(set_id);
        }
        std::sort(match_set_id_list.begin(),match_set_id_list.end());
        sort_vec_id_list.push_back(match_set_id_list);
        candidate_size *= match_set_id_list.size();
    }
    std::vector<std::vector<int>> combination;
    std::vector<int> currentCombination(sort_vec_id_list.size());
    generateCombinations(sort_vec_id_list, currentCombination, 0, combination);

    for(const auto& c_id_list : combination){
        auto hash_value = hash_array(c_id_list);
        tbb::concurrent_hash_map<uint32_t,std::shared_ptr<HistoryQueryIndexNode>>::const_accessor acc;
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

/**
 * LevelThreeStrategy::Insert
 */
bool LevelRangeStrategy::Insert(LevelManager* level_mgr){
    assert(level_mgr);
    assert(level_mgr->GetTotalEquivlences().size());

    bool child_exist = false;
    std::shared_ptr<HistoryQueryIndexNode> child_node = nullptr;
    
    auto top_eqs = level_mgr->GetTotalEquivlences().back();
    /**we just build index on single attribute*/
    for(const auto& lpes : *top_eqs){
        std::vector<std::string>pe_vecs;
        for(const auto& pe : *lpes){
            if(pe->GetPredSet().size() == 1){
                pe_vecs.push_back(*pe->GetPredSet().begin());
            }
        }
        std::sort(pe_vecs.begin(),pe_vecs.end());
        {
            std::unique_lock<std::shared_mutex> lock(rw_mutex_);
            if(inverted_idx_->Serach(pe_vecs)){    
                auto acc = child_map_.find(pe_vecs);
                assert(acc == child_map_.end());

                auto remind_list = acc->second;
                auto iter = remind_list.find(lpes);
                if(iter != remind_list.end()){
                    child_exist = true;
                    auto child =  iter->second;
                    return child->Insert(level_mgr);
                    child_node = child;
                }else{
                    if(child_exist){
                        remind_list[lpes] = child_node;
                        continue;
                    }
                    size_t next_level = FindNextInsertLevel(level_mgr,3);
                    auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
                    if(!new_idx_node->Insert(level_mgr)){
                        return false;
                    }
                    /*update cur level index*/
                    remind_list[lpes] = new_idx_node;
                    child_exist = true;
                    child_node = new_idx_node;
                }
            }else{
                inverted_idx_->Insert(pe_vecs);
                std::unordered_map<LevelPredEquivlences *, std::shared_ptr<HistoryQueryIndexNode>> new_remind_list;
                if(child_exist){
                    new_remind_list[lpes] = child_node;
                    child_map_[pe_vecs] = new_remind_list;
                    continue;
                }

                size_t next_level = FindNextInsertLevel(level_mgr,3);
                auto new_idx_node = std::make_shared<HistoryQueryIndexNode>(next_level,total_height_);
                if(!new_idx_node->Insert(level_mgr)){
                    return false;
                }
                new_remind_list[lpes] = new_idx_node;
                child_map_[pe_vecs] = new_remind_list;
                child_exist = true;
                child_node = new_idx_node;
            }
            return true;
        }   
    }
    return false;
}

/**
 * LevelThreeStrategy::Serach
 */
bool LevelRangeStrategy::Serach(LevelManager* level_mgr,int id){
    assert(level_mgr);

    auto top_eqs = level_mgr->GetTotalEquivlences().back();
    size_t top_idx = 0;
    for(const auto& lpes : *top_eqs){
        std::vector<std::string>pe_vecs;
        LevelPredEquivlences * remind_lpes = new LevelPredEquivlences();
        remind_lpes->SetLpeId(lpes->LpeId());
        for(const auto& pe : *lpes){
            if(pe->GetPredSet().size() == 1){
                pe_vecs.push_back(*pe->GetPredSet().begin());
            }else{
                remind_lpes->Insert(pe);
            }
        }
        std::sort(pe_vecs.begin(),pe_vecs.end());
        
        auto match_list = inverted_idx_->SuperSets(pe_vecs);
        for(const auto& mpe : match_list){
            {
                std::shared_lock<std::shared_mutex> lock(rw_mutex_);
                auto iter = child_map_.find(mpe);
                if(iter != child_map_.end()){
                    auto remind_list = iter->second;
                    for(const auto& lpes_pair: remind_list){
                        /*check lpes if match */
                        auto slow_remind_lpes = lpes_pair.first;
                        if(slow_remind_lpes->Match(remind_lpes)){
                            bool ret = lpes_pair.second->Search(level_mgr,lpes->LpeId());
                            if(ret) return true;
                        }
                    }
                }
            }
        }
        ++top_idx;
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

bool LevelResidualStrategy::Insert(LevelManager* level_mgr){

    return true;
}
bool LevelResidualStrategy::Serach(LevelManager* level_mgr,int id){

    return true;
}
bool LevelResidualStrategy::Remove(LevelManager* level_mgr){

    return true;
}

bool LeafStrategy::Insert(LevelManager* level_mgr){
    if(level_mgr_)return true;
    level_mgr_ = std::shared_ptr<LevelManager>(level_mgr);

    return true;
}

bool LeafStrategy::Serach(LevelManager* level_mgr,int id){
    auto total_lpes_list = level_mgr->GetTotalEquivlences();
    /*check lpes from top to down*/
    int h = total_height_-1;
    int lpe_id = id;
    while(h >= 0){
        auto lpes = total_lpes_list[h]->GetLpesList()[lpe_id];
        if(lpes->EarlyStop()){
            return true;
        }
        auto child_lpes_list = total_lpes_list[h]->GetChildLpesMap()[lpe_id];
        int tmp_h = h;
        for(const auto& child_id : child_lpes_list){
            --tmp_h;
            auto ret = SerachRange(level_mgr,tmp_h,lpe_id) && SerachSort(level_mgr,tmp_h,lpe_id) && SerachAgg(level_mgr,tmp_h,lpe_id) && SerachResidual(level_mgr,tmp_h,lpe_id);
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

/**
 * LevelAggStrategy::Serach: check src_mgr can match dst_mgr 
 */
bool LeafStrategy::SerachAgg(LevelManager* level_mgr,int h,int id){;
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
            if(keys.find(src_key) == keys.end()){
                matched = false;
            }
        }
        if(matched){
            return true;
        }
    }
    return false;
}

bool LeafStrategy::SerachSort(LevelManager* src_mgr,int h,int id){

}

bool LeafStrategy::SerachRange(LevelManager* src_mgr,int h,int id){
    assert(src_mgr);
    auto src_lpes = src_mgr->GetTotalEquivlences()[h]->GetLpesList()[id];
    auto dst_lpes_list = level_mgr_->GetTotalEquivlences()[h];
    for(const auto& dst_lpes : *dst_lpes_list){
        if(src_lpes->Match(dst_lpes)){
            return true;
        }
    }
    return false;
}

bool LeafStrategy::SerachResidual(LevelManager* src_mgr,int h,int id){

}

std::vector<std::string> LevelAggStrategy::findChildren(){
    return std::vector<std::string>();
}
std::vector<std::string> LevelSortStrategy::findChildren(){
    return std::vector<std::string>();
}
std::vector<std::string> LevelRangeStrategy::findChildren(){
    return std::vector<std::string>();
}
std::vector<std::string> LevelResidualStrategy::findChildren(){
    return std::vector<std::string>();
}

size_t LevelStrategy::FindNextInsertLevel(LevelManager* level_mgr, size_t cur_level){
    assert(cur_level>=1);
    for(size_t h = cur_level+1; h<= total_height_; ){
        switch(h){
            case 2:{
                if(level_mgr->GetTotalAggs().size()){
                    return h;
                }
                ++h;
            }break;
            case 3:{

            }break;
            case 4:{

            }break;
            default:{

            }
        }
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