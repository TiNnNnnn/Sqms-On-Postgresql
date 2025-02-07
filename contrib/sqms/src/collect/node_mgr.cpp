#include "collect/node_mgr.hpp"

bool NodeManager::Format(){
    ComputeTotalNodes(hsps_,level_mgr_->GetNodeCollector());
    return true;
}

bool NodeManager::PrintPredEquivlences(){
    return true;
}

void NodeManager::ComputeTotalNodes(HistorySlowPlanStat* hsps,std::unordered_map<HistorySlowPlanStat*, NodeCollector*> nodes_collector_map){
    assert(hsps);
    auto node_collector = nodes_collector_map[hsps];

    node_collector->json_sub_plan = hsps->canonical_json_plan;
    if(NodeTag(hsps->node_tag) == T_NestLoop 
		|| NodeTag(hsps->node_tag) == T_MergeJoin 
		|| NodeTag(hsps->node_tag) == T_HashJoin){
		assert(strlen(hsps->join_type));
		node_collector->join_type_list.push_back(hsps->join_type);
	}

    node_collector_list_.push_back(node_collector);

    for(size_t i = 0; i< hsps->n_childs; ++i){
        ComputeTotalNodes(hsps->childs[i],nodes_collector_map);
    }
}

void NodeManager::PlanPartition(HistorySlowPlanStat* hsps){
    assert(hsps);
    
}

// bool Insert(HistoryQueryLevelTree *shared_index){
//     return true;
// }

// bool Search(HistoryQueryLevelTree *shared_index){
//     return true;
// }