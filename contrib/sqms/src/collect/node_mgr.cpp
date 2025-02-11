#include "collect/node_mgr.hpp"
#include <stack>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>

NodeManager::NodeManager(HistorySlowPlanStat* hsps,std::shared_ptr<LevelManager> level_mgr)
: hsps_(hsps),level_mgr_(level_mgr),pool_(std::make_shared<ThreadPool>(pool_size_,true)){
    bool found = false;
    logger_ = (SqmsLogger*)ShmemInitStruct("SqmsLogger", sizeof(SqmsLogger), &found);
    assert(logger_ && found);

    found = false;
    shared_index_ = (HistoryQueryLevelTree*)ShmemInitStruct(shared_index_name, sizeof(HistoryQueryLevelTree), &found);
    assert(shared_index_ && found);
}

bool NodeManager::Format(){
    ComputeTotalNodes(hsps_,level_mgr_->GetNodeCollector());
    return true;
}

bool NodeManager::PrintPredEquivlences(){
    return true;
}

bool NodeManager::Search(){
    double total_time(0);
    std::mutex time_mtx;

    logger_->Logger("comming",("stitch thread num: "+std::to_string(partition_list.size())).c_str());

    std::vector<bool>finish_list(partition_list.size(),0);
    std::vector<std::condition_variable> cv_list(partition_list.size());
    std::vector<std::mutex> mtx_list(partition_list.size());
    auto node_collector_map = level_mgr_->GetNodeCollector();

    size_t part_idx;
    for(const auto& part : partition_list){
        pool_->submit([&](){
            for(size_t i = 0;i < part.size(); ++i){
                /*check if need wait*/
                auto node_collector = part[i];
                bool has_right = false;
                auto pos = dependencies_.find(node_collector);
                if(pos != dependencies_.end()){
                    has_right = true;
                    {
                        std::unique_lock<std::mutex> lock(mtx_list[pos->second]);
                        cv_list[pos->second].wait(lock, [&] {
                            if (!finish_list[pos->second]) 
                                return false;
                            return true;
                        });
                    }
                }
                
                assert(!node_collector->inputs.size());
                if(i){
                    auto childs = node_collector->childs_;
                    if(childs.size() == 1){
                        assert(!has_right);
                        node_collector->inputs.push_back(childs[0]->output);
                    }else if (childs.size() == 2){
                        assert(has_right);
                        node_collector->inputs.push_back(childs[0]->output);
                        node_collector->inputs.push_back(childs[1]->output);
                    }else{
                        logger_->Logger("comming",("error node_collector's child num: " + std::to_string(childs.size())).c_str());
                    }
                }

                /*search*/
                if(shared_index_->Search(node_collector)){
                    {
                        std::lock_guard<std::mutex> lock(mtx_list[part_idx]);
                        assert(node_collector->output);
                        finish_list[part_idx] = true;
                        cv_list[part_idx].notify_all();
                    }
                    {
                        std::lock_guard<std::mutex> lock(time_mtx);
                        total_time += node_collector->time;
                        if(total_time >= query_min_duration){
                            CancelQuery();
                        }
                    }
                }else{
                    return false;
                }
            }
        });
        ++part_idx;
        return true;
    }
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
        node_collector->output = hsps->actual_rows;
	}

    node_collector_list_.push_back(node_collector);

    for(size_t i = 0; i< hsps->n_childs; ++i){
        auto child_node_collector = nodes_collector_map[hsps->childs[i]];
        child_node_collector->parent_ = node_collector;
        child_node_collector->inputs.push_back(hsps->childs[i]->actual_rows);
        node_collector->childs_.push_back(child_node_collector);
        ComputeTotalNodes(hsps->childs[i],nodes_collector_map);
    }
}

void NodeManager::PlanPartition(HistorySlowPlanStat* hsps){
    assert(hsps);
    auto node_collector_map = level_mgr_->GetNodeCollector();
    std::stack<HistorySlowPlanStat*>st;
    auto cur = hsps;
    while(cur || !st.empty()){
        std::vector<NodeCollector*>partition;
        while(cur){
            partition.push_back(node_collector_map[hsps]);
            st.push(cur);
            if(hsps->n_childs >= 1){
                cur = hsps->childs[0];
            }else{
                cur = nullptr;
            }
        }
        std::reverse(partition.begin(),partition.end());
        partition_list.push_back(partition);

        auto top = st.top();
        st.pop();

        if(hsps->n_childs > 1){
            assert(hsps->n_childs == 2);
            cur = top->childs[1];
            /*mark dependence*/
            dependencies_[node_collector_map[hsps]] = partition_list.size();
        }else{
            cur = nullptr;
        }
    }
}

bool NodeManager::CancelQuery(){
    if(QueryCancelPending){
        return false;
    }
    QueryCancelPending = true;
    InterruptPending = true;
    ProcessInterrupts();
    elog(WARNING, "Query is canceled by sqms.");
    logger_->Logger("comming","Query is canceled by sqms.");
    return true;
}