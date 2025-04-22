#include "collect/node_mgr.hpp"
#include <stack>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>

NodeManager::NodeManager(HistorySlowPlanStat* hsps,std::shared_ptr<LevelManager> level_mgr,pid_t pid)
: hsps_(hsps),level_mgr_(level_mgr),pool_(std::make_shared<ThreadPool>(10,true)),pid_(pid){
    bool found = false;
    logger_ = (SqmsLogger*)ShmemInitStruct("SqmsLogger", sizeof(SqmsLogger), &found);
    assert(logger_ && found);

    found = false;
    shared_index_ = (HistoryQueryLevelTree*)ShmemInitStruct(shared_index_name, sizeof(HistoryQueryLevelTree), &found);
    assert(shared_index_ && found);
}

bool NodeManager::Format(){
    ComputeTotalNodes(hsps_);
    return true;
}

bool NodeManager::PrintPredEquivlences(){
    return true;
}

bool NodeManager::Search(){
    ComputeTotalNodes(hsps_);
    PlanPartition(hsps_);

    int* finish_node_num = (int*)malloc(sizeof(int));
    *finish_node_num = 0;
    double total_time(0);
    auto time_mtx = std::make_shared<std::mutex>();
    
    int p_size = partition_list.size();
    logger_->Logger("comming",("stitch thread num: "+std::to_string(p_size)+",node size:"+std::to_string(node_num_)).c_str());
    auto finish_list= std::make_shared<std::vector<bool>>(p_size,false);
    auto cv_list = std::make_shared<std::vector<std::condition_variable>>(p_size);
    auto mtx_list = std::make_shared<std::vector<std::mutex>>(p_size);

    int part_idx = 0;
    for(const auto& part : partition_list){
        pool_->submit([part_idx,mtx_list,cv_list,finish_list,&part,this,time_mtx,&total_time,finish_node_num](){
            logger_->Logger("comming",("finish_node_num:"+std::to_string(*finish_node_num)).c_str());
            logger_->Logger("comming",("part_size:"+std::to_string(part.size())).c_str());
            for(size_t i = 0;i < part.size(); ++i){
                /*check if need wait*/
                auto node_collector = part[i];
                //bool has_right = false;
                auto pos = dependencies_.find(node_collector);
                if(pos != dependencies_.end()){
                    //has_right = true;
                    {
                        std::unique_lock<std::mutex> lock((*mtx_list)[pos->second]);
                        (*cv_list)[pos->second].wait(lock, [&] {
                            if (!(*finish_list)[pos->second]){
                                logger_->Logger("comming",("task["+std::to_string(part_idx) + "] waiting task["+std::to_string(pos->second)+"].....").c_str());
                                return false;
                            }
                            return true;
                        });
                    }
                    logger_->Logger("comming",("task["+std::to_string(part_idx) + "] wait task["+std::to_string(pos->second)+"] success.").c_str());
                }
                
                //assert(!node_collector->inputs.size());
                if(i){
                    auto childs = node_collector->childs_;
                    if(childs.size() == 1){
                        //assert(!has_right);
                        node_collector->inputs.push_back(childs[0]->output);
                    }else if (childs.size() == 2){
                        //assert(has_right);
                        node_collector->inputs.push_back(childs[0]->output);
                        node_collector->inputs.push_back(childs[1]->output);
                    }else{
                        logger_->Logger("comming",("error node_collector's child num: " + std::to_string(childs.size())).c_str());
                    }
                }

                /*search*/
                if(shared_index_->Search(node_collector)){
                    logger_->Logger("comming","match node success,fetch history output");
                    {
                        logger_->Logger("comming",("part_idx:"+std::to_string(part_idx)).c_str());
                        std::unique_lock<std::mutex> lock((*mtx_list)[part_idx]);
                        (*finish_list)[part_idx] = true;
                        logger_->Logger("comming",("task["+std::to_string(part_idx) + "] has notify.").c_str());
                        (*cv_list)[part_idx].notify_one();
                    }
                    {
                        logger_->Logger("comming",("match node time:"+std::to_string(node_collector->time)).c_str());
                        std::lock_guard<std::mutex> lock(*time_mtx);
                        total_time += node_collector->time;
                        (*finish_node_num) ++;
                        
                        logger_->Logger("comming",("total_time:"+std::to_string(total_time)+",duration: "+std::to_string(query_min_duration)).c_str());
                        logger_->Logger("comming",("finish_node_num:"+std::to_string(*finish_node_num)+",ndoe_num: "+std::to_string(node_num_)).c_str());
                        if(total_time >= query_min_duration && *finish_node_num == node_num_){
                            logger_->Logger("comming",("task["+std::to_string(part_idx) + "] cancel query.").c_str());
                            CancelQuery(pid_);
                            return true;
                        }
                    }
                }else{ 
                    logger_->Logger("comming","match node failed");
                    return false;
                }
            }
            return true;
        });
        ++part_idx;
    }
    return true;
}
    
void NodeManager::ComputeTotalNodes(HistorySlowPlanStat* hsps){
    assert(hsps);
    auto node_collector = level_mgr_->GetNodeCollector()[hsps];

    node_collector->json_sub_plan = hsps->canonical_node_json_plan;
    node_collector->time = hsps->actual_total;
    node_collector->output = hsps->actual_rows;

    if(NodeTag(hsps->node_tag) == T_NestLoop 
		|| NodeTag(hsps->node_tag) == T_MergeJoin 
		|| NodeTag(hsps->node_tag) == T_HashJoin){
		assert(strlen(hsps->join_type));
		node_collector->join_type_list.push_back(hsps->join_type);
	}

    node_collector_list_.push_back(node_collector);
    node_collector->childs_.clear();
    for(size_t i = 0; i< hsps->n_childs; ++i){
        auto child_hsps = hsps->childs[i];
        auto child_node_collector = level_mgr_->GetNodeCollector()[child_hsps];
        child_node_collector->parent_ = node_collector;
        node_collector->inputs.push_back(child_hsps->actual_rows);
        node_collector->childs_.push_back(child_node_collector);
        ComputeTotalNodes(child_hsps);
    }
}

/**
 * divide plan into multi tasks. 
 */
void NodeManager::PlanPartition(HistorySlowPlanStat* hsps){
    assert(hsps);
    auto& node_collector_map = level_mgr_->GetNodeCollector();
    std::stack<HistorySlowPlanStat*>st;
    auto cur = hsps;
    while(cur || !st.empty()){
        std::vector<NodeCollector*>partition;
        bool left_check = false;
        while(cur){
            partition.push_back(node_collector_map[cur]);
            st.push(cur);
            if(cur->n_childs >= 1){
                cur = cur->childs[0];
            }else{
                cur = nullptr;
            }
            left_check = true;
            node_num_++;
        }
        if(left_check){
            std::reverse(partition.begin(),partition.end());
            partition_list.push_back(partition);
        }

        auto top = st.top();
        st.pop();

        if(top->n_childs > 1){
            assert(top->n_childs == 2);
            cur = top->childs[1];
            /*mark dependence*/
            dependencies_[node_collector_map[top]] = partition_list.size();
        }else{
            cur = nullptr;
        }
    }
}

bool NodeManager::CancelQuery(pid_t pid){
    Datum arg = Int32GetDatum(pid);
    Datum result = DirectFunctionCall1(pg_cancel_backend, arg);
    bool success = DatumGetBool(result);
    if(success){
        logger_->Logger("comming","cancel query success...");
        return true;
    } else {
        logger_->Logger("comming","cancel query failed or has been canceled...");
        return false;
    }
}
