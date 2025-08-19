#include "collect/node_mgr.hpp"
#include <stack>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>
#include "collect/explain_slow.h"

NodeManager::NodeManager(HistorySlowPlanStat* hsps,std::shared_ptr<LevelManager> level_mgr,pid_t pid)
: hsps_(hsps),level_mgr_(level_mgr),pool_(std::make_shared<ThreadPool>(10,true)),pid_(pid){
    bool found = false;
    sqmslogger_ptr = (dsa_pointer*)ShmemInitStruct(sqmslogger_name, sizeof(dsa_pointer), &found);

    found = false;
    shared_index_ptr = (dsa_pointer*)ShmemInitStruct(shared_index_name, sizeof(dsa_pointer), &found);

    logger_ = (SqmsLogger*)dsa_get_address(area_, *sqmslogger_ptr);
    assert(logger_ && sqmslogger_ptr);

    shared_index_ = (HistoryQueryLevelTree*)dsa_get_address(area_, *shared_index_ptr);
    assert(shared_index_ && shared_index_ptr);

    root_ = level_mgr_->GetNodeCollector()[hsps];
}

bool NodeManager::Format(){
    ComputeTotalNodes(hsps_);
    return true;
}

bool NodeManager::PrintPredEquivlences(){
    return true;
}

bool NodeManager::SearchInternal(NodeCollector *node,double total_time,int finish_node_num,LevelOrderIterator* iter){
   assert(node);
   if(shared_index_->Search(node) || node->match_cnt){
        for(size_t i = 0; i < node->output_list_.size();++i){    
            total_time += node->time_list_[i];
            finish_node_num++;
            node->pre_candidate_id = node->candidate_id;
            node->candidate_id = i;
            if(node->parent_){
                node->parent_->inputs[node->child_idx] = node->output_list_[i];
            }
            if(total_time >= query_min_duration && finish_node_num >= 1){
                CancelQuery(pid_);
                return true;
            }else{
                if(!iter->hasNext()){
                    return false;
                }
                auto cur = iter->next();
                int ret = SearchInternal(cur, total_time, finish_node_num,iter);
                iter->prev();
                total_time -= node->time_list_[i];
                finish_node_num--;
                node->candidate_id = node->pre_candidate_id;
                if(ret){
                    return true;
                }
            }
        }
        return false;
    }else{ 
        std::cout<<"[comming]match failed for node"<<std::endl;
        return false;
    }    
   return false;
}

bool NodeManager::Search(){
    PlanPartition(hsps_);
    PlanInit(hsps_);
    double total_time(0);
    int finish_node_num = 0;
    int cur_idx = 0;
    LevelOrderIterator iter(level_collector_);
    return SearchInternal(iter.next(), total_time, finish_node_num,&iter);
}

// bool NodeManager::Search(){
//     PlanPartition(hsps_);
//     PlanInit(hsps_);
//     double total_time(0);
//     int finish_node_num = 0; 
//     for(const auto& level : level_collector_){
//         for(const auto& node : level){
//            if(!node->childs_.size()){
//                 /*leaf node ,nothing to do*/
//            }else{
//                 /*fill input from childs*/
//                 if(node->childs_.size() == 1){
//                     node->inputs[0] = node->childs_[0]->output;
//                 }else if(node->childs_.size() == 2){
//                     node->inputs[0] = node->childs_[0]->output;
//                     node->inputs[1] = node->childs_[1]->output;
//                 }else{
//                     logger_->Logger("comming",("error node_collector's child num: " + std::to_string(node->childs_.size())).c_str());
//                     exit(-1);
//                 }
//            }

//            std::string type_str;
//            std::string input_str;
//            for(const auto& ctype: node->childs_){
//                 type_str += ctype->type_name + ",";
//                 input_str += std::to_string(ctype->output) + ",";
//            }
//            logger_->Logger("comming",("node type: "+node->type_name + ", child_types: "+type_str).c_str());
//            logger_->Logger("comming",("node type: "+node->type_name + ", child ouputs: "+ input_str).c_str());
           
//             if(shared_index_->Search(node) || node->match_cnt){
//                     logger_->Logger("comming","match success for node");
//                     total_time += node->time;
//                     finish_node_num++;
//                     logger_->Logger("comming","-----------------------------------------------");
//                     logger_->Logger("comming",("finish_node_num:"+std::to_string(finish_node_num)+",node_num: "+std::to_string(node_num_)).c_str());
//                     for(const auto& info : node->leaf_info_){
//                         std::string input_str;
//                         for(const auto& in : info->inputs_){
//                             input_str += std::to_string(in)+",";
//                         }
//                         logger_->Logger("comming",("current node [" +std::to_string(finish_node_num-1)+"] output: "+std::to_string(info->output_)+", inputs: "+input_str).c_str());
//                     }
//                     logger_->Logger("comming","-----------------------------------------------");
//                     if(total_time >= query_min_duration && finish_node_num == node_num_){
//                         CancelQuery(pid_);
//                         return true;
//                     }
//             }else{ 
//                     logger_->Logger("comming","match failed for node");
//                     return false;
//             } 
//         }
//     }
//     return true;
// }

// bool NodeManager::Search(){
//     PlanPartition(hsps_);

//     int* finish_node_num = (int*)malloc(sizeof(int));
//     *finish_node_num = 0;
//     double total_time(0);
//     auto time_mtx = std::make_shared<std::mutex>();
    
//     int p_size = partition_list.size();
//     logger_->Logger("comming",("stitch thread num: "+std::to_string(p_size)+",node size:"+std::to_string(node_num_)).c_str());
//     auto finish_list= std::make_shared<std::vector<bool>>(p_size,true);
//     auto cv_list = std::make_shared<std::vector<std::condition_variable>>(p_size);
//     auto mtx_list = std::make_shared<std::vector<std::mutex>>(p_size);

//     auto master_cv = std::make_shared<std::condition_variable>();
//     auto master_mtx = std::make_shared<std::mutex>();
//     bool cancel = false;
//     bool stop = false;

//     std::vector<std::thread> threads;

//     int part_idx = 0;
//     for(const auto& part : partition_list){
//         threads.emplace_back([part_idx,mtx_list,cv_list,finish_list,&part,this,time_mtx,&total_time,finish_node_num,master_cv,master_mtx,&cancel,&stop](){
//             logger_->Logger("comming",("finish_node_num:"+std::to_string(*finish_node_num)).c_str());
//             logger_->Logger("comming",("part_size:"+std::to_string(part.size())).c_str());
//             for(size_t i = 0;i < part.size(); ++i){
//                 /*check if need wait*/
//                 auto node_collector = part[i];
//                 //bool has_right = false;
//                 auto pos = dependencies_.find(node_collector);
//                 if(pos != dependencies_.end()){
//                     //has_right = true;
//                     {
//                         std::unique_lock<std::mutex> lock((*mtx_list)[pos->second]);
//                         (*cv_list)[pos->second].wait(lock, [&] {
                            
//                             if(cancel || stop){
//                                 return true;
//                             }

//                             if (!(*finish_list)[pos->second]) {
//                                 logger_->Logger("comming",("task["+std::to_string(part_idx) + "] waiting task["+std::to_string(pos->second)+"].....").c_str());
//                                 return false;
//                             }
//                             return true;
//                         });
//                     }
//                     logger_->Logger("comming",("task["+std::to_string(part_idx) + "] wait task["+std::to_string(pos->second)+"] success.").c_str());
//                 }

//                 if(cancel || stop){
//                     logger_->Logger("comming",("task["+std::to_string(part_idx) + "] stop woking, direct exit!").c_str());
//                     return true;
//                 }
                
//                 //assert(!node_collector->inputs.size());
                
//                 if(i){
//                     auto childs = node_collector->childs_;
//                     if(childs.size() == 1){
//                         //assert(!has_right);
//                         node_collector->inputs[0] = childs[0]->output;
//                     }else if (childs.size() == 2){
//                         //assert(has_right);
//                         node_collector->inputs[0] = childs[0]->output;
//                         node_collector->inputs[1] = childs[1]->output;
//                     }else{
//                         logger_->Logger("comming",("error node_collector's child num: " + std::to_string(childs.size())).c_str());
//                     }
//                 }
//                 logger_->Logger("comming",("task["+std::to_string(part_idx) + "] begin search for node ["+std::to_string(i)+"] in part").c_str());
//                 /*search*/
//                 if(shared_index_->Search(node_collector) && !cancel && !stop){
//                     logger_->Logger("comming",("task["+std::to_string(part_idx) + "] match success for node ["+std::to_string(i)+"] in part").c_str());
//                     {
//                         logger_->Logger("comming",("part_idx:"+std::to_string(part_idx)).c_str());
//                         std::unique_lock<std::mutex> lock((*mtx_list)[part_idx]);
//                         (*finish_list)[part_idx] = true;
//                         logger_->Logger("comming",("task["+std::to_string(part_idx) + "] has notify.").c_str());
//                         (*cv_list)[part_idx].notify_all();
//                     }
//                     {
//                         logger_->Logger("comming",("match node time:"+std::to_string(node_collector->time)).c_str());
//                         std::lock_guard<std::mutex> lock(*time_mtx);
//                         total_time += node_collector->time;
//                         (*finish_node_num) ++;
                        
//                         logger_->Logger("comming",("total_time:"+std::to_string(total_time)+",duration: "+std::to_string(query_min_duration)).c_str());
//                         logger_->Logger("comming",("finish_node_num:"+std::to_string(*finish_node_num)+",node_num: "+std::to_string(node_num_)).c_str());
//                         if(total_time >= query_min_duration && *finish_node_num == node_num_){
//                             logger_->Logger("comming",("task["+std::to_string(part_idx) + "] cancel query.").c_str());
//                             {
//                                 std::unique_lock<std::mutex> master_lock(*master_mtx);
//                                 cancel = true;
//                                 (*master_cv).notify_all();
//                                 CancelQuery(pid_);
//                             }
//                             return true;
//                         }else if(total_time < query_min_duration && *finish_node_num == node_num_){
//                             {
//                                 std::unique_lock<std::mutex> master_lock(*master_mtx);
//                                 stop = true;
//                                 (*master_cv).notify_all();
//                             }
//                         }
//                     }
//                 }else{ 
//                     {
//                         std::unique_lock<std::mutex> master_lock(*master_mtx);
//                         stop = true;
//                         (*master_cv).notify_all();
//                     }
//                     logger_->Logger("comming","match node failed");
//                     return false;
//                 }
//             }
//             return true;
//         });
//         ++part_idx;
//     }

//     for(auto& t : threads){
//         t.join();
//     }

//     // logger_->Logger("comming","begin waiting node task finish...");
//     // std::unique_lock<std::mutex> lock(*master_mtx);
//     // master_cv->wait(lock, [&] {
//     //     if(cancel){
//     //         CancelQuery(pid_);
//     //         return true;
//     //     }else if(stop){
//     //         return true;
//     //     }
//     //     return false;
//     // });
//     // logger_->Logger("comming","finish waiting node task finish...");
//     return true;
// }
    
void NodeManager::ComputeTotalNodes(HistorySlowPlanStat* hsps){
    assert(hsps);
    auto node_collector = level_mgr_->GetNodeCollector()[hsps];
    node_collector->json_sub_plan = hsps->canonical_node_json_plan;
    node_collector->time = hsps->actual_total;
    node_collector->output = hsps->actual_rows;
    node_collector->inputs.resize(0);
    node_collector->type_name = std::string(hsps->node_type);
    node_collector->hsps_pack = PlanStatFormat::PackHistoryPlanState(hsps,node_collector->hsps_pack_size);

    if(NodeTag(hsps->node_tag) == T_NestLoop 
		|| NodeTag(hsps->node_tag) == T_MergeJoin 
		|| NodeTag(hsps->node_tag) == T_HashJoin){
		assert(strlen(hsps->join_type));
		node_collector->join_type_list.push_back(hsps->join_type);
	}

    node_id_list_.push_back(node_collector_list_.size());
    node_collector_list_.push_back(node_collector);
    for(size_t i = 0; i< hsps->n_childs; ++i){
        auto child_hsps = hsps->childs[i];
        auto child_node_collector = level_mgr_->GetNodeCollector()[child_hsps];
        child_node_collector->parent_ = node_collector;
        child_node_collector->child_idx = i;
        node_collector->inputs.push_back(child_hsps->actual_rows);
        node_collector->childs_.push_back(child_node_collector);
        ComputeTotalNodes(child_hsps);
    }
}

NodeExplain* NodeManager::ComputeExplainNodes(NodeCollector* node_collector){
    assert(node_collector);
    int cid = node_collector->candidate_id;
    if(cid != -1){
        auto pack = node_collector->hsps_pack_list_[cid];
        auto pack_size = node_collector->hsps_pack_size_list_[cid];
        auto sub_hsps = PlanStatFormat::UnPackHistoryPlanState(pack,pack_size);
        if(!sub_hsps){
            std::cout<<"ComputeExplainNodes: PlanStatFormat::UnPackHistoryPlanState error"<<std::endl;
            exit(-1);
        }
        node_collector->match_hsps_ = sub_hsps;
    }else{
        node_collector->match_hsps_ = nullptr;
    }
    /**
     * NodeCollector is a cpp style class, we should convert it
     * into NodeExplain, a c styple struct.
     */
    NodeExplain* enode = (NodeExplain*)malloc(sizeof(NodeExplain));
    enode->hsps_ = node_collector->match_hsps_;
    
    int child_count = node_collector->childs_.size();
    enode->childs_ = (NodeExplain**)malloc(sizeof(NodeExplain*) * child_count);
    for (size_t i = 0; i < child_count; ++i) {
        NodeCollector* child = node_collector->childs_[i];
        enode->childs_[i] = ComputeExplainNodes(child);
    }
    return enode;
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

void NodeManager::PlanInit(HistorySlowPlanStat* hsps){
    assert(hsps);
    auto node_collector = level_mgr_->GetNodeCollector()[hsps];
    std::vector<NodeCollector *>levels;
    std::vector<NodeCollector *>tmp_levels;

    levels.push_back(node_collector);
    int idx = 0;
    while(levels.size()){
        size_t sz = levels.size();
        for(size_t i=0;i<sz;i++){
            levels[i]->node_id = idx++;
            for(const auto& child : levels[i]->childs_){
                tmp_levels.push_back(child);
            }
        }
        level_collector_.push_back(levels);
        levels.clear();
        std::swap(levels,tmp_levels);
    }
    /*match node from bottom to top*/
    std::reverse(level_collector_.begin(),level_collector_.end());
}

bool NodeManager::CancelQuery(pid_t pid){
    elog(INFO, ("cancel query pid: "+std::to_string(pid)).c_str());
    Datum arg = Int32GetDatum(pid);
    Datum result = DirectFunctionCall1(pg_cancel_backend, arg);
    bool success = DatumGetBool(result);
    if(success){
        elog(LOG,"[Node View Stitch Match] cancel query success...");
        logger_->Logger("comming","[Node View Stitch Match] cancel query success...");
        return true;
    } else {
        logger_->Logger("comming","cancel query failed or has been canceled...");
        return false;
    }
}
