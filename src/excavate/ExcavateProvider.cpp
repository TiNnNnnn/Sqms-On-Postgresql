#include "excavate/ExcavateProvider.hpp"
#include "common/config.h"
#include <queue>

/**default exavatestrategy*/
// bool CostBasedExcavateStrategy::excavate(std::vector<HistorySlowPlanStat*>& list) const{
//     /*similar topological graph algorithm*/
//     double t = query_min_duration * late_tolerance;
//     std::queue<HistorySlowPlanStat*>q;
//     q.push(hsps_);
//     while(!q.empty()){
//         auto node = q.front();
//         q.pop();
//         size_t child_num = node->n_childs;
//         size_t cnt = 0;
//         if(node->sub_cost >= t && node->n_childs){
//             list.push_back(node);
//         }
//         for(size_t i = 0; i<node->n_childs; i++){
//             q.push(node->childs[i]);
//         }
//     }
//     return true;
// } 

bool CostBasedExcavateStrategy::excavate(std::vector<HistorySlowPlanStat*>& list) const{
    /*similar topological graph algorithm*/
    double t = query_min_duration * late_tolerance;
    std::queue<HistorySlowPlanStat*>q;
    q.push(hsps_);
    while(!q.empty()){
        auto node = q.front();
        q.pop();
        size_t child_num = node->n_childs;
        if(node->sub_cost >= t){
            if(node->n_childs){
                int cnt = 0;
                for(size_t i = 0; i <node->n_childs;++i){
                    if(node->childs[i]->sub_cost >= t){
                        q.push(node->childs[i]);
                        cnt++;
                    }
                }
                if(!cnt){
                    list.push_back(node);
                }
            }else{
                list.push_back(node);
            }
        }
        // if(node->sub_cost >= t && node->n_childs){
        //     list.push_back(node);
        // }
        // for(size_t i = 0; i<node->n_childs; i++){
        //     q.push(node->childs[i]);
        // }
    }
    return true;
} 


/*another exavate strategy*/
bool ExternalResourceExcavateStrategy::excavate(std::vector<HistorySlowPlanStat*>& list) const{
    return true;
}

/*not use any exavate strategy*/
bool NoProcessedExcavateStrategy::excavate(std::vector<HistorySlowPlanStat*>& list) const {
    list.push_back(hsps_);
    return true;
}
