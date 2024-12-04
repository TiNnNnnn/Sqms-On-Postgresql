#include "excavate/ExcavateProvider.hpp"
#include "common/config.h"
#include <queue>

bool CostBasedExcavateStrategy::excavate(std::vector<HistorySlowPlanStat*>& list) const{
    /*similar topological graph algorithm*/
    double t = query_min_duration * late_tolerance;
    std::queue<HistorySlowPlanStat*>q;
    q.push(hsps_);
    while(!q.empty()){
        auto node = q.front();
        q.pop();
        size_t child_num = node->n_childs;
        size_t cnt = 0;
        if(node->sub_cost_ >= t){
            /**
             * TODO: 12-04 We need to handle the subquery operator specifically
             */
            for(size_t i = 0;i<child_num;i++){
                if(node->childs[i]->sub_cost_ < t){
                    cnt++;
                }
            }
        }
        if(cnt == child_num){
            /**
             * all childs's sub_cost are smaller than threshold,and the
             * current root's sub_cost is larger than threshold, so we
             * can regard the sub tree as a slow sub query
            */
            list.push_back(node);
        }else{
            for(size_t i = 0; i<node->n_childs; i++){
                q.push(node->childs[i]);
            }
        }
    }
    return true;
} 

bool ExternalResourceExcavateStrategy::excavate(std::vector<HistorySlowPlanStat*>& list) const{
    return true;
}
