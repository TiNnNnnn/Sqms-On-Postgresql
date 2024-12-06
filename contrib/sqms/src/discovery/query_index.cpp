#include "discovery/query_index.hpp"

HistoryQueryLevelTree::HistoryQueryLevelTree(){
    root_ = std::make_shared<HistoryQueryIndexNode>(0);
    /* rebuild the history query level tree */
}

bool HistoryQueryLevelTree::Search(){

    for(size_t h = 0; h < height_; h++){
        
    }
    return true;
}


bool HistoryQueryIndexNode::InsertSet(HistorySlowPlanStat* hsps){

}

bool HistoryQueryIndexNode::RemoveSet(HistorySlowPlanStat* hsps){

}

bool HistoryQueryIndexNode::SearchSet(HistorySlowPlanStat* hsps){

} 