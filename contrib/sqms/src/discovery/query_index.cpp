#include "discovery/query_index.hpp"

HistoryQueryLevelTree::HistoryQueryLevelTree(){
    root_ = std::make_shared<HistoryQueryIndexNode>(0);
    /* rebuild the history query level tree */
}

bool HistoryQueryLevelTree::Insert(LevelManager* level_mgr,int l){
    
    return true;
}
bool HistoryQueryLevelTree::Remove(LevelManager* level_mgr,int l){
    return true;
}
bool HistoryQueryLevelTree::Search(LevelManager* level_mgr,int l){
    return true;
}

bool HistoryQueryLevelTree::Insert(NodeCollector* node_collector){
    return true;
}
bool HistoryQueryLevelTree::Remove(NodeCollector* node_collector){
    return true;
}
bool HistoryQueryLevelTree::Search(NodeCollector* node_collector){
    return true;
}


bool HistoryQueryIndexNode::InsertSet(HistorySlowPlanStat* hsps){

}

bool HistoryQueryIndexNode::RemoveSet(HistorySlowPlanStat* hsps){

}

bool HistoryQueryIndexNode::SearchSet(HistorySlowPlanStat* hsps){

} 

