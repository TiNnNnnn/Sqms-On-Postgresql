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


/**
 * HistoryQueryIndexNode
 */
HistoryQueryIndexNode::HistoryQueryIndexNode(int l)
    :level_(l),level_strategy_context_(new LevelStrategyContext()){
    level_strategy_context_->SetStrategy(l);
}

bool HistoryQueryIndexNode::InsertSet(HistorySlowPlanStat* hsps){
    //level_strategy_context_->Insert()
    return true;
}

bool HistoryQueryIndexNode::RemoveSet(HistorySlowPlanStat* hsps){
    return true;
}

bool HistoryQueryIndexNode::SearchSet(HistorySlowPlanStat* hsps){
    return true;
} 

