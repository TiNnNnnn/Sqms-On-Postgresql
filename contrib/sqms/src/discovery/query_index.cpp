#include "discovery/query_index.hpp"

 /* rebuild the history query level tree */
HistoryQueryLevelTree::HistoryQueryLevelTree(){
    root_ = std::make_shared<HistoryQueryIndexNode>(1,height_);
}

bool HistoryQueryLevelTree::Insert(LevelManager* level_mgr,int l){
    return root_->Insert(level_mgr);
}

bool HistoryQueryLevelTree::Remove(LevelManager* level_mgr,int l){
    return root_->Remove(level_mgr);
}

bool HistoryQueryLevelTree::Search(LevelManager* level_mgr,int l){
    return root_->Search(level_mgr,-1);
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
HistoryQueryIndexNode::HistoryQueryIndexNode(int l,int total_height)
    :level_(l),level_strategy_context_(new LevelStrategyContext()){
    level_strategy_context_->SetStrategy(l,total_height);
}

bool HistoryQueryIndexNode::Insert(LevelManager* level_mgr){
    return level_strategy_context_->Insert(level_mgr);
}

bool HistoryQueryIndexNode::Remove(LevelManager* level_mgr){
    return level_strategy_context_->Remove(level_mgr);
}

bool HistoryQueryIndexNode::Search(LevelManager* level_mgr,int id){
    return level_strategy_context_->Search(level_mgr,id);
} 

