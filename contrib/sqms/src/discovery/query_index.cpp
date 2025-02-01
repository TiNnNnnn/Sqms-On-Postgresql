#include "discovery/query_index.hpp"

 /* rebuild the history query level tree */
HistoryQueryLevelTree::HistoryQueryLevelTree(int origin_height){
    root_ = (HistoryQueryIndexNode*)ShmemAlloc(sizeof(HistoryQueryIndexNode));
    if (!root_)
        elog(ERROR, "ShmemAlloc failed: not enough shared memory");
    new (root_) HistoryQueryIndexNode(origin_height,height_);
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
    :level_(l){
    level_strategy_context_ = (LevelStrategyContext*)ShmemAlloc(sizeof(LevelStrategyContext));
    if (!level_strategy_context_)
        elog(ERROR, "ShmemAlloc failed: not enough shared memory");
    level_strategy_context_->SetStrategy(l,total_height);
}

HistoryQueryIndexNode:: ~HistoryQueryIndexNode(){
    if (level_strategy_context_) {
        level_strategy_context_->~LevelStrategyContext();
    }
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

void QueryIndexManager::RegisterQueryIndex(){
    std::thread([](){
        auto query_index = std::make_shared<HistoryQueryLevelTree>();
        struct mq_attr attr;
        std::atomic<bool> stop_thread;

        attr.mq_flags = 0;
        attr.mq_maxmsg = 100;            
        attr.mq_msgsize = max_msg_size;
        attr.mq_curmsgs = 0;

        mqd_t mq = mq_open(queue_name, O_CREAT | O_RDONLY, 0666, &attr);
        if (mq == (mqd_t)-1) {
            perror("query index mq_open failed in IndexThreadMain");
            return;
        }

        char buffer[max_msg_size];
        while (!stop_thread) {
            ssize_t bytes_read = mq_receive(mq, buffer, max_msg_size, nullptr);
            if (bytes_read >= 0) {
                // Message* msg = reinterpret_cast<Message*>(buffer);
                // HandleMessage(*msg);
            } else if (errno != EINTR) {
                perror("mq_receive failed");
            }
        }
    });
}