#pragma once
#include "common/logger.hpp"
#include <string>
#include "excavate/ExcavateContext.hpp"
#include "common/thread_pool.hpp"
#include "storage/RedisPlanStatProvider.hpp"
#include <memory>

extern "C"{
    #include "nodes/execnodes.h"
    #include "format.h"
    #include "format.pb-c.h"
    #include <postgres_ext.h>
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
    #include "miscadmin.h"
    #include "tcop/tcopprot.h"
    #include "utils/fmgrprotos.h"
    #include "storage/lwlock.h"
    #include "utils/guc.h"
    #include "miscadmin.h"
};
/** 
 * TODO: the name of HistorySlowPanStat and SlowPlanStat is simlar, we need rename 
 * one of them to keep readilty of codes
*/
class NodeCollector;
class AbstractFormatStrategy;
class PlanFormatContext;
class PlanStatFormat{
    typedef const char *(*explain_get_index_name_hook_type) (Oid indexId);
public:
    static PlanStatFormat& getInstance();

    PlanStatFormat(const PlanStatFormat&) = delete;
    PlanStatFormat& operator=(const PlanStatFormat&) = delete;
    void Serialization();
    void Deserialization();
    bool Preprocessing(QueryDesc* qd);
    bool ProcQueryDesc(QueryDesc* qd, MemoryContext oldcxt,bool slow = false);
private:
    std::string HashCanonicalPlan(char *json_plan);
    void LevelOrder(HistorySlowPlanStat* hsps,std::vector<HistorySlowPlanStat*>& sub_list);
    bool CancelQuery(pid_t pid);
    
    /* debug tools */
    void ShowAllHspsTree(HistorySlowPlanStat* hsps,int hdepth = 0);
    void ShowAllPredTree(HistorySlowPlanStat* hsps,int depth = 0);
    std::string ShowAllNodeCollect(HistorySlowPlanStat* hsps,
        std::unordered_map<HistorySlowPlanStat*, NodeCollector*> map,std::string tag,int h_depth = 0);
    std::string ShowNodeCollect(NodeCollector *nc ,std::string tag,int depth = 0);
    void ShowPredTree(PredExpression* p_expr, int depth = 0);
    void ShowSubPlansTree(HistorySlowPlanStat* hsps,int depth = 0);

    void PrintIndent(int depth);
    std::string GetIndent(int depth);

    PlanStatFormat(int psize = 10);
    ~PlanStatFormat();
private:
    std::shared_ptr<ThreadPool> pool_;
    size_t pool_size_ = 10;
    SqmsLogger* logger_;
    /* HistorySlowPlanStat is a protobuf format structrue,it will be encoding and stored in kv engine*/
    HistorySlowPlanStat hsps_;
    std::shared_ptr<RedisSlowPlanStatProvider> storage_;
    /*Serach Flag*/
    std::mutex search_mutex_;
    bool search_found_ =false; 
    std::condition_variable search_cv_; 
};

/**
 * AbstractFormatStrategy
 */
class AbstractFormatStrategy {
public:
    virtual ~AbstractFormatStrategy() = default;
    virtual bool Format() = 0;
    virtual bool PrintPredEquivlences() = 0;
};

/**
 * PlanFormatContext
 */
class PlanFormatContext{
public:
    /*set plan format strategy*/
    void SetStrategy(std::shared_ptr<AbstractFormatStrategy> strategy) {
        strategy_ = strategy;
    }
    /*execute plan format Strategy*/
    void executeStrategy() const {
        if (strategy_) {
            strategy_->Format();
        } else {
            std::cerr << "No excavate strategy set!" << std::endl;
        }
    }

    void PrintStrategy() const {
        if (strategy_) {
            strategy_->PrintPredEquivlences();
        } else {
            std::cerr << "No excavate strategy set!" << std::endl;
        }
    }

    // bool Insert(HistoryQueryLevelTree* shared_idx) const {
    //     if(strategy_){
    //         return strategy_->Insert(shared_idx);
    //     }else{
    //         std::cerr << "No excavate strategy set!" << std::endl;
    //     }
    // }

    // bool Search(HistoryQueryLevelTree* shared_idx) const {
    //     if(strategy_){
    //         return strategy_->Search(shared_idx);
    //     }else{
    //         std::cerr << "No excavate strategy set!" << std::endl;
    //     }
    // }
private:
    std::shared_ptr<AbstractFormatStrategy> strategy_;
};


