#pragma once
#include <string>
#include "excavate/ExcavateContext.hpp"
#include "common/thread_pool.hpp"
#include "storage/RedisPlanStatProvider.hpp"
#include <memory>

extern "C"{
    #include "postgres.h"
    #include "nodes/execnodes.h"
    #include "format.h"
    #include "format.pb-c.h"
    #include <postgres_ext.h>
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
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
    bool ProcQueryDesc(QueryDesc* qd);
private:
    std::string HashCanonicalPlan(char *json_plan);
    
    /* debug tools */
    void ShowAllHspsTree(HistorySlowPlanStat* hsps,int hdepth = 0);
    void ShowAllPredTree(HistorySlowPlanStat* hsps,int depth = 0);
    
    void ShowAllNodeCollect(HistorySlowPlanStat* hsps,std::unordered_map<HistorySlowPlanStat*, NodeCollector*> map,int h_depth = 0);
    void ShowNodeCollect(NodeCollector *nc ,int depth = 0);
    
    void ShowPredTree(PredExpression* p_expr, int depth = 0);
    void ShowSubPlansTree(HistorySlowPlanStat* hsps,int depth = 0);
    void PrintIndent(int depth);

    PlanStatFormat(int psize = 10);
    ~PlanStatFormat();
private:
    std::shared_ptr<ThreadPool> pool_;
    size_t pool_size_ = 10;
    /* HistorySlowPlanStat is a protobuf format structrue,it will be encoding and stored in kv engine*/
    HistorySlowPlanStat hsps_;
    std::shared_ptr<RedisSlowPlanStatProvider> storage_;
    //std::shared_ptr<HistoryQueryLevelTree> history_index_;
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
private:
    std::shared_ptr<AbstractFormatStrategy> strategy_;
};


