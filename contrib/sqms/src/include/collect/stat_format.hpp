#pragma once
#include <string>
#include "excavate/ExcavateContext.hpp"
#include "common/thread_pool.hpp"
#include "storage/RedisPlanStatProvider.hpp"
#include "discovery/query_index.hpp"
#include <memory>

extern "C"{
#include "postgres.h"
#include "nodes/execnodes.h"
#include "format.h"
#include "format.pb-c.h"
#include <postgres_ext.h>
#include "canonical_strategy.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
};

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
    void ShowPredTree(PredExpression* p_expr, int depth = 0);
    void PrintIndent(int depth);

    PlanStatFormat(int psize = 10);
    ~PlanStatFormat();
private:
    std::shared_ptr<ThreadPool> pool_;
    size_t pool_size_ = 10;
    /* HistorySlowPlanStat is a protobuf format structrue,it will be encoding and stored in kv engine*/
    HistorySlowPlanStat hsps_;
    CanonicalStrategy cs_ = C_SAFE;
    std::shared_ptr<RedisSlowPlanStatProvider> storage_;
    /** 
     * TODO: the name of HistorySlowPanStat and SlowPlanStat is simlar, we need rename 
     * one of them to keep readilty of codes
    */
};

