#pragma once
#include <string>
#include "excavate/ExcavateContext.hpp"
#include "common/thread_pool.hpp"
#include "storage/RedisPlanStatProvider.hpp"
#include "discovery/query_index.hpp"

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

    void ComputeEquivlenceClass(HistorySlowPlanStat* hsps,SlowPlanStat *sps);
    void ComputLevlEquivlenceClass(const std::vector<HistorySlowPlanStat*>& list,SlowPlanStat *sps);

    void ParseExprs(HistorySlowPlanStat* hsps);

    void PredDecompose(HistorySlowPlanStat* pred_expr);

    void ShowPredTree(PredExpression* p_expr, int depth = 0);
    void PrintIndent(int depth);

    PlanStatFormat();
    ~PlanStatFormat();
private:
    ThreadPool* pool_;
    size_t pool_size_ = 10;
    /* HistorySlowPlanStat is a protobuf format structrue,it will be encoding and stored in kv engine*/
    HistorySlowPlanStat hsps_;
    CanonicalStrategy cs_ = C_SAFE;
    RedisSlowPlanStatProvider * storage_;
    /** 
     * TODO: the name of HistorySlowPanStat and SlowPlanStat is simlar, we need rename 
     * one of them to keep readilty of codes
    */
};

