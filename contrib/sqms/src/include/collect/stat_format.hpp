#pragma once
#include <string>
#include "excavate/ExcavateContext.hpp"
#include "common/thread_pool.hpp"

extern "C" {
    #include "postgres.h"
    #include "nodes/execnodes.h"
    #include "format.h"
    #include "format.pb-c.h"
    #include <postgres_ext.h>
}

class PlanStatFormat{
    typedef const char *(*explain_get_index_name_hook_type) (Oid indexId);
public:
    static PlanStatFormat& getInstance();

    PlanStatFormat(const PlanStatFormat&) = delete;
    PlanStatFormat& operator=(const PlanStatFormat&) = delete;

    void serialization();
    void Deserialization();

    bool Preprocessing(QueryDesc* qd);
    bool ProcQueryDesc(QueryDesc* qd);
private:
    PlanStatFormat();
    ~PlanStatFormat();

    void ParsePlan(QueryDesc *planstate, List *ancestors,const char *relationship, 
                const char *plan_name);
private:
    /*PlanStat is the orignal plan with running data,we will parse it*/
    PlanState* ps_;
    ThreadPool* pool_;
    size_t pool_size_ = 10;
    /* ExplainState store the select base info for plan parse and format */
    ExplainState* es_;
    /* HistorySlowPlanStat is a protobuf format structrue,it will be encoding and stored in kv engine*/
    HistorySlowPlanStat* hsps_;
};



