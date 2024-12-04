#pragma once
#include <string>
#include "excavate/ExcavateContext.hpp"
#include "common/thread_pool.hpp"

#include "postgres.h"
#include "nodes/execnodes.h"
#include "format.h"
#include "format.pb-c.h"
#include <postgres_ext.h>
#include "canonical_strategy.h"
#include "storage/RedisPlanStatProvider.hpp"

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
    std::string HashCanonicalPlan(char *json_plan);

    PlanStatFormat();
    ~PlanStatFormat();
private:
    ThreadPool* pool_;
    size_t pool_size_ = 10;
    /* HistorySlowPlanStat is a protobuf format structrue,it will be encoding and stored in kv engine*/
    HistorySlowPlanStat* hsps_;
    CanonicalStrategy cs_ = C_SAFE;
    RedisSlowPlanStatProvider * storage_;
};



