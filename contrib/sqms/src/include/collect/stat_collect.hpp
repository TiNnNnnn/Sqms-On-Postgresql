#pragma once
#include <limits.h>
#include <unordered_set>
#include "discovery/query_index.hpp"

extern "C"{
    #include "postgres.h"
    #include "access/parallel.h"
    #include "collect/format.h"
    #include "executor/executor.h"
    #include "executor/instrument.h"
    #include "jit/jit.h"
    #include "utils/guc.h"
    #include "common/config.h" 
}

//extern HistoryQueryLevelTree* history_idx;
class StatCollecter{
public:
    StatCollecter();
    ~StatCollecter();

    static void StmtExecutorStartWrapper(QueryDesc *queryDesc, int eflags);
    static void StmtExecutorRunWrapper(QueryDesc *queryDesc,ScanDirection direction,uint64 count, bool execute_once);
    static void StmtExecutorFinishWrapper(QueryDesc *queryDesc);
    static void StmtExecutorEndWrapper(QueryDesc *queryDesc);

private:
    static bool auto_explain_enabled(){
        return query_min_duration >= 0;
    }
public:
    static int nesting_level;
    static bool current_query_sampled;
    static std::shared_ptr<HistoryQueryLevelTree> history_index_;
};


