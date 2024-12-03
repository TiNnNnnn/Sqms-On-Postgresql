#pragma once
#include <limits.h>

#include "postgres.h"
#include "access/parallel.h"
//#include "commands/explain.h"
#include "collect/format.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "jit/jit.h"
#include "utils/guc.h"
#include "common/config.h"

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
        return (query_min_duration >= 0 && 
        (nesting_level == 0 || stat_nested_statements) && 
        current_query_sampled);
    }
public:
    static int nesting_level;
    static bool current_query_sampled;
};


