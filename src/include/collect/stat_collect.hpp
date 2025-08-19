#pragma once
#include <limits.h>
#include <unordered_set>
#include "common/logger.hpp"
#include "discovery/query_index.hpp"

extern "C"{
    #include "access/parallel.h"
    #include "collect/format.h"
    #include "executor/executor.h"
    #include "executor/instrument.h"
    #include "jit/jit.h"
    #include "utils/guc.h"
    #include "common/config.h"
    
    #include "catalog/pg_authid.h"
    #include "common/hashfn.h"
    #include "executor/instrument.h"
    #include "funcapi.h"
    #include <time.h>
    #include "mb/pg_wchar.h"
    #include "miscadmin.h"
    #include "optimizer/planner.h"
    #include "parser/analyze.h"
    #include "parser/parsetree.h"
    #include "parser/scanner.h"
    #include "parser/scansup.h"
    #include "pgstat.h"
    #include "storage/fd.h"
    #include "storage/ipc.h"
    #include "storage/spin.h"
    #include "tcop/utility.h"
    #include "utils/acl.h"
    #include "utils/builtins.h"
    #include "utils/memutils.h"
    #include "utils/elog.h"
    #include "storage/lwlock.h"
    #include "utils/guc.h"
    #include "miscadmin.h"
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
    static void ExplainOneQueryWithSlowWrapper(Query *query,
							int cursorOptions,
							IntoClause *into,
							ExplainState *es,
							const char *queryString,
							ParamListInfo params,
							QueryEnvironment *queryEnv);

private:
    static bool auto_explain_enabled(){
        return query_min_duration >= 0;
    }
public:
    static int nesting_level;
    static bool current_query_sampled;
    static std::shared_ptr<HistoryQueryLevelTree> history_index_;
};
