#pragma once
#include "commands/explain.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "parser/parse_node.h"
#include "collect/format.pb-c.h"
#include "common/config.h"
#include "utils/ruleutils.h"


typedef struct NodeExplain{
    HistorySlowPlanStat* hsps_;
    struct NodeExplain** childs_;
}NodeExplain;


extern void PrintSlowPlan(ExplainState *es, QueryDesc *queryDesc, HistorySlowPlanStat* hsps,NodeExplain* node_explain);


