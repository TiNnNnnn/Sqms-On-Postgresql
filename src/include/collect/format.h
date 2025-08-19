/*-------------------------------------------------------------------------
 *
 * explain.h
 *	  prototypes for explain.c
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/explain.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "commands/explain.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "parser/parse_node.h"
#include "collect/format.pb-c.h"
#include "common/config.h"

typedef struct RecureState{
    /*cost of node */
	double cost_;
	/*cost of subquery */
	double subquery_cost_;
	/*in detail_str_,we need reserve all detail about a physical node,it will used
	  for compute equivlence class*/
	StringInfo detail_str_;
	/*in cannoical_str, we just reserve the Type info,it is enough for a basic screening*/
	StringInfo canonical_str_;
	List* node_type_set_;
	List* pred_set_;
	HistorySlowPlanStat hps_;
} RecureState;

typedef const char *(*explain_get_index_name_hook_type) (Oid indexId);
extern PGDLLIMPORT explain_get_index_name_hook_type explain_get_index_name_hook;

extern ExplainState *NewFormatState(void);

extern void FreeFormatState(ExplainState*es);

extern HistorySlowPlanStat FormatPrintPlan(ExplainState *es, ExplainState *ces,QueryDesc *queryDesc);

extern void FormatPrintTriggers(ExplainState *es, QueryDesc *queryDesc);

extern void FormatPrintJITSummary(ExplainState *es, QueryDesc *queryDesc);

extern void FormatQueryText(ExplainState *es, QueryDesc *queryDesc);

extern void FormatBeginOutput(ExplainState *es);
extern void FormatEndOutput(ExplainState *es);
extern void FormatSeparatePlans(ExplainState *es);

extern void FormatPropertyList(const char *qlabel, List *data,
								ExplainState *es);
extern void FormatPropertyListNested(const char *qlabel, List *data,
									  ExplainState *es);
extern void FormatPropertyText(const char *qlabel, const char *value,
								ExplainState *es);
extern void FormatPropertyInteger(const char *qlabel, const char *unit,
								   int64 value, ExplainState *es);
extern void FormatPropertyUInteger(const char *qlabel, const char *unit,
									uint64 value, ExplainState *es);
extern void FormatPropertyFloat(const char *qlabel, const char *unit,
								 double value, int ndigits, ExplainState *es);
extern void FormatPropertyBool(const char *qlabel, bool value,
								ExplainState *es);

extern void FormatOpenGroup(const char *objtype, const char *labelname,
							 bool labeled, ExplainState *es);
extern void FormatCloseGroup(const char *objtype, const char *labelname,
							  bool labeled, ExplainState *es);