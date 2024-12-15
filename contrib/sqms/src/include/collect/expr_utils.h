/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *		Declarations for ruleutils.c
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/ruleutils.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "format.pb-c.h"
#include <postgres_ext.h>

struct Plan;					/* avoid including plannodes.h here */
struct PlannedStmt;

typedef struct ExprRecureState{
	StringInfo detail_str_; 
	int cnt_;
}ExprRecureState;

extern ExprRecureState NewExprRecureStat();

extern char *pg_get_indexdef_string_format(Oid indexrelid);
extern char *pg_get_indexdef_columns_format(Oid indexrelid, bool pretty);

extern char *pg_get_partkeydef_columns_format(Oid relid, bool pretty);
extern char *pg_get_partconstrdef_string_format(Oid partitionId, char *aliasnamem,HistorySlowPlanStat* hsp);

extern char *pg_get_constraintdef_command_format(Oid constraintId);
extern char *deparse_expression_format(Node *expr, List *dpcontext,
								bool forceprefix, bool showimplicit);
extern List *deparse_context_for_format(const char *aliasname, Oid relid);
extern List *deparse_context_for_plan_tree_format(struct PlannedStmt *pstmt,
										   List *rtable_names);
extern List *set_deparse_context_plan_format(List *dpcontext,
									  struct Plan *plan, List *ancestors,HistorySlowPlanStat*hsp);
extern List *select_rtable_names_for_explain_format(List *rtable,
											 Bitmapset *rels_used);
extern char *generate_collation_name_format(Oid collid);
extern char *generate_opclass_name_format(Oid opclass);
extern char *get_range_partbound_string_format(List *bound_datums);

						
