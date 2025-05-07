/*-------------------------------------------------------------------------
 *
 * explain.c
 *	  Explain query execution plans
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "collect/format.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/createas.h"
#include "commands/defrem.h"
//#include "commands/prepare.h"
#include "executor/nodeHash.h"
#include "foreign/fdwapi.h"
#include "jit/jit.h"
#include "nodes/extensible.h"
#include "access/parallel.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include  "collect/expr_utils.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"
#include "utils/xml.h"


/* Hook for plugins to get control in explain_get_index_name() */
explain_get_index_name_hook_type explain_get_index_name_hook = NULL;


/* OR-able flags for ExplainXMLTag() */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4

// static void ExplainOneQuery(Query *query, int cursorOptions,
// 							IntoClause *into, ExplainState *es,
// 							const char *queryString, ParamListInfo params,
// 							QueryEnvironment *queryEnv);
static void ExplainPrintJIT(ExplainState *es, int jit_flags,
							JitInstrumentation *ji);
static void report_triggers(ResultRelInfo *rInfo, bool show_relname,
							ExplainState *es);
static double elapsed_time(instr_time *starttime);
static bool ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used);
static RecureState ExplainNode(PlanState *planstate, List *ancestors,
						const char *relationship, const char *plan_name,
						ExplainState *es,ExplainState *ces);
static void show_plan_tlist(PlanState *planstate, List *ancestors,
							ExplainState *es,HistorySlowPlanStat * hsp);
static void show_expression(Node *node, const char *qlabel,
							PlanState *planstate, List *ancestors,
							bool useprefix, ExplainState *es,HistorySlowPlanStat* hsp);
static void show_qual(List *qual, const char *qlabel,
					  PlanState *planstate, List *ancestors,
					  bool useprefix, ExplainState *es,HistorySlowPlanStat* hsp);
static void show_scan_qual(List *qual, const char *qlabel,
						   PlanState *planstate, List *ancestors,
						   ExplainState *es,HistorySlowPlanStat* hsp);
static void show_upper_qual(List *qual, const char *qlabel,
							PlanState *planstate, List *ancestors,
							ExplainState *es,HistorySlowPlanStat* hsp);
static void show_sort_keys(SortState *sortstate, List *ancestors,
						   ExplainState *es,HistorySlowPlanStat* hsp);
static void show_incremental_sort_keys(IncrementalSortState *incrsortstate,
									   List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp);
static void show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
								   ExplainState *es,HistorySlowPlanStat* hsp);
static void show_agg_keys(AggState *astate, List *ancestors,
						  ExplainState *es,HistorySlowPlanStat* hsp);
static void show_grouping_sets(PlanState *planstate, Agg *agg,
							   List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp);
static void show_grouping_set_keys(PlanState *planstate,
								   Agg *aggnode, Sort *sortnode,
								   List *context, bool useprefix,
								   List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp);
static void show_group_keys(GroupState *gstate, List *ancestors,
							ExplainState *es,HistorySlowPlanStat * hsp);
static void show_sort_group_keys(PlanState *planstate, const char *qlabel,
								 int nkeys, int nPresortedKeys, AttrNumber *keycols,
								 Oid *sortOperators, Oid *collations, bool *nullsFirst,
								 List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp);
static void show_sortorder_options(StringInfo buf, Node *sortexpr,
								   Oid sortOperator, Oid collation, bool nullsFirst,
								   HistorySlowPlanStat *hsp,GroupSortKey *gs_key);
static void show_tablesample(TableSampleClause *tsc, PlanState *planstate,
							 List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp);
static void show_sort_info(SortState *sortstate, ExplainState *es);
static void show_incremental_sort_info(IncrementalSortState *incrsortstate,
									   ExplainState *es);
static void show_hash_info(HashState *hashstate, ExplainState *es);
static void show_hashagg_info(AggState *hashstate, ExplainState *es);
static void show_tidbitmap_info(BitmapHeapScanState *planstate,
								ExplainState *es);
static void show_instrumentation_count(const char *qlabel, int which,
									   PlanState *planstate, ExplainState *es);
static void show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es);
static void show_eval_params(Bitmapset *bms_params, ExplainState *es);
static const char *explain_get_index_name(Oid indexId);
static void show_buffer_usage(ExplainState *es, const BufferUsage *usage,
							  bool planning);
static void show_wal_usage(ExplainState *es, const WalUsage *usage);
static void ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
									ExplainState *es,HistorySlowPlanStat* hsp);
static void ExplainScanTarget(Scan *plan, ExplainState *es,HistorySlowPlanStat *hsp);
static void ExplainModifyTarget(ModifyTable *plan, ExplainState *es,HistorySlowPlanStat *hsp);
static void ExplainTargetRel(Plan *plan, Index rti, ExplainState *es,HistorySlowPlanStat *hsp);
static void show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
								  ExplainState *es,HistorySlowPlanStat *hsp);
static void ExplainMemberNodes(PlanState **planstates, int nplans,
							   List *ancestors, ExplainState *es,ExplainState *ces);
static void ExplainMissingMembers(int nplans, int nchildren, ExplainState *es);
static RecureState ExplainSubPlans(List *plans, List *ancestors,
							const char *relationship, ExplainState *es,ExplainState *total_ces,HistorySlowPlanStat* hsp);
static void ExplainCustomChildren(CustomScanState *css,
								  List *ancestors, ExplainState *es,ExplainState *ces);
static ExplainWorkersState *ExplainCreateWorkersState(int num_workers);
static void ExplainOpenWorker(int n, ExplainState *es);
static void ExplainCloseWorker(int n, ExplainState *es);
static void ExplainFlushWorkersState(ExplainState *es);
static void ExplainProperty(const char *qlabel, const char *unit,
							const char *value, bool numeric, ExplainState *es);
static void ExplainOpenSetAsideGroup(const char *objtype, const char *labelname,
									 bool labeled, int depth, ExplainState *es);
static void ExplainSaveGroup(ExplainState *es, int depth, int *state_save);
static void ExplainRestoreGroup(ExplainState *es, int depth, int *state_save);
static void ExplainDummyGroup(const char *objtype, const char *labelname,
							  ExplainState *es);
static void ExplainXMLTag(const char *tagname, int flags, ExplainState *es);
static void ExplainJSONLineEnding(ExplainState *es);
static void ExplainYAMLLineStarting(ExplainState *es);
static void escape_yaml(StringInfo buf, const char *str);


static RecureState NewRecureState(){
	RecureState rs; 
	rs.canonical_str_ = makeStringInfo();
	rs.detail_str_ = makeStringInfo();
	rs.node_type_set_ = NULL;
	rs.cost_ = 0;
	rs.hps_ =  (HistorySlowPlanStat){0};
	return rs;
}

/**
 * append src into the tail of the dst
 * TODO: maybe we need type check for src & dst listcell
 */
static void push_node_type_set(List* dst, List* src){
	ListCell   *lst;
	foreach(lst, src){
		dst = lappend(dst,lst);
	}
}

/*
 * Create a new ExplainState struct initialized with default options.
 */
ExplainState *
NewFormatState(void)
{
	ExplainState *es = (ExplainState *) palloc0(sizeof(ExplainState));
	if(es == NULL){
		elog(stat_log_level,"new explain state failed");
		exit(-1);
	}
	es->format = EXPLAIN_FORMAT_JSON;
	es->verbose = true;
	es->analyze = true;
	es->costs = true;
    es->wal = false;
    es->summary = true;
    es->timing = true;
	/* Set default options (most fields can be left as zeroes). */
	es->costs = true;
	/* Prepare output buffer. */
	es->str = makeStringInfo();

	return es;
}

void FormatStateCopyStr(ExplainState* dst, ExplainState*src)
{
	*dst = *src;
	dst->str = makeStringInfo();
}

void FreeFormatState(ExplainState*es)
{
	assert(es == NULL);
	resetStringInfo(es->str);
	free(es);
	es = NULL;
}

/*
 * ExplainPrintSettings -
 *    Print summary of modified settings affecting query planning.
 */
static void
ExplainPrintSettings(ExplainState *es)
{
	int			num;
	struct config_generic **gucs;

	/* bail out if information about settings not requested */
	if (!es->settings)
		return;

	/* request an array of relevant settings */
	gucs = get_explain_guc_options(&num);


	StringInfoData str;

		/* In TEXT mode, print nothing if there are no options */
	if (num <= 0)
		return;

	initStringInfo(&str);

	for (int i = 0; i < num; i++)
	{
		char	   *setting;
		struct config_generic *conf = gucs[i];

		if (i > 0)
			appendStringInfoString(&str, ", ");

		setting = GetConfigOptionByName(conf->name, NULL, true);

		if (setting)
			appendStringInfo(&str, "%s = '%s'", conf->name, setting);
		else
			appendStringInfo(&str, "%s = NULL", conf->name);
	}

	FormatPropertyText("Settings", str.data, es);
}

/*
 * FormatPrintPlan -
 *	  convert a QueryDesc's plan tree to text and append it to es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Also, output formatting state
 * such as the indent level is assumed valid.  Plan-tree-specific fields
 * in *es are initialized here.
 *
 * NB: will not work on utility statements
 */
HistorySlowPlanStat FormatPrintPlan(ExplainState *es, ExplainState *ces,QueryDesc *queryDesc)
{
	Bitmapset  *rels_used = NULL; 
	PlanState  *ps;

	/* Set up ExplainState fields associated with this plan tree */
	Assert(queryDesc->plannedstmt != NULL);
	es->pstmt = queryDesc->plannedstmt;
	es->rtable = queryDesc->plannedstmt->rtable;
	ExplainPreScanNode(queryDesc->planstate, &rels_used);
	es->rtable_names = select_rtable_names_for_explain_format(es->rtable, rels_used);
	es->deparse_cxt = deparse_context_for_plan_tree_format(queryDesc->plannedstmt,
													es->rtable_names);
	es->printed_subplans = NULL;
	/*we alaways wish the type of explain is JSON currently*/
	/*
	 * Sometimes we mark a Gather node as "invisible", which means that it's
	 * not to be displayed in EXPLAIN output.  The purpose of this is to allow
	 * running regression tests with force_parallel_mode=regress to get the
	 * same results as running the same tests with force_parallel_mode=off.
	 * Such marking is currently only supported on a Gather at the top of the
	 * plan.  We skip that node, and we must also hide per-worker detail data
	 * further down in the plan tree.
	 */


	ps = queryDesc->planstate;
	if (IsA(ps, GatherState) && ((Gather *) ps->plan)->invisible)
	{
		ps = outerPlanState(ps);
		es->hide_workers = true;
	}
	
	RecureState ret = ExplainNode(ps, NIL, NULL, NULL, es , ces);

	return ret.hps_;
	/*
	 * If requested, include information about GUC parameters with values that
	 * don't match the built-in defaults.
	 */
	//ExplainPrintSettings(es);
}

/*
 * ExplainPrintTriggers -
 *	  convert a QueryDesc's trigger statistics to text and append it to
 *	  es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Other fields in *es are
 * initialized here.
 */
void
FormatPrintTriggers(ExplainState *es, QueryDesc *queryDesc)
{
	ResultRelInfo *rInfo;
	bool		show_relname;
	int			numrels = queryDesc->estate->es_num_result_relations;
	int			numrootrels = queryDesc->estate->es_num_root_result_relations;
	List	   *routerels;
	List	   *targrels;
	int			nr;
	ListCell   *l;

	routerels = queryDesc->estate->es_tuple_routing_result_relations;
	targrels = queryDesc->estate->es_trig_target_relations;

	FormatOpenGroup("Triggers", "Triggers", false, es);

	show_relname = (numrels > 1 || numrootrels > 0 ||
					routerels != NIL || targrels != NIL);
	rInfo = queryDesc->estate->es_result_relations;
	for (nr = 0; nr < numrels; rInfo++, nr++)
		report_triggers(rInfo, show_relname, es);

	rInfo = queryDesc->estate->es_root_result_relations;
	for (nr = 0; nr < numrootrels; rInfo++, nr++)
		report_triggers(rInfo, show_relname, es);

	foreach(l, routerels)
	{
		rInfo = (ResultRelInfo *) lfirst(l);
		report_triggers(rInfo, show_relname, es);
	}

	foreach(l, targrels)
	{
		rInfo = (ResultRelInfo *) lfirst(l);
		report_triggers(rInfo, show_relname, es);
	}

	FormatCloseGroup("Triggers", "Triggers", false, es);
}

/*
 * FormatPrintJITSummary -
 *    Print summarized JIT instrumentation from leader and workers
 */
void
FormatPrintJITSummary(ExplainState *es, QueryDesc *queryDesc)
{
	JitInstrumentation ji = {0};

	if (!(queryDesc->estate->es_jit_flags & PGJIT_PERFORM))
		return;

	/*
	 * Work with a copy instead of modifying the leader state, since this
	 * function may be called twice
	 */
	if (queryDesc->estate->es_jit)
		InstrJitAgg(&ji, &queryDesc->estate->es_jit->instr);

	/* If this process has done JIT in parallel workers, merge stats */
	if (queryDesc->estate->es_jit_worker_instr)
		InstrJitAgg(&ji, queryDesc->estate->es_jit_worker_instr);

	ExplainPrintJIT(es, queryDesc->estate->es_jit_flags, &ji);
}

/*
 * ExplainPrintJIT -
 *	  Append information about JITing to es->str.
 */
static void
ExplainPrintJIT(ExplainState *es, int jit_flags, JitInstrumentation *ji)
{
	instr_time	total_time;

	/* don't print information if no JITing happened */
	if (!ji || ji->created_functions == 0)
		return;

	/* calculate total time */
	INSTR_TIME_SET_ZERO(total_time);
	INSTR_TIME_ADD(total_time, ji->generation_counter);
	INSTR_TIME_ADD(total_time, ji->inlining_counter);
	INSTR_TIME_ADD(total_time, ji->optimization_counter);
	INSTR_TIME_ADD(total_time, ji->emission_counter);

	FormatOpenGroup("JIT", "JIT", true, es);

	
	FormatPropertyInteger("Functions", NULL, ji->created_functions, es);

	FormatOpenGroup("Options", "Options", true, es);
	FormatPropertyBool("Inlining", jit_flags & PGJIT_INLINE, es);
	FormatPropertyBool("Optimization", jit_flags & PGJIT_OPT3, es);
	FormatPropertyBool("Expressions", jit_flags & PGJIT_EXPR, es);
	FormatPropertyBool("Deforming", jit_flags & PGJIT_DEFORM, es);
	FormatCloseGroup("Options", "Options", true, es);

	if (es->analyze && es->timing)
	{
		FormatOpenGroup("Timing", "Timing", true, es);

		FormatPropertyFloat("Generation", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
								 3, es);
		FormatPropertyFloat("Inlining", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
								 3, es);
		FormatPropertyFloat("Optimization", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
								 3, es);
		FormatPropertyFloat("Emission", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
								 3, es);
		FormatPropertyFloat("Total", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(total_time),
								 3, es);

		FormatCloseGroup("Timing", "Timing", true, es);
	}

	FormatCloseGroup("JIT", "JIT", true, es);
}

/*
 * FormatQueryText -
 *	  add a "Query Text" node that contains the actual text of the query
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.
 *
 */
void
FormatQueryText(ExplainState *es, QueryDesc *queryDesc)
{
	if (queryDesc->sourceText)
		FormatPropertyText("Query Text", queryDesc->sourceText, es);
}

/*
 * report_triggers -
 *		report execution stats for a single relation's triggers
 */
static void
report_triggers(ResultRelInfo *rInfo, bool show_relname, ExplainState *es)
{
	int			nt;

	if (!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument)
		return;
	for (nt = 0; nt < rInfo->ri_TrigDesc->numtriggers; nt++)
	{
		Trigger    *trig = rInfo->ri_TrigDesc->triggers + nt;
		Instrumentation *instr = rInfo->ri_TrigInstrument + nt;
		char	   *relname;
		char	   *conname = NULL;

		/* Must clean up instrumentation state */
		InstrEndLoop(instr);

		/*
		 * We ignore triggers that were never invoked; they likely aren't
		 * relevant to the current query type.
		 */
		if (instr->ntuples == 0)
			continue;

		FormatOpenGroup("Trigger", NULL, true, es);

		relname = RelationGetRelationName(rInfo->ri_RelationDesc);
		if (OidIsValid(trig->tgconstraint))
			conname = get_constraint_name(trig->tgconstraint);

		/*
		 * In text format, we avoid printing both the trigger name and the
		 * constraint name unless VERBOSE is specified.  In non-text formats
		 * we just print everything.
		 */
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->verbose || conname == NULL)
				appendStringInfo(es->str, "Trigger %s", trig->tgname);
			else
				appendStringInfoString(es->str, "Trigger");
			if (conname)
				appendStringInfo(es->str, " for constraint %s", conname);
			if (show_relname)
				appendStringInfo(es->str, " on %s", relname);
			if (es->timing)
				appendStringInfo(es->str, ": time=%.3f calls=%.0f\n",
								 1000.0 * instr->total, instr->ntuples);
			else
				appendStringInfo(es->str, ": calls=%.0f\n", instr->ntuples);
		}
		else
		{
			FormatPropertyText("Trigger Name", trig->tgname, es);
			if (conname)
				FormatPropertyText("Constraint Name", conname, es);
			FormatPropertyText("Relation", relname, es);
			if (es->timing)
				FormatPropertyFloat("Time", "ms", 1000.0 * instr->total, 3,
									 es);
			FormatPropertyFloat("Calls", NULL, instr->ntuples, 0, es);
		}

		if (conname)
			pfree(conname);

		FormatCloseGroup("Trigger", NULL, true, es);
	}
}

/* Compute elapsed time in seconds since given timestamp */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_DOUBLE(endtime);
}

/*
 * ExplainPreScanNode -
 *	  Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 * This ensures that we don't confusingly assign un-suffixed aliases to RTEs
 * that never appear in the EXPLAIN output (such as inheritance parents).
 */
static bool
ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_ForeignScan:
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
			break;
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
										 ((CustomScan *) plan)->custom_relids);
			break;
		case T_ModifyTable:
			*rels_used = bms_add_member(*rels_used,
										((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
											((ModifyTable *) plan)->exclRelRTI);
			break;
		case T_Append:
			*rels_used = bms_add_members(*rels_used,
										 ((Append *) plan)->apprelids);
			break;
		case T_MergeAppend:
			*rels_used = bms_add_members(*rels_used,
										 ((MergeAppend *) plan)->apprelids);
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, ExplainPreScanNode, rels_used);
}

/*
 * ExplainNode -
 *	  Appends a description of a plan tree to es->str
 *
 * planstate points to the executor state node for the current plan node.
 * We need to work from a PlanState node, not just a Plan node, in order to
 * get at the instrumentation data (if any) as well as the list of subplans.
 *
 * ancestors is a list of parent Plan and SubPlan nodes, most-closely-nested
 * first.  These are needed in order to interpret PARAM_EXEC Params.
 *
 * relationship describes the relationship of this plan node to its parent
 * (eg, "Outer", "Inner"); it can be null at top level.  plan_name is an
 * optional name to be attached to the node.
 *
 * In text format, es->indent is controlled in this function since we only
 * want it to change at plan-node boundaries (but a few subroutines will
 * transiently increment it).  In non-text formats, es->indent corresponds
 * to the nesting depth of logical output groups, and therefore is controlled
 * by FormatOpenGroup/FormatCloseGroup.
 */
static RecureState
ExplainNode(PlanState *planstate, List *ancestors,
			const char *relationship, const char *plan_name,
			ExplainState *total_es,ExplainState *total_ces)
{
	Plan	   *plan = planstate->plan;
	const char *pname;			/* node type name for text output */
	const char *sname;			/* node type name for non-text output */
	const char *strategy = NULL;
	const char *partialmode = NULL;
	const char *operation = NULL;
	const char *custom_name = NULL;
    double cumulate_cost = 0;
	ExplainWorkersState *save_workers_state = total_es->workers_state;
	
	bool		haschildren;
	HistorySlowPlanStat hsp = HISTORY_SLOW_PLAN_STAT__INIT;

	/*
	 * Prepare per-worker output buffers, if needed.  We'll append the data in
	 * these to the main output string further down.
	 */
	if (planstate->worker_instrument && total_es->analyze && !total_es->hide_workers)
		total_es->workers_state = ExplainCreateWorkersState(planstate->worker_instrument->num_workers);
	else
		total_es->workers_state = NULL;

	/* Identify plan node type, and print generic details */
	switch (nodeTag(plan))
	{
		case T_Result:
			pname = sname = "Result";
			break;
		case T_ProjectSet:
			pname = sname = "ProjectSet";
			break;
		case T_ModifyTable:
			sname = "ModifyTable";
			switch (((ModifyTable *) plan)->operation)
			{
				case CMD_INSERT:
					pname = operation = "Insert";
					break;
				case CMD_UPDATE:
					pname = operation = "Update";
					break;
				case CMD_DELETE:
					pname = operation = "Delete";
					break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_Append:
			pname = sname = "Append";
			break;
		case T_MergeAppend:
			pname = sname = "Merge Append";
			break;
		case T_RecursiveUnion:
			pname = sname = "Recursive Union";
			break;
		case T_BitmapAnd:
			pname = sname = "BitmapAnd";
			break;
		case T_BitmapOr:
			pname = sname = "BitmapOr";
			break;
		case T_NestLoop:
			pname = sname = "Nested Loop";
			break;
		case T_MergeJoin:
			pname = "Merge";	/* "Join" gets added by jointype switch */
			sname = "Merge Join";
			break;
		case T_HashJoin:
			pname = "Hash";		/* "Join" gets added by jointype switch */
			sname = "Hash Join";
			break;
		case T_SeqScan:
			pname = sname = "Seq Scan";
			break;
		case T_SampleScan:
			pname = sname = "Sample Scan";
			break;
		case T_Gather:
			pname = sname = "Gather";
			break;
		case T_GatherMerge:
			pname = sname = "Gather Merge";
			break;
		case T_IndexScan:
			pname = sname = "Index Scan";
			break;
		case T_IndexOnlyScan:
			pname = sname = "Index Only Scan";
			break;
		case T_BitmapIndexScan:
			pname = sname = "Bitmap Index Scan";
			break;
		case T_BitmapHeapScan:
			pname = sname = "Bitmap Heap Scan";
			break;
		case T_TidScan:
			pname = sname = "Tid Scan";
			break;
		case T_SubqueryScan:
			pname = sname = "Subquery Scan";
			break;
		case T_FunctionScan:
			pname = sname = "Function Scan";
			break;
		case T_TableFuncScan:
			pname = sname = "Table Function Scan";
			break;
		case T_ValuesScan:
			pname = sname = "Values Scan";
			break;
		case T_CteScan:
			pname = sname = "CTE Scan";
			break;
		case T_NamedTuplestoreScan:
			pname = sname = "Named Tuplestore Scan";
			break;
		case T_WorkTableScan:
			pname = sname = "WorkTable Scan";
			break;
		case T_ForeignScan:
			sname = "Foreign Scan";
			switch (((ForeignScan *) plan)->operation)
			{
				case CMD_SELECT:
					pname = "Foreign Scan";
					operation = "Select";
					break;
				case CMD_INSERT:
					pname = "Foreign Insert";
					operation = "Insert";
					break;
				case CMD_UPDATE:
					pname = "Foreign Update";
					operation = "Update";
					break;
				case CMD_DELETE:
					pname = "Foreign Delete";
					operation = "Delete";
					break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_CustomScan:
			sname = "Custom Scan";
			custom_name = ((CustomScan *) plan)->methods->CustomName;
			if (custom_name)
				pname = psprintf("Custom Scan (%s)", custom_name);
			else
				pname = sname;
			break;
		case T_Material:
			pname = sname = "Materialize";
			break;
		case T_Sort:
			pname = sname = "Sort";
			break;
		case T_IncrementalSort:
			pname = sname = "Incremental Sort";
			break;
		case T_Group:
			pname = sname = "Group";
			break;
		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;

				sname = "Aggregate";
				switch (agg->aggstrategy)
				{
					case AGG_PLAIN:
						pname = "Aggregate";
						strategy = "Plain";
						break;
					case AGG_SORTED:
						pname = "GroupAggregate";
						strategy = "Sorted";
						break;
					case AGG_HASHED:
						pname = "HashAggregate";
						strategy = "Hashed";
						break;
					case AGG_MIXED:
						pname = "MixedAggregate";
						strategy = "Mixed";
						break;
					default:
						pname = "Aggregate ???";
						strategy = "???";
						break;
				} 

				if (DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
				{
					partialmode = "Partial";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else if (DO_AGGSPLIT_COMBINE(agg->aggsplit))
				{
					partialmode = "Finalize";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else
					partialmode = "Simple";
			}
			break;
		case T_WindowAgg:
			pname = sname = "WindowAgg";
			break;
		case T_Unique:
			pname = sname = "Unique";
			break;
		case T_SetOp:
			sname = "SetOp";
			switch (((SetOp *) plan)->strategy)
			{
				case SETOP_SORTED:
					pname = "SetOp";
					strategy = "Sorted";
					break;
				case SETOP_HASHED:
					pname = "HashSetOp";
					strategy = "Hashed";
					break;
				default:
					pname = "SetOp ???";
					strategy = "???";
					break;
			}
			break;
		case T_LockRows:
			pname = sname = "LockRows";
			break;
		case T_Limit:
			pname = sname = "Limit";
			break;
		case T_Hash:
			pname = sname = "Hash";
			break;
		default:
			pname = sname = "???";
			break;
	}
	
	ExplainState* es = NewFormatState();
	*es = *total_es;
	es->str = makeStringInfo();
	
	ExplainState* ces = NewFormatState();
	*ces = *total_es;
	ces->str = makeStringInfo();

	ExplainState* cn_es = NewFormatState();
	*cn_es = *total_es;
	cn_es->str = makeStringInfo();

	FormatOpenGroup("Plan","Plan",true, es);
	FormatOpenGroup("Plan","Plan",true, ces);
	FormatOpenGroup("Plan","Plan",true, cn_es);

	/*just note physical node type for ces*/
	FormatPropertyText("Node Type",sname,cn_es);
	FormatPropertyText("Node Type",sname,ces);
	FormatPropertyText("Node Type",sname,es);

	hsp.node_type = sname;
	hsp.node_tag = (int)(nodeTag(plan));

	if (strategy){
		FormatPropertyText("Strategy", strategy, es);
		FormatPropertyText("Strategy", strategy, ces);
		FormatPropertyText("Strategy", strategy, cn_es);
		hsp.strategy = strategy;
	}
	if (partialmode){
		FormatPropertyText("Partial Mode", partialmode, es);
		FormatPropertyText("Partial Mode", partialmode, ces);
		FormatPropertyText("Partial Mode", partialmode, cn_es);
		hsp.partial_mode = partialmode;
	}
	if (operation){
		FormatPropertyText("Operation", operation, es);
		FormatPropertyText("Operation", operation, ces);
		FormatPropertyText("Operation", operation, cn_es);
		hsp.operation = operation;
	}
	if (relationship){
		FormatPropertyText("Parent Relationship", relationship, es);
		FormatPropertyText("Parent Relationship", relationship, ces);
		FormatPropertyText("Parent Relationship", relationship, cn_es);
		hsp.relationship = relationship;
	}
	if (plan_name){
		FormatPropertyText("Subplan Name", plan_name, es);
		FormatPropertyText("Subplan Name", plan_name, ces);
		FormatPropertyText("Subplan Name", plan_name, cn_es);
		hsp.sub_plan_name = plan_name;
	}

	if (custom_name){
		FormatPropertyText("Custom Plan Provider", custom_name, es);
		FormatPropertyText("Custom Plan Provider", custom_name, ces);
		FormatPropertyText("Custom Plan Provider", custom_name, cn_es);
	}

	FormatPropertyBool("Parallel Aware", plan->parallel_aware, es);
	FormatPropertyBool("Parallel Aware", plan->parallel_aware, ces);
	FormatPropertyBool("Parallel Aware", plan->parallel_aware, cn_es);

	switch (nodeTag(plan))
	{
		/*scan*/
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
			ExplainScanTarget((Scan *) plan, es, &hsp);
			ExplainScanTarget((Scan *) plan, ces, &hsp);
			ExplainScanTarget((Scan *) plan, cn_es, &hsp);
			break;
		case T_ForeignScan:
		case T_CustomScan:
			if (((Scan *) plan)->scanrelid > 0)
				ExplainScanTarget((Scan *) plan, es,&hsp);
				ExplainScanTarget((Scan *) plan, ces,&hsp);
				ExplainScanTarget((Scan *) plan, cn_es, &hsp);
			break;
		case T_IndexScan:
			{
				IndexScan  *indexscan = (IndexScan *) plan;

				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										es,&hsp);
				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										ces,&hsp);	
				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										cn_es,&hsp);				

				ExplainScanTarget((Scan *) indexscan, es,&hsp);
				ExplainScanTarget((Scan *) indexscan, ces,&hsp);
				ExplainScanTarget((Scan *) indexscan, cn_es,&hsp);
			}
			break;
		case T_IndexOnlyScan:
			{
				IndexOnlyScan *indexonlyscan = (IndexOnlyScan *) plan;

				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										es,&hsp);
				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										ces,&hsp);
				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										cn_es,&hsp);

				ExplainScanTarget((Scan *) indexonlyscan, es, &hsp);
				ExplainScanTarget((Scan *) indexonlyscan, ces, &hsp);
				ExplainScanTarget((Scan *) indexonlyscan, cn_es, &hsp);
			}
			break;
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmapindexscan = (BitmapIndexScan *) plan;
				const char *indexname =
				explain_get_index_name(bitmapindexscan->indexid);

				FormatPropertyText("Index Name", indexname, es);
			}
			break;
		/*insert,update,delete.merge*/
		case T_ModifyTable:
			ExplainModifyTarget((ModifyTable *) plan, es,&hsp);
			break;
		/*join*/
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{ 
				const char *jointype;

				switch (((Join *) plan)->jointype)
				{
					case JOIN_INNER:
						jointype = "Inner";
						break;
					case JOIN_LEFT:
						jointype = "Left";
						break;
					case JOIN_FULL:
						jointype = "Full";
						break;
					case JOIN_RIGHT:
						jointype = "Right";
						break;
					case JOIN_SEMI:
						jointype = "Semi";
						break;
					case JOIN_ANTI:
						jointype = "Anti";
						break;
					default:
						jointype = "???";
						break;
				}
				FormatPropertyText("Join Type", jointype, es);
				hsp.join_type = jointype;
			}
			break;
		case T_SetOp:
			{
				const char *setopcmd;

				switch (((SetOp *) plan)->cmd)
				{
					case SETOPCMD_INTERSECT:
						setopcmd = "Intersect";
						break;
					case SETOPCMD_INTERSECT_ALL:
						setopcmd = "Intersect All";
						break;
					case SETOPCMD_EXCEPT:
						setopcmd = "Except";
						break;
					case SETOPCMD_EXCEPT_ALL:
						setopcmd = "Except All";
						break;
					default:
						setopcmd = "???";
						break;
				}
				FormatPropertyText("Command", setopcmd, es);
			}
			break;
		default:
			break;
	}

	if (es->costs)
	{
		FormatPropertyFloat("Startup Cost", NULL, plan->startup_cost,
								 2, es);
		FormatPropertyFloat("Total Cost", NULL, plan->total_cost,
								 2, es);
		FormatPropertyFloat("Plan Rows", NULL, plan->plan_rows,
								 0, es);
		FormatPropertyInteger("Plan Width", NULL, plan->plan_width,
								   es);
		hsp.estimate_plan_width = plan->plan_width;
	}

	/*
	 * We have to forcibly clean up the instrumentation state because we
	 * haven't done ExecutorEnd yet.  This is pretty grotty ...
	 *
	 * Note: contrib/auto_explain could cause instrumentation to be set up
	 * even though we didn't ask for it here.  Be careful not to print any
	 * instrumentation results the user didn't ask for.  But we do the
	 * InstrEndLoop call anyway, if possible, to reduce the number of cases
	 * auto_explain has to contend with.
	 */
	if (planstate->instrument)
		InstrEndLoop(planstate->instrument);

	if (es->analyze &&
		planstate->instrument && planstate->instrument->nloops > 0)
	{
		double		nloops = planstate->instrument->nloops;
		double		startup_ms = 1000.0 * planstate->instrument->startup / nloops;
		double		total_ms = 1000.0 * planstate->instrument->total / nloops;
		double		rows = planstate->instrument->ntuples / nloops;

		if (es->timing)
		{
			FormatPropertyFloat("Actual Startup Time", "s", startup_ms,
									 3, es);
			hsp.actual_start_up = startup_ms;
			FormatPropertyFloat("Actual Total Time", "s", total_ms,
									 3, es);
			hsp.actual_total = total_ms;
            cumulate_cost += total_ms;
		}
		FormatPropertyFloat("Actual Rows", NULL, rows, 0, es);
		hsp.actual_rows = rows;
		FormatPropertyFloat("Actual Loops", NULL, nloops, 0, es);
		hsp.actual_nloops = nloops;
	}

	/* prepare per-worker general execution details */
	if (es->workers_state && es->verbose)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;

		for (int n = 0; n < w->num_workers; n++)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;
			double		startup_ms;
			double		total_ms;
			double		rows;

			if (nloops <= 0)
				continue;
			startup_ms = 1000.0 * instrument->startup / nloops;
			total_ms = 1000.0 * instrument->total / nloops;
			rows = instrument->ntuples / nloops;

			ExplainOpenWorker(n, es);
			if (es->timing)
			{
				FormatPropertyFloat("Actual Startup Time", "ms",
										 startup_ms, 3, es);
				FormatPropertyFloat("Actual Total Time", "ms",
										 total_ms, 3, es);
				hsp.actual_start_up = startup_ms;
				hsp.actual_total = total_ms;
			}
            cumulate_cost = Max(total_ms,cumulate_cost);
			FormatPropertyFloat("Actual Rows", NULL, rows, 0, es);
			hsp.actual_rows = rows;
			FormatPropertyFloat("Actual Loops", NULL, nloops, 0, es);
			hsp.actual_nloops = nloops;
			ExplainCloseWorker(n, es);
		}
	}

	/* target list */
	if (es->verbose){
		show_plan_tlist(planstate, ancestors, es, &hsp);
	}
	
	/* unique join */
	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* try not to be too chatty about this in text mode */
			if (es->format != EXPLAIN_FORMAT_TEXT ||
				(es->verbose && ((Join *) plan)->inner_unique)){
				FormatPropertyBool("Inner Unique",((Join *) plan)->inner_unique,es);
				hsp.inner_unique = ((Join *) plan)->inner_unique;
			}

			break;
		default:
			break;
	}

	/* quals, sort keys, etc */
	switch (nodeTag(plan))
	{
		case T_IndexScan:
			hsp.cur_expr_tag = PRED_TYPE_TAG__INDEX_COND;
			show_scan_qual(((IndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es,&hsp);
			if (((IndexScan *) plan)->indexqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__ORDER_BY;
			show_scan_qual(((IndexScan *) plan)->indexorderbyorig,
						   "Order By", planstate, ancestors, es,&hsp);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_IndexOnlyScan:
			hsp.cur_expr_tag = PRED_TYPE_TAG__INDEX_COND;
			show_scan_qual(((IndexOnlyScan *) plan)->indexqual,
						   "Index Cond", planstate, ancestors, es,&hsp);
			if (((IndexOnlyScan *) plan)->recheckqual)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__ORDER_BY;
			show_scan_qual(((IndexOnlyScan *) plan)->indexorderby,
						   "Order By", planstate, ancestors, es,&hsp);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				FormatPropertyFloat("Heap Fetches", NULL,
									 planstate->instrument->ntuples2, 0, es);
			break;
		case T_BitmapIndexScan:
			show_scan_qual(((BitmapIndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es,&hsp);
			break;
		case T_BitmapHeapScan:

			hsp.cur_expr_tag = PRED_TYPE_TAG__RECHECK_COND;
			show_scan_qual(((BitmapHeapScan *) plan)->bitmapqualorig,
						   "Recheck Cond", planstate, ancestors, es,&hsp);
			if (((BitmapHeapScan *) plan)->bitmapqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				show_tidbitmap_info((BitmapHeapScanState *) planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_SampleScan:
			show_tablesample(((SampleScan *) plan)->tablesample,
							 planstate, ancestors, es,&hsp);
			/* fall through to print additional fields the same as SeqScan */
			/* FALLTHROUGH */
		case T_SeqScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_SubqueryScan:
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_Gather:
			{
				Gather	   *gather = (Gather *) plan;

				hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				FormatPropertyInteger("Workers Planned", NULL,
									   gather->num_workers, es);

				/* Show params evaluated at gather node */
				if (gather->initParam)
					show_eval_params(gather->initParam, es);

				if (es->analyze)
				{
					int			nworkers;

					nworkers = ((GatherState *) planstate)->nworkers_launched;
					FormatPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}

				if (gather->single_copy || es->format != EXPLAIN_FORMAT_TEXT)
					FormatPropertyBool("Single Copy", gather->single_copy, es);
				hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			}
			break;
		case T_GatherMerge:
			{
				GatherMerge *gm = (GatherMerge *) plan;

				hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				FormatPropertyInteger("Workers Planned", NULL,
									   gm->num_workers, es);

				/* Show params evaluated at gather-merge node */
				if (gm->initParam)
					show_eval_params(gm->initParam, es);

				if (es->analyze)
				{
					int			nworkers;

					nworkers = ((GatherMergeState *) planstate)->nworkers_launched;
					FormatPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}
				hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			}
			break;
		case T_FunctionScan:
			if (es->verbose)
			{
				List	   *fexprs = NIL;
				ListCell   *lc;

				foreach(lc, ((FunctionScan *) plan)->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

					fexprs = lappend(fexprs, rtfunc->funcexpr);
				}
				/* We rely on show_expression to insert commas as needed */
				show_expression((Node *) fexprs,
								"Function Call", planstate, ancestors,
								es->verbose, es,&hsp);
			}
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_TableFuncScan:
			if (es->verbose)
			{
				TableFunc  *tablefunc = ((TableFuncScan *) plan)->tablefunc;

				show_expression((Node *) tablefunc,
								"Table Function Call", planstate, ancestors,
								es->verbose, es,&hsp);
			}
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_TidScan:
			{
				/*
				 * The tidquals list has OR semantics, so be sure to show it
				 * as an OR condition.
				 */
				List	   *tidquals = ((TidScan *) plan)->tidquals;
				if (list_length(tidquals) > 1)
					tidquals = list_make1(make_orclause(tidquals));
				hsp.cur_expr_tag = PRED_TYPE_TAG__TID_COND;
				show_scan_qual(tidquals, "TID Cond", planstate, ancestors, es,&hsp);
				hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			}
			break;
		case T_ForeignScan:
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			show_foreignscan_info((ForeignScanState *) planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_CustomScan:
			{
				CustomScanState *css = (CustomScanState *) planstate;
				hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				if (css->methods->ExplainCustomScan)
					css->methods->ExplainCustomScan(css, ancestors, es);
				hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			}
			break;
		case T_NestLoop:
			hsp.cur_expr_tag = PRED_TYPE_TAG__JOIN_FILTER;
			show_upper_qual(((NestLoop *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es,&hsp);
			if (((NestLoop *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;	
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_MergeJoin:
			hsp.cur_expr_tag = PRED_TYPE_TAG__JOIN_COND;
			show_upper_qual(((MergeJoin *) plan)->mergeclauses,
							"Merge Cond", planstate, ancestors, es,&hsp);
			hsp.cur_expr_tag = PRED_TYPE_TAG__JOIN_FILTER;
			show_upper_qual(((MergeJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es,&hsp);
			if (((MergeJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;							   
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_HashJoin:{
			hsp.cur_expr_tag = PRED_TYPE_TAG__JOIN_COND;
			show_upper_qual(((HashJoin *) plan)->hashclauses,
							"Hash Cond", planstate, ancestors, es,&hsp);
			hsp.cur_expr_tag = PRED_TYPE_TAG__JOIN_FILTER;
			show_upper_qual(((HashJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es,&hsp);
			if (((HashJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			}break;
		case T_Agg:
			show_agg_keys(castNode(AggState, planstate), ancestors, es,&hsp);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			show_hashagg_info((AggState *) planstate, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_Group:
			show_group_keys(castNode(GroupState, planstate), ancestors, es,&hsp);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_Sort:
			show_sort_keys(castNode(SortState, planstate), ancestors, es,&hsp);
			show_sort_info(castNode(SortState, planstate), es);
			break;
		case T_IncrementalSort:
			show_incremental_sort_keys(castNode(IncrementalSortState, planstate),
									   ancestors, es,&hsp);
			show_incremental_sort_info(castNode(IncrementalSortState, planstate),
									   es);
			break;
		case T_MergeAppend:
			show_merge_append_keys(castNode(MergeAppendState, planstate),
								   ancestors, es,&hsp);
			break;
		case T_Result:
			hsp.cur_expr_tag = PRED_TYPE_TAG__ONE_TIME_FILTER;
			show_upper_qual((List *) ((Result *) plan)->resconstantqual,
							"One-Time Filter", planstate, ancestors, es,&hsp);
			hsp.cur_expr_tag = PRED_TYPE_TAG__FILTER;
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es,&hsp);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			hsp.cur_expr_tag = PRED_TYPE_TAG__NONE;
			break;
		case T_ModifyTable:
			show_modifytable_info(castNode(ModifyTableState, planstate), ancestors,
								  es, &hsp);
			break;
		case T_Hash:
			show_hash_info(castNode(HashState, planstate), es);
			break;
		default:
			break;
	}

	/*
	 * Prepare per-worker JIT instrumentation.  As with the overall JIT
	 * summary, this is printed only if printing costs is enabled.
	 */
	if (es->workers_state && es->costs && es->verbose)
	{
		SharedJitInstrumentation *w = planstate->worker_jit_instrument;

		if (w)
		{
			for (int n = 0; n < w->num_workers; n++)
			{
				ExplainOpenWorker(n, es);
				ExplainPrintJIT(es, planstate->state->es_jit_flags,
								&w->jit_instr[n]);
				ExplainCloseWorker(n, es);
			}
		}
	}

	/* Show buffer/WAL usage */
	if (es->buffers && planstate->instrument)
		show_buffer_usage(es, &planstate->instrument->bufusage, false);
	if (es->wal && planstate->instrument)
		show_wal_usage(es, &planstate->instrument->walusage);

	/* Prepare per-worker buffer/WAL usage */
	if (es->workers_state && (es->buffers || es->wal) && es->verbose)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;

		for (int n = 0; n < w->num_workers; n++)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;

			if (nloops <= 0)
				continue;

			ExplainOpenWorker(n, es);
			if (es->buffers)
				show_buffer_usage(es, &instrument->bufusage, false);
			if (es->wal)
				show_wal_usage(es, &instrument->walusage);
			ExplainCloseWorker(n, es);
		}
	}

	/* Show per-worker details for this plan node, then pop that stack */
	if (es->workers_state)
		ExplainFlushWorkersState(es);
	es->workers_state = save_workers_state;

	/*
	 * If partition pruning was done during executor initialization, the
	 * number of child plans we'll display below will be less than the number
	 * of subplans that was specified in the plan.  To make this a bit less
	 * mysterious, emit an indication that this happened.  Note that this
	 * field is emitted now because we want it to be a property of the parent
	 * node; it *cannot* be emitted within the Plans sub-node we'll open next.
	 */
	switch (nodeTag(plan))
	{
		case T_Append:
			ExplainMissingMembers(((AppendState *) planstate)->as_nplans,
								  list_length(((Append *) plan)->appendplans),
								  es);
			break;
		case T_MergeAppend:
			ExplainMissingMembers(((MergeAppendState *) planstate)->ms_nplans,
								  list_length(((MergeAppend *) plan)->mergeplans),
								  es);
			break;
		default:
			break;
	}
	/* Get ready to display the child plans */
	haschildren = planstate->initPlan ||
		outerPlanState(planstate) ||
		innerPlanState(planstate) ||
		IsA(plan, ModifyTable) ||
		IsA(plan, Append) ||
		IsA(plan, MergeAppend) ||
		IsA(plan, BitmapAnd) ||
		IsA(plan, BitmapOr) ||
		IsA(plan, SubqueryScan) ||
		(IsA(planstate, CustomScanState) &&
		 ((CustomScanState *) planstate)->custom_ps != NIL) ||
		planstate->subPlan;

	if (haschildren)
	{
		FormatOpenGroup("Plans", "Plans", false, es);
		FormatOpenGroup("Plans", "Plans", false, ces);
		FormatOpenGroup("Plans", "Plans", false, cn_es);
		/* Pass current Plan as head of ancestors list for children */
		ancestors = lcons(plan, ancestors);
		/**
		 * we should storage subplans and common child plans in separate spaces
		 */
		size_t child_size = 0;
		if(outerPlanState(planstate) && innerPlanState(planstate)){
			child_size = 2;
		}else if(outerPlanState(planstate) || innerPlanState(planstate)){
			child_size = 1;
		}
		hsp.childs = (HistorySlowPlanStat**)malloc(sizeof(HistorySlowPlanStat*)*child_size);
		hsp.n_childs = child_size;
		
		size_t sub_plan_size = 0;
		if(planstate->subPlan){
			size_t sub_size = list_length(planstate->subPlan);
			sub_plan_size+=sub_size;
		}
		if(planstate->initPlan){
			size_t sub_size = list_length(planstate->initPlan);
			sub_plan_size+=sub_size;
		}
		hsp.subplans =(HistorySlowPlanStat**)malloc(sizeof(HistorySlowPlanStat*)*sub_plan_size);
		hsp.n_subplans = sub_plan_size;
	}else{
		hsp.n_childs = 0;
	}

	hsp.child_idx = 0;
	hsp.subplan_idx = -1;

	RecureState rs = NewRecureState();
	rs.node_type_set_ = lappend_int(rs.node_type_set_,(void*)nodeTag(plan));
	/* initPlan-s */
	if (planstate->initPlan){
        RecureState ret = ExplainSubPlans(planstate->initPlan, ancestors, "InitPlan", total_es,total_ces,&hsp);
		cumulate_cost += ret.cost_;
		appendStringInfoString(es->str,ret.detail_str_->data);
		appendStringInfoString(ces->str,ret.canonical_str_->data);
		appendStringInfoString(cn_es->str,ret.canonical_str_->data);
		push_node_type_set(rs.node_type_set_,ret.node_type_set_);
    }
	/* lefttree */
	if (outerPlanState(planstate)){
        RecureState ret =  ExplainNode(outerPlanState(planstate), ancestors,
					"Outer", NULL, total_es,total_ces);
        cumulate_cost += ret.cost_;
		appendStringInfoString(es->str,ret.detail_str_->data);
		appendStringInfoString(ces->str,ret.canonical_str_->data);
		push_node_type_set(rs.node_type_set_,ret.node_type_set_);

		HistorySlowPlanStat* child = malloc(sizeof(HistorySlowPlanStat));
		history_slow_plan_stat__init(child);
		*child = ret.hps_;
		hsp.childs[hsp.child_idx++] = child;
    }
	/* righttree */
	if (innerPlanState(planstate)){
        RecureState ret = ExplainNode(innerPlanState(planstate), ancestors,
					"Inner", NULL, total_es,total_ces);
        cumulate_cost += ret.cost_;
		appendStringInfoString(es->str,ret.detail_str_->data);
		appendStringInfoString(ces->str,ret.canonical_str_->data);
		push_node_type_set(rs.node_type_set_,ret.node_type_set_);

		HistorySlowPlanStat* child = malloc(sizeof(HistorySlowPlanStat));
		history_slow_plan_stat__init(child);
		*child = ret.hps_;
		hsp.childs[hsp.child_idx++] = child;
    }

	/**
	 * TODO: we need update here, cn_es not note these node info. 
	 */
	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			ExplainMemberNodes(((ModifyTableState *) planstate)->mt_plans,
							   ((ModifyTableState *) planstate)->mt_nplans,
							   ancestors, es,ces);
			break;
		case T_Append:
			ExplainMemberNodes(((AppendState *) planstate)->appendplans,
							   ((AppendState *) planstate)->as_nplans,
							   ancestors, es,ces);
			break;
		case T_MergeAppend:
			ExplainMemberNodes(((MergeAppendState *) planstate)->mergeplans,
							   ((MergeAppendState *) planstate)->ms_nplans,
							   ancestors, es,ces);
			break;
		case T_BitmapAnd:
			ExplainMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
							   ((BitmapAndState *) planstate)->nplans,
							   ancestors, es,ces);
			break;
		case T_BitmapOr:
			ExplainMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
							   ((BitmapOrState *) planstate)->nplans,
							   ancestors, es,ces);
			break;
		case T_SubqueryScan:{
			RecureState ret = ExplainNode(((SubqueryScanState *) planstate)->subplan, ancestors,
						"Subquery", NULL, es,ces);
			cumulate_cost += ret.cost_;

			}break;
		case T_CustomScan:
			ExplainCustomChildren((CustomScanState *) planstate,
								  ancestors, es,ces);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan){
		/**
		 * FIX: while processing sub/init plan,we need precess its childs in ExplainSubplans, otherwise,it 
		 * will lead a empty node exists in finall plan,it is unecessily;
		 */
		RecureState ret = ExplainSubPlans(planstate->subPlan, ancestors, "SubPlan", es,ces,&hsp);
		cumulate_cost += ret.cost_;
		appendStringInfoString(es->str,ret.detail_str_->data);
		appendStringInfoString(ces->str,ret.canonical_str_->data);
		appendStringInfoString(cn_es->str,ret.canonical_str_->data);
		push_node_type_set(rs.node_type_set_,ret.node_type_set_);
	}

	/* end of child plans */
	if (haschildren){
		ancestors = list_delete_first(ancestors);
		FormatCloseGroup("Plans", "Plans", false, es);
		FormatCloseGroup("Plans", "Plans", false, ces);
		FormatCloseGroup("Plans", "Plans", false, cn_es);
	}

	/**
	 * note the history stat plan for storaging
	 * TODO: maybe we need simultaneously retain both standardized and non standardized plans
	 */
	hsp.json_plan = es->str->data;
	hsp.canonical_json_plan = ces->str->data;
	hsp.canonical_node_json_plan = cn_es->str->data;
	hsp.sub_cost = cumulate_cost;
	/*mark the recurestat for parent to use,we need a deep copy for infostring*/
	FormatCloseGroup("Plan",relationship ? NULL : "Plan",true, es);
	FormatCloseGroup("Plan",relationship ? NULL : "Plan",true, ces);
	FormatCloseGroup("Plan",relationship ? NULL : "Plan",true, cn_es);
	//ExplainEndOutput(es);
	
	rs.cost_ = cumulate_cost;
	appendStringInfoString(rs.detail_str_,es->str->data);
	appendStringInfoString(rs.canonical_str_,ces->str->data);
	rs.hps_ = hsp;

	if(debug){
		//std::cout<<stat_log_level,es->str->data<<std::endl;
		//elog(LOG, "explain plan:%s",es->str->data);
	}

	/*here we can't free es, hsp still use its data*/
	/*FreeFormatState(es);*/
	return rs;
}

/*
 * Show the targetlist of a plan node
 */
static void
show_plan_tlist(PlanState *planstate, List *ancestors, ExplainState *es, HistorySlowPlanStat* hsp)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	bool		useprefix;
	ListCell   *lc;

	/* No work if empty tlist (this occurs eg in bitmap indexscans) */
	if (plan->targetlist == NIL)
		return;
	/* The tlist of an Append isn't real helpful, so suppress it */
	if (IsA(plan, Append))
		return;
	/* Likewise for MergeAppend and RecursiveUnion */
	if (IsA(plan, MergeAppend))
		return;
	if (IsA(plan, RecursiveUnion))
		return;

	/*
	 * Likewise for ForeignScan that executes a direct INSERT/UPDATE/DELETE
	 *
	 * Note: the tlist for a ForeignScan that executes a direct INSERT/UPDATE
	 * might contain subplan output expressions that are confusing in this
	 * context.  The tlist for a ForeignScan that executes a direct UPDATE/
	 * DELETE always contains "junk" target columns to identify the exact row
	 * to update or delete, which would be confusing in this context.  So, we
	 * suppress it in all the cases.
	 */
	if (IsA(plan, ForeignScan) &&
		((ForeignScan *) plan)->operation != CMD_SELECT)
		return;

	/* Set up deparsing context */
	context = set_deparse_context_plan_format(es->deparse_cxt,
									   plan,
									   ancestors,hsp);
	useprefix = list_length(es->rtable) > 1;


	hsp->output = (char**)malloc(list_length(plan->targetlist) * sizeof(char*));
	hsp->n_output = list_length(plan->targetlist);

	if (!hsp->output) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }

	/* Deparse each result column (we now include resjunk ones) */
	int i = 0;
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		char *expr =  deparse_expression_format((Node *) tle->expr, context,
											useprefix, false);
		result = lappend(result,expr);
		hsp->output[i] = malloc(strlen(expr)+1);
		memcpy(hsp->output[i],expr,strlen(expr));
		hsp->output[i][strlen(expr)] = '\0';
		++i;
	}
	FormatPropertyList("Output", result, es);
}

/*
 * Show a generic expression
 */
static void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es,HistorySlowPlanStat* hsp)
{
	List	   *context;
	char	   *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_plan_format(es->deparse_cxt,
									   planstate->plan,
									   ancestors,hsp);

	/* Deparse the expression */
	exprstr = deparse_expression_format(node, context, useprefix, false);

	/* And add to es->str */
	FormatPropertyText(qlabel, exprstr, es);
	hsp->qlabel = qlabel;
	/**we need a more fine-grained exprstr */
	hsp->exprstr = exprstr;
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
static void
show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ExplainState *es,HistorySlowPlanStat *hsp)
{
	Node	   *node;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* And show it */
	show_expression(node, qlabel, planstate, ancestors, useprefix, es,hsp);
}

/*
 * Show a qualifier expression for a scan plan node
 */
static void
show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ExplainState *es,HistorySlowPlanStat* hsp)
{
	bool		useprefix;

	useprefix = (IsA(planstate->plan, SubqueryScan) || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es,hsp);
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
static void
show_upper_qual(List *qual, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ExplainState *es,HistorySlowPlanStat* hsp)
{
	bool		useprefix;

	useprefix = (list_length(es->rtable) > 1 || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es,hsp);
}

/*
 * Show the sort keys for a Sort node.
 */
static void
show_sort_keys(SortState *sortstate, List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp)
{
	Sort	   *plan = (Sort *) sortstate->ss.ps.plan;

	show_sort_group_keys((PlanState *) sortstate, "Sort Key",
						 plan->numCols, 0, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es, hsp);
}

/*
 * Show the sort keys for a IncrementalSort node.
 */
static void
show_incremental_sort_keys(IncrementalSortState *incrsortstate,
						   List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp)
{
	IncrementalSort *plan = (IncrementalSort *) incrsortstate->ss.ps.plan;

	show_sort_group_keys((PlanState *) incrsortstate, "Sort Key",
						 plan->sort.numCols, plan->nPresortedCols,
						 plan->sort.sortColIdx,
						 plan->sort.sortOperators, plan->sort.collations,
						 plan->sort.nullsFirst,
						 ancestors, es,hsp);
}

/*
 * Likewise, for a MergeAppend node.
 */
static void
show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
					   ExplainState *es,HistorySlowPlanStat* hsp)
{
	MergeAppend *plan = (MergeAppend *) mstate->ps.plan;

	show_sort_group_keys((PlanState *) mstate, "Sort Key",
						 plan->numCols, 0, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es,hsp);
}

/*
 * Show the grouping keys for an Agg node.
 */
static void
show_agg_keys(AggState *astate, List *ancestors,
			  ExplainState *es,HistorySlowPlanStat* hsp)
{
	Agg		   *plan = (Agg *) astate->ss.ps.plan;

	if (plan->numCols > 0 || plan->groupingSets)
	{
		/* The key columns refer to the tlist of the child plan */
		ancestors = lcons(plan, ancestors);

		if (plan->groupingSets)
			show_grouping_sets(outerPlanState(astate), plan, ancestors, es,hsp);
		else
			show_sort_group_keys(outerPlanState(astate), "Group Key",
								 plan->numCols, 0, plan->grpColIdx,
								 NULL, NULL, NULL,
								 ancestors, es,hsp);

		ancestors = list_delete_first(ancestors);
	}
}

static void
show_grouping_sets(PlanState *planstate, Agg *agg,
				   List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp)
{
	List	   *context;
	bool		useprefix;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_plan_format(es->deparse_cxt,
									   planstate->plan,
									   ancestors,hsp);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	FormatOpenGroup("Grouping Sets", "Grouping Sets", false, es);

	show_grouping_set_keys(planstate, agg, NULL,
						   context, useprefix, ancestors, es, hsp);

	foreach(lc, agg->chain)
	{
		Agg		   *aggnode = lfirst(lc);
		Sort	   *sortnode = (Sort *) aggnode->plan.lefttree;

		show_grouping_set_keys(planstate, aggnode, sortnode,
							   context, useprefix, ancestors, es,hsp);
	}

	FormatCloseGroup("Grouping Sets", "Grouping Sets", false, es);
}

static void
show_grouping_set_keys(PlanState *planstate,
					   Agg *aggnode, Sort *sortnode,
					   List *context, bool useprefix,
					   List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp)
{
	Plan	   *plan = planstate->plan;
	char	   *exprstr;
	ListCell   *lc;
	List	   *gsets = aggnode->groupingSets;
	AttrNumber *keycols = aggnode->grpColIdx;
	const char *keyname;
	const char *keysetname;

	if (aggnode->aggstrategy == AGG_HASHED || aggnode->aggstrategy == AGG_MIXED)
	{
		keyname = "Hash Key";
		keysetname = "Hash Keys";
	}
	else
	{
		keyname = "Group Key";
		keysetname = "Group Keys";
	}

	FormatOpenGroup("Grouping Set", NULL, true, es);

	if (sortnode)
	{
		show_sort_group_keys(planstate, "Sort Key",
							 sortnode->numCols, 0, sortnode->sortColIdx,
							 sortnode->sortOperators, sortnode->collations,
							 sortnode->nullsFirst,
							 ancestors, es,hsp);
		if (es->format == EXPLAIN_FORMAT_TEXT)
			es->indent++;
	}

	FormatOpenGroup(keysetname, keysetname, false, es);

	/**
	 * TODO: fix gsets here 
	 */
	hsp->g_sets = (GroupKeys **)malloc(list_length(gsets)*sizeof(GroupKeys));
	hsp->n_g_sets = list_length(gsets);
	
	size_t g_sets_idx = 0;
	foreach(lc, gsets)
	{
		List	   *result = NIL;
		ListCell   *lc2;

		foreach(lc2, (List *) lfirst(lc))
		{
			Index		i = lfirst_int(lc2);
			AttrNumber	keyresno = keycols[i];
			TargetEntry *target = get_tle_by_resno(plan->targetlist,
												   keyresno);

			if (!target)
				elog(ERROR, "no tlist entry for key %d", keyresno);
			/* Deparse the expression, showing any top-level cast */
			exprstr = deparse_expression_format((Node *) target->expr, context,
										 useprefix, true);
			result = lappend(result, exprstr);
		}

		if (!result && es->format == EXPLAIN_FORMAT_TEXT)
			FormatPropertyText(keyname, "()", es);
		else{
			FormatPropertyListNested(keyname, result, es);
			hsp->key_name = keyname;
			hsp->keysetname = keysetname;
			
			GroupKeys * gkey = (GroupKeys *)malloc(sizeof(GroupKeys));
			group_keys__init(gkey);

			gkey->key_name = keyname;
			gkey->keys = (char**)malloc(list_length(result)*sizeof(char*));
			gkey->n_keys = list_length(result);
			for(int i=0;i<gkey->n_keys;i++){
				gkey->keys = (char*)result[i].elements->ptr_value;	
			}
			hsp->g_sets[g_sets_idx] = gkey;
		}
		g_sets_idx++;
	}

	FormatCloseGroup(keysetname, keysetname, false, es);

	if (sortnode && es->format == EXPLAIN_FORMAT_TEXT)
		es->indent--;

	FormatCloseGroup("Grouping Set", NULL, true, es);
}

/*
 * Show the grouping keys for a Group node.
 */
static void
show_group_keys(GroupState *gstate, List *ancestors,
				ExplainState *es,HistorySlowPlanStat* hsp)
{
	Group	   *plan = (Group *) gstate->ss.ps.plan;

	/* The key columns refer to the tlist of the child plan */
	ancestors = lcons(plan, ancestors);
	show_sort_group_keys(outerPlanState(gstate), "Group Key",
						 plan->numCols, 0, plan->grpColIdx,
						 NULL, NULL, NULL,
						 ancestors, es,hsp);
	ancestors = list_delete_first(ancestors);
}

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
static void
show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, int nPresortedKeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	List	   *resultPresorted = NIL;
	StringInfoData sortkeybuf;
	bool		useprefix;
	int			keyno;

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
	context = set_deparse_context_plan_format(es->deparse_cxt,
									   plan,
									   ancestors,hsp);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	hsp->group_sort_qlabel = qlabel;
	hsp->group_sort_keys = (GroupSortKey **)malloc(nkeys * sizeof(GroupSortKey *));
	hsp->n_group_sort_keys = nkeys;

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
											   keyresno);
		char	   *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression_format((Node *) target->expr, context,
									 useprefix, true);
		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);
		/*
         * Append sort order information, if relevant
         */
		GroupSortKey *gs_key = (GroupSortKey *)malloc(sizeof(GroupSortKey));
		group_sort_key__init(gs_key);

		gs_key->key = malloc(strlen(exprstr)+1);
		memcpy(gs_key->key,exprstr,strlen(exprstr));
		gs_key->key[strlen(exprstr)] = '\0';

		gs_key->sort_operators = (sortOperators != NULL);

		if (sortOperators != NULL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   sortOperators[keyno],
								   collations[keyno],
								   nullsFirst[keyno],hsp,gs_key);
		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
		if (keyno < nPresortedKeys){
			resultPresorted = lappend(resultPresorted, exprstr);
			gs_key->presorted_key = true;
		}
		hsp->group_sort_keys[keyno] = gs_key;
	}
	FormatPropertyList(qlabel, result, es);
	if (nPresortedKeys > 0)
		FormatPropertyList("Presorted Key", resultPresorted, es);
}

/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
static void
show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst,
					   HistorySlowPlanStat *hsp,GroupSortKey* gs_key)
{
	Oid			sortcoltype = exprType(sortexpr);
	bool		reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default for the column's type.  There are
	 * some cases where this is redundant, eg if expression is a column whose
	 * declared collation is that collation, but it's hard to distinguish that
	 * here (and arguably, printing COLLATE explicitly is a good idea anyway
	 * in such cases).
	 */
	if (OidIsValid(collation) && collation != get_typcollation(sortcoltype))
	{
		char	   *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
		
		gs_key->sort_collation = malloc(strlen(quote_identifier(collname))+1);
		strcpy(gs_key->sort_collation,quote_identifier(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		gs_key->sort_direction = "DESC";
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char	   *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);

		gs_key->sort_direction = malloc(strlen(opname)+1);
		strcpy(gs_key->sort_direction,opname);
		
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
		gs_key->sort_null_pos = "NULLS FIRST";
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
		gs_key->sort_null_pos = "NULLS LAST";
	}
}

/*
 * Show TABLESAMPLE properties
 */
static void
show_tablesample(TableSampleClause *tsc, PlanState *planstate,
				 List *ancestors, ExplainState *es,HistorySlowPlanStat* hsp)
{
	List	   *context;
	bool		useprefix;
	char	   *method_name;
	List	   *params = NIL;
	char	   *repeatable;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_plan_format(es->deparse_cxt,
									   planstate->plan,
									   ancestors,hsp);
	useprefix = list_length(es->rtable) > 1;

	/* Get the tablesample method name */
	method_name = get_func_name(tsc->tsmhandler);

	/* Deparse parameter expressions */
	foreach(lc, tsc->args)
	{
		Node	   *arg = (Node *) lfirst(lc);

		params = lappend(params,
						 deparse_expression_format(arg, context,
											useprefix, false));
	}
	if (tsc->repeatable)
		repeatable = deparse_expression_format((Node *) tsc->repeatable, context,
										useprefix, false);
	else
		repeatable = NULL;


	FormatPropertyText("Sampling Method", method_name, es);
	FormatPropertyList("Sampling Parameters", params, es);
	if (repeatable)
		FormatPropertyText("Repeatable Seed", repeatable, es);
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for a sort node
 */
static void
show_sort_info(SortState *sortstate, ExplainState *es)
{
	if (!es->analyze)
		return;

	if (sortstate->sort_Done && sortstate->tuplesortstate != NULL)
	{
		Tuplesortstate *state = (Tuplesortstate *) sortstate->tuplesortstate;
		TuplesortInstrumentation stats;
		const char *sortMethod;
		const char *spaceType;
		int64		spaceUsed;

		tuplesort_get_stats(state, &stats);
		sortMethod = tuplesort_method_name(stats.sortMethod);
		spaceType = tuplesort_space_type_name(stats.spaceType);
		spaceUsed = stats.spaceUsed;

		FormatPropertyText("Sort Method", sortMethod, es);
		FormatPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
		FormatPropertyText("Sort Space Type", spaceType, es);
	}

	/*
	 * You might think we should just skip this stanza entirely when
	 * es->hide_workers is true, but then we'd get no sort-method output at
	 * all.  We have to make it look like worker 0's data is top-level data.
	 * This is easily done by just skipping the OpenWorker/CloseWorker calls.
	 * Currently, we don't worry about the possibility that there are multiple
	 * workers in such a case; if there are, duplicate output fields will be
	 * emitted.
	 */
	if (sortstate->shared_info != NULL)
	{
		int			n;

		for (n = 0; n < sortstate->shared_info->num_workers; n++)
		{
			TuplesortInstrumentation *sinstrument;
			const char *sortMethod;
			const char *spaceType;
			int64		spaceUsed;

			sinstrument = &sortstate->shared_info->sinstrument[n];
			if (sinstrument->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
				continue;		/* ignore any unfilled slots */
			sortMethod = tuplesort_method_name(sinstrument->sortMethod);
			spaceType = tuplesort_space_type_name(sinstrument->spaceType);
			spaceUsed = sinstrument->spaceUsed;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

		
			FormatPropertyText("Sort Method", sortMethod, es);
			FormatPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
			FormatPropertyText("Sort Space Type", spaceType, es);

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * Incremental sort nodes sort in (a potentially very large number of) batches,
 * so EXPLAIN ANALYZE needs to roll up the tuplesort stats from each batch into
 * an intelligible summary.
 *
 * This function is used for both a non-parallel node and each worker in a
 * parallel incremental sort node.
 */
static void
show_incremental_sort_group_info(IncrementalSortGroupInfo *groupInfo,
								 const char *groupLabel, bool indent, ExplainState *es)
{
	ListCell   *methodCell;
	List	   *methodNames = NIL;

	/* Generate a list of sort methods used across all groups. */
	for (int bit = 0; bit < NUM_TUPLESORTMETHODS; bit++)
	{
		TuplesortMethod sortMethod = (1 << bit);

		if (groupInfo->sortMethods & sortMethod)
		{
			const char *methodName = tuplesort_method_name(sortMethod);

			methodNames = lappend(methodNames, unconstify(char *, methodName));
		}
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (indent)
			appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "%s Groups: " INT64_FORMAT "  Sort Method", groupLabel,
						 groupInfo->groupCount);
		/* plural/singular based on methodNames size */
		if (list_length(methodNames) > 1)
			appendStringInfo(es->str, "s: ");
		else
			appendStringInfo(es->str, ": ");
		foreach(methodCell, methodNames)
		{
			appendStringInfo(es->str, "%s", (char *) methodCell->ptr_value);
			if (foreach_current_index(methodCell) < list_length(methodNames) - 1)
				appendStringInfo(es->str, ", ");
		}

		if (groupInfo->maxMemorySpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalMemorySpaceUsed / groupInfo->groupCount;
			const char *spaceTypeName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_MEMORY);
			appendStringInfo(es->str, "  Average %s: " INT64_FORMAT "kB  Peak %s: " INT64_FORMAT "kB",
							 spaceTypeName, avgSpace,
							 spaceTypeName, groupInfo->maxMemorySpaceUsed);
		}

		if (groupInfo->maxDiskSpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalDiskSpaceUsed / groupInfo->groupCount;

			const char *spaceTypeName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_DISK);
			appendStringInfo(es->str, "  Average %s: " INT64_FORMAT "kB  Peak %s: " INT64_FORMAT "kB",
							 spaceTypeName, avgSpace,
							 spaceTypeName, groupInfo->maxDiskSpaceUsed);
		}
	}
	else
	{
		StringInfoData groupName;

		initStringInfo(&groupName);
		appendStringInfo(&groupName, "%s Groups", groupLabel);
		FormatOpenGroup("Incremental Sort Groups", groupName.data, true, es);
		FormatPropertyInteger("Group Count", NULL, groupInfo->groupCount, es);

		FormatPropertyList("Sort Methods Used", methodNames, es);

		if (groupInfo->maxMemorySpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalMemorySpaceUsed / groupInfo->groupCount;
			const char *spaceTypeName;
			StringInfoData memoryName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_MEMORY);
			initStringInfo(&memoryName);
			appendStringInfo(&memoryName, "Sort Space %s", spaceTypeName);
			FormatOpenGroup("Sort Space", memoryName.data, true, es);

			FormatPropertyInteger("Average Sort Space Used", "kB", avgSpace, es);
			FormatPropertyInteger("Peak Sort Space Used", "kB",
								   groupInfo->maxMemorySpaceUsed, es);

			FormatCloseGroup("Sort Space", memoryName.data, true, es);
		}
		if (groupInfo->maxDiskSpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalDiskSpaceUsed / groupInfo->groupCount;
			const char *spaceTypeName;
			StringInfoData diskName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_DISK);
			initStringInfo(&diskName);
			appendStringInfo(&diskName, "Sort Space %s", spaceTypeName);
			FormatOpenGroup("Sort Space", diskName.data, true, es);

			FormatPropertyInteger("Average Sort Space Used", "kB", avgSpace, es);
			FormatPropertyInteger("Peak Sort Space Used", "kB",
								   groupInfo->maxDiskSpaceUsed, es);

			FormatCloseGroup("Sort Space", diskName.data, true, es);
		}

		FormatCloseGroup("Incremental Sort Groups", groupName.data, true, es);
	}
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for an incremental sort node
 */
static void
show_incremental_sort_info(IncrementalSortState *incrsortstate,
						   ExplainState *es)
{
	IncrementalSortGroupInfo *fullsortGroupInfo;
	IncrementalSortGroupInfo *prefixsortGroupInfo;

	fullsortGroupInfo = &incrsortstate->incsort_info.fullsortGroupInfo;

	if (!es->analyze)
		return;

	/*
	 * Since we never have any prefix groups unless we've first sorted a full
	 * groups and transitioned modes (copying the tuples into a prefix group),
	 * we don't need to do anything if there were 0 full groups.
	 *
	 * We still have to continue after this block if there are no full groups,
	 * though, since it's possible that we have workers that did real work
	 * even if the leader didn't participate.
	 */
	if (fullsortGroupInfo->groupCount > 0)
	{
		show_incremental_sort_group_info(fullsortGroupInfo, "Full-sort", true, es);
		prefixsortGroupInfo = &incrsortstate->incsort_info.prefixsortGroupInfo;
		if (prefixsortGroupInfo->groupCount > 0)
		{
			if (es->format == EXPLAIN_FORMAT_TEXT)
				appendStringInfo(es->str, "\n");
			show_incremental_sort_group_info(prefixsortGroupInfo, "Pre-sorted", true, es);
		}
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfo(es->str, "\n");
	}

	if (incrsortstate->shared_info != NULL)
	{
		int			n;
		bool		indent_first_line;

		for (n = 0; n < incrsortstate->shared_info->num_workers; n++)
		{
			IncrementalSortInfo *incsort_info =
			&incrsortstate->shared_info->sinfo[n];

			/*
			 * If a worker hasn't processed any sort groups at all, then
			 * exclude it from output since it either didn't launch or didn't
			 * contribute anything meaningful.
			 */
			fullsortGroupInfo = &incsort_info->fullsortGroupInfo;

			/*
			 * Since we never have any prefix groups unless we've first sorted
			 * a full groups and transitioned modes (copying the tuples into a
			 * prefix group), we don't need to do anything if there were 0
			 * full groups.
			 */
			if (fullsortGroupInfo->groupCount == 0)
				continue;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

			indent_first_line = es->workers_state == NULL || es->verbose;
			show_incremental_sort_group_info(fullsortGroupInfo, "Full-sort",
											 indent_first_line, es);
			prefixsortGroupInfo = &incsort_info->prefixsortGroupInfo;
			if (prefixsortGroupInfo->groupCount > 0)
			{
				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, "\n");
				show_incremental_sort_group_info(prefixsortGroupInfo, "Pre-sorted", true, es);
			}
			if (es->format == EXPLAIN_FORMAT_TEXT)
				appendStringInfo(es->str, "\n");

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * Show information on hash buckets/batches.
 */
static void
show_hash_info(HashState *hashstate, ExplainState *es)
{
	HashInstrumentation hinstrument = {0};

	/*
	 * Collect stats from the local process, even when it's a parallel query.
	 * In a parallel query, the leader process may or may not have run the
	 * hash join, and even if it did it may not have built a hash table due to
	 * timing (if it started late it might have seen no tuples in the outer
	 * relation and skipped building the hash table).  Therefore we have to be
	 * prepared to get instrumentation data from all participants.
	 */
	if (hashstate->hinstrument)
		memcpy(&hinstrument, hashstate->hinstrument,
			   sizeof(HashInstrumentation));

	/*
	 * Merge results from workers.  In the parallel-oblivious case, the
	 * results from all participants should be identical, except where
	 * participants didn't run the join at all so have no data.  In the
	 * parallel-aware case, we need to consider all the results.  Each worker
	 * may have seen a different subset of batches and we want to report the
	 * highest memory usage across all batches.  We take the maxima of other
	 * values too, for the same reasons as in ExecHashAccumInstrumentation.
	 */
	if (hashstate->shared_info)
	{
		SharedHashInfo *shared_info = hashstate->shared_info;
		int			i;

		for (i = 0; i < shared_info->num_workers; ++i)
		{
			HashInstrumentation *worker_hi = &shared_info->hinstrument[i];

			hinstrument.nbuckets = Max(hinstrument.nbuckets,
									   worker_hi->nbuckets);
			hinstrument.nbuckets_original = Max(hinstrument.nbuckets_original,
												worker_hi->nbuckets_original);
			hinstrument.nbatch = Max(hinstrument.nbatch,
									 worker_hi->nbatch);
			hinstrument.nbatch_original = Max(hinstrument.nbatch_original,
											  worker_hi->nbatch_original);
			hinstrument.space_peak = Max(hinstrument.space_peak,
										 worker_hi->space_peak);
		}
	}

	if (hinstrument.nbatch > 0)
	{
		long		spacePeakKb = (hinstrument.space_peak + 1023) / 1024;

		if (es->format != EXPLAIN_FORMAT_TEXT)
		{
			FormatPropertyInteger("Hash Buckets", NULL,
								   hinstrument.nbuckets, es);
			FormatPropertyInteger("Original Hash Buckets", NULL,
								   hinstrument.nbuckets_original, es);
			FormatPropertyInteger("Hash Batches", NULL,
								   hinstrument.nbatch, es);
			FormatPropertyInteger("Original Hash Batches", NULL,
								   hinstrument.nbatch_original, es);
			FormatPropertyInteger("Peak Memory Usage", "kB",
								   spacePeakKb, es);
		}
		else if (hinstrument.nbatch_original != hinstrument.nbatch ||
				 hinstrument.nbuckets_original != hinstrument.nbuckets)
		{

			appendStringInfo(es->str,
							 "Buckets: %d (originally %d)  Batches: %d (originally %d)  Memory Usage: %ldkB\n",
							 hinstrument.nbuckets,
							 hinstrument.nbuckets_original,
							 hinstrument.nbatch,
							 hinstrument.nbatch_original,
							 spacePeakKb);
		}
		else
		{
			appendStringInfo(es->str,
							 "Buckets: %d  Batches: %d  Memory Usage: %ldkB\n",
							 hinstrument.nbuckets, hinstrument.nbatch,
							 spacePeakKb);
		}
	}
}

/*
 * Show information on hash aggregate memory usage and batches.
 */
static void
show_hashagg_info(AggState *aggstate, ExplainState *es)
{
	Agg		   *agg = (Agg *) aggstate->ss.ps.plan;
	int64		memPeakKb = (aggstate->hash_mem_peak + 1023) / 1024;

	if (agg->aggstrategy != AGG_HASHED &&
		agg->aggstrategy != AGG_MIXED)
		return;



	if (es->costs)
		FormatPropertyInteger("Planned Partitions", NULL,
								   aggstate->hash_planned_partitions, es);

	/*
	* During parallel query the leader may have not helped out.  We
	* detect this by checking how much memory it used.  If we find it
	* didn't do any work then we don't show its properties.
	*/
	if (es->analyze && aggstate->hash_mem_peak > 0)
	{
		FormatPropertyInteger("HashAgg Batches", NULL,
								   aggstate->hash_batches_used, es);
		FormatPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
		FormatPropertyInteger("Disk Usage", "kB",aggstate->hash_disk_used, es);
	}
	

	/* Display stats for each parallel worker */
	if (es->analyze && aggstate->shared_info != NULL)
	{
		for (int n = 0; n < aggstate->shared_info->num_workers; n++)
		{
			AggregateInstrumentation *sinstrument;
			uint64		hash_disk_used;
			int			hash_batches_used;

			sinstrument = &aggstate->shared_info->sinstrument[n];
			/* Skip workers that didn't do anything */
			if (sinstrument->hash_mem_peak == 0)
				continue;
			hash_disk_used = sinstrument->hash_disk_used;
			hash_batches_used = sinstrument->hash_batches_used;
			memPeakKb = (sinstrument->hash_mem_peak + 1023) / 1024;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

		
			FormatPropertyInteger("HashAgg Batches", NULL,
									   hash_batches_used, es);
			FormatPropertyInteger("Peak Memory Usage", "kB", memPeakKb,
									   es);
			FormatPropertyInteger("Disk Usage", "kB", hash_disk_used, es);

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show exact/lossy pages for a BitmapHeapScan node
 */
static void
show_tidbitmap_info(BitmapHeapScanState *planstate, ExplainState *es)
{
	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		FormatPropertyInteger("Exact Heap Blocks", NULL,
							   planstate->exact_pages, es);
		FormatPropertyInteger("Lossy Heap Blocks", NULL,
							   planstate->lossy_pages, es);
	}
	else
	{
		if (planstate->exact_pages > 0 || planstate->lossy_pages > 0)
		{
			appendStringInfoString(es->str, "Heap Blocks:");
			if (planstate->exact_pages > 0)
				appendStringInfo(es->str, " exact=%ld", planstate->exact_pages);
			if (planstate->lossy_pages > 0)
				appendStringInfo(es->str, " lossy=%ld", planstate->lossy_pages);
			appendStringInfoChar(es->str, '\n');
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
static void
show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ExplainState *es)
{
	double		nfiltered;
	double		nloops;

	if (!es->analyze || !planstate->instrument)
		return;

	if (which == 2)
		nfiltered = planstate->instrument->nfiltered2;
	else
		nfiltered = planstate->instrument->nfiltered1;
	nloops = planstate->instrument->nloops;

	/* In text mode, suppress zero counts; they're not interesting enough */
	if (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)
	{
		if (nloops > 0)
			FormatPropertyFloat(qlabel, NULL, nfiltered / nloops, 0, es);
		else
			FormatPropertyFloat(qlabel, NULL, 0.0, 0, es);
	}
}

/*
 * Show extra information for a ForeignScan node.
 */
static void
show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es)
{
	FdwRoutine *fdwroutine = fsstate->fdwroutine;

	/* Let the FDW emit whatever fields it wants */
	if (((ForeignScan *) fsstate->ss.ps.plan)->operation != CMD_SELECT)
	{
		if (fdwroutine->ExplainDirectModify != NULL)
			fdwroutine->ExplainDirectModify(fsstate, es);
	}
	else
	{
		if (fdwroutine->ExplainForeignScan != NULL)
			fdwroutine->ExplainForeignScan(fsstate, es);
	}
}

/*
 * Show initplan params evaluated at Gather or Gather Merge node.
 */
static void
show_eval_params(Bitmapset *bms_params, ExplainState *es)
{
	int			paramid = -1;
	List	   *params = NIL;

	Assert(bms_params);

	while ((paramid = bms_next_member(bms_params, paramid)) >= 0)
	{
		char		param[32];

		snprintf(param, sizeof(param), "$%d", paramid);
		params = lappend(params, pstrdup(param));
	}

	if (params)
		FormatPropertyList("Params Evaluated", params, es);
}

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 *
 * Note: names returned by this function should be "raw"; the caller will
 * apply quoting if needed.  Formerly the convention was to do quoting here,
 * but we don't want that in non-text output formats.
 */
static const char *
explain_get_index_name(Oid indexId)
{
	const char *result;

	if (explain_get_index_name_hook)
		result = (*explain_get_index_name_hook) (indexId);
	else
		result = NULL;
	if (result == NULL)
	{
		/* default behavior: look it up in the catalogs */
		result = get_rel_name(indexId);
		if (result == NULL)
			elog(ERROR, "cache lookup failed for index %u", indexId);
	}
	return result;
}

/*
 * Show buffer usage details.
 */
static void
show_buffer_usage(ExplainState *es, const BufferUsage *usage, bool planning)
{
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool		has_shared = (usage->shared_blks_hit > 0 ||
								  usage->shared_blks_read > 0 ||
								  usage->shared_blks_dirtied > 0 ||
								  usage->shared_blks_written > 0);
		bool		has_local = (usage->local_blks_hit > 0 ||
								 usage->local_blks_read > 0 ||
								 usage->local_blks_dirtied > 0 ||
								 usage->local_blks_written > 0);
		bool		has_temp = (usage->temp_blks_read > 0 ||
								usage->temp_blks_written > 0);
		bool		has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) ||
								  !INSTR_TIME_IS_ZERO(usage->blk_write_time));
		bool		show_planning = (planning && (has_shared ||
												  has_local || has_temp || has_timing));

		if (show_planning)
		{
			appendStringInfoString(es->str, "Planning:\n");
			es->indent++;
		}

		/* Show only positive counter values. */
		if (has_shared || has_local || has_temp)
		{
			appendStringInfoString(es->str, "Buffers:");

			if (has_shared)
			{
				appendStringInfoString(es->str, " shared");
				if (usage->shared_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
									 usage->shared_blks_hit);
				if (usage->shared_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->shared_blks_read);
				if (usage->shared_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
									 usage->shared_blks_dirtied);
				if (usage->shared_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->shared_blks_written);
				if (has_local || has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_local)
			{
				appendStringInfoString(es->str, " local");
				if (usage->local_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
									 usage->local_blks_hit);
				if (usage->local_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->local_blks_read);
				if (usage->local_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
									 usage->local_blks_dirtied);
				if (usage->local_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->local_blks_written);
				if (has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_temp)
			{
				appendStringInfoString(es->str, " temp");
				if (usage->temp_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->temp_blks_read);
				if (usage->temp_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->temp_blks_written);
			}
			appendStringInfoChar(es->str, '\n');
		}

		/* As above, show only positive counter values. */
		if (has_timing)
		{
			appendStringInfoString(es->str, "I/O Timings:");
			if (!INSTR_TIME_IS_ZERO(usage->blk_read_time))
				appendStringInfo(es->str, " read=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time));
			if (!INSTR_TIME_IS_ZERO(usage->blk_write_time))
				appendStringInfo(es->str, " write=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time));
			appendStringInfoChar(es->str, '\n');
		}

		if (show_planning)
			es->indent--;
	}
	else
	{
		FormatPropertyInteger("Shared Hit Blocks", NULL,
							   usage->shared_blks_hit, es);
		FormatPropertyInteger("Shared Read Blocks", NULL,
							   usage->shared_blks_read, es);
		FormatPropertyInteger("Shared Dirtied Blocks", NULL,
							   usage->shared_blks_dirtied, es);
		FormatPropertyInteger("Shared Written Blocks", NULL,
							   usage->shared_blks_written, es);
		FormatPropertyInteger("Local Hit Blocks", NULL,
							   usage->local_blks_hit, es);
		FormatPropertyInteger("Local Read Blocks", NULL,
							   usage->local_blks_read, es);
		FormatPropertyInteger("Local Dirtied Blocks", NULL,
							   usage->local_blks_dirtied, es);
		FormatPropertyInteger("Local Written Blocks", NULL,
							   usage->local_blks_written, es);
		FormatPropertyInteger("Temp Read Blocks", NULL,
							   usage->temp_blks_read, es);
		FormatPropertyInteger("Temp Written Blocks", NULL,
							   usage->temp_blks_written, es);
		if (track_io_timing)
		{
			FormatPropertyFloat("I/O Read Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time),
								 3, es);
			FormatPropertyFloat("I/O Write Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time),
								 3, es);
		}
	}
}

/*
 * Show WAL usage details.
 */
static void
show_wal_usage(ExplainState *es, const WalUsage *usage)
{
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		/* Show only positive counter values. */
		if ((usage->wal_records > 0) || (usage->wal_fpi > 0) ||
			(usage->wal_bytes > 0))
		{
			appendStringInfoString(es->str, "WAL:");

			if (usage->wal_records > 0)
				appendStringInfo(es->str, " records=%ld",
								 usage->wal_records);
			if (usage->wal_fpi > 0)
				appendStringInfo(es->str, " fpi=%ld",
								 usage->wal_fpi);
			if (usage->wal_bytes > 0)
				appendStringInfo(es->str, " bytes=" UINT64_FORMAT,
								 usage->wal_bytes);
			appendStringInfoChar(es->str, '\n');
		}
	}
	else
	{
		FormatPropertyInteger("WAL Records", NULL,
							   usage->wal_records, es);
		FormatPropertyInteger("WAL FPI", NULL,
							   usage->wal_fpi, es);
		FormatPropertyUInteger("WAL Bytes", NULL,
								usage->wal_bytes, es);
	}
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
static void
ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
						ExplainState *es,HistorySlowPlanStat * hsp)
{
	const char *indexname = explain_get_index_name(indexid);
	const char *scandir;
       
	switch (indexorderdir)
	{
		case BackwardScanDirection:
			scandir = "Backward";
			break;
		case NoMovementScanDirection:
			scandir = "NoMovement";
			break;
		case ForwardScanDirection:
			scandir = "Forward";
			break;
		default:
			scandir = "???";
			break;
	}
	FormatPropertyText("Scan Direction", scandir, es);
	hsp->scan_dir = scandir;
	FormatPropertyText("Index Name", indexname, es);
	hsp->idx_name = indexname;
}

/*
 * Show the target of a Scan node
 */
static void
ExplainScanTarget(Scan *plan, ExplainState *es, HistorySlowPlanStat* hsp)
{
	ExplainTargetRel((Plan *) plan, plan->scanrelid, es ,hsp);
}

/*
 * Show the target of a ModifyTable node
 *
 * Here we show the nominal target (ie, the relation that was named in the
 * original query).  If the actual target(s) is/are different, we'll show them
 * in show_modifytable_info().
 */
static void
ExplainModifyTarget(ModifyTable *plan, ExplainState *es,HistorySlowPlanStat * hsp)
{
	ExplainTargetRel((Plan *) plan, plan->nominalRelation, es, hsp);
}

/*
 * Show the target relation of a scan or modify node
 */
static void
ExplainTargetRel(Plan *plan, Index rti, ExplainState *es,HistorySlowPlanStat* hsp)
{
	char	   *objectname = NULL;
	char	   *namespace = NULL;
	const char *objecttag = NULL;
	RangeTblEntry *rte;
	char	   *refname;

	/*if refname is null ,we will get real name of table*/
	rte = rt_fetch(rti, es->rtable);
	refname = (char *) list_nth(es->rtable_names, rti - 1);
	if (refname == NULL)
		refname = rte->eref->aliasname;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_ModifyTable:
			/* Assert it's on a real relation */
			Assert(rte->rtekind == RTE_RELATION);
			objectname = get_rel_name(rte->relid);
			if (es->verbose)
				namespace = get_namespace_name(get_rel_namespace(rte->relid));
			objecttag = "Relation Name";
			break;
		case T_FunctionScan:
			{
				FunctionScan *fscan = (FunctionScan *) plan;
				/* Assert it's on a RangeFunction */
				Assert(rte->rtekind == RTE_FUNCTION);
				/*
				 * If the expression is still a function call of a single
				 * function, we can get the real name of the function.
				 * Otherwise, punt.  (Even if it was a single function call
				 * originally, the optimizer could have simplified it away.)
				 */
				if (list_length(fscan->functions) == 1)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(fscan->functions);

					if (IsA(rtfunc->funcexpr, FuncExpr))
					{
						FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
						Oid			funcid = funcexpr->funcid;

						objectname = get_func_name(funcid);
						if (es->verbose)
							namespace =
								get_namespace_name(get_func_namespace(funcid));
					}
				}
				objecttag = "Function Name";
			}
			break;
		case T_TableFuncScan:
			Assert(rte->rtekind == RTE_TABLEFUNC);
			objectname = "xmltable";
			objecttag = "Table Function Name";
			break;
		case T_ValuesScan:
			Assert(rte->rtekind == RTE_VALUES);
			break;
		case T_CteScan:
			/* Assert it's on a non-self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(!rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		case T_NamedTuplestoreScan:
			Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);
			objectname = rte->enrname;
			objecttag = "Tuplestore Name";
			break;
		case T_WorkTableScan:
			/* Assert it's on a self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		default:
			break;
	}
	if (objecttag != NULL && objectname != NULL){
		FormatPropertyText(objecttag, objectname, es);
		hsp->object_tag = objecttag;
		hsp->object_name = objectname;
	}
	if (namespace != NULL){
		FormatPropertyText("Schema", namespace, es);
		hsp->schema = namespace;
	}
	FormatPropertyText("Alias", refname, es);
	hsp->alia_name = refname;
}

/*
 * Show extra information for a ModifyTable node
 *
 * We have three objectives here.  First, if there's more than one target
 * table or it's different from the nominal target, identify the actual
 * target(s).  Second, give FDWs a chance to display extra info about foreign
 * targets.  Third, show information about ON CONFLICT.
 */
static void
show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
					  ExplainState *es, HistorySlowPlanStat * hsp)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	const char *operation;
	const char *foperation;
	bool		labeltargets;
	int			j;
	List	   *idxNames = NIL;
	ListCell   *lst;

	switch (node->operation)
	{
		case CMD_INSERT:
			operation = "Insert";
			foperation = "Foreign Insert";
			break;
		case CMD_UPDATE:
			operation = "Update";
			foperation = "Foreign Update";
			break;
		case CMD_DELETE:
			operation = "Delete";
			foperation = "Foreign Delete";
			break;
		default:
			operation = "???";
			foperation = "Foreign ???";
			break;
	}

	/* Should we explicitly label target relations? */
	labeltargets = (mtstate->mt_nplans > 1 ||
					(mtstate->mt_nplans == 1 &&
					 mtstate->resultRelInfo[0].ri_RangeTableIndex != node->nominalRelation));

	if (labeltargets)
		FormatOpenGroup("Target Tables", "Target Tables", false, es);

	for (j = 0; j < mtstate->mt_nplans; j++)
	{
		ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + j;
		FdwRoutine *fdwroutine = resultRelInfo->ri_FdwRoutine;

		if (labeltargets)
		{
			/* Open a group for this target */
			FormatOpenGroup("Target Table", NULL, true, es);

			/*
			 * In text mode, decorate each target with operation type, so that
			 * ExplainTargetRel's output of " on foo" will read nicely.
			 */
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoString(es->str,
									   fdwroutine ? foperation : operation);
			}

			/* Identify target */
			ExplainTargetRel((Plan *) node,
							 resultRelInfo->ri_RangeTableIndex,
							 es,hsp);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoChar(es->str, '\n');
				es->indent++;
			}
		}

		/* Give FDW a chance if needed */
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			fdwroutine != NULL &&
			fdwroutine->ExplainForeignModify != NULL)
		{
			List	   *fdw_private = (List *) list_nth(node->fdwPrivLists, j);

			fdwroutine->ExplainForeignModify(mtstate,
											 resultRelInfo,
											 fdw_private,
											 j,
											 es);
		}

		if (labeltargets)
		{
			/* Undo the indentation we added in text format */
			if (es->format == EXPLAIN_FORMAT_TEXT)
				es->indent--;

			/* Close the group */
			FormatCloseGroup("Target Table", NULL, true, es);
		}
	}

	/* Gather names of ON CONFLICT arbiter indexes */
	foreach(lst, node->arbiterIndexes)
	{
		char	   *indexname = get_rel_name(lfirst_oid(lst));

		idxNames = lappend(idxNames, indexname);
	}

	if (node->onConflictAction != ONCONFLICT_NONE)
	{
		FormatPropertyText("Conflict Resolution",
							node->onConflictAction == ONCONFLICT_NOTHING ?
							"NOTHING" : "UPDATE",
							es);

		/*
		 * Don't display arbiter indexes at all when DO NOTHING variant
		 * implicitly ignores all conflicts
		 */
		if (idxNames)
			FormatPropertyList("Conflict Arbiter Indexes", idxNames, es);

		/* ON CONFLICT DO UPDATE WHERE qual is specially displayed */
		if (node->onConflictWhere)
		{
			show_upper_qual((List *) node->onConflictWhere, "Conflict Filter",
							&mtstate->ps, ancestors, es, &hsp);
			show_instrumentation_count("Rows Removed by Conflict Filter", 1, &mtstate->ps, es);
		}

		/* EXPLAIN ANALYZE display of actual outcome for each tuple proposed */
		if (es->analyze && mtstate->ps.instrument)
		{
			double		total;
			double		insert_path;
			double		other_path;

			InstrEndLoop(mtstate->mt_plans[0]->instrument);

			/* count the number of source rows */
			total = mtstate->mt_plans[0]->instrument->ntuples;
			other_path = mtstate->ps.instrument->ntuples2;
			insert_path = total - other_path;

			FormatPropertyFloat("Tuples Inserted", NULL,
								 insert_path, 0, es);
			FormatPropertyFloat("Conflicting Tuples", NULL,
								 other_path, 0, es);
		}
	}
	
	if (labeltargets)
		FormatCloseGroup("Target Tables", "Target Tables", false, es);
}

/*
 * Explain the constituent plans of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 */
static void
ExplainMemberNodes(PlanState **planstates, int nplans,
				   List *ancestors, ExplainState *es,ExplainState *ces)
{
	int			j;

	for (j = 0; j < nplans; j++)
		ExplainNode(planstates[j], ancestors,
					"Member", NULL, es,ces);
}

/*
 * Report about any pruned subnodes of an Append or MergeAppend node.
 *
 * nplans indicates the number of live subplans.
 * nchildren indicates the original number of subnodes in the Plan;
 * some of these may have been pruned by the run-time pruning code.
 */
static void
ExplainMissingMembers(int nplans, int nchildren, ExplainState *es)
{
	if (nplans < nchildren || es->format != EXPLAIN_FORMAT_TEXT)
		FormatPropertyInteger("Subplans Removed", NULL,
							   nchildren - nplans, es);
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlans.
 */
static RecureState
ExplainSubPlans(List *plans, List *ancestors,
				const char *relationship, ExplainState *total_es,ExplainState *total_ces,HistorySlowPlanStat* hsp)
{
	ListCell   *lst;
	RecureState rs = NewRecureState();
	// if(strcmp(relationship,"SubPlan")){
	// 	rs.node_type_set_ = lappend(rs.node_type_set_,T_SubPlan);
	// }else if(strcmp(relationship,"InitPlan")){
	// 	rs.node_type_set_ = lappend(rs.node_type_set_);
	// }
	size_t p_size = list_length(plans);
	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
		SubPlan    *sp = sps->subplan;
		/*
		 * There can be multiple SubPlan nodes referencing the same physical
		 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
		 * We should print a subplan only once, so track which ones we already
		 * printed.  This state must be global across the plan tree, since the
		 * duplicate nodes could be in different plan nodes, eg both a bitmap
		 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
		 * do not worry too much about which plan node we show the subplan as
		 * attached to in such cases.)
		 */
		if (bms_is_member(sp->plan_id, total_es->printed_subplans)){
			continue;
		}
		hsp->subplan_idx++;
		total_es->printed_subplans = bms_add_member(total_es->printed_subplans,
											  sp->plan_id);
		/*
		 * Treat the SubPlan node as an ancestor of the plan node(s) within
		 * it, so that ruleutils.c can find the referents of subplan
		 * parameters.
		 */
		ancestors = lcons(sp, ancestors);
		RecureState ret = ExplainNode(sps->planstate, ancestors,relationship, sp->plan_name, total_es,total_ces);
		rs.cost_ += ret.cost_;
		appendStringInfoString(rs.detail_str_,ret.detail_str_->data);
		appendStringInfoString(rs.canonical_str_,ret.canonical_str_->data);
		push_node_type_set(rs.node_type_set_,ret.node_type_set_);
		ancestors = list_delete_first(ancestors);

		HistorySlowPlanStat* child = (HistorySlowPlanStat*)malloc(sizeof(HistorySlowPlanStat));
		history_slow_plan_stat__init(child);
		*child = ret.hps_;
		hsp->subplans[hsp->subplan_idx] = child;
		//++hsp->subplan_idx;
	}
	hsp->n_subplans = hsp->subplan_idx + 1;
	return rs;
}

/*
 * Explain a list of children of a CustomScan.
 */
static void
ExplainCustomChildren(CustomScanState *css, List *ancestors, ExplainState *es,ExplainState *ces)
{
	ListCell   *cell;
	const char *label =
	(list_length(css->custom_ps) != 1 ? "children" : "child");

	foreach(cell, css->custom_ps)
		ExplainNode((PlanState *) lfirst(cell), ancestors, label, NULL, es,ces);
}

/*
 * Create a per-plan-node workspace for collecting per-worker data.
 *
 * Output related to each worker will be temporarily "set aside" into a
 * separate buffer, which we'll merge into the main output stream once
 * we've processed all data for the plan node.  This makes it feasible to
 * generate a coherent sub-group of fields for each worker, even though the
 * code that produces the fields is in several different places in this file.
 * Formatting of such a set-aside field group is managed by
 * ExplainOpenSetAsideGroup and ExplainSaveGroup/ExplainRestoreGroup.
 */
static ExplainWorkersState *
ExplainCreateWorkersState(int num_workers)
{
	ExplainWorkersState *wstate;

	wstate = (ExplainWorkersState *) palloc(sizeof(ExplainWorkersState));
	wstate->num_workers = num_workers;
	wstate->worker_inited = (bool *) palloc0(num_workers * sizeof(bool));
	wstate->worker_str = (StringInfoData *)
		palloc0(num_workers * sizeof(StringInfoData));
	wstate->worker_state_save = (int *) palloc(num_workers * sizeof(int));
	return wstate;
}

/*
 * Begin or resume output into the set-aside group for worker N.
 */
static void
ExplainOpenWorker(int n, ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	Assert(wstate);
	Assert(n >= 0 && n < wstate->num_workers);

	/* Save prior output buffer pointer */
	wstate->prev_str = es->str;

	if (!wstate->worker_inited[n])
	{
		/* First time through, so create the buffer for this worker */
		initStringInfo(&wstate->worker_str[n]);
		es->str = &wstate->worker_str[n];

		/*
		 * Push suitable initial formatting state for this worker's field
		 * group.  We allow one extra logical nesting level, since this group
		 * will eventually be wrapped in an outer "Workers" group.
		 */
		ExplainOpenSetAsideGroup("Worker", NULL, true, 2, es);

		/*
		 * In non-TEXT formats we always emit a "Worker Number" field, even if
		 * there's no other data for this worker.
		 */
		if (es->format != EXPLAIN_FORMAT_TEXT)
			FormatPropertyInteger("Worker Number", NULL, n, es);

		wstate->worker_inited[n] = true;
	}
	else
	{
		/* Resuming output for a worker we've already emitted some data for */
		es->str = &wstate->worker_str[n];

		/* Restore formatting state saved by last ExplainCloseWorker() */
		ExplainRestoreGroup(es, 2, &wstate->worker_state_save[n]);
	}
}

/*
 * End output for worker N --- must pair with previous ExplainOpenWorker call
 */
static void
ExplainCloseWorker(int n, ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	Assert(wstate);
	Assert(n >= 0 && n < wstate->num_workers);
	Assert(wstate->worker_inited[n]);

	/*
	 * Save formatting state in case we do another ExplainOpenWorker(), then
	 * pop the formatting stack.
	 */
	ExplainSaveGroup(es, 2, &wstate->worker_state_save[n]);

	/*
	 * In TEXT format, if we didn't actually produce any output line(s) then
	 * truncate off the partial line emitted by ExplainOpenWorker.  (This is
	 * to avoid bogus output if, say, show_buffer_usage chooses not to print
	 * anything for the worker.)  Also fix up the indent level.
	 */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		while (es->str->len > 0 && es->str->data[es->str->len - 1] != '\n')
			es->str->data[--(es->str->len)] = '\0';

		es->indent--;
	}

	/* Restore prior output buffer pointer */
	es->str = wstate->prev_str;
}

/*
 * Print per-worker info for current node, then free the ExplainWorkersState.
 */
static void
ExplainFlushWorkersState(ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	FormatOpenGroup("Workers", "Workers", false, es);
	for (int i = 0; i < wstate->num_workers; i++)
	{
		if (wstate->worker_inited[i])
		{
			/* This must match previous ExplainOpenSetAsideGroup call */
			FormatOpenGroup("Worker", NULL, true, es);
			appendStringInfoString(es->str, wstate->worker_str[i].data);
			FormatCloseGroup("Worker", NULL, true, es);

			pfree(wstate->worker_str[i].data);
		}
	}
	FormatCloseGroup("Workers", "Workers", false, es);

	pfree(wstate->worker_inited);
	pfree(wstate->worker_str);
	pfree(wstate->worker_state_save);
	pfree(wstate);
}

/*
 * Explain a property, such as sort keys or targets, that takes the form of
 * a list of unlabeled items.  "data" is a list of C strings.
 */
void
FormatPropertyList(const char *qlabel, List *data, ExplainState *es)
{
	ListCell   *lc;
	bool		first = true;

	ExplainJSONLineEnding(es);
	//appendStringInfoSpaces(es->str, es->indent * 2);
	escape_json(es->str, qlabel);
	appendStringInfoString(es->str, ": [");
	foreach(lc, data){
		if (!first)
			appendStringInfoString(es->str, ", ");
		escape_json(es->str, (const char *) lfirst(lc));
		first = false;
	}
	appendStringInfoChar(es->str, ']');

}

/*
 * Explain a property that takes the form of a list of unlabeled items within
 * another list.  "data" is a list of C strings.
 */
void
FormatPropertyListNested(const char *qlabel, List *data, ExplainState *es)
{
	ListCell   *lc;
	bool		first = true;

	ExplainJSONLineEnding(es);
	//appendStringInfoSpaces(es->str, es->indent * 2);
	appendStringInfoChar(es->str, '[');
	foreach(lc, data){
		if (!first)
			appendStringInfoString(es->str, ", ");
		escape_json(es->str, (const char *) lfirst(lc));
		first = false;
	}
	appendStringInfoChar(es->str, ']');
}

/*
 * Explain a simple property.
 *
 * If "numeric" is true, the value is a number (or other value that
 * doesn't need quoting in JSON).
 *
 * If unit is non-NULL the text format will display it after the value.
 *
 * This usually should not be invoked directly, but via one of the datatype
 * specific routines FormatPropertyText, FormatPropertyInteger, etc.
 */
static void
ExplainProperty(const char *qlabel, const char *unit, const char *value,
				bool numeric, ExplainState *es)
{

	ExplainJSONLineEnding(es);
	//appendStringInfoSpaces(es->str, es->indent * 2);
	escape_json(es->str, qlabel);
	appendStringInfoString(es->str, ": ");
	if (numeric)
		appendStringInfoString(es->str, value);
	else
		escape_json(es->str, value);
}

/*
 * Explain a string-valued property.
 */
void
FormatPropertyText(const char *qlabel, const char *value, ExplainState *es)
{
	ExplainProperty(qlabel, NULL, value, false, es);
}

/*
 * Explain an integer-valued property.
 */
void
FormatPropertyInteger(const char *qlabel, const char *unit, int64 value,
					   ExplainState *es)
{
	char		buf[32];

	snprintf(buf, sizeof(buf), INT64_FORMAT, value);
	ExplainProperty(qlabel, unit, buf, true, es);
}

/*
 * Explain an unsigned integer-valued property.
 */
void
FormatPropertyUInteger(const char *qlabel, const char *unit, uint64 value,
						ExplainState *es)
{
	char		buf[32];

	snprintf(buf, sizeof(buf), UINT64_FORMAT, value);
	ExplainProperty(qlabel, unit, buf, true, es);
}

/*
 * Explain a float-valued property, using the specified number of
 * fractional digits.
 */
void
FormatPropertyFloat(const char *qlabel, const char *unit, double value,
					 int ndigits, ExplainState *es)
{
	char	   *buf;

	buf = psprintf("%.*f", ndigits, value);
	ExplainProperty(qlabel, unit, buf, true, es);
	pfree(buf);
}

/*
 * Explain a bool-valued property.
 */
void
FormatPropertyBool(const char *qlabel, bool value, ExplainState *es)
{
	ExplainProperty(qlabel, NULL, value ? "true" : "false", true, es);
}

/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
void
FormatOpenGroup(const char *objtype, const char *labelname,
				 bool labeled, ExplainState *es)
{
	ExplainJSONLineEnding(es);
	//appendStringInfoSpaces(es->str, 2 * es->indent);
	if (labelname)
	{
		escape_json(es->str, labelname);
		appendStringInfoString(es->str, ": ");
	}
	appendStringInfoChar(es->str, labeled ? '{' : '[');
	/*
		* In JSON format, the grouping_stack is an integer list.  0 means
		* we've emitted nothing at this grouping level, 1 means we've
		* emitted something (and so the next item needs a comma). See
		* ExplainJSONLineEnding().
	*/
	es->grouping_stack = lcons_int(0, es->grouping_stack);
	es->indent++;
}

/*
 * Close a group of related objects.
 * Parameters must match the corresponding FormatOpenGroup call.
 */
void
FormatCloseGroup(const char *objtype, const char *labelname,
				  bool labeled, ExplainState *es)
{
	es->indent--;
	appendStringInfoChar(es->str, '\n');
	//appendStringInfoSpaces(es->str, 2 * es->indent);
	appendStringInfoChar(es->str, labeled ? '}' : ']');
	es->grouping_stack = list_delete_first(es->grouping_stack);
}

/*
 * Open a group of related objects, without emitting actual data.
 *
 * Prepare the formatting state as though we were beginning a group with
 * the identified properties, but don't actually emit anything.  Output
 * subsequent to this call can be redirected into a separate output buffer,
 * and then eventually appended to the main output buffer after doing a
 * regular FormatOpenGroup call (with the same parameters).
 *
 * The extra "depth" parameter is the new group's depth compared to current.
 * It could be more than one, in case the eventual output will be enclosed
 * in additional nesting group levels.  We assume we don't need to track
 * formatting state for those levels while preparing this group's output.
 *
 * There is no ExplainCloseSetAsideGroup --- in current usage, we always
 * pop this state with ExplainSaveGroup.
 */
static void
ExplainOpenSetAsideGroup(const char *objtype, const char *labelname,
						 bool labeled, int depth, ExplainState *es)
{
	es->grouping_stack = lcons_int(0, es->grouping_stack);
	es->indent += depth;
}

/*
 * Pop one level of grouping state, allowing for a re-push later.
 *
 * This is typically used after ExplainOpenSetAsideGroup; pass the
 * same "depth" used for that.
 *
 * This should not emit any output.  If state needs to be saved,
 * save it at *state_save.  Currently, an integer save area is sufficient
 * for all formats, but we might need to revisit that someday.
 */
static void
ExplainSaveGroup(ExplainState *es, int depth, int *state_save)
{
	es->indent -= depth;
	*state_save = linitial_int(es->grouping_stack);
	es->grouping_stack = list_delete_first(es->grouping_stack);
	
}

/*
 * Re-push one level of grouping state, undoing the effects of ExplainSaveGroup.
 */
static void
ExplainRestoreGroup(ExplainState *es, int depth, int *state_save)
{
	es->grouping_stack = lcons_int(*state_save, es->grouping_stack);
	es->indent += depth;
}

/*
 * Emit a "dummy" group that never has any members.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 */
static void
ExplainDummyGroup(const char *objtype, const char *labelname, ExplainState *es)
{

	ExplainJSONLineEnding(es);
	//appendStringInfoSpaces(es->str, 2 * es->indent);
	if (labelname)
	{
		escape_json(es->str, labelname);
		appendStringInfoString(es->str, ": ");
	}
	escape_json(es->str, objtype);
}

/*
 * Emit the start-of-output boilerplate.
 *
 * This is just enough different from processing a subgroup that we need
 * a separate pair of subroutines.
 */
void
FormatBeginOutput(ExplainState *es)
{
	/* top-level structure is an array of plans */
	//appendStringInfoChar(es->str, '[');
	es->grouping_stack = lcons_int(0, es->grouping_stack);
	es->indent++;
}

/*
 * Emit the end-of-output boilerplate.
 */
void
FormatEndOutput(ExplainState *es)
{
	es->indent--;
	//appendStringInfoString(es->str, "\n]");
	es->grouping_stack = list_delete_first(es->grouping_stack);
}

/*
 * Put an appropriate separator between multiple plans
 */
void
FormatSeparatePlans(ExplainState *es)
{
	/*nothing to do */
}

/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML restricts tag names more than our other output formats, eg they can't
 * contain white space or slashes.  Replace invalid characters with dashes,
 * so that for example "I/O Read Time" becomes "I-O-Read-Time".
 */
static void
ExplainXMLTag(const char *tagname, int flags, ExplainState *es)
{
	const char *s;
	const char *valid = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";

	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoSpaces(es->str, 2 * es->indent);
	appendStringInfoCharMacro(es->str, '<');
	if ((flags & X_CLOSING) != 0)
		appendStringInfoCharMacro(es->str, '/');
	for (s = tagname; *s; s++)
		appendStringInfoChar(es->str, strchr(valid, *s) ? *s : '-');
	if ((flags & X_CLOSE_IMMEDIATE) != 0)
		appendStringInfoString(es->str, " /");
	appendStringInfoCharMacro(es->str, '>');
	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoCharMacro(es->str, '\n');
}

/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.  To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
static void
ExplainJSONLineEnding(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_JSON);
	if (linitial_int(es->grouping_stack) != 0){
		appendStringInfoChar(es->str, ',');
	}else{
		linitial_int(es->grouping_stack) = 1;
	}
	appendStringInfoChar(es->str, '\n');
}

/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabeled group, for which
 * it begins immediately after the "- " that introduces the group.  The first
 * property of the group appears on the same line as the opening "- ".
 */
static void
ExplainYAMLLineStarting(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_YAML);
	if (linitial_int(es->grouping_stack) == 0)
	{
		linitial_int(es->grouping_stack) = 1;
	}
	else
	{
		appendStringInfoChar(es->str, '\n');
		appendStringInfoSpaces(es->str, es->indent * 2);
	}
}

/*
 * YAML is a superset of JSON; unfortunately, the YAML quoting rules are
 * ridiculously complicated -- as documented in sections 5.3 and 7.3.3 of
 * http://yaml.org/spec/1.2/spec.html -- so we chose to just quote everything.
 * Empty strings, strings with leading or trailing whitespace, and strings
 * containing a variety of special characters must certainly be quoted or the
 * output is invalid; and other seemingly harmless strings like "0xa" or
 * "true" must be quoted, lest they be interpreted as a hexadecimal or Boolean
 * constant rather than a string.
 */
static void
escape_yaml(StringInfo buf, const char *str)
{
	escape_json(buf, str);
}
