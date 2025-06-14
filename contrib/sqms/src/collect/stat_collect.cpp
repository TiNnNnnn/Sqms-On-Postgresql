#include "collect/stat_collect.hpp"
#include "collect/stat_format.hpp"
#include <threads.h>
#include <sys/file.h>  // for flock
#include <time.h>      // for time functions
#include "utils/dsa.h"
#include "utils/snapmgr.h"
#include "commands/explain.h"

extern "C" {
	PG_MODULE_MAGIC;

	static ExecutorStart_hook_type prev_ExecutorStart = NULL;
	static ExecutorRun_hook_type prev_ExecutorRun = NULL;
	static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
	static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
	static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
	static emit_log_hook_type prev_log_hook = NULL;
	static ExplainOneQuery_hook_type prev_explain_one_query_hook = NULL;
	
	static char time_str[20];
	#define MY_DSA_TRANCHE_ID  100

	void StmtExecutorStart(QueryDesc *queryDesc, int eflags);
	void StmtExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once);
	void StmtExecutorFinish(QueryDesc *queryDesc);
	void StmtExecutorEnd(QueryDesc *queryDesc);
	void RegisterQueryIndex();
	void ShuntLog(ErrorData *edata);
	void ExplainOneQueryWithSlow(Query *query,
							int cursorOptions,
							IntoClause *into,
							ExplainState *es,
							const char *queryString,
							ParamListInfo params,
							QueryEnvironment *queryEnv);

    void		_PG_init(void);
    void		_PG_fini(void);

    void _PG_init(void){
		DefineCustomIntVariable("sqms.query_min_duration",
							"Sets the minimum execution time above which plans will be logged.",
							"Zero prints all plans. -1 turns this feature off.",
							&query_min_duration,
							-1,
							-1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL); 
        prev_ExecutorStart = ExecutorStart_hook;
        ExecutorStart_hook = StmtExecutorStart;

        prev_ExecutorRun = ExecutorRun_hook;
        ExecutorRun_hook = StmtExecutorRun;

        prev_ExecutorFinish = ExecutorFinish_hook;
        ExecutorFinish_hook = StmtExecutorFinish;

        prev_ExecutorEnd = ExecutorEnd_hook;
        ExecutorEnd_hook = StmtExecutorEnd;

		RequestAddinShmemSpace(20485760000);	
		
		prev_explain_one_query_hook = ExplainOneQuery_hook;
		ExplainOneQuery_hook = ExplainOneQueryWithSlow;
		
		prev_shmem_startup_hook = shmem_startup_hook;
		shmem_startup_hook = RegisterQueryIndex;
    }

    void _PG_fini(void){
        ExecutorStart_hook = prev_ExecutorStart;
        ExecutorRun_hook = prev_ExecutorRun;
        ExecutorFinish_hook = prev_ExecutorFinish;
        ExecutorEnd_hook = prev_ExecutorEnd;       
		ExplainOneQuery_hook = prev_explain_one_query_hook;
		shmem_startup_hook = prev_shmem_startup_hook;
    }
};

int StatCollecter::nesting_level = 0;
bool StatCollecter::current_query_sampled = false;
/*we hope the index built while database starting*/

StatCollecter::StatCollecter(){
}

void StatCollecter::StmtExecutorStartWrapper(QueryDesc *queryDesc, int eflags){

	/*sqi just process cmd_select*/
    if(auto_explain_enabled()){
		/* Enable per-node instrumentation iff log_analyze is required. */
		if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		{
			queryDesc->instrument_options |= INSTRUMENT_TIMER;
			queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
			queryDesc->instrument_options |= INSTRUMENT_WAL;
		}
		
        if (prev_ExecutorStart)
            prev_ExecutorStart(queryDesc, eflags);
        else
            standard_ExecutorStart(queryDesc, eflags);

        if (auto_explain_enabled() && queryDesc->operation == CMD_SELECT)
        {
            //global elapsed for query
            if (queryDesc->totaltime == NULL)
            {
                MemoryContext oldcxt;
                oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
                queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
				
				/*analyse query whether a slow query or not*/
				PlanStatFormat& es = PlanStatFormat::getInstance();
				es.ProcQueryDesc(queryDesc,oldcxt,false);
                
				MemoryContextSwitchTo(oldcxt);
            }
        }
    }
}

void StatCollecter::StmtExecutorRunWrapper(QueryDesc *queryDesc,ScanDirection direction,uint64 count, bool execute_once){	
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

void StatCollecter::StmtExecutorFinishWrapper(QueryDesc *queryDesc){
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();    
}

void StatCollecter::StmtExecutorEndWrapper(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && auto_explain_enabled() && queryDesc->operation == CMD_SELECT){
		MemoryContext oldcxt;
		double		msec; 
		/*
		 * Make sure we operate in the per-query context, so any cruft will be
		 * discarded later during ExecutorEnd.
		 */
		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);
		
		/*stoage plan stats*/
		PlanStatFormat& es = PlanStatFormat::getInstance();
		es.ProcQueryDesc(queryDesc,oldcxt,true); 
		
		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd){
		prev_ExecutorEnd(queryDesc);
	}else{
		standard_ExecutorEnd(queryDesc);
	}
}

void StatCollecter::ExplainOneQueryWithSlowWrapper(Query *query,
							int cursorOptions,
							IntoClause *into,
							ExplainState *es,
							const char *queryString,
							ParamListInfo params,
							QueryEnvironment *queryEnv){
	if (prev_explain_one_query_hook)
        prev_explain_one_query_hook(query, cursorOptions, into, es, queryString, params, queryEnv);
    else{
		PlannedStmt *plan;
		instr_time	planstart,
					planduration;
		BufferUsage bufusage_start,
					bufusage;

		if (es->buffers)
			bufusage_start = pgBufferUsage;
		INSTR_TIME_SET_CURRENT(planstart);

		/* plan the query */
		plan = pg_plan_query(query, queryString, cursorOptions, params);

		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planstart);
		/* calc differences of buffer counters. */
		if (es->buffers)
		{
			memset(&bufusage, 0, sizeof(BufferUsage));
			BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
		}
		/* run it (if needed) and produce output */
		int			instrument_option = 0;
		DestReceiver *dest = None_Receiver;
		int			eflags;

		if (es->analyze && es->timing)
			instrument_option |= INSTRUMENT_TIMER;
		else if (es->analyze)
			instrument_option |= INSTRUMENT_ROWS;
		if (es->buffers)
			instrument_option |= INSTRUMENT_BUFFERS;
		if (es->wal)
			instrument_option |= INSTRUMENT_WAL;

		/* Select execution options,SlowViews has not support explain with analyzing yet*/
		if (es->analyze)
			eflags = 0;
		else
			eflags = EXEC_FLAG_EXPLAIN_ONLY;
		
		es->format = ExplainFormat::EXPLAIN_FORMAT_TEXT;
		QueryDesc  *queryDesc = CreateQueryDesc(plan, queryString,
								InvalidSnapshot, InvalidSnapshot,
								dest, params, queryEnv, instrument_option);
		
								standard_ExecutorStart(queryDesc, eflags);
		ExplainOpenGroup("Query", NULL, true, es);
		/* Create textual dump of plan tree */
		ExplainPrintPlan(es, queryDesc);
		
		/* search history slow view index*/
		PlanStatFormat& psf = PlanStatFormat::getInstance();
		psf.ExplainQueryDesc(queryDesc,es);

		standard_ExecutorEnd(queryDesc);
		FreeQueryDesc(queryDesc);
		
		ExplainCloseGroup("Query", NULL, true, es);
	}
}



extern "C" void RegisterQueryIndex(){

	std::cout<<"begin building history query index..."<<std::endl;
	bool found = true;
	auto shared_index = (HistoryQueryLevelTree*)ShmemInitStruct(shared_index_name, sizeof(HistoryQueryLevelTree), &found);

	if (!shared_index) {
        elog(ERROR, "Failed to allocate shared memory for root node.");
        return;
	}
	if(!found){
		new (shared_index) HistoryQueryLevelTree(1);
	}
	std::cout<<"finish building history query index..."<<std::endl;
	
	found = true;
	auto scan_index = (HistoryQueryLevelTree*)ShmemInitStruct(scan_index_name, sizeof(HistoryQueryLevelTree), &found);
	if(!scan_index) {
		elog(ERROR, "Failed to allocate shared memory for scan index.");
		return;
	}
	if(!found){
		new (scan_index) HistoryQueryLevelTree(1);
	}

	/**
	 * TODO: here we should load history slow queries in redis into shared_index
	 */

	std::cout<<"begin building sqms logger..."<<std::endl;
	found = false;
	auto logger = (SqmsLogger*)ShmemInitStruct("SqmsLogger", sizeof(SqmsLogger), &found);
	if(!found){
		new (logger) SqmsLogger();
	}
	std::cout<<"finsh building sqms logger..."<<std::endl;
}


extern "C" void StmtExecutorStart(QueryDesc *queryDesc, int eflags) {
    StatCollecter::StmtExecutorStartWrapper(queryDesc, eflags);
}

extern "C" void StmtExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
    StatCollecter::StmtExecutorRunWrapper(queryDesc, direction, count, execute_once);
}

extern "C" void StmtExecutorFinish(QueryDesc *queryDesc) {
    StatCollecter::StmtExecutorFinishWrapper(queryDesc);
}

extern "C" void StmtExecutorEnd(QueryDesc *queryDesc) {
    StatCollecter::StmtExecutorEndWrapper(queryDesc);
}

extern "C" void ExplainOneQueryWithSlow(Query *query,int cursorOptions,IntoClause *into,
							ExplainState *es,const char *queryString,ParamListInfo params,
							QueryEnvironment *queryEnv) {
    StatCollecter::ExplainOneQueryWithSlowWrapper(query,cursorOptions,into,
							es,queryString,params,queryEnv);
}

static const char* error_severity(int elevel)
{
    if (elevel >= ERROR)
        return "ERROR";
    else if (elevel >= WARNING)
        return "WARNING";
    else if (elevel >= NOTICE)
        return "NOTICE";
    else if (elevel >= INFO)
        return "INFO";
    else
        return "DEBUG";
}

extern "C" void ShuntLog(ErrorData *edata){
    FILE *log_file = NULL;
    int fd;
    char log_filename[MAXPGPATH] = {0};
    
    /* timestamp str */
    const char* log_directory = "/home/yyk/Sqms-On-Postgresql/log";
    char log_prefix[32] = "DEFAULT"; 

    /* parse hint，use hint as prefix of log name */
    if (edata->hint && strlen(edata->hint) > 0)
    {
        snprintf(log_prefix, sizeof(log_prefix), "%s", edata->hint);
    }
    
    /* generate log file name */
    snprintf(log_filename, sizeof(log_filename), "%s/%s_%s.log", log_directory, log_prefix, time_str);

    /* open file with file descriptor */
    fd = open(log_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd < 0) return; 

    /* acquire lock */
    flock(fd, LOCK_EX);

    /* write log */
    log_file = fdopen(fd, "a");
    if (log_file)
    {
        fprintf(log_file, "[%s] %s\n", error_severity(edata->elevel), edata->message);
        fclose(log_file);
    }

    /* release lock */
    flock(fd, LOCK_UN);
    close(fd);

    /* call previous log hook */
    if (prev_log_hook)
        prev_log_hook(edata);
}