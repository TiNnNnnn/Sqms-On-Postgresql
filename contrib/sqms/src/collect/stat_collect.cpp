#include "collect/stat_collect.hpp"
#include "collect/stat_format.hpp"
#include <threads.h>

extern "C" {
	PG_MODULE_MAGIC;

	static ExecutorStart_hook_type prev_ExecutorStart = NULL;
	static ExecutorRun_hook_type prev_ExecutorRun = NULL;
	static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
	static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

	void StmtExecutorStart(QueryDesc *queryDesc, int eflags);
	void StmtExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once);
	void StmtExecutorFinish(QueryDesc *queryDesc);
	void StmtExecutorEnd(QueryDesc *queryDesc);

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
		EmitWarningsOnPlaceholders("sqms");

        prev_ExecutorStart = ExecutorStart_hook;
        ExecutorStart_hook = StmtExecutorStart;

        prev_ExecutorRun = ExecutorRun_hook;
        ExecutorRun_hook = StmtExecutorRun;

        prev_ExecutorFinish = ExecutorFinish_hook;
        ExecutorFinish_hook = StmtExecutorFinish;

        prev_ExecutorEnd = ExecutorEnd_hook;
        ExecutorEnd_hook = StmtExecutorEnd;

		/* create index in pg shared_memory */
		
    }

    void _PG_fini(void){
        ExecutorStart_hook = prev_ExecutorStart;
        ExecutorRun_hook = prev_ExecutorRun;
        ExecutorFinish_hook = prev_ExecutorFinish;
        ExecutorEnd_hook = prev_ExecutorEnd;       
    }
};

int StatCollecter::nesting_level = 0;
bool StatCollecter::current_query_sampled = false;
/*we hope the index built while database starting*/

StatCollecter::StatCollecter(){
}

void StatCollecter::StmtExecutorStartWrapper(QueryDesc *queryDesc, int eflags){
    
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
        
        if (auto_explain_enabled())
        {
            //global elapsed for query
            if (queryDesc->totaltime == NULL)
            {
                MemoryContext oldcxt;
                oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
                queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
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
	if (queryDesc->totaltime && auto_explain_enabled()){
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
		msec = queryDesc->totaltime->total * 1000.0;
		if (msec >= query_min_duration){
		   PlanStatFormat& es = PlanStatFormat::getInstance();
		   es.ProcQueryDesc(queryDesc);
		}
		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd){
		prev_ExecutorEnd(queryDesc);
	}else{
		standard_ExecutorEnd(queryDesc);
	}
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
