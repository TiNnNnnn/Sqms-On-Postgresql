#include "postgres.h"
#include "common/config.h"
#include "commands/explain.h"


/* GUC variables */
int	query_min_duration = -1; /* msec or -1 */
double late_tolerance = 1.00;

/*unused variables */
//static bool stat_verbose = false;
//bool stat_buffers = false;
//bool stat_wal = false;
//static bool stat_triggers = false;
//static bool stat_timing = true;
//static bool statsettings = false;
//static int	stat_format = EXPLAIN_FORMAT_JSON;
//bool stat_nested_statements = false;
//double stat_sample_rate = 1;

int	stat_log_level = LOG;
int redis_port = 99999;
long totalFetchTimeoutMillis = 1000;
long totalSetTimeMillis = 1000;
long defaultTTLSeconds = INT_MAX;
const size_t shared_mem_size = 1024* 1024* 1024 * 10;
const int max_msg_size = 1024; 

bool debug = false;
bool excavate_enabled = false;
bool plan_match_enabled = true;
bool node_match_enabled = true;

const char* redis_host = "127.0.0.1";
const char* shared_index_name = "HISTORY_QUERY_TREE_INDEX";
const char* queue_name = "query_index";
