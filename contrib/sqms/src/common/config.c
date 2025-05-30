#include "postgres.h"
#include "common/config.h"
#include "commands/explain.h"


/* GUC variables */
int	query_min_duration = -1; /* msec or -1 */
double late_tolerance = 1.00;
//static bool stat_verbose = false;
//bool stat_buffers = false;
//bool stat_wal = false;
//static bool stat_triggers = false;
//static bool stat_timing = true;
//static bool statsettings = false;
//static int	stat_format = EXPLAIN_FORMAT_JSON;
int	stat_log_level = LOG;
//bool stat_nested_statements = false;
//double stat_sample_rate = 1;

const char* redis_host = "127.0.0.1";
int redis_port = 99999;
long totalFetchTimeoutMillis = 1000;
long totalSetTimeMillis = 1000;
long defaultTTLSeconds = INT_MAX;

bool debug = true;
const char* queue_name = "query_index";
const size_t shared_mem_size = 1024* 1024* 1024 * 10;
const char* shared_index_name = "HISTORY_QUERY_TREE_INDEX";
const int max_msg_size = 1024; 
// const struct config_enum_entry format_options[] = {
// 	{"text", EXPLAIN_FORMAT_TEXT, false},
// 	{"xml", EXPLAIN_FORMAT_XML, false},
// 	{"json", EXPLAIN_FORMAT_JSON, false},
// 	{"yaml", EXPLAIN_FORMAT_YAML, false},
// 	{NULL, 0, false}
// };

// const struct config_enum_entry loglevel_options[] = {
// 	{"debug5", DEBUG5, false},
// 	{"debug4", DEBUG4, false},
// 	{"debug3", DEBUG3, false},
// 	{"debug2", DEBUG2, false},
// 	{"debug1", DEBUG1, false},
// 	{"debug", DEBUG2, true},
// 	{"info", INFO, false},
// 	{"notice", NOTICE, false},
// 	{"warning", WARNING, false},
// 	{"log", LOG, false},
// 	{NULL, 0, false}
// };