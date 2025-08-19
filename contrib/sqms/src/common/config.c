#include "postgres.h"
#include "common/config.h"
#include "commands/explain.h"

/* GUC variables */
int	query_min_duration = -1; /* msec or -1 */
double late_tolerance = 1.00;

int	stat_log_level = LOG;
int redis_port = 99999;
long totalFetchTimeoutMillis = 1000;
long totalSetTimeMillis = 1000;
long defaultTTLSeconds = INT_MAX;
const size_t shared_mem_size = 1024* 1024* 1024 * 10;
const int node_search_topk = 5;
const int max_msg_size = 1024; 

bool debug = false;
bool excavate_enabled = true;
bool plan_match_enabled = true;
bool node_match_enabled = true;
bool collect_scans_enabled = false;
bool plan_equal_enabled = false;
bool prune_constants_enabled = false;
char *sqms_log_directory = NULL;

const char* redis_host = "127.0.0.1";
const char* shared_index_name = "HISTORY_QUERY_TREE_INDEX";
const char* scan_index_name = "HISTORY_QUERY_SCAN_INDEX";
const char* queue_name = "query_index";
const char* plan_hash_table_name = "PLAN_HASH_TABLE";
const char* sqmslogger_name = "SQMS_LOGGER";

/* dsa */
dsa_pointer *shared_index_ptr = NULL;
dsa_pointer *scan_index_ptr = NULL;
dsa_pointer *plan_hash_table_ptr = NULL;
dsa_pointer *sqmslogger_ptr = NULL;
dsa_area *area_ = NULL;