#pragma once
#include <limits.h>
#include <stdbool.h>
#include <stdlib.h>

/* GUC variables */
extern int	query_min_duration;
extern double late_tolerance;

#define bit_map_size 1000
#define LOWER_LIMIT "SQMS_LOW_LIMIT"
#define UPPER_LIMIT "SQMS_UPPER_LIMIT"

extern int	stat_log_level;
extern const char* redis_host;
extern int redis_port;
extern long totalFetchTimeoutMillis;
extern long totalSetTimeMillis;
extern long defaultTTLSeconds;

/*if using debug mode*/
extern bool debug;
/*if enable to use excavate core sub plan*/
extern bool excavate_enabled;
/*if enable to use plan phyiscal view metod to match slow plans*/
extern bool plan_match_enabled;
/*if enable to use node phyiscla view stitch metod to match slow plans*/
extern bool node_match_enabled;
/*gc params*/
extern bool collect_scans_enabled;
/*baseline strategy params, only for test*/
extern bool plan_equal_enabled;

extern bool parallel_search_enabled;
/**
 * if enable to prune constants from equal predicates
 * For System Secutrity,we don't open this prams to users;
 **/
extern bool prune_constants_enabled;
extern char* sqms_log_directory;
/*unused param,max msg size for plan storaging*/
extern const int max_msg_size;
/*share mem size*/
extern const size_t shared_mem_size;
/*top k node search for node strategy*/
extern const int node_search_topk;

extern const char* queue_name;
extern const char* shared_index_name;
extern const char* scan_index_name;
extern const char* plan_hash_table_name;

extern double plan_match_time;
extern double node_match_time;
extern double clear_time;

extern int  plan_match_cnt;
extern int  node_match_cnt;
extern int  clear_cnt;

extern int total_match_time;
extern int total_match_cnt;

extern int plan_search_cnt;
extern int node_search_cnt;

extern int cur_finish_plan_cnt;
extern int cur_finish_node_num;

extern double cur_plan_overhead;
extern double cur_node_overhead;