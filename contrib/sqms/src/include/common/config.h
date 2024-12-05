#pragma once
#include <limits.h>
#include <stdbool.h>
#include <stdlib.h>
// extern "C" {
//     #include "utils/guc.h"
// };

/* GUC variables */
extern int	query_min_duration;
extern double late_tolerance;
//static bool stat_verbose = false;
//extern bool stat_buffers;
//extern bool stat_wal ;
//static bool stat_triggers = false;
//static bool stat_timing = true;
//static bool statsettings = false;
//static int	stat_format = EXPLAIN_FORMAT_JSON;
extern int	stat_log_level;
//extern bool stat_nested_statements;
//extern double stat_sample_rate;

//extern constexpr size_t bit_map_size;
#define bit_map_size 100


extern const char* redis_host;
extern int redis_port;
extern long totalFetchTimeoutMillis;
extern long totalSetTimeMillis;
extern long defaultTTLSeconds;

extern bool debug;

// extern const struct config_enum_entry format_options[];

// extern const struct config_enum_entry loglevel_options[];

