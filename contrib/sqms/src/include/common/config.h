#pragma once
#include <limits.h>

#include "utils/guc.h"

/* GUC variables */
static int	query_min_duration = -1; /* msec or -1 */
static bool stat_analyze = false;
//static bool stat_verbose = false;
static bool stat_buffers = false;
static bool stat_wal = false;
//static bool stat_triggers = false;
static bool stat_timing = true;
//static bool statsettings = false;
//static int	stat_format = EXPLAIN_FORMAT_JSON;
//static int	stat_log_level = LOG;
static bool stat_nested_statements = false;
static double stat_sample_rate = 1;

static const int bit_map_size = 100;

static const struct config_enum_entry format_options[] = {
	{"text", EXPLAIN_FORMAT_TEXT, false},
	{"xml", EXPLAIN_FORMAT_XML, false},
	{"json", EXPLAIN_FORMAT_JSON, false},
	{"yaml", EXPLAIN_FORMAT_YAML, false},
	{NULL, 0, false}
};

static const struct config_enum_entry loglevel_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"log", LOG, false},
	{NULL, 0, false}
};

