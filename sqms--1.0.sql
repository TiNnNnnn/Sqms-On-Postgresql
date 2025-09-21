CREATE FUNCTION match_avg_overhead()
RETURNS double precision
AS 'sqms', 'match_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION plan_match_avg_overhead()
RETURNS double precision
AS 'sqms', 'plan_match_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION plan_search_avg_overhead()
RETURNS double precision
AS 'sqms', 'plan_search_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION node_match_avg_overhead()
RETURNS double precision
AS 'sqms', 'node_match_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION node_search_avg_overhead()
RETURNS double precision
AS 'sqms', 'node_search_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION clear_avg_overhead()
RETURNS double precision
AS 'sqms', 'clear_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION cur_plan_search_cnt()
RETURNS int
AS 'sqms', 'cur_plan_search_cnt'
LANGUAGE C STRICT;

CREATE FUNCTION cur_node_search_cnt()
RETURNS int
AS 'sqms', 'cur_node_search_cnt'
LANGUAGE C STRICT;

CREATE FUNCTION cur_plan_match_overhead()
RETURNS double precision
AS 'sqms', 'cur_plan_match_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION cur_node_match_overhead()
RETURNS double precision
AS 'sqms', 'cur_node_match_overhead'
LANGUAGE C STRICT;

