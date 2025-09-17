CREATE FUNCTION match_avg_overhead()
RETURNS double precision
AS 'sqms', 'match_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION plan_match_avg_overhead()
RETURNS double precision
AS 'sqms', 'plan_match_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION node_match_avg_overhead()
RETURNS double precision
AS 'sqms', 'node_match_avg_overhead'
LANGUAGE C STRICT;

CREATE FUNCTION clear_avg_overhead()
RETURNS double precision
AS 'sqms', 'clear_avg_overhead'
LANGUAGE C STRICT;