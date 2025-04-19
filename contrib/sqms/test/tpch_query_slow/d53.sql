-- using 1745049466 as a seed to the RNG


select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1995-01-01'
	and l_shipdate < date '1995-01-01' + interval '1' year
	and l_discount >= 0.07 - 0.01 
	and l_discount <= 0.07 + 0.01
	and l_quantity < 24
LIMIT 1;
