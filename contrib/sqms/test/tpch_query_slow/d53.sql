-- using 1745679353 as a seed to the RNG


select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1993-01-01'
	and l_shipdate < date '1993-01-01' + interval '1' year
	and l_discount >= 0.07 - 0.01 
	and l_discount <= 0.07 + 0.01
	and l_quantity < 24
LIMIT 1;
