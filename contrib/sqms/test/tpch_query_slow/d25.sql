-- using 1746620641 as a seed to the RNG


select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1995-01-01'
	and l_shipdate < date '1995-01-01' + interval '2' year
	and l_discount >= 0.05 - 0.02 
	and l_discount <= 0.05 + 0.02
	and l_quantity < 25
LIMIT 1;