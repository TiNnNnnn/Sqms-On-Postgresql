-- using 1746620641 as a seed to the RNG


select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp,
			(
				select
					l_partkey agg_partkey,
					l_suppkey agg_suppkey,
					0.5 * sum(l_quantity) AS agg_quantity
				from
					lineitem
				where
					l_shipdate >= date '1994-01-01'
					and l_shipdate < date '1994-01-01' + interval '1' year
				group by
					l_partkey,
					l_suppkey
			) agg_lineitem
		where
			agg_partkey = ps_partkey
			and agg_suppkey = ps_suppkey
			and agg_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'moccasin%'
			)
			and ps_availqty > agg_quantity
	)
	and s_nationkey = n_nationkey
order by
	s_name
LIMIT 1;
