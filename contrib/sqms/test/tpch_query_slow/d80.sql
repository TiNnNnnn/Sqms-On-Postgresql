-- using 1745049466 as a seed to the RNG


select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('MAIL', 'AIR')
	and l_shipdate = l_receiptdate
	and l_receiptdate >= date '1993-01-01'
	and l_receiptdate < date '1993-01-01' + interval '2' year
group by
	l_shipmode
order by
	l_shipmode
LIMIT 1;