-- using 1746620641 as a seed to the RNG


select
	c_count,
	count(*) as custdist
from
	(
		select
			o_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
		group by
			o_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc
LIMIT 1;