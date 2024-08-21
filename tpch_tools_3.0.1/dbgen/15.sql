-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998
:x
:o
with revenue:s as (select
		l_suppkey supplier_no,
		sum(l_extendedprice * (1 - l_discount)) total_revenue
	from
		lineitem
	where
		l_shipdate >= date ':1'
		and l_shipdate < date ':1' + interval '3' month
	group by
		l_suppkey)
select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue:s
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue:s
	)
order by
	s_suppkey;

:n -1
