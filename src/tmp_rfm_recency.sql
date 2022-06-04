insert into analysis.tmp_rfm_recency(user_id, recency)
	select u.id user_id,
			ntile(5) over(order by max(order_ts) nulls first) as recency
	from analysis.users u 
		left join analysis.orders o on o.user_id = u.id and o.status = 4
	group by 1
