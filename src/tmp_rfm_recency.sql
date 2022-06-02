insert into analysis.tmp_rfm_recency (user_id, recency) 
	select user_id, 
		ntile(5) over(order by MAX(order_ts), user_id) recency 
	from analysis.orders
	where date_part('year', order_ts) >= 2021 and status = 4
	group by user_id 
