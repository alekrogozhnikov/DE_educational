insert into tmp_rfm_recency (user_id, recency)
	select user_id,
		ntile(5) over(order by MAX(order_ts), user_id) recency
	from orders
	group by user_id
