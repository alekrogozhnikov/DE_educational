insert into tmp_rfm_frequency (user_id, frequency)
	select user_id,
		ntile(5) over(order by count(order_id),user_id) frequency
	from orders
	group by user_id
