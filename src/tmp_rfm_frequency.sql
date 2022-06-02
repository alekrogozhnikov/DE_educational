insert into analysis.tmp_rfm_frequency (user_id, frequency) 
	select user_id, 
		ntile(5) over(order by count(order_id),user_id) frequency 
	from analysis.orders
	where date_part('year', order_ts) >= 2021 and status = 4
	group by user_id
