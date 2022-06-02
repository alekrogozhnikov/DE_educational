insert into analysis.tmp_rfm_monetary_value(user_id, monetary_value)
	select user_id,
		ntile(5) over(order by sum(cost), user_id) monetary_value
	from analysis.orders
	where date_part('year', order_ts) >= 2021 and status = 4
	group by user_id
