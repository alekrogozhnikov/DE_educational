insert into analysis.tmp_rfm_frequency (user_id, frequency)
	select u.id user_id,
			ntile(5) over(order by count(o.order_id)) as frequency
	from analysis.users u 
		left join analysis.orders o on o.user_id = u.id and o.status = 4
	group by 1
