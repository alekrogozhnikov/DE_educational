insert into analysis.tmp_rfm_monetary_value(user_id, monetary_value)
	select u.id user_id,
			ntile(5) over(order by sum(payment) nulls first) as monetary_value
	from analysis.users u 
		left join analysis.orders o on o.user_id = u.id and o.status = 4
	group by 1
