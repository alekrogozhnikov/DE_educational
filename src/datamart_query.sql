insert into dm_rfm_segments (user_id, recency, frequency, monetary_value)
	select user_id, recency, frequency, monetary_value
	from tmp_rfm_recency 
		left join tmp_rfm_frequency using(user_id)
		left join tmp_rfm_monetary_value using(user_id)
