insert into dm_rfm_segments (user_id, recency, frequency, monetary_value)
	select user_id, recency, frequency, monetary_value
	from tmp_rfm_recency 
		left join tmp_rfm_frequency using(user_id)
		left join tmp_rfm_monetary_value using(user_id)
	
	
user_id|recency|frequency|monetary_value|
-------+-------+---------+--------------+
      0|      3|        5|             5|
      1|      4|        3|             4|
      2|      3|        3|             3|
      3|      3|        3|             2|
      4|      3|        3|             2|
      5|      4|        5|             5|
      6|      1|        2|             2|
      7|      5|        1|             2|
      8|      4|        1|             2|
      9|      4|        3|             2|
