insert into tmp_rfm_monetary_value (user_id, monetary_value)
select user_id,
	ntile(5) over(order by sum(cost), user_id) monetary_value
from orders
group by user_id
