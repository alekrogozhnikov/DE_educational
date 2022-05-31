select user_id,
	sum(cost) total_orders,
	ntile(5) over(order by sum(cost), user_id) monetary_value
from orders
group by user_id
order by total_orders, user_id
