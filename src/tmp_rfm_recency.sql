select user_id,
	count(order_id) cnt_orders,
	ntile(5) over(order by count(order_id)) recency
from orders
group by user_id
order by cnt_orders
