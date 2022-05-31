select user_id,
	MAX(order_ts) lasttime_orders,
	ntile(5) over(order by MAX(order_ts), user_id) recency
from orders
group by user_id
order by lasttime_orders, user_id
