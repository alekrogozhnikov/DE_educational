select orders.user_id,
		count(order_id) cnt_order,
		ntile(5) over(order by count(order_id)) group_frequency
from orderstatuslog o 
	left join orderstatuses os on o.status_id = os.id
	left join orders using(order_id)
where key = 'Closed' and date_part('year', orders.order_ts) >= 2021
group by orders.user_id 
order by cnt_order
