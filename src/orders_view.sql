create or replace view orders as
select o.*, olog.status
from production.orders o
left join (
	select distinct(order_id), first_value(status_id) over(partition by order_id order by dttm desc) status
	from production.orderstatuslog
	) olog on o.order_id = olog.order_id 
