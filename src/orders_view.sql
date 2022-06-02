create or replace view analysis.orders as 
	select o.order_id, o.order_ts, o.user_id, o.bonus_payment, o.payment, o."cost", o.bonus_grant, olog.status
	from production.orders o 
	left join ( 
		select distinct(order_id), first_value(status_id) over(partition by order_id order by dttm desc) status 
		from production.orderstatuslog
		) olog on o.order_id = olog.order_id
