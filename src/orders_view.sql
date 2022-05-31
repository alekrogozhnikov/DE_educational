CREATE OR REPLACE view orders as
	select o.*, (select max(status_id)
				from production.orderstatuslog ol
				where o.order_id = ol.order_id
				group by order_id) status
	from production.orders o;
