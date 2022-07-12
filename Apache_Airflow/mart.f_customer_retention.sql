insert into mart.f_customer_retention(
									new_customers_count,
									returning_customers_count,
									refunded_customer_count,
									period_id,
									item_id,
									new_customers_revenue,
									returning_customers_revenue,
									customers_refunded
									)
with stnd as (
	select date_id,
			item_id,
			customer_id,
			payment_amount,
			status,
			week_of_year 
	from mart.f_sales
		left join mart.d_calendar dc using(date_id)
)
select sum(case when count = 1 then 1 else 0 end) new_customers_count,
		count(distinct stnd.customer_id) - sum(case when count = 1 then 1 else 0 end) returning_customers_count,
		count(distinct case when status = 'refunded' then stnd.customer_id else null end) refunded_customer_count,
		stnd.week_of_year period_id,
		stnd.item_id,
		(sum(case when count = 1 then payment_amount else 0 end))::numeric(10,2) new_customers_revenue,
		(sum(case when count != 1 then payment_amount else 0 end))::numeric(10,2) returning_customers_revenue,
		sum(case when status = 'refunded' then 1 else 0 end) customers_refunded
from stnd
	left join (select customer_id, week_of_year, stnd.item_id, count(*)
				from stnd
				group by customer_id, week_of_year, item_id) count_sales on count_sales.customer_id = stnd.customer_id and 
																	count_sales.week_of_year = stnd.week_of_year and 
																	count_sales.item_id = stnd.item_id
group by stnd.week_of_year, stnd.item_id