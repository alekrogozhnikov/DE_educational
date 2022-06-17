--------------------------------- shipping_country_rates ---------------------------------

-- Удалим таблицу если она есть:
drop table if exists public.shipping_country_rates cascade;

-- Создадим таблицу `shipping_country_rates ` - справочник стоимости доставки в страны
create table public.shipping_country_rates (
	shipping_country_id serial primary key,
	shipping_country text,
	shipping_country_base_rate numeric(14,3)
);

-- Наполним таблицу `shipping_country_rates` данными:
insert into public.shipping_country_rates (shipping_country, shipping_country_base_rate)
select distinct shipping_country, shipping_country_base_rate 
from public.shipping s;


--------------------------------- shipping_agreement ---------------------------------
-- Удалим таблицу если она есть:
drop table if exists public.shipping_agreement cascade;

-- Создадим таблицу `shipping_agreement ` - справочник тарифов доставки вендора по договору
create table public.shipping_agreement(
	agreementid int primary key,
	agreement_number text,
	agreement_rate numeric(14,3),
	agreement_commission numeric(14,3)
);

-- Наполним таблицу `shipping_agreement` данными:
insert into public.shipping_agreement
select distinct (regexp_split_to_array(vendor_agreement_description, ':'))[1]::integer agreementid,
	(regexp_split_to_array(vendor_agreement_description, ':'))[2]::text,
	(regexp_split_to_array(vendor_agreement_description, ':'))[3]::numeric(14,3),
	(regexp_split_to_array(vendor_agreement_description, ':'))[4]::numeric(14,3)
from public.shipping
order by agreementid;


--------------------------------- shipping_transfer ---------------------------------
-- Удалим таблицу если она есть:
drop table if exists public.shipping_transfer cascade;

-- Создадим таблицу `shipping_transfer ` - справочник о типах доставки
create table public.shipping_transfer(
	transfer_type_id serial primary key,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric(14,3)
);

-- Наполним таблицу `shipping_transfer` данными:
insert into public.shipping_transfer(transfer_type, transfer_model, shipping_transfer_rate)
select distinct (regexp_split_to_array(shipping_transfer_description, ':'))[1]::text,
	(regexp_split_to_array(shipping_transfer_description, ':'))[2]::text,
	shipping_transfer_rate 
from public.shipping;


--------------------------------- shipping_info ---------------------------------
-- Удалим таблицу если она есть:
drop table if exists public.shipping_info cascade;

-- Создадим таблицу `shipping_info `
create table public.shipping_info(
	shippingid bigint primary key,
	vendorid int,
	payment_amount numeric(14,2),
	shipping_plan_datetime timestamp,
	transfer_type_id bigint references public.shipping_transfer(transfer_type_id),
	shipping_country_id bigint references public.shipping_country_rates(shipping_country_id),
	agreementid bigint references public.shipping_agreement (agreementid)
);

-- Наполним таблицу `shipping_info` данными:
insert into public.shipping_info
select distinct
		q_s.shippingid,
		q_s.vendorid,
		q_s.payment_amount,
		q_s.shipping_plan_datetime,
		s_t.transfer_type_id,
		s_c_r.shipping_country_id,
		s_a.agreementid
from (
		select distinct 
			shippingid,
			vendorid,
			payment_amount,
			shipping_plan_datetime,
			shipping_transfer_description,
			shipping_country,
			vendor_agreement_description
		from public.shipping s) q_s
left join public.shipping_transfer s_t
on 
	(regexp_split_to_array(q_s.shipping_transfer_description, ':'))[1]::text = s_t.transfer_type and
	(regexp_split_to_array(q_s.shipping_transfer_description, ':'))[2]::text = s_t.transfer_model
left join public.shipping_country_rates s_c_r on q_s.shipping_country = s_c_r.shipping_country
left join public.shipping_agreement s_a on (regexp_split_to_array(q_s.vendor_agreement_description, ':'))[1]::integer = s_a.agreementid;


--------------------------------- shipping_status ---------------------------------
-- Удалим таблицу если она есть:
drop table if exists public.shipping_status cascade;

-- Создадим таблицу `shipping_status `
create table public.shipping_status(
	shippingid bigint primary key,
	status text, --статус доставки в таблице shipping по данному shippingid. Может принимать значения in_progress — доставка в процессе, либо finished — доставка завершена--
	state text,  --промежуточные точки заказа, которые изменяются в соответствии с обновлением информации о доставке по времени state_datetime
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp
);

-- Наполним таблицу `shipping_status` данными:
insert into public.shipping_status
select distinct shippingid::int,
	(first_value(status) over(partition by shippingid order by state_datetime desc))::text status,
	(first_value(state) over(partition by shippingid order by state_datetime desc))::text state,
	s_s_f_d.shipping_start_fact_datetime,
	shipping_end_fact_datetime
from public.shipping q_s
	left join (
		select shippingid shipping_id_1,
				state_datetime as shipping_start_fact_datetime		
		from public.shipping
		where state = 'booked'
	) s_s_f_d on s_s_f_d.shipping_id_1 = q_s.shippingid
	left join (
		select shippingid shipping_id_2,
				state_datetime as shipping_end_fact_datetime 		
		from public.shipping
		where state = 'recieved'
	) s_e_f_d on s_e_f_d.shipping_id_2 = q_s.shippingid;


--------------------------------- shipping_datamart ---------------------------------
-- Создадим представление `shipping_datamart`
create or replace view public.shipping_datamart as
select shippingid,
		vendorid,
		st.transfer_type,
		date_part('day', ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime)::int full_day_at_shipping,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
			then 1
			else 0
			end is_delay,
		case when ss.status = 'finished'
			then 1
			else 0
			end is_shipping_finish,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime
			then 
				date_part('day', ss.shipping_end_fact_datetime - si.shipping_plan_datetime)::int
			else 0
			end delay_day_at_shipping,
		payment_amount,
		si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) vat,
		si.payment_amount *  sa.agreement_commission profit
from public.shipping_info si
	left join public.shipping_transfer st using(transfer_type_id)
	left join public.shipping_status ss using(shippingid)
	left join public.shipping_country_rates scr using(shipping_country_id)
	left join public.shipping_agreement sa using(agreementid);





