-- Создание витрины данных в слое cdm
drop table cdm.dm_courier_ledger cascade;
create table cdm.dm_courier_ledger (
	id serial primary key,
	courier_id int not null,
	courier_name varchar(50) not null,
	settlement_year int not null,
	settlement_month int not null,
	orders_count int not null default 0 check (orders_count >= 0),
	orders_total_sum numeric(14,2) not null default 0 check (orders_total_sum >= 0),
	rate_avg numeric(14,2) not null default 0 check (rate_avg >= 0),
	order_processing_fee numeric(14,2) not null default 0 check (rate_avg >= 0),
	courier_order_sum numeric(14,2) not null default 0 check (rate_avg >= 0),
	courier_tips_sum numeric(14,2) not null default 0 check (rate_avg >= 0),
	courier_reward_sum numeric(14,2) not null default 0 check (rate_avg >= 0)	
);

-- Проектирование слоя dds
-- Таблица dm_restaurants
drop table dds.dm_restaurants cascade;
create table dds.dm_restaurants (
	id serial primary key,
	restaurant_id varchar not null,
	restaurant_name varchar not null
);

-- Таблица dm_courier
drop table dds.dm_courier cascade;
create table dds.dm_courier(
	id serial primary key,
	courier_id varchar not null,
	courier_name varchar not null
);

-- Таблица dm_timestamps
drop table dds.dm_timestamps cascade;
create table dds.dm_timestamps (
	id serial primary key,
	ts timestamp,
	"year" int check("year" >= 2022 and "year" <= 2500),
	"month" int check("month" >= 1 and "month" <= 12),
	"day" int check("day" >= 1 and "day" <= 31),
	"time" time,
	"date" date
);

-- Таблица dm_orders
drop table dds.dm_orders cascade;
create table dds.dm_orders (
	id serial primary key,
	courier_id int not null,
	timestamp_id int not null,
	order_id varchar not null
);
alter table dds.dm_orders add foreign key (courier_id) references dds.dm_courier (id);
alter table dds.dm_orders add foreign key (timestamp_id) references dds.dm_timestamps (id);

-- Таблица dm_delivery
drop table dds.dm_delivery cascade;
create table dds.dm_delivery(
	id serial primary key,
	timestamp_id int not null,
	delivery_id varchar not null,
	address varchar not null
);
alter table dds.dm_delivery add foreign key (timestamp_id) references dds.dm_timestamps (id);

--Таблица fct_sales
drop table dds.fct_sales cascade;
create table dds.fct_sales (
	id serial primary key,
	order_id int not null,
	courier_id int not null,
	"count" int not null default 0 check ("count" >= 0),
	total_sum numeric(14,2) not null default 0 check (total_sum >= 0),
	rate int not null not null default 0 check (rate >= 0),
	tip_sum numeric(14,2) not null default 0 check (tip_sum >= 0)
);
alter table dds.fct_sales add foreign key (order_id) references dds.dm_orders (id);
alter table dds.fct_sales add foreign key (courier_id) references dds.dm_courier (id);

-- Проектирование слоя stg
-- Таблица restaurants
drop table stg.restaurants cascade;
create table stg.restaurants(
	restaurant_id text not null,
	"name" text not null
);

-- Таблица restaurants couriers
drop table stg.couriers cascade;
create table stg.couriers(
	couriers_id text not null,
	"name" text not null
);

-- Таблица restaurants couriers
drop table stg.deliveries cascade;
create table stg.deliveries(
	order_id text not null,
	order_ts timestamp not null,
	delivery_id text not null,
	courier_id text not null,
	address text not null,
	delivery_ts timestamp not null,
	rate int not null default 0,
	"sum" numeric(14,2) not null default 0,
	tip_sum numeric(14,2) not null default 0
);



















