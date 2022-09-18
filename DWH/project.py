from email import contentmanager
import time
import requests
import json
import pandas as pd
import pendulum

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy_operator import DummyOperator

postgres_hook_export = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
conn = postgres_hook_export.get_conn()
cur = conn.cursor()

header = {
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
    'X-Nickname': 'ilgibitym',
    'X-Cohort': '3'
    
}

def upload_stg_restaurants(pg_table_load):
    cur.execute(f"truncate table {pg_table_load} restart identity cascade;")
    response_restaurants = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?', headers=header)
    json_restaurants = json.loads(response_restaurants.content)
    columns = json_restaurants[0].keys()
    columns_str = ", ".join(columns)
    values = [[value for value in row.values()] for row in json_restaurants]
    query = f"insert into {pg_table_load} ({columns_str}) values (%s,%s)"
    cur.executemany(query, values)
    conn.commit()

def upload_stg_couriers(pg_table_load):
    cur.execute(f"truncate table {pg_table_load} restart identity cascade;")
    i=0
    for j in range(300):
        response_couriers = requests.get(f'''https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?limit=50&offset={i}''', headers=header)
        json_couriers = json.loads(response_couriers.content)
        if len(json_couriers)==0:
            break
        else:
            columns = json_couriers[0].keys()
            columns_str = ", ".join(columns)
            values = [[value for value in row.values()] for row in json_couriers]
            i+=50
            query = f"insert into {pg_table_load} ({columns_str}) values (%s,%s)"
            try:
                cur.executemany(query, values)
                conn.commit()
            except:
                conn.rollback()

def upload_stg_deliveries(pg_table_load):
    cur.execute(f"truncate table {pg_table_load} restart identity cascade;")
    i = 0
    for j in range(1000):
        response_deliveries = requests.get(f'''https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?limit=50&offset={i}&sort_field=order_id''', headers=header)
        json_deliveries = response_deliveries.json()
        if len(json_deliveries)==0:
            break
        else:
            columns = json_deliveries[0].keys()
            columns_str = ", ".join(columns)
            values = [[value for value in row.values()] for row in json_deliveries]
            i+=50
            query = f"insert into {pg_table_load} ({columns_str}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            try:
                cur.executemany(query, values)
                conn.commit() 
            except:
                conn.rollback()

migration_dds_couriers = """
INSERT INTO dds.dm_courier (courier_id, courier_name)
SELECT _id, name FROM stg.couriers;
"""
migration_dds_restaurants = """
INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name) 
SELECT _id, "name" FROM stg.restaurants;
"""
migration_dds_timestamps = """
INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date) 
SELECT order_ts as ts, extract(year from order_ts) as year, extract(month from order_ts) as month, extract(day from order_ts) as day, order_ts::time as time, order_ts::date as date
FROM stg.deliveries;
"""
migration_dds_delivery = """
INSERT INTO dds.dm_delivery (timestamp_id , delivery_id , address) 
SELECT dt.id, d.delivery_id, d.address FROM stg.deliveries d 
LEFT JOIN dds.dm_timestamps dt on d.order_ts=dt.ts;
"""
migration_dds_orders = """
INSERT INTO dds.dm_orders (courier_id , timestamp_id , order_id) 
SELECT dc.id as courier_id , dt.id as timestamp_id, d.order_id FROM stg.deliveries d
LEFT JOIN dds.dm_courier dc on dc.courier_id=d.courier_id 
LEFT JOIN dds.dm_timestamps dt on dt.ts=d.order_ts;
"""
migration_dds_fct_sales = """
INSERT INTO dds.fct_sales (order_id, courier_id , count , total_sum, rate, tip_sum) 
select do2.id as "order_id", do2.courier_id, count(distinct do2.order_id) as "count", sum(d.sum) as total_sum, avg(d.rate) as rate, sum(d.tip_sum) as tip_sum
from dds.dm_orders do2 
left join stg.deliveries d 
on d.order_id=do2.order_id
group by 1,2;
"""
migration_cdm_dm_courier_ledger = """
INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name , settlement_year, settlement_month, orders_count, 
orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
with cte as(
SELECT t.courier_id, t.courier_name, t."year", t."month", sum(t.orders_count) as orders_count, sum(t.orders_total_sum) as orders_total_sum,
	sum(t.rate_avg) as rate_avg, 
    sum(order_processing_fee) as order_processing_fee, 
	sum(case when t.rate_avg < 4 then GREATEST(0.05 * t.orders_total_sum, 100.00)
	when t.rate_avg < 4.5 and t.rate_avg >= 4 then GREATEST(0.07 * t.orders_total_sum, 150.00)
	when t.rate_avg < 4.9 and t.rate_avg >= 4.5 then GREATEST(0.08 * t.orders_total_sum, 175.00)
	when t.rate_avg >= 4.9 then GREATEST(0.10 * t.orders_total_sum, 200.00) end) as courier_order_sum,
	sum(courier_tips_sum) as courier_tips_sum
FROM(
SELECT do2.courier_id, dc.courier_name, dt."year", dt."month", sum(fs2.count) as orders_count, sum(fs2.total_sum) as orders_total_sum,
	avg(fs2.rate) as rate_avg, sum(fs2.total_sum * 0.25) as order_processing_fee, sum(tip_sum) as courier_tips_sum
FROM dds.fct_sales fs2 
left join dds.dm_orders do2 
	on do2.id=fs2.order_id
left join dds.dm_courier dc 
	on do2.courier_id=dc.id 
left join dds.dm_timestamps dt 
	on do2.timestamp_id=dt.id 
group by 1,2,3,4) as t
group by 1,2,3,4)    
select *, (c.courier_order_sum + c.courier_tips_sum * 0.95) as courier_reward_sum
from cte c
"""

with DAG(
        'project',
        schedule_interval='0/15 * * * *',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False) as dag:

    begin = DummyOperator(task_id="begin")

    upload_stg_restaurants = PythonOperator(
        task_id='upload_stg_restaurants',
        python_callable=upload_stg_restaurants,
        op_kwargs={'pg_table_load': 'stg.restaurants'})
    
    upload_stg_couriers = PythonOperator(
        task_id='upload_stg_couriers',
        python_callable=upload_stg_couriers,
        op_kwargs={'pg_table_load': 'stg.couriers'})

    upload_stg_deliveries = PythonOperator(
        task_id='upload_stg_deliveries',
        python_callable=upload_stg_deliveries,
        op_kwargs={'pg_table_load': 'stg.deliveries'})

    migration_dds_couriers = PostgresOperator(
        task_id='migration_dds_couriers',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=migration_dds_couriers)

    migration_dds_restaurants = PostgresOperator(
        task_id='migration_dds_restaurants',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=migration_dds_restaurants)
    
    migration_dds_timestamps = PostgresOperator(
        task_id='migration_dds_timestamps',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=migration_dds_timestamps)

    migration_dds_delivery = PostgresOperator(
        task_id='migration_dds_delivery',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=migration_dds_delivery)
    
    migration_dds_orders = PostgresOperator(
        task_id='migration_dds_orders',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=migration_dds_orders)

    migration_dds_fct_sales = PostgresOperator(
        task_id='migration_dds_fct_sales',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=migration_dds_fct_sales)

    migration_cdm_dm_courier_ledger = PostgresOperator(
        task_id='migration_cdm_dm_courier_ledger',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=migration_cdm_dm_courier_ledger)

    end = DummyOperator(task_id="end")

    begin >> upload_stg_restaurants >> upload_stg_couriers >> upload_stg_deliveries >> migration_dds_couriers >> migration_dds_restaurants \
    >> migration_dds_timestamps >> migration_dds_delivery >> migration_dds_orders >> migration_dds_fct_sales >> migration_cdm_dm_courier_ledger >> end