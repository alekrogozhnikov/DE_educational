import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import pendulum
import vertica_python

conn_info = {
    'host': '51.250.75.20',
    'port': '5433',
    'user': 'ILGIBITYMYANDEXRU',
    'password': 'Af7Z2Vj3JY8aNu7',
    'database': 'dwh',
    'autocommit': True
    }

logger = logging.getLogger(__name__)
s3_files = ['groups.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

def get_files():

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

    for i in s3_files:
        s3_client.download_file(
            Bucket='sprint6',
            Key=i,
            Filename=f'{i}')
        logging.info(f'TOP 10 string in file {i}:\n{pd.read_csv(i, nrows=10)}')


def load_files(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            COPY ILGIBITYMYANDEXRU__STAGING.users (id,chat_name,registration_dt,country,age)
            FROM LOCAL 'users.csv' DELIMITER ',';
            """
        )
        cur.execute(
            """
            COPY ILGIBITYMYANDEXRU__STAGING.groups (id,admin_id,group_name,registration_dt,is_private)
            FROM LOCAL 'groups.csv' DELIMITER ',';
            """
        )
        cur.execute(
            """
            COPY ILGIBITYMYANDEXRU__STAGING.dialogs (message_id,message_ts,message_from,message_to,message,message_group)
            FROM LOCAL 'dialogs.csv' DELIMITER ',';
            """
        )
        cur.execute(
            """
            COPY ILGIBITYMYANDEXRU__STAGING.group_log (group_id,user_id,user_id_from,event,datetime)
            FROM LOCAL 'group_log.csv' DELIMITER ',';
            """
        )
        res = cur.fetchall()
        return res

with DAG(
        'downloud_STAGING',
        schedule_interval=None,
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
        catchup=False,
        tags=['sprint6'],
        is_paused_upon_creation=False
    ) as dag:

    get_files = PythonOperator(
        task_id='get_files',
        python_callable=get_files)

    load_files = PythonOperator(
        task_id='load_files',
        python_callable=load_files)
        
get_files >> load_files