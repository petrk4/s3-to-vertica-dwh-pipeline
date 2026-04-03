from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
import vertica_python
from datetime import datetime
import boto3


def download_from_s3(filename : str):
    AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
    AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file(
        Bucket='sprint6',
        Key=filename,
        Filename=f'/data/{filename}'
    )


def print_lines(filename : str):
    with open(f'/data/{filename}') as f:
        for i in range(11):
            print(f.readline())


def load_to_staging(filename, table):
    conn = BaseHook.get_connection('vert_conn')
    conn_info = {
        'host': conn.host,
        'port': conn.port,
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema,
    }
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f'TRUNCATE TABLE VT260322CCD83B__STAGING.{table}')
        cur.execute(f"""COPY VT260322CCD83B__STAGING.{table} 
                        FROM LOCAL '/data/{filename}'
                        delimiter ','
                        ENCLOSED BY '"'
                        """)


with DAG(
    dag_id='load_groups_from_s3_taskflow',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    @task
    def get_groups():
        download_from_s3('groups.csv')
        print_lines('groups.csv')

    @task
    def get_users():
        download_from_s3('users.csv')
        print_lines('users.csv')

    @task
    def get_dialogs():
        download_from_s3('dialogs.csv')
        print_lines('dialogs.csv')

    @task
    def get_group_log():
        download_from_s3('group_log.csv')
        print_lines('group_log.csv')

    with TaskGroup('load_from_s3') as load_from_s3:
        get_groups = get_groups()
        get_users = get_users()
        get_dialogs = get_dialogs()
        get_group_log = get_group_log()

    @task
    def load_stg_users():
        load_to_staging('users.csv', 'users')

    @task
    def load_stg_groups():
        load_to_staging('groups.csv', 'groups')

    @task
    def load_stg_dialogs():
        load_to_staging('dialogs.csv', 'dialogs')

    @task
    def load_stg_group_log():
        load_to_staging('group_log.csv', 'group_log')

    with TaskGroup('load_to_stg') as load_to_stg:
        load_stg_users = load_stg_users()
        load_stg_groups = load_stg_groups()
        load_stg_dialogs = load_stg_dialogs()
        load_stg_group_log = load_stg_group_log()
        #load_stg_users >> load_stg_groups >> load_stg_dialogs

    load_from_s3 >> load_to_stg