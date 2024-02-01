from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.utils.dates import days_ago
import os
default_args = {
   'owner' : 'Damaine'
}

dir = os.getcwd()

def hello_world():
    print (dir)


with DAG(
    dag_id = 'first_dag',
    description = 'trying this out',
    default_args = default_args,
    start_date = days_ago(2),
    schedule_interval = '* 0 2 * *',
    catchup = True,
    template_searchpath = '/home/codespace/airflow/dags/queries'
) as dag:

    task1 = PythonOperator(
        task_id = 'task1',
        python_callable = hello_world
    )

    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = 'create_tabl.sql',
        sqlite_conn_id = 'airflow_sqlite_db'
    )

task1 >> create_table
