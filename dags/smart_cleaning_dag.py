from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins.smart_external_task_sensor import SmartExternalTaskSensor

from datetime import datetime, timedelta
    
@dag(start_date=datetime(2021, 1, 1),
    schedule_interval='@daily', catchup=False)
def smart_cleaning_dag():

    waiting_for_task = SmartExternalTaskSensor(
        task_id='waiting_for_task',
        external_dag_id='partner',
        external_task_id='storing',
        execution_date='{{ ds }}',
        execution_date_fn=True,
        failed_states=['skipped', 'failed'],
        check_existence=True
    )

    cleaning_xcoms = PostgresOperator(
        task_id='cleaning_xcoms',
        sql='sql/CLEANING_XCOMS.sql',
        postgres_conn_id='postgres'
    )

    waiting_for_task >> cleaning_xcoms
    
dag = smart_cleaning_dag()