from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta

def _get_execution_dates(current_execution_date):
    return [current_execution_date - timedelta(hours=10)]

@dag(start_date=datetime(2021, 1, 1),
    schedule_interval='0 10 * * *', catchup=False)
def cleaning_dag():

    waiting_for_partner = ExternalTaskSensor(
        task_id='waiting_for_partner',
        external_dag_id='partner',
        external_task_id='storing',
        execution_delta=timedelta(hours=10),
        failed_states=['skipped', 'failed'],
        check_existence=True,
    )

    cleaning_xcoms = PostgresOperator(
        task_id='cleaning_xcoms',
        sql='sql/CLEANING_XCOMS.sql',
        postgres_conn_id='postgres'
    )

    waiting_for_partner >> cleaning_xcoms
    
dag = cleaning_dag()