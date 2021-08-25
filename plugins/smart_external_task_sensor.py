from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import parse_execution_date

from typing import Any


class SmartExternalTaskSensor(ExternalTaskSensor):
    template_fields = ['external_dag_id', 'external_task_id', 'execution_date']

    poke_context_fields = ('external_dag_id', 'external_task_id', 
                           'execution_date_fn', 'failed_states', 'check_existence', 'execution_date')

    def __init__(self, execution_date, *args, **kwargs):
        super(SmartExternalTaskSensor, self).__init__(*args, **kwargs)
        self.execution_date = execution_date
    
    # overwrite _handle_execution_date_fn to assign dttm directly (hack :/ )
    # to avoid serialization error with timedelta object or function object
    def _handle_execution_date_fn(self, context) -> Any:

        print(context)
        return parse_execution_date(self.execution_date)
        
    def is_smart_sensor_compatible(self):
        result = (
            not self.soft_fail
            and super().is_smart_sensor_compatible()
        )
        return result