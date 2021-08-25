DELETE 
FROM xcom 
WHERE dag_id='partner'
AND execution_date='{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z") }}'