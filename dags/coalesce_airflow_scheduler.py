from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=20)
}

dag = DAG(
    'coalesce_airflow_scheduler',
    default_args=default_args,
    description='Coalesce Airflow DAG'
)

execute_api_call_command = """
response=$(curl --location 'https://app.coalescesoftware.io/scheduler/startRun' \
--header 'accept: application/json' \
--header 'content-type: application/json' \
--header 'Authorization: Bearer {{ params.coalesce_token }}' \
--data-raw '{
"runDetails": {
"parallelism": 1,
"environmentID": "2",
"jobID": "1"
},
"userCredentials": {
"snowflakeAuthType": "KeyPair",
"SnowflakeKeyPairKey": "{{ params.sf_private_key }}"
}
}')
"""

# Define Tasks
task_execute_api_call = BashOperator(
task_id='execute_api_call',
params={'coalesce_token': Variable.get('AIRFLOW_VAR_COALESCE_TOKEN'),
'sf_private_key': str(Variable.get('AIRFLOW_VAR_SNOWFLAKE_PRIVATE_KEY'))},
bash_command=execute_api_call_command,
dag=dag,
)