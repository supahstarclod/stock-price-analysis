from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.sensors.external_task import ExternalTaskSensor


conn = BaseHook.get_connection('snowflake_conn')
with DAG(
    "build_elt_dbt",
    start_date=datetime(2024, 10, 14),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    catchup=False,
    tags=['ELT'],
    schedule = '20 0 * * *',
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:
    run_dbt_job = DbtCloudRunJobOperator(
        task_id='run_dbt_job',
        dbt_cloud_conn_id='dbt_cloud',
        job_id=70471823397451, 
        check_interval=10, 
        timeout=600
    )
    run_dbt_job 