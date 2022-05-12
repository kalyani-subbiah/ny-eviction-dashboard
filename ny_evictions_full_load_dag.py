
from ny_eviction_dashboard.operators.soda_to_s3_operator import SodaToS3Operator
from ny_eviction_dashboard.operators.s3_to_postgres_operator import S3ToPostgresOperator

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from ny_eviction_dashboard.utils.read_soda_config import get_app_tokens

config = get_app_tokens()

soda_headers = {
  "keyId": config[0],
  "keySecret" : config[1],
  "Accept": "application/json"
}

default_args = {
  "owner" : "airflow",
  "depends_on_past" : False,
  "start_date": days_ago(2),
  "email" : ["airflow@example.com"],
  "email_on_failure" : False,
  "email_on_retry" : False,
  "retries" : 1,
  "retry_delay": timedelta(seconds=30)
}

with DAG("ny-evictions-full_load",
  default_args=default_args,
  description="Executes full load from SODA API to Production DW.",
  max_active_runs=1,
  schedule_interval=None) as dag:

  op1 = SodaToS3Operator(
    task_id="get_evictions_data",
    http_conn_id="API_Evictions",
    endpoint="resource/6z8x-wfk4.json",
    headers=soda_headers,
    s3_conn_id="S3_Evictions",
    s3_bucket="ny-evictions",
    s3_directory="soda_jsons",
    size_check=True,
    max_bytes=1000000000,
    dag=dag
  )
  
  op2 = PostgresOperator(
    task_id="initialize_target_db",
    postgres_conn_id="RDS_Evictions",
    sql="ny_eviction_dashboard/sql/init_db_schema.sql",
    dag=dag
  )

  op3 = S3ToPostgresOperator(
		task_id="load_evictions_data",
		s3_conn_id="S3_Evictions",
		s3_bucket="ny-evictions",
		s3_prefix="soda_jsons/soda_evictions_import",
		source_data_type="json",
		postgres_conn_id="RDS_Evictions",
		schema="raw",
		table="soda_evictions",
		get_latest=True,
		dag=dag
	)

  op4 = BashOperator(
		task_id="run_dashboard",
		bash_command='python3 ${AIRFLOW_HOME}/dags/ny_eviction_dashboard/app/app.py',
		dag=dag
	)

op1 >> op2 >> op3 >> op4