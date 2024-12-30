from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


notebook_task = {
    'notebook_path': '/Workspace/Users/michaelgowie1@gmail.com/pinterest-data-pipeline/final-S3-connector',
}


notebook_params = {
    "Variable":0
}


default_args = {
    'owner': '121704c19363',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('121704c19363_dag',
    start_date=datetime(2024,12,26),
    # check out possible intervals, should be a string
    schedule_interval='0 0 * * *',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run