from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def sample_task():
    print("✅ DAG is running successfully!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="elt_validation_dag",
    description="ELT validation DAG for CI pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["validation", "ci", "elt"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    validation_task = PythonOperator(
        task_id="run_validation",
        python_callable=sample_task,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> validation_task >> end
