from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

with DAG(
    dag_id="simple_task_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def print_hello():
        print("Hello, Airflow!")

    print_hello()
