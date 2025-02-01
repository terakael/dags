from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

with DAG(
    dag_id="mega_login",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def get_servers():
        import requests

        response = requests.get("http://mega-api-service/api/servers")

        print(response.content)

    get_servers()
