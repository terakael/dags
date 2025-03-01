from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests

with DAG(
    dag_id="mega_login",
    schedule_interval="0 8 * * 1",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def get_servers():
        response = requests.get("http://mega-api-service/api/servers")
        return [obj["email"] for obj in response.json()]

    @task
    def login(email):
        response = requests.post(
            "http://mega-api-service/api/df", json={"email": email}
        )

        print(response.content)

    login.expand(email=get_servers())
