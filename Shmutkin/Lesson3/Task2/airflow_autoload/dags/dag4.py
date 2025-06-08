from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base import BaseHook
from dag4_plugin.dag4_operator import ExampleOperator

# Достаются параметры БД и потом передаются в таску (в данном случае таску 1)
# которые указаны в аирфлоу в созданном конекте "main_postgresql_connection"
connection = BaseHook.get_connection("main_postgresql_connection")


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 7),
}

# С кастомным оператором
# Загрузка погоды каждый час
with DAG(
    dag_id='dag4',
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    max_active_tasks=3,
    max_active_runs=1,
    tags=["City_weather"]
) as dag:

    task1 = ExampleOperator(
        task_id='task1',
        postgre_conn=connection,
        data_list=["Москва", "Бишкек", "Ташкент", "Анталия"]
    )

    task1