from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

file_path = "/usr/local/airflow/target.txt"

with DAG(dag_id="Create_file_DAG", schedule_interval=None,
         start_date=datetime(2019, 10, 29)) as dag:
    create_file_task = BashOperator(task_id="create_file",
                                    bash_command=f"touch {file_path}")
    dummy_one = DummyOperator(task_id="dummy_one")
    dummy_two = DummyOperator(task_id="dummy_two")
    dummy_three = DummyOperator(task_id="dummy_three")
    dummy_one >> dummy_two >> dummy_three >> create_file_task

