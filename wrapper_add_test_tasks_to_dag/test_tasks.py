# apache airflow DAG
from os import path
from airflow.operators.python_operator import PythonOperator
from test_dag_wrapper import create_test_dag
from bash_dag import dag

file_path = "/usr/local/airflow/target.txt"
validation_task_file_name = PythonOperator(task_id="validate_file_exists",
                                           python_callable=lambda: path.exists(file_path))
# create test DAG using wrapper
test_dag = create_test_dag(dag, [validation_task_file_name])
globals()[test_dag._dag_id] = test_dag