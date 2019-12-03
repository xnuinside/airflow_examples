from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.timezone import make_aware
from airflow.models import XCom

# one DAG will write to Xcom
dag_1 = DAG('write_to_xcom', schedule_interval='*/15 * * * *', start_date=datetime(2019, 12, 1))


def push_xcom_call(**kwargs):
    kwargs['task_instance'].xcom_push(key='test_dag', value=str({'key1': 'value1'}))


xcom_push_task = PythonOperator(
    task_id='xcom_push_task',
    dag=dag_1,
    python_callable=push_xcom_call,
    provide_context=True
)


# second Read from Xcom values of first DAG
dag_2 = DAG('read_from_xcom', schedule_interval='*/15 * * * *', start_date=datetime(2019, 12, 3))


def pull_xcom_call(**kwargs):
    # xcom will get all values, that was written before this date with using Xcom directly (without context object)
    get_mane_xcoms_values__with_xcom_class = XCom.get_many(
        execution_date=make_aware(datetime(2019, 12, 3, 0, 51, 00, 00)),
        dag_ids=["write_to_xcom"], include_prior_dates=True)
    print('XCom.get_many ')
    print(get_mane_xcoms_values__with_xcom_class)

    get_xcom_with_ti = kwargs['ti'].xcom_pull(dag_id="write_to_xcom", include_prior_dates=True)
    print('ti.xcom_pull with include_prior_dates')
    print(get_xcom_with_ti)


xcom_pull_task = PythonOperator(
    task_id='xcom_pull_task',
    dag=dag_2,
    python_callable=pull_xcom_call,
    provide_context=True
)
