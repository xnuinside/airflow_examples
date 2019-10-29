from copy import copy


def create_test_dag(production_dag, tasks_to_add):

    test_dag = copy(production_dag)
    # we overwrite airflow DAG name
    test_dag._dag_id = test_dag._dag_id + "_test"
    last_tasks_list = []
    for task in test_dag.task_dict:
        test_dag.task_dict[task]._dag = test_dag
        last_tasks_list.append(task) if not test_dag.task_dict[task].downstream_task_ids else None

    for task in tasks_to_add:
        test_dag.log.info('Adding task: %s', tasks_to_add)
        # also exists method add_tasks to pass list of tasks
        if task.task_id not in test_dag.task_dict:
            # if not added previous to avoid errors
            test_dag.add_task(task)
            # set last tasks of production DAG to downstream our validation tasks
            task_in_dag = test_dag.get_task(task.task_id)
            [test_dag.get_task(task) for task in last_tasks_list] >> task_in_dag

    return test_dag
