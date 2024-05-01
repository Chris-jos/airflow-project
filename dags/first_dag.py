import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


@dag(start_date=datetime.datetime(2024, 1, 1), schedule=None)
def example_dag():
    task1 = EmptyOperator(task_id="task1")

    @task.branch(task_id="task2")
    def branch_task(condition: bool = True) -> str:
        if condition:
            return "task3"
        
        return "task4"
    

    task3 = BashOperator(
        task_id = "task3",
        bash_command = "sleep 60"
    )
    
    task4 = EmptyOperator(task_id="task4")

    @task(task_id="finally", trigger_rule=TriggerRule.ONE_SUCCESS)
    def print_str():
        print("DAG completed successfully")

    task1 >> branch_task() >> [task3, task4]
    [task3, task4] >> print_str()


example_dag()