import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator

# Define the default arguments for the DAG.
default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 28),
    'retries': 0,
}

# Define the DAG object.
dag = DAG(
    dag_id='run_notebooks_daily',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Define the notebook tasks.
notebook1_task = PapermillOperator(
    task_id='run_extraction_notebook',
    input_nb='/project/1_Extract.ipynb',
    output_nb='/project/1_Extract.ipynb',
    dag=dag,
)

notebook2_task = PapermillOperator(
    task_id='run_transformation_notebook',
    input_nb='/project/2_Transform.ipynb',
    output_nb='/project/2_Transforma.ipynb',
    dag=dag,
)

notebook3_task = PapermillOperator(
    task_id='run_loading_notebook',
    input_nb='/project/3_Load.ipynb',
    output_nb='/project/3_Load.ipynb',
    dag=dag,
)


# Set the task dependencies.
notebook1_task >> notebook2_task >> notebook3_task

# Set the DAG object in the globals namespace.
globals()['dag'] = dag
