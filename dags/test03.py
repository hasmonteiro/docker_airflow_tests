"""
Scheduling conditional DAGs with Airflow

Author: Humberto Monteiro
"""

# Libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

# Default arguments
default_args = {
    'owner': 'hasmonteiro',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 28), #UTC hour
    'email': ['hasmonteiro_dev@gmail.com'],
    'email_on_failure': False, # if want to notify, change smtp in airflow.cfg
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Defining the first DAG (flow)
dag = DAG(
    'test-03',
    description='Extracts the Titanic dataset and computes the mean age for men or women',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

# ------------------------------------------
# TASKS
# ------------------------------------------

# ------------------------------------------
# BASH
# ------------------------------------------
# Download data set (via bash operator) task
get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv -o /usr/local/airflow/data/titanic.csv',
    dag=dag
)

# ------------------------------------------
# PY
# ------------------------------------------
# Sort sex randomly
def sort_sex():
    return random.choice(['male', 'female'])

task_pick_sex = PythonOperator(
    task_id='pick-sex',
    python_callable=sort_sex,
    dag=dag
)

# Compute mean for men
def compute_mean_man():
    df = pd.read_csv('/usr/local/airflow/data/titanic.csv')
    df  = df.loc[df.Sex == 'male']
    med = df.Age.mean()
    print(f'Men mean age in the Titanic: {med}')

branch_man = PythonOperator(
    task_id='branch-man',
    python_callable=compute_mean_man,
    dag=dag
)

# Compute mean for women
def compute_mean_woman():
    df = pd.read_csv('/usr/local/airflow/data/titanic.csv')
    df  = df.loc[df.Sex == 'female']
    med = df.Age.mean()
    print(f'Women mean age in the Titanic: {med}')

branch_woman = PythonOperator(
    task_id='branch-woman',
    python_callable=compute_mean_woman,
    dag=dag
)

# Check sex
def is_man_or_woman(**context):
    value = context['task_instance'].xcom_pull(task_ids='pick-sex')
    if value == 'male':
        return 'branch-man'
    if value == 'female':
        return 'branch-woman'

task_man_or_woman = BranchPythonOperator(
    task_id='conditional',
    python_callable=is_man_or_woman,
    provide_context=True,
    dag=dag
)

# Ordering tasks
get_data >> task_pick_sex >> task_man_or_woman >> [branch_man, branch_woman]
