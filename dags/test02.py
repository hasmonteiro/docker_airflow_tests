"""
Scheduling DAG with Airflow

Author: Humberto Monteiro
"""

# Libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

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
    'test-02',
    description='Extracts the Titanic dataset and computes the mean age',
    default_args=default_args,
    schedule_interval='*/2 * * * *'
)

"""
OBS: for the schedule_interval we can use the cron schedule
expressions: @daily, @hourly etc and also
"At every minute past hour 4.” => * 4 * * *
min hour day month day
*	any value
,	value list separator
-	range of values
/	step values
"""

# Tasks

# BASH
# Download data set (via bash operator) task
get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv -o /usr/local/airflow/data/titanic.csv',
    dag=dag
)

# PY
# Compute mean function
def compute_mean_age():
    df = pd.read_csv('~/titanic.csv')
    med = df.Age.mean()
    return med

# Print age function
def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='compute-mean-age')
    print(f'The mean age in Titanic was {value} years.')

# Tasks
task_mean_age = PythonOperator(
    task_id='compute-mean-age',
    python_callable=compute_mean_age,
    dag=dag
)

task_print_age = PythonOperator(
    task_id='print-age',
    python_callable=print_age,
    provide_context=True,
    dag=dag
)

# Ordering tasks
get_data >> task_mean_age >> task_print_age
