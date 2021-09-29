"""
First DAG with Airflow

Author: Humberto Monteiro
"""

# Libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

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
    'test-01',
    description='Testing Bash Operators and Python Operators',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

# Adding tasks
# For Bash, the command is a default bash command string
hello_bash = BashOperator(
    task_id='Hello_Bash',
    bash_command='echo "Hello Airflow from Bash"',
    dag=dag
)

# For Python, we need to define the function previously
def say_hello():
    print("Hello Airflow from Python")

hello_python = PythonOperator(
    task_id='Hello_Python',
    python_callable=say_hello,
    dag=dag
)

# Ordering the Airflow operations
hello_bash >> hello_python















