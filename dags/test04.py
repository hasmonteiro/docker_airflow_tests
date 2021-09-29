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
import zipfile

# Constants
DATA_PATH = '/usr/local/airflow/data/3.DADOS/'
ORIGINAL_FILE = DATA_PATH + 'microdados_enade_2019.txt'
FILE_PATH = DATA_PATH + "microdados_enade_2019_2.txt"

# Default arguments
default_args = {
    'owner': 'hasmonteiro',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 29, 16), #UTC hour
    'email': ['hasmonteiro_dev@gmail.com'],
    'email_on_failure': False, # if want to notify, change smtp in airflow.cfg
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Defining the first DAG (flow)
dag = DAG(
    'test-04',
    description='Parallel processing',
    default_args=default_args,
    schedule_interval='*/10 * * * *'
)

# ------------------------------------------
# TASKS
# ------------------------------------------

# Start check
start_preprocessing = BashOperator(
    task_id='start_processing',
    bash_command='echo "Start Preprocessing! Go!" ',
    dag=dag
)

# Download
task_get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    dag=dag
)

# Unzip
def unzip_file():
    with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
        zipped.extractall('/usr/local/airflow/data/')

task_unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_file,
    dag=dag
)

# Change decimal and separator characters
def change_decimal():
    text = open(ORIGINAL_FILE, "r")
    text = ''.join([i for i in text]) \
        .replace(",", ".") \
        .replace('; ',';')  
    x = open(FILE_PATH,"w")
    x.writelines(text)
    x.close()

task_change_decimal = PythonOperator(
    task_id='change_decimal',
    python_callable=change_decimal,
    dag=dag
)

# Applying filters to the data set
def apply_filters():
    cols = ['CO_GRUPO', 
            'TP_SEXO', 
            'NU_IDADE', 
            'NT_GER',
            'NT_FG',
            'NT_CE',
            'QE_I01',
            'QE_I02',
            'QE_I04',
            'QE_I05',
            'QE_I08']
    enade = pd.read_csv(FILE_PATH, sep=';', decimal='.', usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    enade.to_csv(DATA_PATH + 'enade_filtered.csv', index=False)

task_apply_filters = PythonOperator(
    task_id='apply_filters',
    python_callable=apply_filters,
    dag=dag
)

# Computing centered age attributes
def center_age():
    age = pd.read_csv(DATA_PATH + 'enade_filtered.csv', usecols=['NU_IDADE'])
    age['age_centered'] = age.NU_IDADE - age.NU_IDADE.mean()
    age[['age_centered']].to_csv(DATA_PATH + 'age_centered.csv', index=False)

def center_age_square():
    age = pd.read_csv(DATA_PATH + 'age_centered.csv')
    age['age_square'] = age.age_centered ** 2
    age[['age_square']].to_csv(DATA_PATH + 'age_squared.csv', index=False)

task_center_age = PythonOperator(
    task_id='center_age',
    python_callable=center_age,
    dag=dag
)

task_center_age_square = PythonOperator(
    task_id='center_age_squared',
    python_callable=center_age_square,
    dag=dag
)

# Changing marriage status descriptor
def set_marriage_status():
    marry = pd.read_csv(DATA_PATH + 'enade_filtered.csv', usecols=['QE_I01'])
    marry['marriage_status'] = marry.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viuvo',
        'E': 'Outro'
    })
    marry[['marriage_status']].to_csv(DATA_PATH + 'marriage_status.csv', index=False)

task_set_marriage_status = PythonOperator(
    task_id='set_marriage_status',
    python_callable=set_marriage_status,
    dag=dag
)

# Changing skin color descriptor
def set_skin_color():
    skin = pd.read_csv(DATA_PATH + 'enade_filtered.csv', usecols=['QE_I02'])
    skin['skin_color'] = skin.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indigena',
        'F': "",
        ' ': ""
    })
    skin[['skin_color']].to_csv(DATA_PATH + 'skin_color.csv', index=False)

task_set_skin_color = PythonOperator(
    task_id='set_skin_color',
    python_callable=set_skin_color,
    dag=dag
)

# Join
def join_data():
    filtered = pd.read_csv(DATA_PATH + 'enade_filtered.csv')
    age_cent = pd.read_csv(DATA_PATH + 'age_centered.csv')
    age_sqr = pd.read_csv(DATA_PATH + 'age_squared.csv')
    marry = pd.read_csv(DATA_PATH + 'marriage_status.csv')
    color = pd.read_csv(DATA_PATH + 'skin_color.csv')

    final = pd.concat([
        filtered,
        age_cent,
        age_sqr,
        marry,
        color
    ],
    axis=1
    )

    final.to_csv(DATA_PATH + 'enade_preprocessed.csv', index=False)
    print(final)

task_join_data = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)

# Ordering tasks
start_preprocessing >> task_get_data >> task_unzip_data >> task_change_decimal >> task_apply_filters 
task_apply_filters >> [task_center_age, task_set_marriage_status, task_set_skin_color]

task_center_age_square.set_upstream(task_center_age)

task_join_data.set_upstream([
    task_set_marriage_status, 
    task_set_skin_color,
    task_center_age_square
])











