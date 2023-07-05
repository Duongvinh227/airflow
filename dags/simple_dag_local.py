from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'print_date',
    default_args={
        'email': ['vinhduong227@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='A simple DAG sample',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 7, 4),
    tags=['vinh227'],
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date > /home/vinh/vinh_python/air_flow/output_1.txt',
    dag=dag
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag
)

t3 = BashOperator(
    task_id='echo',
    bash_command='echo t3 running',
    dag=dag
)

t1 >> t2 >> t3

# t2 >> t3