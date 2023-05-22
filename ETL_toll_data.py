# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Peppa Pig',
    'start_date': days_ago(0),
    'email': ['peppapig@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# task to unzip data

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag
)

# task to extract vehicle-data.csv

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='''
        cut -d, -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv \
        > /home/project/airflow/dags/finalassignment/csv_data.csv
    ''',
    dag=dag
)

# task to extract tollplaza-data.tsv

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='''
        cut -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv \
        | tr "\\t" "," \
        > /home/project/airflow/dags/finalassignment/tsv_data.csv
    ''',
    dag=dag
)

# task to extract payment-data.txt

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
        awk '{print substr($0, 6, 7)}' \
        /home/project/airflow/dags/finalassignment/payment-data.txt \
        > /home/project/airflow/dags/finalassignment/fixed_width_data.csv
    """,
    dag=dag
)

# task to consolidate data

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
        cat /home/project/airflow/dags/finalassignment/csv_data.csv \
        /home/project/airflow/dags/finalassignment/tsv_data.csv \
        /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
        > /home/project/airflow/dags/finalassignment/extracted_data.csv
    """,
    dag=dag
)

# transform data

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
        awk 'BEGIN{{FS=OFS=","}}
            {{$5 = toupper($5)}}
            1' /home/project/airflow/dags/finalassignment/extracted_data.csv \
        > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv
    """,
    dag=dag
)

#define the pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data