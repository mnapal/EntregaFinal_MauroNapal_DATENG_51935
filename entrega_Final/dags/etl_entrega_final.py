# Este es el DAG que orquesta el ETL Yahoo Finance

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.models import Variable

from datetime import datetime, timedelta
from os import environ as env
#--
from datetime import datetime, timedelta
from airflow.decorators import task
# Para envío de mails automáticos
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
from email import message




QUERY_CREATE_TABLE=f"""CREATE TABLE IF NOT EXISTS {env['REDSHIFT_USER']}.{env['REDSHIFT_TABLE']}
    (openn numeric(10,2) 
    ,high numeric(10,2)
    ,low numeric(10,2)
    ,close  numeric(10,2) 
    ,date TIMESTAMP WITHOUT TIME ZONE 
    ,difference numeric(10,2)
    ,process_date VARCHAR(10) distkey) sortkey(process_date);"""

QUERY_CLEAN_PROCESS_DATE = f"""
DELETE FROM {env['REDSHIFT_USER']}.{env['REDSHIFT_TABLE']} WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


defaul_args = {
    "owner": "Mauro Napal",
    "start_date": datetime(2023, 7, 7),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    'email': ['mnapal@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}


with DAG(
    dag_id="etl_entrega_final",
    default_args=defaul_args,
    description="ETL que toma datos de yFinance",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tareas
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    spark_etl_entrega_final = SparkSubmitOperator(
        task_id="spark_etl_entrega_final",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_entrega_final.py',

        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )


    send_email_task = EmailOperator(
        task_id="send_email",
        to='mnapal@gmail.com',
        subject="Resultado de la ejecucion de ETL",
        html_content="<p>El ETL ha finalizado.</p>",
        dag=dag,
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_entrega_final >> send_email_task
