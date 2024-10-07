import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import pandas as pd

###############################################
# Parameters
###############################################
spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark:7077"
spark_app_name = "Spark Retail Data"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def load_file():
    dataset = pd.read_csv('/usr/local/spark/assets/data/Retail and wherehouse Sale.csv')


def show_data():
    pass

dag = DAG(
    dag_id="retail-data",
    description="Retail Sales Data with Seasonal Trends & Marketing.",
    default_args=default_args,
    schedule_interval="@daily"
)

start = DummyOperator(task_id="start", dag=dag)

load_data_job = SparkSubmitOperator(
    task_id="load_data_job",
    application=load_file,
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    dag=dag)

show_data_job = SparkSubmitOperator(
    task_id="show_data_job",
    application=show_data,
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> load_data_job >> show_data_job >> end