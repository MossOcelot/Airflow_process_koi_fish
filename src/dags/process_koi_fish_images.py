import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_conn = os.environ.get("spark_default", "spark_default")
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/assets/jars/postgresql-42.2.6.jar"

postgres_db = "jdbc:postgresql://postgres:5432/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"


###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="process-koi-fish-images",
    description="This dags for porcess images from camera censers ",
    default_args=default_args,
    schedule_interval="37 23 * * *",
)

start = DummyOperator(task_id="start", dag=dag)

remove_old_folder = SparkSubmitOperator(
    task_id="remove_old_folder",
    application="/usr/local/spark/applications/remove_old_folder.py",
    name="remove_old_folder",
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag,
)

get_koi_images = SparkSubmitOperator(
    task_id="get_koi_images",
    application="/usr/local/spark/applications/get_koi_images.py",
    name="get_koi_images",
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag,
)

convert_to_video = SparkSubmitOperator(
    task_id="convert_to_video",
    application="/usr/local/spark/applications/convert_to_video.py",
    name="convert_to_video",
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> remove_old_folder >> get_koi_images >> convert_to_video >> end
