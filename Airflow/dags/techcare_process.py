from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator


###############################################
# Parameters
###############################################
other_spark_master="spark://spark-master:7077"
spark_master = "spark://master:7077"
# csv_file = "./dags/data/house_price_data.csv"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["pioneer22022001@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="techcare_process_data_1", 
        description="This DAG runs a Pyspark app",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)
# spark_save = SparkSubmitOperator(
#     task_id="spark_save_to_hdfs",
#     application="./dags/spark/save_to_hdfs.py", # Spark application path created in airflow and spark cluster
#     name="spark_save_data",
#     conn_id="spark_local",
#     verbose=1,
#     conf={"spark.master":spark_master},
#     # application_args=[csv_file],
#     dag=dag)

spark_job = SparkSubmitOperator(
    task_id="techcare_clean_data_11",
    application="/opt/spark/app/process_techcare.py", # Spark application path created in airflow and spark cluster
    name="trungtran_clean_data",
    conn_id="other_spark_local",
    verbose=1,
    conf={"spark.master":other_spark_master},
    # application_args=[csv_file],
    dag=dag)
# spark_job = SSHOperator(
#     task_id="spark_job_clean_data_by_ssh",
#     ssh_conn_id="ssh_connection_spark",
#     command='/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.master=spark://spark-master:7077 --name spark_clean_data --verbose /opt/spark/app/preprocess_data.py',
#     dag=dag
# )
# train_model = BashOperator(
#     task_id='train_model',
#     bash_command='python ./dags/src/data_analysis.py',
#     dag=dag
# )
#spark-submit --master spark://spark-master:7077 --conf spark.master=spark://spark-master:7077 --name spark_clean_data --verbose /opt/spark/app/preprocess_data.py
end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end