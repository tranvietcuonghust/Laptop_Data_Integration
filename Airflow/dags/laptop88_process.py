from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
import csv
import psycopg2


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
        dag_id="laptop88_process_data_2", 
        description="This DAG runs a Pyspark app",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

def load_to_postgres():


    # Create a connection to your Azure Cosmos DB account
    host = "c.tichhopdulieu-gr1.postgres.database.azure.com"
    dbname = "citus"
    user = "citus"
    password = "Lufe22022001"
    sslmode = "require"
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)
    # Create a new container in your database
    container_name = "Laptop88"
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS {0} (Name VARCHAR(255), Price VARCHAR(255), URL VARCHAR(255), ImgURL VARCHAR(255), CPU VARCHAR(255), Ram VARCHAR(255), Storage VARCHAR(255), Graphics VARCHAR(255), Display VARCHAR(255), Status VARCHAR(255), PRIMARY KEY (Name, CPU, Ram, Storage));".format(container_name))
    conn.commit()

# Open the CSV file and read its contents
    csv_file = "../spark/resources/data/cleaned_laptop_laptop88.csv"
    with open(csv_file, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Insert the row into the container
            cur.execute("INSERT INTO {0} (Name, Price, URL, ImgURL, CPU, Ram, Storage, Graphics, Display, Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Name, CPU, Ram, Storage) DO UPDATE SET Price = EXCLUDED.Price, URL = EXCLUDED.URL, ImgURL = EXCLUDED.ImgURL, Graphics = EXCLUDED.Graphics, Display = EXCLUDED.Display, Status = EXCLUDED.Status;".format(container_name), (row["Name"], row["Price"], row["URL"], row["ImgURL"], row["CPU"], row["Ram"], row["Storage"], row["Graphics"], row["Display"], row["Status"]))
            conn.commit()

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
    task_id="laptop88_clean_data_11",
    application="/opt/spark/app/process_laptop88.py", # Spark application path created in airflow and spark cluster
    name="trungtran_clean_data",
    conn_id="other_spark_local",
    verbose=1,
    conf={"spark.master":other_spark_master},
    # application_args=[csv_file],
    dag=dag)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    # op_kwargs={
    #     'conn_id': 'azure_cosmos_default',
    #     'database_name': 'test_Database',
    #     'container_name': 'test_Container'
    # },
    dag=dag,
)
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

start >> spark_job>>load_to_postgres_task >> end