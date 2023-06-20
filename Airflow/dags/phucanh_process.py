from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
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
        dag_id="phucanh_process_data_1", 
        description="This DAG runs a Pyspark app",
        default_args=default_args, 
        max_active_runs=1,
        schedule_interval= None 
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
    table_name = "PhucAnh"
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS {0} (Name varchar(255), Price varchar(255), URL varchar(255), ImgURL varchar(255), CPU varchar(255), Ram varchar(255), Ram_slot varchar(255), Storage varchar(255), Graphics varchar(255), Display varchar(255), Battery varchar(255), Wireless varchar(255), Weight varchar(255), Size varchar(255), Color varchar(255), OS varchar(255), PRIMARY KEY (Name, CPU, Ram, Storage));".format(table_name))
    conn.commit()
# Open the CSV file and read its contents
    csv_file = "../spark/resources/data/cleaned_laptop_phucanh.csv"
    with open(csv_file, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Insert the row into the container
            cur.execute("INSERT INTO {0} (Name, Price, URL, ImgURL, CPU, Ram, Ram_slot, Storage, Graphics, Display, Battery, Wireless, Weight, Size, Color, OS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Name, CPU, Ram, Storage) DO UPDATE SET Price = EXCLUDED.Price, URL = EXCLUDED.URL, ImgURL = EXCLUDED.ImgURL, Graphics = EXCLUDED.Graphics, Display = EXCLUDED.Display, Battery = EXCLUDED.Battery, Wireless = EXCLUDED.Wireless, Weight = EXCLUDED.Weight, Size = EXCLUDED.Size, Color = EXCLUDED.Color, OS = EXCLUDED.OS;".format(table_name), (row["Name"], row["Price"], row["URL"], row["ImgURL"], row["CPU"], row["Ram"], row["Ram_slot"], row["Storage"], row["Graphics"], row["Display"], row["Battery"], row["Wireless"], row["Weight"], row["Size"], row["Color"], row["OS"]))
            conn.commit()


start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="phucanh_clean_data_11",
    application="/opt/spark/app/process_phucanh.py", # Spark application path created in airflow and spark cluster
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
end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end