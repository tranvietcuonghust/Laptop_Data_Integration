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
    table_name = "TrungTran"
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS {0} (Name VARCHAR(255) NOT NULL, Price VARCHAR(255), URL VARCHAR(255), ImgURL VARCHAR(255), CPU VARCHAR(255), Ram VARCHAR(255), Storage VARCHAR(255), Graphics VARCHAR(255), Display VARCHAR(255), Status VARCHAR(255), Battery VARCHAR(255), Wireless VARCHAR(255), Weight VARCHAR(255), Size VARCHAR(255), Color VARCHAR(255), OS VARCHAR(255), Brand VARCHAR(255), MFG_year VARCHAR(255), PRIMARY KEY (Name, CPU, Ram, Storage));".format(table_name))
    conn.commit()

# Open the CSV file and read its contents
    csv_file = "../spark/resources/data/cleaned_laptop_trungtran.csv"
    with open(csv_file, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Insert the row into the container
            cur.execute("INSERT INTO {0} (Name, Price, URL, ImgURL, CPU, Ram, Storage, Graphics, Display, Status, Battery, Wireless, Weight, Size, Color, OS, Brand, MFG_year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Name, CPU, Ram, Storage) DO UPDATE SET Price = EXCLUDED.Price, URL = EXCLUDED.URL, ImgURL = EXCLUDED.ImgURL, Graphics = EXCLUDED.Graphics, Display = EXCLUDED.Display, Status = EXCLUDED.Status, Battery = EXCLUDED.Battery, Wireless = EXCLUDED.Wireless, Weight = EXCLUDED.Weight, Size = EXCLUDED.Size, Color = EXCLUDED.Color, OS = EXCLUDED.OS, Brand = EXCLUDED.Brand, MFG_year = EXCLUDED.MFG_year;".format(table_name), (row["Name"], row["Price"], row["URL"], row["ImgURL"], row["CPU"], row["Ram"], row["Storage"], row["Graphics"], row["Display"], row["Status"], row["Battery"], row["Wireless"], row["Weight"], row["Size"], row["Color"], row["OS"], row["Brand"], row["MFG_year"]))
            conn.commit()

dag = DAG(
        dag_id="trungtran_process_data_2", 
        description="This DAG runs a Pyspark app",
        default_args=default_args, 
        schedule_interval=None 
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="trungtran_clean_data_11",
    application="/opt/spark/app/process_trungtran.py", # Spark application path created in airflow and spark cluster
    name="trungtran_clean_data",
    conn_id="other_spark_local",
    verbose=1,
    conf={"spark.master":other_spark_master},
    # application_args=[csv_file],
    dag=dag)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    
    dag=dag,
)
end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end