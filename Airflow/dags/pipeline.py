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
from airflow.sensors.external_task import ExternalTaskSensor 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.sensors.file_sensor import FileSensor

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
    "start_date":datetime(2023, 6, 18),
    "email": ["pioneer22022001@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depend_on_past" : True
}


dag = DAG(
        dag_id="data_matching_20", 
        description="This DAG runs a Pyspark app",
        default_args=default_args, 
        schedule_interval="0 9 * * *",
        start_date = datetime(2023, 6, 18),
        max_active_runs=1
    )

def load_to_postgres():


    host = "c.tichhopdulieu-gr1.postgres.database.azure.com"
    dbname = "citus"
    user = "citus"
    password = "Lufe22022001"
    sslmode = "require"
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

    # Establish a connection to Azure Cosmos DB for PostgreSQL
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    # Create the table in Azure Cosmos DB for PostgreSQL
    table_name = "LAPTOP_TABLE_6"
    cur.execute("""
    CREATE TABLE IF NOT EXISTS {0} (
        Product_ID SERIAL PRIMARY KEY,
        Product_Group TEXT,
        Image TEXT,
        CPU TEXT,
        RAM TEXT,
        Storage TEXT,
        Graphics TEXT,
        Screen TEXT,
        Status TEXT,
        Color TEXT,
        Weight TEXT,
        Battery TEXT,
        OS TEXT,
        Brand TEXT,
        Originals TEXT,
        UNIQUE (Product_Group)
    );
""".format(table_name))
    conn.commit()


    # Import the DataFrame into the table
    csv_file = "../spark/resources/data/all_laptop.csv"
    with open(csv_file, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Insert the row into the container
                cur.execute("""
    INSERT INTO {0} (Product_Group, Image, CPU, RAM, Storage, Graphics, Screen, Status, Color, Weight, Battery, OS, Brand, Originals)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (Product_Group) DO UPDATE SET
    Image = EXCLUDED.Image,
    CPU = EXCLUDED.CPU,
    RAM = EXCLUDED.RAM,
    Storage = EXCLUDED.Storage,
    Graphics = EXCLUDED.Graphics,
    Screen = EXCLUDED.Screen,
    Status = EXCLUDED.Status,
    Color = EXCLUDED.Color,
    Weight = EXCLUDED.Weight,
    Battery = EXCLUDED.Battery,
    OS = EXCLUDED.OS,
    Brand = EXCLUDED.Brand,
    Originals = EXCLUDED.Originals;
""".format(table_name), (
    row["Product_Group"],
    row["Image"],
    row["CPU"],
    row["RAM"],
    row["Storage"],
    row["Graphics"],
    row["Screen"],
    row["Status"],
    row["Color"],
    row["Weight"],
    row["Battery"],
    row["OS"],
    row["Brand"],
    row["Originals"]
))
                conn.commit()
    cur.close()
    conn.close()
trigger_dag1 = TriggerDagRunOperator(
    task_id="laptop88_et",
    trigger_dag_id="laptop88_process_data_2",
    dag=dag,

)

trigger_dag2 = TriggerDagRunOperator(
    task_id="trungtran_et",
    trigger_dag_id="trungtran_process_data_2",
    dag=dag,
)

trigger_dag3 = TriggerDagRunOperator(
    task_id="phucanh_et",
    trigger_dag_id="phucanh_process_data_1",
    dag=dag,
)
trigger_dag4 = TriggerDagRunOperator(
    task_id="thinkpro_et",
    trigger_dag_id="thinkpro_process_data_2",
    dag=dag,
)

trigger_dag5 = TriggerDagRunOperator(
    task_id="techcare_et",
    trigger_dag_id="techcare_process_data_7",
    dag=dag,
)

trigger_dag6 = TriggerDagRunOperator(
    task_id="tgdd_et",
    trigger_dag_id="tgdd_process_data_7",
    dag=dag,
)
######################################sensor######################################3
file_sensor_1 = FileSensor(
    task_id="wait_for_laptop88et_ouput",
    filepath="/opt/spark/resources/data/cleaned_laptop_laptop88.csv",
    poke_interval=30,  # Adjust as needed
    dag=dag
)
file_sensor_2 = FileSensor(
    task_id="wait_for_trungtranet_ouput",
    filepath="/opt/spark/resources/data/cleaned_laptop_trungtran.csv",
    poke_interval=30,  # Adjust as needed
    dag=dag
)
file_sensor_3 = FileSensor(
    task_id="wait_for_phucanhet_ouput",
    filepath="/opt/spark/resources/data/cleaned_laptop_phucanh.csv",
    poke_interval=30,  # Adjust as needed
    dag=dag
)
file_sensor_4 = FileSensor(
    task_id="wait_for_thinkproet_ouput",
    filepath="/opt/spark/resources/data/cleaned_laptop_thinkpro.csv",
    poke_interval=30,  # Adjust as needed
    dag=dag
)
file_sensor_5 = FileSensor(
    task_id="wait_for_techcareet_ouput",
    filepath="/opt/spark/resources/data/cleaned_laptop_techcare.csv",
    poke_interval=30,  # Adjust as needed
    dag=dag
)
file_sensor_6 = FileSensor(
    task_id="wait_for_tgddet_ouput",
    filepath="/opt/spark/resources/data/cleaned_laptop_tgdd.csv",
    poke_interval=30,  # Adjust as needed
    dag=dag
)
##################################################




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

ssh_to_local = SSHOperator(
        task_id="data_matching",
        ssh_conn_id="ssh_default",
        command="/bin/python3 /home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/app/data_matching.py",
        cmd_timeout = 600,
        dag=dag
    )

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


delete_temp_files = SSHOperator(
        task_id="ssh_to_local",
        ssh_conn_id="ssh_default",
        command="rm /home/tranvietcuong/Desktop/Tich_hop_du_lieu/group-1-it5420/Spark/resources/data/cleaned_*.csv",
        cmd_timeout = 600,
        dag=dag
    )


end = DummyOperator(task_id="end", dag=dag)

start>> [trigger_dag1, trigger_dag2,trigger_dag3,trigger_dag4,trigger_dag5,trigger_dag6] 

trigger_dag1 >> file_sensor_1
trigger_dag2 >> file_sensor_2
trigger_dag3 >> file_sensor_3
trigger_dag4 >> file_sensor_4
trigger_dag5 >> file_sensor_5
trigger_dag6 >> file_sensor_6
[file_sensor_1, file_sensor_2, file_sensor_3, file_sensor_4, file_sensor_5, file_sensor_6] >> ssh_to_local>>load_to_postgres_task >> delete_temp_files >> end