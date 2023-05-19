from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
date_process = None
if (sys.argv[1]!=None):
    date_process = sys.argv[1]
else:
    os._exit
def get_list_files_by_date(date_file):
    hdfs_dir_path = "hdfs://mhtt-master:8020/user/Encrypt_TTCNTT/cty2/DataUsage"
    # use os.popen to execute the Hadoop fs -ls command to get the list of file names in the HDFS directory
    file_list = os.popen("hadoop fs -ls " + hdfs_dir_path).readlines()
    # create an empty list to store the file names that contain the search string
    search_file_list = []
    # loop through the file list and extract the file names that contain the search string
    for file_name in file_list:
        if date_file in file_name:
            search_file_list.append(file_name.split()[-1])
    return search_file_list
spark = SparkSession.builder.\
    appName("process_data_usage").\
    master("local").\
    config("spark.jars", "/u01/quangtc/preprocess_data/lib/postgresql-42.2.5.jar") .\
    getOrCreate()
history_schema = StructType([
     #StructField('code_id', StringType(), True),
     StructField('isdn', StringType(), True),
     StructField('DATE_ID', StringType(), True),
     StructField('ARPU_TOTAL', IntegerType(), True),
     StructField('TOTAL_DATA', IntegerType(), True),
     StructField('TOTAL_CALL', IntegerType(), True),
     StructField('TOTAL_SMS', IntegerType(), True)
 ])
df_history = spark.read.option("delimiter","|")\
    .csv(get_list_files_by_date(date_process),schema=history_schema)
# df_history = spark.read.option("delimiter","|")\
#     .csv("/user/Encrypt_TTCNTT/cty2/DataUsage/FileName=datausage_20230307.txt/datausage_20230307.txt______part-00013-90644edf-49be-4ace-af89-b17a4738fac9.c000.csv",schema=history_schema)
# df_history.groupBy("isdn").agg(count("arpu_total")).show()
# df_history.printSchema()
# df_history.show()

df_history = df_history.withColumn("TOTAL_DATA", col("TOTAL_DATA").cast("double") / 1000.0)
df_history.show()
df_history \
    .write.mode("append").format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/bangoi") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "f_historical_code") \
    .option("user", "airflow").option("password", "airflow").save()