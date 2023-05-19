from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
# import findspark
# findspark.init()
date_process = None
if (sys.argv[1]!=None):
    date_process = sys.argv[1]
else:
    os._exit
def get_list_files_by_date(date_file):
    hdfs_dir_path = "hdfs://mhtt-master:8020/user/Encrypt_TTCNTT/cty2/Archive/CalloutCT2_pack_active"
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
    appName("process_pack_active").\
    master("local").\
    config("spark.jars", "/u01/quangtc/preprocess_data/lib/postgresql-42.2.5.jar") .\
    getOrCreate()
# sc = spark.sparkContext

# Add the PostgreSQL driver JAR to the classpath
# sc.addJar("/u01/quangtc/preprocess_data/lib/postgresql-42.2.5.jar")
pack_active_schema = StructType([
     StructField('isdn', StringType(), True),
     StructField('CODE', StringType(), True),
     StructField('REG_DATETIME', TimestampType(), True),
     StructField('EXPIRE_DATETIME', TimestampType(), True),
     StructField('END_DATETIME', TimestampType(), True),
     StructField('DATE_ID', StringType(), True),
     StructField('PRICE', IntegerType(), True),
 ])
df_pack_active = spark.read.option("delimiter","|").csv(get_list_files_by_date(date_process),schema=pack_active_schema)\
    .na.fill(value=0,subset=["price"]).dropDuplicates(["isdn","code"])
# print(os.getcwd())
# df_pack_active = spark.read.option("delimiter","|").csv("/user/Encrypt_TTCNTT/cty2/Archive/CalloutCT2_pack_active/pack_active_20230411.txt______part-00003-9eee1418-2460-4444-9218-f492aa73ac4d.c000.csv",schema=pack_active_schema)\
#     .na.fill(value=0,subset=["price"]).dropDuplicates(["isdn","code"])
df_today = df_pack_active.withColumn("REG_DATE_ID", date_format(to_date(col("REG_DATETIME"),"yyyyMMdd")).cast(StringType()))
df_today = df_today.filter(col("REG_DATE_ID") == date_process). \
            drop(col("REG_DATE_ID"))
df_today.show()
# df_pack_active.show()
# path_local='/u01/quangtc/Airflow_Docker_Celery/data/tmp/pack_active/'
# path_file= path_local+'20230422'+'.csv'
# if not os.path.exists(path_local):
#     os.makedirs(path_local)
# df_pack_active \
#     .write.mode("append").format("jdbc")\
#     .option("url", "jdbc:postgresql://localhost:5432/bangoi") \
#     .option("driver", "org.postgresql.Driver")\
#     .option("dbtable", "f_pack_active") \
#     .option("user", "airflow")\
#     .option("password", "airflow").save()
# df_pack_active.toPandas().to_csv("/u01/quangtc/Airflow_Docker_Celery/data/tmp/pack_active/20230422.csv", index=False)
# df_pack_active.write.option("delimiter", ",").csv("file:///u01/quangtc/Airflow_Docker_Celery/data/tmp/pack_active/20230422.csv", header=True, mode="overwrite")
# spark.stop()