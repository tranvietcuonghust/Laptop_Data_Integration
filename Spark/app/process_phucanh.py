
from pyspark.sql.functions import col, when
from pyspark.sql.functions import trim, regexp_replace,translate,regexp_extract,split, concat_ws

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import numpy as np
import re
import pandas as pd
import datetime

spark = SparkSession.builder.appName('clean_data_phucanh').getOrCreate()
sqlContext = SQLContext(spark)

schema = StructType([ \
    StructField("Battery", StringType(), True), \
    StructField("CPU", StringType(), True), \
    StructField("CPU_code", StringType(), True), \
    StructField("CPU_speed", StringType(), True), \
    StructField("Color", StringType(), True), \
    StructField("Display", StringType(), True), \
    StructField("Graphics", StringType(), True), \
    StructField("ImgURL", StringType(), True), \
    StructField("Name", StringType(), True), \
    StructField("Num_cores", StringType(), True), \
    StructField("Num_thread", StringType(), True), \
    StructField("OS", StringType(), True), \
    StructField("Ports", StringType(), True), \
    StructField("Price", StringType(), True), \
    StructField("Ram", StringType(), True), \
    StructField("Ram_slot", StringType(), True), \
    StructField("Ram_speed", StringType(), True), \
    StructField("Ram_type", StringType(), True), \
    StructField("Resolutions", StringType(), True), \
    StructField("Size", StringType(), True), \
    StructField("Storage", StringType(), True), \
    StructField("Storage_drive", StringType(), True), \
    StructField("Storage_type", StringType(), True), \
    StructField("URL", StringType(), True), \
    StructField("Weight", StringType(), True), \
    StructField("Wireless", StringType(), True) \
  ])

df = spark.read.option("delimiter", ",") \
      .option("escape", "\"") \
      .option("multiLine","true") \
      .format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("../spark/resources/data/laptops_phucanh.csv")
# # for col in df.columns:
# df = df.withColumn("Battery", regexp_replace(col('Battery'), "^\n", "")).withColumn("Battery", trim(col('Battery')))
# for column in ["Battery","Brand","CPU","Color","Display","Graphics","MFG_year","Name","OS","Price","Ram","Size","Storage","URL","Weight","Wireless"]:
# df = df.filter(~(col("Price").like("%Liên hệ%")))
# df = df.filter(df["Price"] != "Liên hệ")
df = df.filter(col("Price").isNotNull() & ~(col("Price").contains("Liên hệ")))

df = df.select(col("Name"), col("Price"), col("URL"),col("ImgURL"),col("CPU"),col("CPU_code"),col("CPU_speed"),col("Ram"),col("Ram_type"),col("Ram_speed"),col("Ram_type"),col("Ram_slot") \
               ,col("Storage"),col("Storage_type"),col("Storage_drive"),col("Graphics"),col("Display"),col("Battery"),col("Wireless"),\
               col("Weight"),col("Size"),col("Color"),col("OS"))
df = df.withColumn("CPU", translate("CPU", "®™", "")) \
        .withColumn("Graphics", regexp_replace("Graphics", "(Đồ họa | cho Bộ xử lý Intel)", "")) \
        .withColumn("Price", translate("Price", ",.₫", "")) \
        .withColumn("Ram", regexp_replace("Ram", " GB", "GB")) \
        .withColumn("Storage", regexp_replace("Storage", " GB", "GB")) \
        .withColumn("Weight", regexp_replace("Weight", "(kg|KG)", "")) \
        .withColumn("Name", regexp_replace("Name", "\(.*?\)", "")) \
        .withColumn("Battery", when(regexp_extract("Battery", "\d+", 0) == "", None).otherwise("Battery")) \
        .withColumn("Display", regexp_replace("Display", "(\"|″|”)", " inch")) \
        .withColumn("CPU", concat_ws(" ", col("CPU"), col("CPU_code"), col("CPU_speed")))\
        .withColumn("Ram", concat_ws(" ", col("Ram"), col("Ram_type"), col("Ram_speed"))) \
        .withColumn("Storage", concat_ws(" ", col("Storage"), col("Storage_type"), col("Storage_drive") )) \
        .drop("CPU_code", "CPU_speed","Ram_type","Ram_speed","Storage_type", "Storage_drive" )
        

for col_name in df.columns:
    df = df.withColumn(col_name, when(df[col_name].isNull(), df[col_name]).otherwise(regexp_replace(df[col_name], "(^\n|\")", ""))) 
    df = df.withColumn(col_name, when(df[col_name].isNull(), df[col_name]).otherwise(regexp_replace(df[col_name], ": ", ""))) 
    df = df.withColumn(col_name, when(df[col_name].isNull(), df[col_name]).otherwise(translate(df[col_name], "®™", ""))) 
    df=df.withColumn(col_name, when(df[col_name].isNull(), df[col_name]).otherwise(trim(df[col_name])))
    mode_value = df.select(col_name).groupBy(col_name).count().orderBy('count', ascending=False).first()[0]
    df = df.withColumn(col_name, when(col(col_name).isNull(), mode_value).otherwise(col(col_name)))


df.show()

df1pd = df.toPandas()
# df1.write.format("csv").mode('append').options(header='True', delimiter=',',escape ='\"') \
#  .save('../spark/resources/data/cleaned_house_price_data.csv')
df1pd.to_csv("../spark/resources/data/cleaned_laptop_phucanh.csv", index=False, header=True)
spark.stop()

