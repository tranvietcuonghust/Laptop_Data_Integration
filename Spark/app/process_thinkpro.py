from pyspark.sql.functions import col, when
from pyspark.sql.functions import trim, regexp_replace, translate,regexp_extract

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import numpy as np
import re
import pandas as pd
import datetime

spark = SparkSession.builder.appName('clean_data_thinkpro').getOrCreate()

sqlContext = SQLContext(spark)
schema = StructType([ \
    StructField("Product", StringType(), True), \
    StructField("Price", StringType(), True), \
    StructField("Image", StringType(), True), \
    StructField("Link", StringType(), True), \
    StructField("CPU", StringType(), True), \
    StructField("Ram", StringType(), True), \
    StructField("Storage", StringType(), True), \
    StructField("Graphics", StringType(), True), \
    StructField("Screen", StringType(), True), \
    StructField("Status", StringType(), True), \
    StructField("Color", StringType(), True), \
    StructField("Weight", StringType(), True), \
    StructField("Battery", StringType(), True), \
    StructField("Brand", StringType(), True), \
    
  ])

df = spark.read.option("delimiter", ",") \
      .option("escape", "\"") \
      .option("multiLine","true") \
      .format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("../spark/resources/data/laptops_thinkpro.csv")
# # for col in df.columns:
# df = df.withColumn("Battery", regexp_replace(col('Battery'), "^\n", "")).withColumn("Battery", trim(col('Battery')))
# for column in ["Battery","Brand","CPU","Color","Display","Graphics","MFG_year","Name","OS","Price","Ram","Size","Storage","URL","Weight","Wireless"]:
# df = df.filter(~(col("Price").like("%Liên hệ%")))
# df = df.filter(df["Price"] != "Liên hệ")
df = df.filter(col("Price").isNotNull() & ~(col("Price").contains("Liên hệ")))
# df = df.select(col(""), col("Price"), col("URL"),col("CPU"),col("Ram"),col("Storage"),col("Graphics"),col("Display"),col("Battery"),\
#                col("Color"),col("Version"))
df = df.withColumn("Price", translate("Price", ".đ", "")) \
      .withColumn("RAM", regexp_replace("Ram", " GB", "GB")) \
      .withColumn("Storage", regexp_replace("Storage", " GB", "GB")) \
      .withColumn("Screen", regexp_replace("Screen", "inches", " inch")) \
      .withColumn("Battery", when(regexp_extract("Battery", "\d+", 0) == "", None).otherwise("Battery"))
for col_name in df.columns:
    df = df.withColumn(col_name, when(df[col_name].isNull(), df[col_name]).otherwise(regexp_replace(df[col_name], "(^\n|\")", ""))) 
    df = df.withColumn(col_name, when(df[col_name].isNull(), df[col_name]).otherwise(translate(df[col_name], "®™", ""))) 
    df=df.withColumn(col_name, when(df[col_name].isNull(), df[col_name]).otherwise(trim(df[col_name])))
    mode_value = df.select(col_name).groupBy(col_name).count().orderBy('count', ascending=False).first()[0]
    df = df.withColumn(col_name, when(col(col_name).isNull(), mode_value).otherwise(col(col_name)))

df.show()
df1pd = df.toPandas()
# df1.write.format("csv").mode('append').options(header='True', delimiter=',',escape ='\"') \
#  .save('../spark/resources/data/cleaned_house_price_data.csv')
df1pd.to_csv("../spark/resources/data/cleaned_laptop_thinkpro.csv", index=False, header=True)
spark.stop()

