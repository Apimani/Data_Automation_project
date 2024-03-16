"""This file will be starting point for automation execution"""

from Utility.files_read_lib import *
from Utility.validation_lib import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
import datetime

batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#print(batch_id)


spark = SparkSession.builder.master("local") \
    .appName("test") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.29,net.snowflake:spark-snowflake_2.13:2.15.0-spark_3.4") \
    .config("spark.driver.extraClassPath", "net.snowflake:snowflake-jdbc:3.13.29") \
    .config("spark.executor.extraClassPath", "net.snowflake:snowflake-jdbc:3.13.29") \
    .getOrCreate()


sfOptions =  {
    "sfURL": "https://zintvmu-pz14565.snowflakecomputing.com",
    "sfDatabase": "CONTACT_INFO",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfUser": "katsreen100",
    "sfPassword": "Dharmavaram2@"
  }

df = spark.read \
            .format('snowflake') \
            .options(**sfOptions) \
            .option("dbtable", "CONTACT_INFO.PUBLIC.CONTACT_INDO") \
            .load()

