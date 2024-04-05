print("hi")

from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

import json
jar_path = r"C:\Users\A4952\PycharmProjects\Data_Automation_project\jars\postgresql-42.2.5.jar"
# Create SparkSession
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

conf_file_path = r"C:\Users\A4952\PycharmProjects\Data_Automation_project\Config\Config.json"
with open(conf_file_path, 'r') as f:
      config_data = json.loads(f.read())['postgre_db']

sql_path = r"C:\Users\A4952\PycharmProjects\Data_Automation_project\Transformations_queries\contact_info_t.sql"
with open(sql_path, "r") as file:
      sql_query = file.read()
      print(sql_query)
      print(config_data)

df = spark.read.format("jdbc"). \
      option("url", config_data['url']). \
      option("user", config_data['user']). \
      option("password", config_data['password']). \
      option("query", "SELECT * FROM CONTACT_INFO_bronze"). \
      option("driver", config_data['driver']).load()