import datetime
import json
import os
import sys
import pandas as pd
from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode, current_timestamp
from pyspark.sql import SparkSession
os.environ.setdefault("project_path", os.getcwd())
project_path = os.environ.get("project_path")
import getpass

# Fetch system user's login name
system_user = getpass.getuser()

postgre_jar = project_path + "/jars/postgresql-42.2.5.jar"
snow_jar = project_path + "/jars/snowflake-jdbc-3.14.3.jar"
oracle_jar = project_path + "/jars/ojdbc11.jar"

batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

jar_path = postgre_jar+','+snow_jar + ','+oracle_jar
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()




file = spark.read.csv("source_files/Contact_info.csv", header=True)
file = file.filter(file.Identifier.isNotNull())
file2 = spark.read.csv("source_files/Contact_info.csv", header=True)
file2 = file2.filter(file2.Identifier.isNotNull())

file= file.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))

file2= file2.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))

file2.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "contact_info_raw") \
    .option("user", "postgres") \
    .option("password", "Dharmavaram1@") \
    .option("driver", 'org.postgresql.Driver') \
    .save()


file3 = file.union(file2)
file.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/ORCL") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "contact_info_raw") \
    .option("user", "system") \
    .option("password", "Dharmavaram1@") \
    .save()

file.createOrReplaceTempView("file")

contact_info_bronze = spark.sql(
    """ select
    cast(Identifier as decimal(10)) Identifier,
    upper(Surname) Surname,
    upper(given_name) given_name,
    upper(middle_initial) middle_initial,
    suffix,
    Primary_street_number,
    primary_street_name,
    city,
    state,
    zipcode,
    Primary_street_number_prev,
    primary_street_name_prev,
    city_prev,
    state_prev,
    zipcode_prev,
    Email,
    translate(Phone,'+-','') phone,
    rpad(birthmonth,8,'0') birthmonth
    from file
    """
)

contact_info_bronze= contact_info_bronze.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))

contact_info_bronze.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/ORCL") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "contact_info_bronze") \
    .option("user", "system") \
    .option("password", "Dharmavaram1@") \
    .save()

contact_info_bronze.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "contact_info_bronze") \
    .option("user", "postgres") \
    .option("password", "Dharmavaram1@") \
    .option("driver", 'org.postgresql.Driver') \
    .save()

contact_info_bronze.createOrReplaceTempView("contact_info_bronze")
contact_info_bronze.show()

contact_info_silver= spark.sql(
        """
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number,
        primary_street_name,
        city,
        state,
        zipcode,
        Email,
        Phone,
        birthmonth,
        'Y' as Current_ind
        from contact_info_bronze 
        union 
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number_prev,
        primary_street_name_prev,
        city_prev,
        state_prev,
        zipcode_prev,
        Email,
        Phone,
        birthmonth,
        'N' as Current_ind
        from contact_info_bronze
        """)

contact_info_silver=contact_info_silver.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))

contact_info_silver.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/ORCL") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "contact_info_silver") \
    .option("user", "system") \
    .option("password", "Dharmavaram1@") \
    .save()

print("load to postgre")
contact_info_silver.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "contact_info_silver") \
    .option("user", "postgres") \
    .option("password", "Dharmavaram1@") \
    .option("driver", 'org.postgresql.Driver') \
    .save()