import logging
import json
import os

import pkg_resources

from pyspark.sql.types import StructType
from Utility.general_utility import *


logging.basicConfig(filename="newfile.log",
                    level=logging.INFO,  # NDIWEC
                    filemode='w',
                    format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()

project_path = os.environ.get("project_path")
def read_data(row,format, path, spark, multiline="NOT APPL", sql_path=None, database=None, schema=None):
    #print(schema)
    if format.lower() == 'csv':
        if schema == 'NOT APPL':
            df = spark.read.option("header", True).option("delimiter", ",").csv(path)
            logger.info("CSV file has read successfully from the below path" + path)
        else:
            schema = read_schema(row['schema_path'])
            df = spark.read.schema(schema).option("header", True).option("delimiter", ",").csv(path)
            logger.info("CSV file has read successfully from the below path" + path)

    elif format.lower() == 'json':
        if multiline == 'NOT APPL':
            df = spark.read.json(path)
            logger.info("Json file has read successfully from the below path" + path)

        elif multiline == True:
            df = spark.read.option("multiline", True).json(path)
            logger.info("Json file has read successfully from the below path" + path)

    elif format.lower() == 'parquet':
        df = spark.read.parquet(path)
        logger.info("parquet file has read successfully from the below path" + path)

    elif format.lower() == 'avro':
        df = spark.read.avro(path)
        logger.info("Avro file has read successfully from the below path" + path)

    elif format.lower() == 'table':
        config_data = read_config(database)
        if sql_path != "NOT APPL":
            sql_query = fetch_transformation_query_path(sql_path)
            print(sql_query)
            print(config_data)
            df = spark.read.format("jdbc"). \
                option("url", config_data['url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("query", sql_query). \
                option("driver", config_data['driver']).load()
        elif sql_path == 'NOT APPL':
            df = spark.read.format("jdbc"). \
                option("url", config_data['url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("dbtable", path). \
                option("driver", config_data['driver']).load()
    elif format.lower() =='adls':
        pass
    elif format.lower() == 'snowflake':
        config_data = read_config(database)
        if sql_path != "NOT APPL":
            sql_path = pkg_resources.resource_filename('Transformations_queries', sql_path)
            with open(sql_path, "r") as file:
                sql_query = file.read()
            print(sql_query)

        # Read data from Snowflake
        df = spark.read \
            .format("jdbc") \
            .option("driver", config_data['driver']) \
            .option("url", config_data['jdbc_url']) \
            .option("query",sql_query ) \
            .load()
    else:
        logger.critical("File format is not found ")

    return df
    #return df.drop(row['exclude'].split(',')
