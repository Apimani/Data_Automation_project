import logging
import json
import os

import pkg_resources

from pyspark.sql.types import StructType

logging.basicConfig(filename="newfile.log",
                    level=logging.INFO,  # NDIWEC
                    filemode='w',
                    format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()


def read_data(format, path, spark, multiline="NA", sql_path=None, database=None, schema=None):
    print(schema)
    if format.lower() == 'csv':
        if schema == 'NA':
            df = spark.read.option("header", True).option("delimiter", ",").csv(path)
            logger.info("CSV file has read successfully from the below path" + path)
        else:
            print(type(schema))
            #schema = pkg_resources.resource_filename("schema", schema)
            schema = os.environ.get("project_path")
            with open(schema+'/schema/contact_info_schema.json', 'r') as schema_file:
                schema = StructType.fromJson(json.load(schema_file))
                print(schema)
                print(path)
            df = spark.read.schema(schema).option("header", True).option("delimiter", ",").csv(path)
            logger.info("CSV file has read successfully from the below path" + path)

    elif format.lower() == 'json':
        if multiline == 'NA':
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
        conf_file_path = pkg_resources.resource_filename('Config', 'config.json')
        with open(conf_file_path, 'r') as f:
            config_data = json.loads(f.read())[database]
        if sql_path != "NA":
            sql_path = pkg_resources.resource_filename('Transformations_queries', sql_path)
            with open(sql_path, "r") as file:
                sql_query = file.read()
            print(sql_query)
            print(config_data)
            df = spark.read.format("jdbc"). \
                option("url", config_data['url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("query", sql_query). \
                option("driver", config_data['driver']).load()
        elif sql_path == 'NA':
            df = spark.read.format("jdbc"). \
                option("url", config_data['url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("dbtable", path). \
                option("driver", config_data['driver']).load()
    elif format.lower() =='adls':
        pass

    #cosmosdb, synapse, redshift, google BQ, dynamodb, hbase, AWS S3 bucket, snowflake
    elif format.lower() == 'snowflake':
        conf_file_path = pkg_resources.resource_filename('Config', 'config.json')
        with open(conf_file_path, 'r') as f:
            config_data = json.loads(f.read())[database]
        conf_file_path = pkg_resources.resource_filename('Config', 'config.json')
        with open(conf_file_path, 'r') as f:
            config_data = json.loads(f.read())[database]
        if sql_path != "NA":
            sql_path = pkg_resources.resource_filename('Transformations_queries', sql_path)
            with open(sql_path, "r") as file:
                sql_query = file.read()
            print(sql_query)
            print(config_data)
        jar_path = r"C:\Users\A4952\Downloads\snowflake-jdbc-3.14.3.jar"

        # Initialize Spark session

        # JDBC URL for Snowflake
        jdbc_url = "jdbc:snowflake://zintvmu-pz14565.snowflakecomputing.com/?user=KATSREEN100&password=Dharmavaram2@&warehouse=COMPUTE_WH&db=CONTACT_INFO&schema=PUBLIC"

        # Read data from Snowflake
        df = spark.read \
            .format("jdbc") \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("url", jdbc_url) \
            .option("query", "select * from CONTACT_INFO.PUBLIC.CONTACT_INDO") \
            .load()
    else:
        logger.critical("File format is not found ")

    return df
