import json
import os
from pyspark.sql.types import StructType

# Given absolute path
given_path = os.path.abspath(os.path.dirname(__file__))


def read_config(database):
    parent_path = os.path.dirname(given_path) + '\Config\Config.json'
    # Read the JSON configuration file
    with open(parent_path) as f:
        config = json.load(f)[database]
    return config

def read_schema(schema_file_path):
    path = os.path.dirname(given_path) + '/schema/' +schema_file_path
    # Read the JSON configuration file
    with open(path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
    return schema

def fetch_source_file_path(file_path):
    path = os.path.dirname(given_path) + '/source_files/'+file_path
    return path

def fetch_transformation_query_path(file_path):
    path = os.path.dirname(given_path) + '/Transformations_queries/' + file_path
    with open(path, "r") as file:
        sql_query = file.read()
    return sql_query





