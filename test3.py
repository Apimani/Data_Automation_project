from Utility.general_utility import *

print(read_config('oracle_db'))

print(read_schema('contact_info_schema.json'))

print(fetch_source_file_path('Contact_info.csv'))

print(fetch_transformation_query_path('contact_info_s.sql'))

