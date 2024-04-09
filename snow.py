from pyspark.sql import SparkSession

jar_path = r"/Workspace/Repos/etlbigdataautomation@gmail.com/Data_Automation_project/jars/snowflake-jdbc-3.14.3.jar"

# Initialize Spark session
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

# JDBC URL for Snowflake
jdbc_url = "jdbc:snowflake://zintvmu-pz14565.snowflakecomputing.com/?user=KATSREEN100&password=Dharmavaram2@&warehouse=COMPUTE_WH&db=CONTACT_INFO&schema=PUBLIC"
#jdbc_url = "jdbc:snowflake://autodesk_dev.snowflakecomputing.com/?user=svc_aad_d_edh_dbt_pa&password=(W+XKhl.s$R|<T$n7iYf?(*2jV<|&warehouse=EDH_SPM_PARTNER_ANALYTICS_WH&db=EDH_SPM&schema=PARTNER_ANALYTICS_STAGE"
# Read data from Snowflake
df = spark.read \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", jdbc_url) \
    .option("query", "select * from CONTACT_INFO.PUBLIC.CONTACT_INDO") \
    .load()

# Show the data
df.show()

# Stop Spark session
spark.stop()