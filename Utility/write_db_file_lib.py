def write_summary_table(df):
    jdbc_url = "jdbc:snowflake://zintvmu-pz14565.snowflakecomputing.com/?user=KATSREEN100&password=Dharmavaram2@&warehouse=COMPUTE_WH&db=CONTACT_INFO&schema=PUBLIC"
    df.write.mode("overwrite") \
        .format("jdbc") \
        .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
        .option("url", jdbc_url) \
        .option("dbtable", "CONTACT_INFO.PUBLIC.summary") \
        .save()


