print("hi")

from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
df=spark.createDataFrame(dataList, schema=['Language','fee'])

df.show()



df = spark.read.csv(r"C:\Users\A4952\PycharmProjects\Data_Automation_project\source_files\Contact_info.csv", header=True, inferSchema=True)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
