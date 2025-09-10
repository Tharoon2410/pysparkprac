from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Start Spark session with spark-xml package
spark = SparkSession.builder \
    .appName("ReadXMLExample") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0") \
    .getOrCreate()

# Read XML file
df = spark.read \
    .format("xml") \
    .option("rowTag", "customer") \
    .load("/content/sample_data/customer.xml")

# Show the DataFrame
df.show()
df.printSchema()
filter_df = df.filter((col("customer_name") == "Alice"))
filter_df.show()
