##leetcode1683##
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("SimpleDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("content", StringType(), True)
])

# Create data
data = [
    (1, "Let us Code"),
    (2, "More than fifteen chars are here!")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show(truncate=False)

filter_df = df.filter(char_length("content") > 15)
filter_df.show()