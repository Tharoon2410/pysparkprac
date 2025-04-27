##leet1699##
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("CallsTable").getOrCreate()

# Define schema
schema = StructType([
    StructField("from_id", IntegerType(), True),
    StructField("to_id", IntegerType(), True),
    StructField("duration", IntegerType(), True)
])

# Create data
data = [
    (1, 2, 59),
    (2, 1, 11),
    (1, 3, 20),
    (3, 4, 100),
    (3, 4, 200),
    (3, 4, 200),
    (4, 3, 499)
]

# Create DataFrame
calls_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
calls_df.show()

withcall_df = calls_df.withColumn("person1",expr("""CASE WHEN from_id < to_id THEN from_id ELSE to_id END""")) \
    .withColumn("person2",expr("""CASE WHEN from_id < to_id then to_id ELSE from_id END"""))

final_df = withcall_df.groupBy("person1","person2").agg(expr("count(*) as call_count"),expr("sum(duration) as total_duration")).select("person1","person2","call_count","total_duration")
final_df.show()