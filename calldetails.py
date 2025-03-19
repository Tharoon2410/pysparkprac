from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CallDetails") \
    .getOrCreate()

# Define the data as a list of tuples
data = [
    ('OUT', '181868', 13), ('OUT', '2159010', 8), ('OUT', '2159010', 178),
    ('SMS', '4153810', 1), ('OUT', '2159010', 152), ('OUT', '9140152', 18),
    ('SMS', '4162672', 1), ('SMS', '9168204', 1), ('OUT', '9168204', 576),
    ('INC', '2159010', 5), ('INC', '2159010', 4), ('SMS', '2159010', 1),
    ('SMS', '4535614', 1), ('OUT', '181868', 20), ('INC', '181868', 54),
    ('INC', '218748', 20), ('INC', '2159010', 9), ('INC', '197432', 66),
    ('SMS', '2159010', 1), ('SMS', '4535614', 1)
]

# Define the schema
schema = StructType([
    StructField("call_type", StringType(), True),
    StructField("call_number", StringType(), True),
    StructField("call_duration", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

procdf = df.groupBy("call_number").agg(
    sum(when(col("call_type") == "OUT", col("call_duration"))).alias("out_duration"),
    sum(when(col("call_type") == "INC", col("call_duration"))).alias("inc_duration")
)

# Filter the DataFrame based on the conditions
resultdf = procdf.filter(
    (col("out_duration").isNotNull()) &
    (col("inc_duration").isNotNull()) &
    (col("out_duration") > col("inc_duration"))
).select("call_number")

# Show the result
resultdf.show()

===================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CallDetails") \
    .getOrCreate()

# Sample DataFrame
data = [
    ('OUT', '181868', 13), ('OUT', '2159010', 8), ('OUT', '2159010', 178),
    ('SMS', '4153810', 1), ('OUT', '2159010', 152), ('OUT', '9140152', 18),
    ('SMS', '4162672', 1), ('SMS', '9168204', 1), ('OUT', '9168204', 576),
    ('INC', '2159010', 5), ('INC', '2159010', 4), ('SMS', '2159010', 1),
    ('SMS', '4535614', 1), ('OUT', '181868', 20), ('INC', '181868', 54),
    ('INC', '218748', 20), ('INC', '2159010', 9), ('INC', '197432', 66),
    ('SMS', '2159010', 1), ('SMS', '4535614', 1)
]

# Create DataFrame
columns = ["call_type", "call_number", "call_duration"]
df = spark.createDataFrame(data, columns)

# Create temporary DataFrame with sum of durations
cte = df.groupBy("call_number").agg(
    _sum(when(col("call_type") == "OUT", col("call_duration"))).alias("out_duration"),
    _sum(when(col("call_type") == "INC", col("call_duration"))).alias("inc_duration")
)

# Filter the DataFrame based on the conditions
resultdf = cte.filter(
    (col("out_duration").isNotNull()) &
    (col("inc_duration").isNotNull()) &
    (col("out_duration") > col("inc_duration"))
).select("call_number")

# Show the result
resultdf.show()
