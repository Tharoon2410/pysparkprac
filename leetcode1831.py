##leetcode1831##
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("Transactions").getOrCreate()

# Define schema
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("day", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create data
transactions_data = [
    (8, "2021-04-03 15:57:28", 57),
    (9, "2021-04-28 08:47:25", 21),
    (1, "2021-04-29 13:28:30", 58),
    (5, "2021-04-28 16:39:59", 40),
    (6, "2021-04-29 23:39:28", 58)
]

# Create DataFrame
df = spark.createDataFrame(transactions_data, schema=transactions_schema)

df = df.withColumn("day", to_timestamp("day", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("date_only", to_date("day"))
# Show the DataFrame
df.show(truncate=False)
tran_df = df.withColumn("maximum_amount",first_value("amount").over(Window.partitionBy("date_only").orderBy(col("amount").desc()))).filter(col("amount") == col("maximum_amount"))

result_df = tran_df.orderBy("transaction_id").select("transaction_id")
result_df.show()