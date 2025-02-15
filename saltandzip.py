from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#from pyspark.sql.functions import col, lit, rand,sum

# Create a Spark session
spark = SparkSession.builder \
    .appName("Salting Example") \
    .getOrCreate()

# Sample Data
data = [
    (1, 100),
    (1, 200),
    (2, 150),
    (3, 50),
    (3, 300),
    (4, 500),
    (4, 700),
    (5, 200),
    (5, 100),
    (6, 50)
]

# Create DataFrame
columns = ["UserID", "TransactionAmount"]
df = spark.createDataFrame(data, columns)

# Show original data
print("Original DataFrame:")
df.show()

# Add a "salt" column by creating a random value between 0 and 2 (for example)
df_salted = df.withColumn("Salt", (rand() * 3).cast("int"))

# Combine the salt with the UserID for a new key
df_salted = df_salted.withColumn("UserID_Salted", (col("UserID") * 3 + col("Salt")))

# Show the salted DataFrame
print("DataFrame with Salting:")
df_salted.show()

# Perform an operation (e.g., aggregation) based on salted keys
df_aggregated = df_salted.groupBy("UserID_Salted").agg(
    sum("TransactionAmount").alias("TotalAmount")
)

# Show the aggregated data
print("Aggregated Data based on Salted Key:")
df_aggregated.show()

# Stop the Spark session
spark.stop()
=========================
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a Spark session
spark = SparkSession.builder \
    .appName("Using zipWithIndex") \
    .getOrCreate()

# Sample Data
data = [
    (1, 100),
    (1, 200),
    (2, 150),
    (3, 50),
    (3, 300),
    (4, 500),
    (4, 700),
    (5, 200),
    (5, 100),
    (6, 50)
]

# Create DataFrame
columns = ["UserID", "TransactionAmount"]
df = spark.createDataFrame(data, columns)

# Show original data
print("Original DataFrame:")
df.show()

# Using zipWithIndex to create an index for each row
rdd_with_index = df.rdd.zipWithIndex()

# Convert back to DataFrame, adding the index as a new column
df_with_index = rdd_with_index.map(lambda x: Row(
    UserID=x[0][0],
    TransactionAmount=x[0][1],
    Index=x[1]
)).toDF()

# Show the DataFrame with the index column
print("DataFrame with Index (via zipWithIndex):")
df_with_index.show()

# Stop the Spark session
spark.stop()
