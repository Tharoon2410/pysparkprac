from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Start Spark session
spark = SparkSession.builder.appName("UnionByNameExample").getOrCreate()

# Define schema for df1
schema1 = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True)
])

# Define schema for df2 (note: different column order and an extra column)
schema2 = StructType([
    StructField("city", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loyalty_score", IntegerType(), True)  # Extra column
])

# Create sample data
data1 = [
    ("C001", "Alice", "Mumbai"),
    ("C002", "Bob", "Delhi")
]

data2 = [
    ("Chennai", "Charlie", "C003", 85),
    ("Bangalore", "Diana", "C004", 92)
]

# Create DataFrames
df1 = spark.createDataFrame(data1, schema=schema1)
df2 = spark.createDataFrame(data2, schema=schema2)

# Union by name, allowing missing columns
union_df = df1.unionByName(df2, allowMissingColumns=True)

# Show result
union_df.show()
