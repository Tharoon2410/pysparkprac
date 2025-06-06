from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("ActivitiesData").getOrCreate()

# Define schema
schema = StructType([
    StructField("sell_date", DateType(), True),
    StructField("product", StringType(), True)
])

# Create data
data = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Bible"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-Shirt")
]

# Convert data into DataFrame
df = spark.createDataFrame(data, schema=schema)
df = df.withColumn("sell_date", to_date("sell_date", "yyyy-MM-dd"))
# Show DataFrame
df.show()
# Perform aggregation
result_df = df.groupBy("sell_date").agg(
    countDistinct("product").alias("num_sold"),
    concat_ws(", ", collect_list("product")).alias("products")
).orderBy(col("sell_date"))

# Show result
result_df.show(truncate=False)
