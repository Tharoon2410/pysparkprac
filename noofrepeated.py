from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, when, sum

# Initialize Spark session
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Sample data for customer_orders
data = [
    (1, 100, '2022-01-01', 2000),
    (2, 200, '2022-01-01', 2500),
    (3, 300, '2022-01-01', 2100),
    (4, 100, '2022-01-02', 2000),
    (5, 400, '2022-01-02', 2200),
    (6, 500, '2022-01-02', 2700),
    (7, 100, '2022-01-03', 3000),
    (8, 400, '2022-01-03', 1000),
    (9, 600, '2022-01-03', 3000)
]

columns = ["order_id", "customer_id", "order_date", "order_amount"]

# Create DataFrame
customerorders_df = spark.createDataFrame(data=data, schema=columns)

# Show the initial DataFrame
customerorders_df.show()

# Group by customer_id and find the first order date
first_visit_df = customerorders_df.groupBy("customer_id").agg(min('order_date').alias("first_visit_date"))

# Show the first visit DataFrame
print("First Visit DataFrame:")
first_visit_df.show()

# Join to flag the first and repeat visits
visit_flag_df = customerorders_df.join(first_visit_df, on="customer_id", how="inner")\
    .withColumn("first_visit_flag", when(col("order_date") == col("first_visit_date"), 1).otherwise(0))\
    .withColumn("repeat_visit_flag", when(col("order_date") != col("first_visit_date"), 1).otherwise(0))

# Show the DataFrame with flags
print("DataFrame with Flags:")
visit_flag_df.show()

# Group by order_date and aggregate the counts for new and repeated customers
result_df = visit_flag_df.groupBy("order_date")\
    .agg(
        sum("first_visit_flag").alias("no_of_new_customers"),
        sum("repeat_visit_flag").alias("no_of_repeated_customers")
    ).orderBy("order_date")

# Show the final result
result_df.show()

# Stop the Spark session
spark.stop()

