from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, sum, countDistinct, when, to_date, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType  # Import necessary types

# Initialize Spark session
spark = SparkSession.builder.appName("SpendAnalysis").getOrCreate()

# Sample data
data = [
    (1, '2019-07-01', 'mobile', 100),
    (1, '2019-07-01', 'desktop', 100),
    (2, '2019-07-01', 'mobile', 100),
    (2, '2019-07-02', 'mobile', 100),
    (3, '2019-07-01', 'desktop', 100),
    (3, '2019-07-02', 'desktop', 100)
]

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("spend_date", StringType(), True),  # spend_date is initially StringType
    StructField("platform", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create DataFrame
spending_df = spark.createDataFrame(data, schema)

# Convert 'spend_date' to DateType after DataFrame creation
spending_df = spending_df.withColumn("spend_date", to_date("spend_date", "yyyy-MM-dd"))

# Show the initial DataFrame
spending_df.show()

# Step 1: First part of the UNION ALL (COUNT DISTINCT platform = 1)
part1_df = spending_df.groupBy("spend_date", "user_id").agg(
    max("platform").alias("platform"),
    sum("amount").alias("amount"),
    countDistinct("platform").alias("platform_count")
).filter(
    col("platform_count") == 1
).drop("platform_count")  # Drop the count column after filtering

# Step 2: Second part of the UNION ALL (COUNT DISTINCT platform = 2)
part2_df = spending_df.groupBy("spend_date", "user_id").agg(
    lit("both").alias("platform"),
    sum("amount").alias("amount"),
    countDistinct("platform").alias("platform_count")
).filter(
    col("platform_count") == 2
).drop("platform_count")  # Drop the count column after filtering

# Step 3: Third part of the UNION ALL (Distinct spend_date with NULL user_id and platform = 'both')
part3_df = spending_df.select(
    "spend_date",
    lit(None).cast("integer").alias("user_id"),
    lit("both").alias("platform"),
    lit(0).alias("amount")
).distinct()

# Step 4: Combine all parts using union
all_spend_df = part1_df.union(part2_df).union(part3_df)

# Step 5: Final aggregation (GROUP BY spend_date, platform)
final_result_df = all_spend_df.groupBy("spend_date", "platform").agg(
    sum("amount").alias("total_amount"),
    countDistinct("user_id").alias("total_users")
).orderBy("spend_date", "platform", ascending=[True, False])

# Show the final result
final_result_df.show()
