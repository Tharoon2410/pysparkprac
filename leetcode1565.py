#leetcode1565#
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, countDistinct, count

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV (adjust path if needed)
df = spark.read.option("header", True).option("inferSchema", True).csv("file:///D:/sampledata/leetcode1565.csv")

# Filter where invoice > 20
filtered_df = df.filter(df.invoice > 20)

# Group by month (formatting order_date to yyyy-MM)
result_df = filtered_df.groupBy(date_format("order_date", "yyyy-MM").alias("month")) \
    .agg(
        count("order_id").alias("order_count"),
        countDistinct("customer_id").alias("customer_count")
    ).orderBy("month")

# Show result
result_df.show()
