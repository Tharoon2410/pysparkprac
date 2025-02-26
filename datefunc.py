from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("Date Functions Example").getOrCreate()

# Create a sample DataFrame with a string date column
data = [("2025-01-20",), ("2025-02-15",), ("2025-03-10",)]
columns = ["date_string"]

# Creating the DataFrame
df = spark.createDataFrame(data, columns)

# Convert the string column to DateType using to_date
df = df.withColumn("date", to_date(col("date_string"), "yyyy-MM-dd"))

# Adding new columns to demonstrate various date functions

df = df.withColumn("current_date", current_date()) \
    .withColumn("current_timestamp", current_timestamp()) \
    .withColumn("date_plus_5", date_add(col("date"), 5)) \
    .withColumn("date_minus_5", date_sub(col("date"), 5)) \
    .withColumn("days_diff", datediff(col("date"), lit("2025-01-15"))) \
    .withColumn("date_plus_3_months", add_months(col("date"), 3)) \
    .withColumn("month", month(col("date"))) \
    .withColumn("year", year(col("date"))) \
    .withColumn("day_of_month", dayofmonth(col("date"))) \
    .withColumn("day_of_week", dayofweek(col("date"))) \
    .withColumn("hour", hour(current_timestamp())) \
    .withColumn("minute", minute(current_timestamp())) \
    .withColumn("second", second(current_timestamp())) \
    .withColumn("truncated_month", trunc(col("date"), "MM")) \
    .withColumn("last_day_of_month", last_day(col("date"))) \
    .withColumn("unix_time", unix_timestamp(col("date_string"), "yyyy-MM-dd")) \
    .withColumn("from_unixtime", from_unixtime(col("unix_time")))

# Show the resulting DataFrame
df.show(truncate=False)