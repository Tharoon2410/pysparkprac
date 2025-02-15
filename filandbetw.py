column.between(lower_bound, upper_bound)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("BetweenOperatorExample").getOrCreate()

# Sample DataFrame
data = [("John", 30000), ("Alice", 40000), ("Bob", 60000)]
columns = ["name", "salary"]
df = spark.createDataFrame(data, columns)

# Filter the DataFrame using the between operator
filtered_df = df.filter(col("salary").between(30000, 50000))

filtered_df.show()
===========================
from pyspark.sql import functions as F

# Sample data with dates
data = [("John", "2023-01-10"), ("Alice", "2023-02-15"), ("Bob", "2023-03-01")]
columns = ["name", "date"]
df = spark.createDataFrame(data, columns)

# Convert the date column to a date type
df = df.withColumn("date", F.to_date(df["date"], "yyyy-MM-dd"))

# Specify the date range
start_date = "2023-01-01"
end_date = "2023-02-28"

# Filter the DataFrame using the between operator for dates
filtered_df = df.filter(col("date").between(start_date, end_date))

filtered_df.show()
