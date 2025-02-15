from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sequence, lit, date_format, sum as _sum, to_date, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("RecursiveCTEtoPySpark").getOrCreate()

# Define schema for the data (period_start and period_end as StringType)
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("period_start", StringType(), True),  # Keep as StringType for now
    StructField("period_end", StringType(), True),    # Keep as StringType for now
    StructField("average_daily_sales", IntegerType(), True)
])

# Create a DataFrame from the provided data (dates as strings)
data = [
    (1, '2019-01-25', '2019-02-28', 100),
    (2, '2018-12-01', '2020-01-01', 10),
    (3, '2019-12-01', '2020-01-31', 1)
]

# Create the DataFrame with the initial data (dates as strings)
df = spark.createDataFrame(data, schema=schema)

# Convert 'period_start' and 'period_end' to DateType using to_date function
df = df.withColumn("period_start", to_date(col("period_start"), "yyyy-MM-dd")) \
       .withColumn("period_end", to_date(col("period_end"), "yyyy-MM-dd"))
df.show()

# Create a new DataFrame to generate a date range from period_start to period_end for each product_id
date_range_df = df.withColumn(
    "date_range", 
    explode(sequence(col("period_start"), col("period_end"), expr("interval 1 day"))) # Use expr to define the interval
)
date_range_df.show()
# Now, select the necessary fields and calculate the total sales for each product per year
result_df = date_range_df.withColumn("report_year", date_format(col("date_range"), 'yyyy')) \
    .groupBy("product_id", "report_year") \
    .agg(_sum("average_daily_sales").alias("product_sale")) \
    .orderBy("product_id", "report_year", "product_sale")

# Show the result
result_df.show()