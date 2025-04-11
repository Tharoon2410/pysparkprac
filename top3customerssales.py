from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("TopCustomers2024") \
    .getOrCreate()

# 2. Load CSV file
df = spark.read.option("header", True).csv("/content/sample_data/sales.csv")

# 3. Cast data types
df = df.withColumn("saleamount", col("saleamount").cast("double")) \
       .withColumn("saledate", col("saledate").cast("date"))

# 4. Filter records from 2024
df_2024 = df.filter(year(col("saledate")) == 2024)

# 5. Aggregate total sales by customer
customer_sales = df_2024.groupBy("customerid") \
    .agg(sum("saleamount").alias("total_sales"))

# 6. Get top 3 customers by total sales
top_3_customers = customer_sales.orderBy(col("total_sales").desc()).limit(3)

# 7. Show result
top_3_customers.show()
