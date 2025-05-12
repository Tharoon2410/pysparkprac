##SELECT p.product_id,ROUND(SUM(p.price*u.units)/SUM(u.units),2)AS average_price from prices p inner join UnitsSold u on p.product_id = u.product_id where u.purchase_date BETWEEN p.start_date AND p.end_date GROUP BY p.product_id##

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("PriceSalesData").getOrCreate()

# Define schema for Prices table
prices_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Prices data
prices_data = [
    (1, "2019-02-17", "2019-02-28", 5),
    (1, "2019-03-01", "2019-03-22", 20),
    (2, "2019-02-01", "2019-02-20", 15),
    (2, "2019-02-21", "2019-03-31", 30)
]

# Create Prices DataFrame
prices_df = spark.createDataFrame(
    data=prices_data,
    schema=prices_schema
)
prices_df = prices_df.withColumn("start_date", to_date("start_date"))\
    .withColumn("end_date", to_date("end_date"))


# Define schema for UnitsSold table
units_sold_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("purchase_date", StringType(), True),
    StructField("units", IntegerType(), True)
])

# UnitsSold data
units_sold_data = [
    (1, "2019-02-25", 100),
    (1, "2019-03-01", 15),
    (2, "2019-02-10", 200),
    (2, "2019-03-22", 30)
]

# Create UnitsSold DataFrame
units_sold_df = spark.createDataFrame(
    data=units_sold_data,
    schema=units_sold_schema
)
units_sold_df = units_sold_df.withColumn("purchase_date",to_date("purchase_date"))
# Show the data
prices_df.show()
units_sold_df.show()

# Join Prices and UnitsSold on product_id and where purchase_date is between start_date and end_date
joined_df = prices_df.join(
    units_sold_df,
    (prices_df.product_id == units_sold_df.product_id) &
    (units_sold_df.purchase_date.between(prices_df.start_date, prices_df.end_date)),
    "inner"
)

# Calculate average price
result_df = joined_df.groupBy(prices_df.product_id).agg(
    round(sum(prices_df.price * units_sold_df.units) / sum(units_sold_df.units), 2).alias("average_price")
)
result_df.show()
