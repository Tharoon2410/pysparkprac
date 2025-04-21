from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder.appName("OrdersItemsDF").getOrCreate()

# ---------------------
# Define schema for Orders table
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

# Data for Orders table
orders_data = [
    (1, 1, "2020-06-01", "1", 10),
    (2, 1, "2020-06-08", "2", 10),
    (3, 2, "2020-06-02", "1", 5),
    (4, 3, "2020-06-03", "3", 5),
    (5, 4, "2020-06-04", "4", 1),
    (6, 4, "2020-06-05", "5", 5),
    (7, 5, "2020-06-05", "1", 10),
    (8, 5, "2020-06-14", "4", 5),
    (9, 5, "2020-06-21", "3", 5)
]

# Create Orders DataFrame
df1 = spark.createDataFrame(data=orders_data, schema=orders_schema)
df1 = df1.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# ---------------------
# Define schema for Items table
items_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("item_category", StringType(), True)
])

# Data for Items table
items_data = [
    ("1", "LC Alg. Book", "Book"),
    ("2", "LC DB. Book", "Book"),
    ("3", "LC SmarthPhone", "Phone"),
    ("4", "LC Phone 2020", "Phone"),
    ("5", "LC SmartGlass", "Glasses"),
    ("6", "LC T-Shirt XL", "T-Shirt")
]

# Create Items DataFrame
df2 = spark.createDataFrame(data=items_data, schema=items_schema)

# ---------------------
# Show DataFrames
df1.show()
df2.show()

joindf = df1.join(df2,df1.item_id == df2.item_id,"Right")
resultdf = joindf.groupBy("item_category").agg(
expr("sum(CASE WHEN dayofweek(order_date) = 2 THEN quantity ELSE 0 END) AS Monday"),
expr("SUM(CASE WHEN dayofweek(order_date) = 3 THEN quantity ELSE 0 END) AS Tuesday"),
expr("SUM(CASE WHEN dayofweek(order_date) = 4 THEN quantity ELSE 0 END) AS Wednesday"),
expr("SUM(CASE WHEN dayofweek(order_date) = 5 THEN quantity ELSE 0 END) AS Thursday"),
expr("SUM(CASE WHEN dayofweek(order_date) = 6 THEN quantity ELSE 0 END) AS Friday"),
expr("SUM(CASE WHEN dayofweek(order_date) = 7 THEN quantity ELSE 0 END) AS Saturday"),
expr("SUM(CASE WHEN dayofweek(order_date) = 1 THEN quantity ELSE 0 END) AS Sunday")
).orderBy("item_category")
#resultdf.show()
df1.createOrReplaceTempView("orders")
df2.createOrReplaceTempView("items")
spark.sql("""SELECT i.item_category AS Category,
SUM(CASE WHEN DAYOFWEEK(o.order_date) = 2 THEN o.quantity ELSE 0 END) AS Monday,
SUM(CASE WHEN DAYOFWEEK(o.order_date) = 3 THEN o.quantity ELSE 0 END) AS Tuesday,
SUM(CASE WHEN DAYOFWEEK(o.order_date) = 4 THEN o.quantity ELSE 0 END) AS Wednesday,
SUM(CASE WHEN DAYOFWEEK(o.order_date) = 5 THEN o.quantity ELSE 0 END) AS Thursday,
SUM(CASE WHEN DAYOFWEEK(o.order_date) = 6 THEN o.quantity ELSE 0 END) AS Friday,
SUM(CASE WHEN DAYOFWEEK(o.order_date) = 7 THEN o.quantity ELSE 0 END) AS Saturday,
SUM(CASE WHEN DAYOFWEEK(o.order_date) = 1 THEN o.quantity ELSE 0 END) AS Sunday
FROM Items i
LEFT JOIN Orders o
ON i.item_id = o.item_id
GROUP BY i.item_category
ORDER BY Category""").show()