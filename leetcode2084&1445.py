from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('SalesData').getOrCreate()

# Define the schema for the data
schema = StructType([
    StructField("sale_date", StringType(), True),
    StructField("fruit", StringType(), True),
    StructField("sold_num", IntegerType(), True)
])

# Create a list of data (same as provided)
data = [
    ('2020-05-01', 'apples', 10),
    ('2020-05-01', 'oranges', 8),
    ('2020-05-02', 'apples', 15),
    ('2020-05-02', 'oranges', 15),
    ('2020-05-03', 'apples', 20),
    ('2020-05-03', 'oranges', 0),
    ('2020-05-04', 'apples', 15),
    ('2020-05-04', 'oranges', 16)
]

# Create the PySpark DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame to verify the data
df = df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
df.show()

laeddf = df.withColumn("orange_num", lead("sold_num",1).over(Window.partitionBy("sale_date").orderBy ("sale_date")))\
            .withColumn("apple_num",lag("sold_num",1).over(Window.partitionBy("sale_date").orderBy ("sale_date")))
laeddf.show()          
final_df = laeddf.filter(col("orange_num").isNotNull()).withColumn("diff",expr("sold_num - orange_num")).orderBy("sale_date").select("sale_date","diff")
final_df.show()

df.createOrReplaceTempView("sales")

spark.sql("""
WITH cte AS (
    SELECT * 
    FROM sales
),
cte2 AS (
    SELECT *,
           LEAD(SOLD_NUM, 1) OVER (PARTITION BY sale_date ORDER BY fruit) AS orange_num
    FROM cte
)
SELECT sale_date,
       (sold_num - orange_num) AS diff
FROM cte2
WHERE orange_num IS NOT NULL
ORDER BY sale_date
""").show()
#########################
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *  # Import all SQL functions
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("OrderTable").getOrCreate()

# Define the schema using StructType
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_type", IntegerType(), True)
])

# Data to insert into DataFrame
data = [
    (1, 1, 0),
    (2, 1, 0),
    (11, 2, 0),
    (12, 2, 1),
    (21, 3, 1),
    (22, 3, 0),
    (31, 4, 1),
    (32, 4, 1)
]

# Create a DataFrame using the data and schema
df = spark.createDataFrame(data, schema)

# Apply some SQL functions (for example, adding a new column with `withColumn`)
#df = df.withColumn("order_type_description", when(col("order_type") == 0, lit("Standard")).otherwise(lit("Express")))

# Show the DataFrame with the new column
df.show()

withdf = df.withColumn("min_order_type", min("order_type").over(Window.partitionBy("customer_id")))
resultdf = withdf.filter("order_type+min_order_type != 1").select("order_id","customer_id","order_type")
resultdf.show()
df.createOrReplaceTempView("orders")
spark.sql("WITH cte AS(SELECT *,MIN(order_type) over(partition by customer_id)as min_order_type from orders)select order_id,customer_id,order_type from cte where(order_type+min_order_type <> 1)").show()