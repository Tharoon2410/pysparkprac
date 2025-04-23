leetcode1285
#WITH cte AS (SELECT log_id,log_id - ROW_NUMBER()OVER(order by log_id) as diff from logs)
#SELECT MIN(log_id) AS start_id,MAX(log_id) AS end_id from cte group by diff order by start_id

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("LogIDDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("log_id", IntegerType(), True)
])

# Create data
data = [
    (1,),
    (2,),
    (3,),
    (7,),
    (8,),
    (10,)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()
winspec = Window.orderBy(log_id)
df = df.withColumn("row_num", row_number().over(winspec))
df = df.withColumn("diff",expr("log_id - row_num"))
grouped_df = df.groupBy("diff").agg(expr("min(log_id) as start_id"),expr("max(log_id) as end_id")).orderBy("start_id").select("start_id","end_id")
grouped_df.show()
df.createOrReplaceTempView("logs")
spark.sql("WITH cte AS (SELECT log_id,log_id - ROW_NUMBER()OVER(order by log_id) as diff from logs) \
SELECT MIN(log_id) AS start_id,MAX(log_id) AS end_id from cte group by diff order by start_id").show()

###############