-- Write your MySQL query statement below
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("SessionData").getOrCreate()

# Define schema and create DataFrame
schema = StructType([
    StructField("session_id", IntegerType(), True),
    StructField("duration", IntegerType(), True)
])

data = [
    (1, 30),
    (2, 199),
    (3, 299),
    (4, 580),
    (5, 1000)
]

df = spark.createDataFrame(data, schema=schema)

# Apply binning logic
df = df.withColumn("bin", expr("""
    CASE
        WHEN duration BETWEEN 0 AND 299 THEN '[0-5>'
        WHEN duration BETWEEN 300 AND 599 THEN '[5-10>'
        WHEN duration BETWEEN 600 AND 899 THEN '[10-15>'
        ELSE '15 or more'
    END
"""))

# Create binning reference DataFrame
bin_data = [("[0-5>"), ("[5-10>"), ("[10-15>"), ("15 or more")]
bin_df = spark.createDataFrame(bin_data, StringType()).toDF("bin")

# Perform left join with count aggregation
agg_df = df.groupBy("bin").agg(count("session_id").alias("Total"))
result_df = bin_df.join(agg_df, "bin", "left").fillna({"Total": 0})

# Show result
result_df.show()

spark.sql("""with cte(SELECT *, CASE 
    WHEN duration BETWEEN 0 AND 299 THEN '[0-5>'
    WHEN duration BETWEEN 300 AND 599 THEN '[5-10>'
    WHEN duration BETWEEN 600 AND 899 THEN '[10-15>'
    ELSE '15 or more' 
END AS bin
FROM Sessions),
cte2 as(SELECT '[0-5>' AS bin
UNION
SELECT '[5-10>'
UNION
SELECT '[10-15>'
UNION
SELECT '15 or more')

select cte2.bin,CASE WHEN W.c IS NOT NULL THEN W.c else END as Total from cte2 left join (select bin,count(session_id) AS c from GROUP BY bin)W ON cte2.bin =W.bin""").show()
