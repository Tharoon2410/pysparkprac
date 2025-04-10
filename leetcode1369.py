from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('useractivity').getOrCreate()

schema = StructType([
    StructField("username", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("startDate", StringType(), True),
    StructField("endDate", StringType(), True)
])


df = spark.read.format("csv") \
    .schema(schema) \
    .option("delimiter", "|") \
    .load("/content/sample_data/useractivity.csv")


df = df.withColumn("startDate", to_date("startDate", "yyyy-MM-dd")) \
       .withColumn("endDate", to_date("endDate", "yyyy-MM-dd"))


winspec = Window.partitionBy("username").orderBy(col("startDate").desc())


rkdf = df.withColumn("rk", rank().over(winspec)) \
         .withColumn("cnt", count("*").over(Window.partitionBy("username")))

finaldf = rkdf.filter((col("rk") == 2) | (col("cnt") == 1)) \
              .select("username", "activity", "startDate", "endDate")

finaldf.show()
df.createOrReplaceTempView("useractivity")
spark.sql("SELECT username,activity,startdate,enddate FROM (SELECT *,RANK() OVER (PARTITION BY username ORDER BY startdate DESC) AS rk,\
COUNT(username) OVER (PARTITION BY username) AS cnt \
FROM UserActivity) AS a \
WHERE a.rk = 2 OR a.cnt = 1").show()


