##leetcode1294##
##select c.country_name,CASE WHEN AVG(w.weather_state)<=15 THEN 'Cold' WHEN AVG(w.weather_state)>=25 THEN 'Hot' ELSE 'warm' END AS weather_type FROM Weather w LEFT JOIN Countries c ON w.country_id = c.country_id where month(day) = 11 and Year (day) = 2019 group by c.country name##
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("Create DataFrames").getOrCreate()

# ---- Define schema and data for Countries (df1) ----
countries_schema = StructType([
    StructField("country_id", IntegerType(), True),
    StructField("country_name", StringType(), True)
])

countries_data = [
    (2, "USA"),
    (3, "Australia"),
    (7, "Peru"),
    (5, "China"),
    (8, "Morocco"),
    (9, "Spain")
]

df1 = spark.createDataFrame(data=countries_data, schema=countries_schema)

# ---- Define schema and data for Weather (df2) ----
weather_schema = StructType([
    StructField("country_id", IntegerType(), True),
    StructField("weather_state", IntegerType(), True),
    StructField("day", StringType(), True)  # can also use DateType with parsing
])

weather_data = [
    (2, 15, "2019-11-01"),
    (2, 12, "2019-10-28"),
    (2, 12, "2019-10-27"),
    (3, -2, "2019-11-10"),
    (3, 0, "2019-11-11"),
    (3, 3, "2019-11-12"),
    (5, 16, "2019-11-07"),
    (5, 18, "2019-11-09"),
    (5, 21, "2019-11-23"),
    (7, 25, "2019-11-28"),
    (7, 22, "2019-12-01"),
    (7, 20, "2019-12-02"),
    (8, 25, "2019-11-05"),
    (8, 27, "2019-11-15"),
    (8, 31, "2019-11-25"),
    (9, 7, "2019-10-23"),
    (9, 3, "2019-12-23")
]

df2 = spark.createDataFrame(data=weather_data, schema=weather_schema)

# Optional: convert day to DateType
df2 = df2.withColumn("day", to_date("day", "yyyy-MM-dd"))

# Show the dataframes
df1.show()
df2.show()

joined_df = df1.join(df2,df2.country_id == df1.country_id,"right")

filter_df = joined_df.filter((month("day") == 11)&(year("day") == 2019))

result_df = filter_df.groupBy("country_name").agg(avg("weather_state").alias("avg_temp")).withColumn("weather_type",expr("""CASE WHEN avg_temp <= 15 THEN 'Cold' WHEN avg_temp >= 25 THEN 'Hot' ELSE 'warm' END""")).orderBy("weather_type").select("country_name","weather_type")
result_df.show()
df1.createOrReplaceTempView("countries")
df2.createOrReplaceTempView("weather")
spark.sql("""select c.country_name,CASE WHEN AVG(w.weather_state)<=15 THEN 'Cold' WHEN AVG(w.weather_state)>=25 THEN 'Hot' ELSE 'warm' END AS weather_type FROM Weather w LEFT JOIN Countries c ON w.country_id = c.country_id where month(day) = 11 and Year (day) = 2019 group by c.country name""").show() 