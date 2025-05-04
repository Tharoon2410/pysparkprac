from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Step 1: Start SparkSession
spark = SparkSession.builder.appName("ScoresExample").getOrCreate()

# Step 2: Define Schema
schema = StructType([
    StructField("player_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("day", StringType(), True),  # we'll convert to DateType
    StructField("score_points", IntegerType(), True)
])

# Step 3: Create data
data = [
    ("Aron", "F", "2020-01-01", 17),
    ("Alice", "F", "2020-01-07", 23),
    ("Bajrang", "M", "2020-01-07", 7),
    ("Khali", "M", "2019-12-25", 11),
    ("Slaman", "M", "2019-12-30", 13),
    ("Joe", "M", "2019-12-31", 3),
    ("Jose", "M", "2019-12-18", 2),
    ("Priya", "F", "2019-12-31", 23),
    ("Priyanka", "F", "2019-12-30", 17)
]

# Step 4: Create DataFrame with the schema
df = spark.createDataFrame(data, schema=schema)

# Step 5: Convert 'day' to DateType
df = df.withColumn("day", to_date(col("day"), "yyyy-MM-dd"))

# Step 6: Show DataFrame
df.show()
#winspec = Window.partitionBy("gender").orderBy("day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
winspec = Window.partitionBy("gender").orderBy("day")
sum_df = df.withColumn("total",sum("score_points").over(winspec))
final_df = sum_df.select("gender","day","total")
final_df.show()
df.createOrReplaceTempView("scores")
spark.sql("""select gender,day,sum(score_points)over(PARTITION BY gender ORDER BY gender,day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total from scores""").show()