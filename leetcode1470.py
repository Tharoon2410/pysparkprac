from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("UserRides").getOrCreate()

# Sample data for Users table
users_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Alex"),
    (4, "Donald"),
    (7, "Lee"),
    (13, "Jonathan"),
    (19, "Elvis")
]

# Sample data for Rides table
rides_data = [
    (1, 1, 120),
    (2, 2, 317),
    (3, 3, 222),
    (4, 7, 100),
    (5, 13, 312),
    (6, 19, 50),
    (7, 7, 120),
    (8, 19, 400),
    (9, 7, 230)
]

# Define column names
users_columns = ["id", "name"]
rides_columns = ["id", "user_id", "distance"]

# Create DataFrames
users_df = spark.createDataFrame(users_data, users_columns)
rides_df = spark.createDataFrame(rides_data, rides_columns)

# Perform LEFT JOIN equivalent
result_df = users_df.join(
    rides_df, users_df["id"] == rides_df["user_id"], "left"
).groupBy(users_df["name"]).agg(
    coalesce(sum(rides_df["distance"]), lit(0)).alias("travelled_distance")
).orderBy(col("travelled_distance").desc(), col("name").asc())

# Show result
result_df.show()
users_df.createOrReplaceTempView("Users")
rides_df.createOrReplaceTempView("Rides")
spark.sql("""SELECT a.name, COALESCE(SUM(b.distance), 0) AS travelled_distance
FROM Users AS a
LEFT JOIN Rides AS b
ON a.id = b.user_id
GROUP BY a.name
ORDER BY travelled_distance DESC, name ASC""").show()