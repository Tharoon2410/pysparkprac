from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("ConsecutiveSeats").getOrCreate()

# Sample data (replace with actual data)
data = [
    (1, "N"), (2, "Y"), (3, "N"), (4, "Y"), (5, "Y"), (6, "Y"), (7, "N"), (8, "Y"),(9,"Y"),(10,"Y"),(11,"Y"),(12,"N"),(13,"Y"),(14,"Y")
]
columns = ["seat_no", "empty_status"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Define window specification
window_spec = Window.orderBy("seat_no")

# Add previous and next statuses using lag and lead functions
df = df.withColumn("prev_status_1", lag("empty_status", 1).over(window_spec)) \
       .withColumn("prev_status_2", lag("empty_status", 2).over(window_spec)) \
       .withColumn("next_status_1", lead("empty_status", 1).over(window_spec)) \
       .withColumn("next_status_2", lead("empty_status", 2).over(window_spec))
df.show()
# Filter rows based on the specified conditions
result_df = df.filter(
    ((col("empty_status") == "Y") & (col("prev_status_1") == "Y") & (col("prev_status_2") == "Y")) |
    ((col("empty_status") == "Y") & (col("next_status_1") == "Y") & (col("next_status_2") == "Y")) |
    ((col("empty_status") == "Y") & (col("prev_status_1") == "Y") & (col("next_status_1") == "Y"))
)

# Show the result
result_df.select("seat_no", "empty_status").show()