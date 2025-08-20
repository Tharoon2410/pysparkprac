from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead, coalesce, lit

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (1, "2017-01-01", 10),
    (2, "2017-01-02", 109),
    (3, "2017-01-03", 150),
    (4, "2017-01-04", 99),
    (5, "2017-01-05", 145),
    (6, "2017-01-06", 1455),
    (7, "2017-01-07", 199),
    (8, "2017-01-09", 188)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "visit_date", "people"])

# Define window spec
w = Window.orderBy("id")

# Add lag and lead columns using coalesce with lit(0)
df_with_context = df.withColumn("previousday1", coalesce(lag("people", 1).over(w), lit(0))) \
                    .withColumn("previousday2", coalesce(lag("people", 2).over(w), lit(0))) \
                    .withColumn("nextday1", coalesce(lead("people", 1).over(w), lit(0))) \
                    .withColumn("nextday2", coalesce(lead("people", 2).over(w), lit(0)))

# Apply filter condition
result = df_with_context.filter(
    (col("people") > 100) & (
        ((col("nextday1") >= 100) & (col("nextday2") >= 100)) |
        ((col("previousday1") >= 100) & (col("previousday2") >= 100)) |
        ((col("previousday1") >= 100) & (col("nextday1") >= 100))
    )
).select("id", "visit_date", "people")

# Show result
result.show()
