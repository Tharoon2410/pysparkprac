from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Step 1: Initialize a Spark session
spark = SparkSession.builder \
    .appName("Word Count") \
    .getOrCreate()

# Step 2: Load data into a DataFrame
# Replace 'path/to/textfile.txt' with your actual file path
df = spark.read.text("path/to/textfile.txt")

# Step 3: Process the text to count words
word_counts = df.select(
    explode(split(col("value"), "\\s+")).alias("word")
).groupBy("word").count()

# Step 4: Show the results
word_counts.show(truncate=False)

# Optionally, you can save the results to a file
# word_counts.write.csv("path/to/output/word_counts.csv")

# Stop the Spark session
spark.stop()
