from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, regexp_replace, split
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TopWords") \
    .getOrCreate()

# Load the text file into a DataFrame
input_file = "path/to/your/textfile.txt"
data = spark.read.text(input_file)

# Define a list of common words to ignore
common_words = {'the', 'a', 'is', 'in', 'and', 'of', 'to', 'it', 'that', 'this'}

# Process the text data
words = data.select(explode(split(lower(regexp_replace(col("value"), "[^a-zA-Z\\s]", "")))).alias("word"))

# Filter out common words and empty strings
filtered_words = words.filter((col("word") != "") & (~col("word").isin(common_words)))

# Count occurrences of each word
word_counts = filtered_words.groupBy("word").count()

# Get the top 10 most frequent words
top_words = word_counts.orderBy(col("count").desc()).limit(10)

# Show the result
top_words.show()

# Stop the Spark session
spark.stop()