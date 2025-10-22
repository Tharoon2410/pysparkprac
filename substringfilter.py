from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, substring, lower

# Start a Spark session
spark = SparkSession.builder.appName("CityVowelFilter").getOrCreate()

# Sample data
data = [
    ("Chicago",),
    ("Boston",),
    ("Seattle",),
    ("Miami",),
    ("Oslo",),
    ("Delhi",),
    ("New York",),
    ("Atlanta",),
    ("Toronto",),
]

# Create DataFrame
columns = ["city"]
df = spark.createDataFrame(data, columns)

# Show the sample data
df.show()

# Extract last character (case-insensitive) and filter if it's a vowel
vowels = ['a', 'e', 'i', 'o', 'u']

filtered_df = df \
    .filter(lower(substring(col("city"), length("city"), 1)).isin(vowels)) \
    .select("city") \
    .distinct()

# Show the result
filtered_df.show()
