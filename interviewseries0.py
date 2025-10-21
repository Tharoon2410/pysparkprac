from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession
spark = SparkSession.builder.master("local").appName("Book analysis").getOrCreate()

# Define schema for Books.csv
bookschema = StructType([
    StructField("bookid", IntegerType(), True),
    StructField("book", StringType(), True),
    StructField("Author", StringType(), True),
    StructField("Unit_price", IntegerType(), True)
])

# Define schema for sales.csv
saleschema = StructType([
    StructField("id", IntegerType(), True),
    StructField("unit", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Load CSVs
df1 = spark.read.format("csv").schema(bookschema).load("/content/sample_data/Books.csv")
df2 = spark.read.format("csv").schema(saleschema).load("/content/sample_data/sales.csv")
df1.show()
# Convert date string to date format
df2 = df2.withColumn("date", to_date(col("date"), "yy-MM-dd"))

# Join dataframes on bookid = id
join_df = df1.join(df2, df1.bookid == df2.id, "left")
join_df.show()
# Calculate total_sales and handle nulls
result_df = join_df.withColumn(
    "total_sales", coalesce(col("Unit_price") * col("unit"), lit(0))
).withColumn("date", to_date(lit("21-10-2025"), "dd-MM-yyyy"))


# Show final result
result_df.select("bookid", "book", "total_sales", "date").show()