from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder \
        .master("local") \
        .appName("Hands-on PySpark on Google Colab") \
        .getOrCreate()

# Create a DataFrame
first_df = spark.read.format("json").option("multiline", "true").load("/content/address.json")
first_df.show()
first_df.printSchema()

# Flatten the 'billing_address' struct and select required columns
flatten_df = first_df.select(
    col("age"), 
    col("billing_address.address").alias("billing_address"),
    col("billing_address.city").alias("billing_city"),
    col("billing_address.postal_code").alias("billing_postal_code"),
    col("billing_address.state").alias("billing_state"),
    col("date_of_birth"),
    col("email_address"),
    col("first_name"),
    col("height_cm"),
    col("is_alive"),
    col("last_name"),
    col("phone_numbers"),
    col("shipping_address.address").alias("shipping_address"),
    col("shipping_address.city").alias("shipping_city"),
    col("shipping_address.postal_code").alias("shipping_postal_code"),
    col("shipping_address.state").alias("shipping_state")
)

flatten_df.show()
flatten_df.printSchema()

# Explode the 'phone_numbers' array
flatten_df1 = flatten_df.withColumn("phone_numbers", explode(col("phone_numbers"))) \
                         .select(col("*"), 
                                 col("phone_numbers.type").alias("phone_type"), 
                                 col("phone_numbers.number").alias("phone_number")) \
                         .drop("phone_numbers")

flatten_df1.show()
flatten_df1.printSchema()