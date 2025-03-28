from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Example").getOrCreate()

# Define the schema
schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("subject", StringType(), True)
])

# Create data
data = [
        (1, "Spark"),
        (1, "Scala"),
        (1, "Hive"),
        (2, "Scala"),
        (3, "Spark"),
        (3, "Scala")
]

# Create DataFrame
df = spark.createDataFrame(data, schema).coalesce(1)

# Show the DataFrame
df.show()


colagg = df.groupBy("id").agg(collect_list("subject").alias("subject"))
colagg.show()


colagg.printSchema()


strcon = colagg.withColumn("subject",expr("cast(subject as string)"))
strcon.show()
strcon.printSchema()


repdata = strcon.withColumn("subject",expr("replace(subject,'[','')")).withColumn("subject",expr("replace(subject,']','')"))
repdata.show()
repdata.printSchema()