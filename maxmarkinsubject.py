from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("studentmax_marksinsub") \
    .getOrCreate()

# Define the data as a list of tuples
data = [('A','X',75),('A','X',75),('A','Z',80),('B','X',90),('B','Y',91),('B','Z',75)]

schema = StructType([
    StructField("sname", StringType(), True),
    StructField("sid", StringType(), True),
    StructField("marks", IntegerType(), True)
])

df = spark.createDataFrame(data,schema)
df.show()

rndf = df.withColumn('rn',row_number().over(Window.partitionBy('sname').orderBy(col('marks').desc())))
sumdf = rndf.filter(col('rn').isin('1','2')).groupBy('sname').agg(sum('marks').alias('totalmarks'))
findf = sumdf.select('sname','totalmarks')
findf.show()