from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark =SparkSession.builder.appName('sparkpivotby').getOrCreate()
data= [
('Rudra','math',79),
('Rudra','eng',60),
('Shivu','math',68),
('Shivu','eng',68),
('Anu','math',65),
('Anu','eng',80)
]
schema="Name","Sub","Marks"
df = spark.createDataFrame(data,schema)
df.show()
#dfmethod1
pivoted_df = df.groupBy("Name").pivot("Sub",['math','eng']).agg(max("marks"))
result_df = pivoted_df.select("Name","math","eng")
result_df.show()
#query
print('query')
df.createOrReplaceTempView('df')
spark.sql("select Name,math,eng FROM (SELECT * FROM df PIVOT(MAX(Marks)FOR sub IN ('math','eng')))").show()
#dfmethod2
df_output = df.groupBy(col("Name")).agg(collect_list(col("marks")).alias("marks_new"))
df_output1= df_output.select(col("Name"),col("marks_new")[0].alias('math'),col("marks_new")[1].alias('eng'))
df_output1.show()
spark.stop