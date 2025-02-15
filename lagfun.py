from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# create the input dataframe
data = [
    ("2020-06-01","Won"),
    ("2020-06-02","Won"),
    ("2020-06-03","Won"),
    ("2020-06-04","Lost"),
    ("2020-06-05","Lost"),
    ("2020-06-06","Lost"),
    ("2020-06-07","Won")
]
df =spark.createDataFrame(data,["event_date","event_status"])
df.show()
#convert the event_to date type
df1=df.withColumn("event_date",to_date(col("event_date")))
df1.display()
#
df2=df.withColumn("event_change",when(col("event_status") != lag("event_status").over(Window.orderBy("event_date")),1).otherwise(0))
df2.display()
df3=df2.withColumn("event_group",sum("event_change").over(Window.orderBy("event_date")))
df3.display()
output_df=df3.groupBy("event_group","event_status").agg(first("event_date").alias("start_date"),last("event_date").alias("end_date")).drop("event_group")
output_df.display()