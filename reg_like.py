from pyspark.sql import SparkSession
from pyspark.sql.functions import col,regexp_replace

SparkSession
spark = SparkSession.builder.appName("YourAppName").getOrCreate()
simpleData_1 = [(2,"Shivam","U6753679iy"),(1,"Sagar","7654367862"),(3,"Muni","89765415U7")]
columns=["ID","Student_Name","Mobile_no"]
df=spark.createDataFrame(data =simpleData_1,schema =columns)
df.show()
df.select("*").filter(col("Mobile_no").rlike('^[0-9]*$')).show()
df.createOrReplaceTempView("student")
spark.sql("select * from student where Mobile_no rlike('^[0-9]*$')").show()
spark.stop()