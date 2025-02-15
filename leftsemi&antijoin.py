from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .master("local") \
        .appName("Hands-on PySpark on Google Colab") \
        .getOrCreate()
data1 = [(1,'maheer',2000,2),(2,'wafa',3000,1),(3,'abcd',1000,4),(5,'tofa',9000,5)]
schema1=['id','name','salary','dep']
data2 = [(1,'IT'),(2,'HR'),(3,'Finance'),(4,'Sales')]
schema2=['id','name']
df1 = spark.createDataFrame(data=data1,schema=schema1)
df2 = spark.createDataFrame(data=data2,schema=schema2)
df1.show()
df2.show()
lsj = df1.join(df2,df1.dep==df2.id,"leftsemi")
lsj.show()
laj = df1.join(df2,df1.dep==df2.id,"leftanti")
laj.show()