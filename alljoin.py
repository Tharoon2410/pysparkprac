from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark=SparkSession.builder.appName("jiontype").getOrCreate()
data1=[(1,"A",1000,"IT"),(2,"B",1500,"IT"),(3,"C",2500,"IT"),(4,"D",3000,"HR"),(5,"E",2000,"HR"),(6,"F",1000,"HR")
       ,(7,"G",4000,"Sales"),(8,"H",4000,"Sales"),(9,"I",1000,"Sales"),(10,"J",2000,"Sales")]
schema1=["EmpId","Empcode","Salary","DeptName"]
data2=[(1,'ganesh','icici'),(1,'ganesh','sbi'),(2,'karthik','HDFC'),(2,'karthi','Axisbank'),(3,'shanthi','standardchatered'),(3,'shanthi','TMBbank'),(4,'shiva','Amex'),(4,'shiva','canarabank'),(5,'muruganantham','PNB'),(5,'muruganantham','bankofindia'),(6,'saraswathi','Indianoverseasbank'),(6,'saraswathi','Yesbank')]
schema2=["id","EmpName","Bankaccount"]
df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)
df1.show()
df2.show()
ljion=df1.join(df2,df1.EmpId == df2.id,how:"left")
rjion=df1.join(df2,df1.EmpId == df2.id,how:"right")
ijion=df1.join(df2,df1.EmpId == df2.id,how:"inner")
ojion=df1.join(df2,df1.EmpId == df2.id,how:"outer")
print('ljoin')
ljion.show()
print('rjoin')
rjion.show()
print('ijoin')
ijion.show()
print('ojoin')
ojion.show()

spark.stop()