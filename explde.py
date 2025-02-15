from pyspark.sql.functions import col,explode

simpleData = [(1,["Sagar","Prajapati"]),(2,["Shivam","Gupta"]),(3,["kunal","Verma"]),(4,["Kim"])]
columns=["ID","NAME"]
df1=spark.createDataFrame(data =simpleData,schema =columns)
df1.show()
df_output=df1.select(col("ID"),explode(col("NAME")))
df_output.show()

spark.stop()