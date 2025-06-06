#leetcode 1571#
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *
#from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("WarehouseProducts").getOrCreate()

# Warehouse data
warehouse_data = [
    ("LCHouse1", 1, 1),
    ("LCHouse1", 2, 10),
    ("LCHouse1", 3, 5),
    ("LCHouse2", 1, 2),
    ("LCHouse2", 2, 2),
    ("LCHouse3", 4, 1)
]
warehouse_columns = ["name", "product_id", "units"]
warehouse_df = spark.createDataFrame(warehouse_data, warehouse_columns)

# Products data
products_data = [
    (1, "LC-TV", 5, 50, 40),
    (2, "LC-KeyChain", 5, 5, 5),
    (3, "LC-Phone", 2, 10, 10),
    (4, "LC-T-Shirt", 4, 10, 20)
]
products_columns = ["product_id", "product_name", "Width", "Length", "Height"]
products_df = spark.createDataFrame(products_data, products_columns)

# Show the data
warehouse_df.show()
products_df.show()

join_df = warehouse_df.join(products_df,"product_id", "inner")
final_df = join_df.groupBy("name").agg(sum(col("units")*col("width")*col("length")*col("height")).alias("volume")).selectExpr("name as warehousename","volume").orderBy("warehousename")
final_df.show()
warehouse_df.createOrReplaceTempview("warehouse")
products_df.createOrReplaceTempview("product")
spark.sql("""SELECT w.name AS warehouse_name,sum(w.units*p.Width*p.Length*p.Height) from Warehouse w INNER JOIN Products p ON w.product_id = p.product_id group by w.name""").show()
