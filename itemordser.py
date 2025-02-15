from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Create a DataFrame for the orders table and compute the rank
orders_df = spark.table("prac1.orders")

# Define a window specification to partition by 'seller_id' and order by 'order_date'
window_spec = Window.partitionBy("seller_id").orderBy("order_date")

# Compute the rank and create a new DataFrame with 'rnk' column
ranked_orders_df = orders_df.withColumn("rnk", F.rank().over(window_spec))

# Step 2: Create DataFrame for the users table
users_df = spark.table("users")

# Step 3: Create DataFrame for the items table
items_df = spark.table("prac1.items")

# Step 4: Perform the join operations
result_df = (users_df
    .join(ranked_orders_df, (ranked_orders_df.seller_id == users_df.user_id) & (ranked_orders_df.rnk == 2), "left")
    .join(items_df, items_df.item_id == ranked_orders_df.item_id, "left")
)

# Step 5: Use 'when' and 'otherwise' to apply the CASE logic (item_fav_brand)
result_df = result_df.withColumn(
    "item_fav_brand", 
    F.when(items_df.item_brand == users_df.favorite_brand, "Yes").otherwise("No")
)

# Step 6: Select the required columns
final_result_df = result_df.select("user_id", "item_fav_brand")

# Show the final result
final_result_df.show()

===========================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("itemordser").getOrCreate()
# Step 1: Create a DataFrame for the orders table and compute the rank
orders_df = spark.read.csv("/content/orders1.csv", header=True, inferSchema=True)
orders_df.show()
# Define a window specification to partition by 'seller_id' and order by 'order_date'
window_spec = Window.partitionBy("seller_id").orderBy("order_date")

# Compute the rank and create a new DataFrame with 'rnk' column
ranked_orders_df = orders_df.withColumn("rnk", F.rank().over(window_spec))
# Step 2: Create DataFrame for the users table
users_df = spark.read.csv("/content/users.csv",header=True,inferSchema=True)
users_df.show()
# Step 3: Create DataFrame for the items table
items_df = spark.read.csv("/content/items.csv",header=True,inferSchema=True)
items_df.show()
# Step 4: Perform the join operations
result_df = (users_df.join(ranked_orders_df, 
                           (ranked_orders_df.seller_id == users_df.user_id) & (ranked_orders_df.rnk == 2), # Combine join conditions using logical AND
                           "left")
              .join(items_df, items_df.item_id == ranked_orders_df.item_id, "left"))  
result_df.show()
# Step 5: Use 'when' and 'otherwise' to apply the CASE logic (item_fav_brand)
result_df = result_df.withColumn(
    "item_fav_brand", 
    F.when(items_df.item_brand == users_df.favorite_brand, "Yes").otherwise("No")
)

# Step 6: Select the required columns
final_result_df = result_df.select("user_id", "item_fav_brand")

# Show the final result
final_result_df.show()

****************************************************
from pyspark.sql import SparkSession
from pyspark.sql.functions import *  # Import all functions from pyspark.sql.functions
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("itemordser").getOrCreate()

# Step 1: Create a DataFrame for the orders table and compute the rank
orders_df = spark.read.csv("/content/orders1.csv", header=True, inferSchema=True)
orders_df.show()

# Define a window specification to partition by 'seller_id' and order by 'order_date'
window_spec = Window.partitionBy("seller_id").orderBy("order_date")

# Compute the rank and create a new DataFrame with 'rnk' column
ranked_orders_df = orders_df.withColumn("rnk", rank().over(window_spec))

# Step 2: Create DataFrame for the users table
users_df = spark.read.csv("/content/users.csv", header=True, inferSchema=True)
users_df.show()

# Step 3: Create DataFrame for the items table
items_df = spark.read.csv("/content/items.csv", header=True, inferSchema=True)
items_df.show()

# Step 4: Perform the join operations
result_df = (users_df.join(ranked_orders_df, 
                           (ranked_orders_df.seller_id == users_df.user_id) & (ranked_orders_df.rnk == 2),  # Combine join conditions using logical AND
                           "left")
              .join(items_df, items_df.item_id == ranked_orders_df.item_id, "left"))  
result_df.show()

# Step 5: Use 'CASE WHEN' SQL expression to apply the logic (item_fav_brand)
result_df = result_df.withColumn(
    "item_fav_brand", 
    expr("CASE WHEN items.item_brand = users.favorite_brand THEN 'Yes' ELSE 'No' END")
)

# Step 6: Select the required columns
final_result_df = result_df.select("user_id", "item_fav_brand")

# Show the final result
final_result_df.show()
