from pyspark.sql import SparkSession
from src.logic import products_with_categories

spark = SparkSession.builder.appName("ProductCategoryApp").getOrCreate()

# Sample data
products = spark.createDataFrame([
    (1, "Apple"),
    (2, "Banana"),
    (3, "Carrot")
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (10, "Fruits"),
    (20, "Vegetables")
], ["category_id", "category_name"])

product_category = spark.createDataFrame([
    (1, 10),
    (3, 20)
], ["product_id", "category_id"])

result = products_with_categories(products, categories, product_category)
result.show()
