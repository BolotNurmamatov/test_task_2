import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.logic import products_with_categories

class TestLogic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder             .appName("PySparkTest")             .master("local[2]")             .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_products_with_categories(self):
        # Define schema for input dataframes
        product_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True)
        ])
        category_schema = StructType([
            StructField("category_id", IntegerType(), True),
            StructField("category_name", StringType(), True)
        ])
        product_category_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("category_id", IntegerType(), True)
        ])

        # Sample data
        products_data = [(1, "Apple"), (2, "Banana"), (3, "Carrot"), (4, "Milk")]
        categories_data = [(10, "Fruits"), (20, "Vegetables")]
        product_category_data = [(1, 10), (3, 20)] # Banana (product_id 2) and Milk (product_id 4) have no category

        products_df = self.spark.createDataFrame(products_data, schema=product_schema)
        categories_df = self.spark.createDataFrame(categories_data, schema=category_schema)
        product_category_df = self.spark.createDataFrame(product_category_data, schema=product_category_schema)

        # Expected output data
        expected_data = [
            ("Apple", "Fruits"),
            ("Banana", None),
            ("Carrot", "Vegetables"),
            ("Milk", None)
        ]
        expected_schema = StructType([
            StructField("product_name", StringType(), True),
            StructField("category_name", StringType(), True)
        ])
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        # Call the function
        result_df = products_with_categories(products_df, categories_df, product_category_df)

        # Assertions
        self.assertEqual(result_df.count(), expected_df.count(), "Row count should match")
        
        # Sort by product_name to ensure order doesn't affect comparison
        result_list = sorted([row.asDict() for row in result_df.collect()], key=lambda x: x['product_name'])
        expected_list = sorted([row.asDict() for row in expected_df.collect()], key=lambda x: x['product_name'])
        
        self.assertEqual(result_list, expected_list, "Data content should match")

if __name__ == '__main__':
    unittest.main()
