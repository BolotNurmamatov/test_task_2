from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def products_with_categories(products: DataFrame, categories: DataFrame, product_category: DataFrame) -> DataFrame:
    """
    Returns a DataFrame of (product_name, category_name) pairs.
    Includes products without categories (category_name will be null).
    """
    # Join product_category with categories
    pc_with_cat = product_category.join(categories, on="category_id", how="left") \
                                  .select("product_id", "category_name")

    # Join products with category data
    result = products.join(pc_with_cat, on="product_id", how="left") \
                     .select("product_name", "category_name")

    return result
