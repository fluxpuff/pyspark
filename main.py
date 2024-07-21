from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def get_product_category_pairs(products_df, categories_df, product_category_relations_df):
    # Join products and relations on product_id
    product_relations_df = products_df.join(product_category_relations_df,
                                            products_df.product_id == product_category_relations_df.product_id)

    # Join the above dataframe with categories on category_id
    product_category_df = product_relations_df.join(categories_df,
                                                    product_relations_df.category_id == categories_df.category_id)

    # Select only the product name and category name
    product_category_pairs_df = product_category_df.select("product_name", "category_name")

    # Get products without categories
    products_without_categories_df = products_df.join(product_category_relations_df,
                                                      products_df.product_id == product_category_relations_df.product_id,
                                                      'left_anti')

    # Select only the product name
    products_without_categories_df = products_without_categories_df.select("product_name")

    return product_category_pairs_df, products_without_categories_df


# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Assume we have the following dataframes
products_df = spark.createDataFrame([(1, "Product1"), (2, "Product2"), (3, "Product3"), (4, "Product4")], ["product_id", "product_name"])
categories_df = spark.createDataFrame([(1, "Category1"), (2, "Category2")], ["category_id", "category_name"])
product_category_relations_df = spark.createDataFrame([(1, 1), (2, 2), (4, 1), (4, 2)], ["product_id", "category_id"])

# Call the function
product_category_pairs_df, products_without_categories_df = get_product_category_pairs(products_df, categories_df, product_category_relations_df)

# Show the results
product_category_pairs_df.show()
print('Продукты, у которых нет категорий:\n')
products_without_categories_df.show()
