from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.spark_helpers.products import Products
from src.data_enrichment.userservice import UserService


class DimensionsLoader:
    """
    Loads dimension tables (dim_products, dim_users) with reference data.
    
    This replaces per-row Python UDFs with pre-computed lookup tables,
    enabling Silver transforms to use efficient JOINs instead.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_all(self):
        """Load both product and user dimension tables."""
        self.load_products()
        self.load_users()

    def load_products(self):
        """
        Load all 25 products from the catalog into dim_products.
        Maps each product URL to its ID, name, and segment (category).
        """
        products = Products()
        base_url = "https://www.shoptillyoudrop.com/product/"

        product_rows = []
        for category, category_products in products.product_categories.items():
            for product_id, product_name in category_products:
                product_rows.append((
                    f"{base_url}{product_id}",  # product_url (matches page_url in events)
                    product_id,                  # product_id
                    product_name,                # product_name
                    category                     # product_segment
                ))

        schema = StructType([
            StructField("product_url", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("product_segment", StringType(), False),
        ])

        df = self.spark.createDataFrame(product_rows, schema=schema)
        print(f"Loading {df.count()} products into dim_products:")
        df.show(truncate=False)
        df.writeTo("nessie.bronze.dim_products").append()

    def load_users(self):
        """
        Pre-compute attributes for user_1 through user_5 into dim_users.
        Uses the same deterministic Faker logic as the original UserService,
        ensuring consistent enrichment results.
        """
        user_service = UserService()

        user_rows = []
        for i in range(1, 6):
            user_id = f"user_{i}"
            attrs = user_service.generate_user_attributes(user_id)
            user_rows.append((
                attrs["user_id"],
                attrs["user_gender"],
                attrs["user_age"],
                attrs["user_location"]
            ))

        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("user_gender", StringType(), False),
            StructField("user_age", IntegerType(), False),
            StructField("user_location", StringType(), False),
        ])

        df = self.spark.createDataFrame(user_rows, schema=schema)
        print(f"Loading {df.count()} users into dim_users:")
        df.show(truncate=False)
        df.writeTo("nessie.bronze.dim_users").append()
