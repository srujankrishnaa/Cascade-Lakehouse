from src.spark_helpers.sparksessionutils import SparkSessionUtils
from src.ingestion.dimensions import DimensionsLoader


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSessionUtils().get_spark_session()

    # Load dimension tables (dim_products + dim_users)
    # This must run AFTER bronze DDL creates the tables
    # and BEFORE Silver transforms that JOIN against them
    DimensionsLoader(spark).load_all()

    print("Dimension tables loaded successfully.")
