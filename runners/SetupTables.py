from argparse import ArgumentParser
from src.table_setup.bronze.setup import BronzeSetup
from src.table_setup.silver.setup import SilverSetup
from src.table_setup.gold.setup import GoldSetup
from src.spark_helpers.sparksessionutils import SparkSessionUtils


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--ddl", required=True,
                        help="Type of ddl to perform (bronze or silver or gold)")
    
    args = parser.parse_args()
    ddl = args.ddl
    
    # Initialize Spark session
    spark = SparkSessionUtils().get_spark_session()
    
    # Create ingestion instance based on type
    if ddl == "bronze":
        BronzeSetup(spark).setup()
    elif ddl == "silver":
        SilverSetup(spark).setup()
    elif ddl == "gold":
        GoldSetup(spark).setup()
    else:
        raise ValueError(f"Unsupported ddl type: {ddl}")