from pyspark.sql import SparkSession

class BronzeSetup:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def setup(self):
        # Create the specified namespace
        self.spark.sql(f"""CREATE NAMESPACE IF NOT EXISTS nessie.bronze""").show()
        self.spark.sql(f"""DROP TABLE IF EXISTS nessie.bronze.page_views""").show()
        self.spark.sql(f"""DROP TABLE IF EXISTS nessie.bronze.click_events""").show()
        self.spark.sql(f"""DROP TABLE IF EXISTS nessie.bronze.dim_products""").show()
        self.spark.sql(f"""DROP TABLE IF EXISTS nessie.bronze.dim_users""").show()

        # Create the page_views table
        # bucket(3, event_id): partition pruning for MERGE INTO dedup JOINs
        # month(created_ts): time-based pruning for analytical queries
        self.spark.sql(f"""
                       CREATE TABLE nessie.bronze.page_views (
                            event_type STRING,
                            event_id STRING,
                            created_ts TIMESTAMP,
                            session_id STRING,
                            user_id STRING,
                            page_url STRING
                        )
                        USING ICEBERG
                        PARTITIONED BY (bucket(3, event_id), month(created_ts))
                       """)
        
        # Create the click_events table
        self.spark.sql(f"""
                       CREATE TABLE nessie.bronze.click_events (
                            event_type STRING,
                            event_id STRING,
                            created_ts TIMESTAMP,
                            session_id STRING,
                            user_id STRING,
                            page_url STRING,
                            element_id STRING,
                            element_text STRING,
                            element_tag STRING
                        )
                        USING ICEBERG
                        PARTITIONED BY (bucket(3, event_id), month(created_ts))
                       """)

        # ─── Dimension Tables ───────────────────────────────────────────
        # These are lookup tables used by Silver transforms via JOINs
        # instead of per-row Python UDFs (better for Catalyst optimization)

        # Product dimension: maps product URLs to product details
        self.spark.sql(f"""
                       CREATE TABLE nessie.bronze.dim_products (
                            product_url STRING,
                            product_id STRING,
                            product_name STRING,
                            product_segment STRING
                        )
                        USING ICEBERG
                       """)

        # User dimension: pre-computed user attributes for enrichment
        self.spark.sql(f"""
                       CREATE TABLE nessie.bronze.dim_users (
                            user_id STRING,
                            user_gender STRING,
                            user_age INT,
                            user_location STRING
                        )
                        USING ICEBERG
                       """)