import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class PageViewsTransformation:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.checkpointBase = os.getenv("WAREHOUSE")

    def aggregate(self):
        
        # Create a streaming DataFrame from the silver page_views table
        df = self.spark.readStream \
            .format("iceberg") \
            .option("streaming-max-files-per-micro-batch", "1") \
            .load("nessie.silver.page_views")
        
        # Write the streaming DataFrame to the page_views_agg table
        stream = df.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.checkpointBase}/checkpoint/silver_page_views_agg") \
            .trigger(processingTime='10 seconds') \
            .toTable("nessie.silver.page_views_agg") \
        
        stream.awaitTermination()
        
    def transform(self):
        # Define a function to handle each micro-batch
        def _upsert(input_df: DataFrame, _batch_id: int):

            spark_session = input_df.sparkSession

            # Register the micro-batch as a temp view for SQL
            input_df.createOrReplaceTempView("page_views_input")

            # ─── JOIN-based enrichment ──────────────────────────────────
            # Instead of per-row Python UDFs, we JOIN against pre-loaded
            # dimension tables. This lets Spark's Catalyst optimizer:
            #   - Broadcast the small dim tables (25 products, 5 users)
            #   - Execute entirely in JVM (no Python serialization)
            #   - Reuse matched rows across duplicates
            query = """
                MERGE INTO nessie.silver.page_views AS target
                USING (
                    SELECT 
                        pv.event_type, pv.event_id, pv.created_ts, pv.session_id,
                        pv.page_url,
                        dp.product_id,
                        dp.product_name,
                        dp.product_segment,
                        pv.user_id,
                        du.user_location,
                        du.user_gender
                    FROM page_views_input pv
                    LEFT JOIN nessie.bronze.dim_products dp
                        ON dp.product_url = pv.page_url
                    LEFT JOIN nessie.bronze.dim_users du
                        ON du.user_id = pv.user_id
                ) AS source
                ON target.event_id = source.event_id
                WHEN NOT MATCHED THEN INSERT *
            """
            print(query)
            output_df = spark_session.sql(query)
            output_df.show()

        # Create a streaming DataFrame from the bronze layer
        df = self.spark.readStream \
            .format("iceberg") \
            .option("streaming-max-files-per-micro-batch", "1") \
            .load("nessie.bronze.page_views")
        
        # Configure and start the streaming job
        stream = df.writeStream \
            .foreachBatch(_upsert) \
            .trigger(processingTime='10 seconds') \
            .option("checkpointLocation", f"{self.checkpointBase}/checkpoint/silver_page_views_transf") \
            .start()
        
        stream.awaitTermination()