import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class ClickEventsTransformation:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.checkpointBase = os.getenv("WAREHOUSE")

    def aggregate(self):
        
        # Create a streaming DataFrame from the silver click_events table
        df = self.spark.readStream \
            .format("iceberg") \
            .option("streaming-max-files-per-micro-batch", "1") \
            .load("nessie.silver.click_events")
        
        # Write the streaming DataFrame to the click_events_agg table
        stream = df.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.checkpointBase}/checkpoint/silver_click_events_agg") \
            .trigger(processingTime='10 seconds') \
            .toTable("nessie.silver.click_events_agg") \
        
        stream.awaitTermination()
        
    def transform(self):
        # Define a function to handle each micro-batch
        def _upsert(input_df: DataFrame, _batch_id: int):

            spark_session = input_df.sparkSession

            # Register the micro-batch as a temp view for SQL
            input_df.createOrReplaceTempView("click_events_input")

            # ─── JOIN-based enrichment ──────────────────────────────────
            # Instead of per-row Python UDFs, we JOIN against pre-loaded
            # dimension tables. This lets Spark's Catalyst optimizer:
            #   - Broadcast the small dim table (25 products)
            #   - Execute entirely in JVM (no Python serialization)
            #   - Reuse matched rows across duplicates
            # Note: click_events only enriches product data, not user data
            #       (no user columns in Silver click_events schema)
            query = """
                MERGE INTO nessie.silver.click_events AS target
                USING (
                    SELECT 
                        ce.event_type, ce.event_id, ce.created_ts, ce.session_id,
                        ce.user_id, ce.page_url,
                        dp.product_id,
                        dp.product_name,
                        dp.product_segment,
                        ce.element_id,
                        ce.element_text,
                        ce.element_tag
                    FROM click_events_input ce
                    LEFT JOIN nessie.bronze.dim_products dp
                        ON dp.product_url = ce.page_url
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
            .load("nessie.bronze.click_events")
        
        # Configure and start the streaming job
        stream = df.writeStream \
            .foreachBatch(_upsert) \
            .trigger(processingTime='10 seconds') \
            .option("checkpointLocation", f"{self.checkpointBase}/checkpoint/silver_click_events_transf") \
            .start()
        
        stream.awaitTermination()