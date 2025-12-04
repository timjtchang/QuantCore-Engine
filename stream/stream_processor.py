import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "order_book"

# Schema matches the data sent by your Producer
# Example: {"s": "BTCUSDT", "bids": [["90000", "0.5"], ...], "asks": [...], "ingest_ts": 173...}
schema = StructType([
    StructField("s", StringType()),
    # Bids/Asks are Arrays of Arrays: [ ["Price", "Qty"], ["Price", "Qty"] ]
    StructField("bids", ArrayType(ArrayType(StringType()))),
    StructField("asks", ArrayType(ArrayType(StringType()))),
    StructField("ingest_ts", LongType())
])

def run_processor():
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("QuantCoreEngine") \
        .getOrCreate()

    # Reduce log noise
    spark.sparkContext.setLogLevel("WARN")

    # 2. Read Stream from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parse JSON
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select(
        col("data.s").alias("symbol"),
        (col("data.ingest_ts") / 1000).cast("timestamp").alias("timestamp"),
        col("data.bids"),
        col("data.asks")
    )

    # 4. The "Quant" Logic (OBI Calculation)
    # We use Spark SQL 'Higher Order Functions' to handle arrays efficiently without exploding them.
    # Logic: 
    #   1. transform(bids, x -> x[1])  --> Extract the 2nd element (Quantity) from every order
    #   2. cast(... as double)         --> Convert string "0.5" to number 0.5
    #   3. aggregate(..., 0.0, sum)    --> Sum them all up
    
    obi_df = parsed_df.select(
        col("symbol"),
        col("timestamp"),
        expr("aggregate(transform(bids, x -> cast(x[1] as double)), 0.0D, (acc, x) -> acc + x)").alias("bid_vol"),
        expr("aggregate(transform(asks, x -> cast(x[1] as double)), 0.0D, (acc, x) -> acc + x)").alias("ask_vol")
    ).withColumn(
        "OBI",
        (col("bid_vol") - col("ask_vol")) / (col("bid_vol") + col("ask_vol"))
    )

    # 5. Output to Console 
    query = obi_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_processor()