import logging
import redis
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "order_book"

# Redis Config (Internal Docker Hostname)
REDIS_HOST = "redis"
REDIS_PORT = 6379

# Schema matches the data sent by your Producer
# Example: {"s": "BTCUSDT", "bids": [["90000", "0.5"], ...], "asks": [...], "ingest_ts": 173...}
schema = StructType([
    StructField("s", StringType()),
    # Bids/Asks are Arrays of Arrays: [ ["Price", "Qty"], ["Price", "Qty"] ]
    StructField("b", ArrayType(ArrayType(StringType()))),
    StructField("a", ArrayType(ArrayType(StringType()))),
    StructField("ingest_ts", LongType())
])

def write_to_redis(batch_df, batch_id):
    """
    This function runs on the Spark Driver for every micro-batch.
    Since OBI data is small (10 rows), we can collect it to the driver 
    and write to Redis efficiently using a Pipeline.
    """
    # 1. Check if batch is empty
    if batch_df.isEmpty():
        return
    
    proc_start = time.time()
        
    # 2. Collect data to Driver (Valid for small summary data like this)
    rows = batch_df.collect()
    

    current_time_ms = int(time.time() * 1000)

    mp = {}

    for row in rows:
        symbol = row['symbol']
        obi = row['OBI']
        ts = row['ingest_ts']

        if symbol not in mp or mp[symbol][1]<ts:
            mp[symbol] = (obi, ts)


    # 3. Connect to Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipe = r.pipeline()
        
        total_lag = 0
        count = 0
        
        # 4. Loop through rows and queue Redis commands
        print(f"💾 Writing Batch {batch_id} to Redis ({len(rows)} symbols)...")
        for symbol, (obi, ingest_ts) in mp.items():

            lag = current_time_ms - ingest_ts

            if lag < 0: lag = 0

            total_lag += lag
            count += 1
            
            payload = f"{obi}:{ingest_ts}"
            pipe.hset("market_metrics", symbol, payload)
            print("symbol: "+symbol+" obi: "+str(obi))

            pubsub_message = {
                "symbol": symbol,
                "obi": str(obi),
                "update": str(ingest_ts),
                "process_ts": str(current_time_ms)  # ← 新增（Spark 寫完時間）
            }
            pipe.publish("market_updates_channel", json.dumps(pubsub_message))
 
        

        avg_lag = total_lag / count if count > 0 else 0
        
        pipe.hset("system_metrics", "avg_latency_ms", str(int(avg_lag)))
        pipe.hset("system_metrics", "processed_count", str(count))
        pipe.hset("system_metrics", "last_updated", str(current_time_ms))
        
        pipe.execute()
        
        print(f"⚡ Batch {batch_id} | processed: {count} | avg_lag: {int(avg_lag)}ms")
        
    except Exception as e:
        print(f"❌ Redis Error: {e}")

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
        col("data.ingest_ts").alias("ingest_ts"),
        col("data.b").alias("bids"),
        col("data.a").alias("asks"),
    )

    # 4. The "Quant" Logic (OBI Calculation)
    # We use Spark SQL 'Higher Order Functions' to handle arrays efficiently without exploding them.
    # Logic: 
    #   1. transform(bids, x -> x[1])  --> Extract the 2nd element (Quantity) from every order
    #   2. cast(... as double)         --> Convert string "0.5" to number 0.5
    #   3. aggregate(..., 0.0, sum)    --> Sum them all up
    
    obi_df = parsed_df.select(
        col("symbol"),
        col("ingest_ts"),
        expr("aggregate(transform(bids, x -> cast(x[1] as double)), 0.0D, (acc, x) -> acc + x)").alias("bid_vol"),
        expr("aggregate(transform(asks, x -> cast(x[1] as double)), 0.0D, (acc, x) -> acc + x)").alias("ask_vol")
    ).withColumn(
        "OBI",
        (col("bid_vol") - col("ask_vol")) / (col("bid_vol") + col("ask_vol"))
    )

    # --- WRITING TO REDIS ---
    # Instead of format("console"), we use foreachBatch(write_to_redis)
    query = obi_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_redis) \
        .start()
    
    query.awaitTermination()


if __name__ == "__main__":
    run_processor()