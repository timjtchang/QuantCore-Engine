import json
import time
import logging
from websocket import create_connection, WebSocketConnectionClosedException
from confluent_kafka import Producer

# --- Configuration ---
KAFKA_TOPIC = "order_book"
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'quantcore-producer',
    'queue.buffering.max.messages': 100000, 
    'queue.buffering.max.ms': 10, 
}

SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "adausdt", "xrpusdt", "dotusdt", "dogeusdt", "avaxusdt", "maticusdt"]

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

class OrderBookProducer:
    def __init__(self):
        self.producer = Producer(KAFKA_CONF)
        self.url = self._build_stream_url()
        self.running = True
    
    def _build_stream_url(self):
        # Combined Stream URL
        streams = "/".join([f"{s}@depth5@100ms" for s in SYMBOLS])
        return f"wss://stream.binance.us:9443/stream?streams={streams}"

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f"❌ Delivery failed: {err}")

    def run(self):
        logging.info(f"🚀 Connecting to Binance Order Book Stream...")
        logging.info(f"📡 URL: {self.url}")
        
        while self.running:
            try:
                ws = create_connection(self.url)
                logging.info("✅ Connected! Streaming Order Books to Kafka...")
                
                while self.running:
                    msg = ws.recv()
                    response = json.loads(msg)
                    
                    # 1. Unwrap Payload
                    if 'data' not in response:
                        continue
                    
                    data = response['data']
                    stream_name = response['stream'] # e.g., "btcusdt@depth5@100ms"
                    
                    # 2. CRITICAL FIX: Extract Symbol from Stream Name
                    # "btcusdt@depth5@100ms" -> "BTCUSDT"
                    symbol = stream_name.split('@')[0].upper()
                    
                    # 3. Inject Symbol into Data (Spark needs this!)
                    data['s'] = symbol
                    data['ingest_ts'] = int(time.time() * 1000)
                    
                    # 4. Produce to Kafka
                    # Use the extracted symbol as the Partition Key
                    self.producer.produce(
                        KAFKA_TOPIC, 
                        key=symbol, 
                        value=json.dumps(data), 
                        callback=self.delivery_report
                    )
                    
                    self.producer.poll(0)

            except WebSocketConnectionClosedException:
                logging.warning("⚠️ Connection closed. Reconnecting...")
                time.sleep(2)
            except Exception as e:
                logging.error(f"❌ Error: {e}")
                time.sleep(5)
            finally:
                if 'ws' in locals() and ws.connected:
                    ws.close()

if __name__ == "__main__":
    p = OrderBookProducer()
    try:
        p.run()
    except KeyboardInterrupt:
        p.producer.flush()
        logging.info("🛑 Stopped.")