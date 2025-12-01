import json
import time
from websocket import create_connection

SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "adausdt", "xrpusdt", "dotusdt", "dogeusdt", "avaxusdt", "maticusdt"]

def main():
    streams = "/".join([f"{s}@depth5@100ms" for s in SYMBOLS])
    # Combined Stream URL
    url = f"wss://stream.binance.us:9443/stream?streams={streams}"

    print(f"🚀 Connecting to Combined Stream...")
    print(f"📡 URL: {url}")
    
    try:
        ws = create_connection(url)
        print("✅ Connected! Parsing Payload (Ctrl+C to stop)...")
        print("-" * 60)
        
        while True:
            msg = ws.recv()
            response = json.loads(msg)
            
            # 1. Unwrap Payload
            if 'data' not in response:
                continue

            print(response)
            
            data = response['data']
            stream_name = response['stream'] # e.g., "btcusdt@depth5@100ms"
            
            # 2. CRITICAL FIX: Extract Symbol from the Stream Name
            # "btcusdt@depth5@100ms" -> split('@') -> ["btcusdt", "depth5", "100ms"] -> take [0]
            symbol = stream_name.split('@')[0].upper()
            
            # 3. Inject Symbol into Data (so Kafka/Spark can use it later)
            data['s'] = symbol
            data['ingest_ts'] = int(time.time() * 1000)

            # 4. Parse Bids/Asks
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            best_bid = bids[0] if bids else ["N/A", "0"]
            best_ask = asks[0] if asks else ["N/A", "0"]
            
            print(f"[{symbol}] \tBid: {best_bid[0]} \t| Ask: {best_ask[0]}")

    except KeyboardInterrupt:
        print("\n🛑 Stopping...")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()