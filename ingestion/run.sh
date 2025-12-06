#!/bin/bash

TOTAL_SHARDS=10
VENV_PYTHON="./venv/bin/python"
SCRIPT="ingestion/producer.py"

cleanup() {
    echo ""
    echo "🛑 Shutting down all shards..."
    # Kill all child processes of this script
    kill $(jobs -p)
    wait
    echo "✅ All shards stopped."
}

trap cleanup SIGINT

echo "=================================================="
echo "🚀 Starting $TOTAL_SHARDS Producer Shards"
echo "=================================================="

for (( i=0; i<TOTAL_SHARDS; i++ ))
do
    echo "   > Launching Shard ID: $i"
    SHARD_ID=$i TOTAL_SHARDS=$TOTAL_SHARDS $VENV_PYTHON $SCRIPT &
done

echo "=================================================="
echo "✅ All shards running. Logs will mix below."
echo "👉 Press Ctrl+C to stop everything."
echo "=================================================="

wait