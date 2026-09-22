import os
import json
import time
import asyncio
import websockets
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from collections import deque

SYMBOL_FUTURES = "btcusdt"
SYMBOL_SPOT = "BTCUSDT"
TARGET_DELAY = 300 # 5 minutes

live_state = {
    "agg_buy_vol_1s": 0.0,
    "agg_sell_vol_1s": 0.0,
    "liqs_long_vol_1s": 0.0,
    "liqs_short_vol_1s": 0.0,
    "body_1m": 0.0,
    "body_3m": 0.0,
    "body_5m": 0.0,
    "current_price": 0.0,
    "open_interest": 0.0,
    "funding_rate": 0.0
}

LOB = {"bids": {}, "asks": {}}
last_update_id = 0
buffered_events = []
is_synced = False

trades_q = deque()
liqs_q = deque()

def get_daily_parquet_filename():
    return f"binance_ml_data_MS_{datetime.now().strftime('%Y-%m-%d')}.parquet"

# Save in batches for HFT performance
parquet_buffer = []
def flush_parquet_buffer():
    global parquet_buffer
    if not parquet_buffer: return
    df = pd.DataFrame(parquet_buffer)
    table = pa.Table.from_pandas(df)
    filename = get_daily_parquet_filename()
    if not os.path.exists(filename): pq.write_table(table, filename)
    else: pq.write_table(pa.concat_tables([pq.read_table(filename), table]), filename)
    parquet_buffer = []

# --- LOB SYNC & AGGREGATION ---
def apply_lob_event(event):
    for p, v in event['b']: LOB["bids"][float(p)] = float(v)
    for p, v in event['a']: LOB["asks"][float(p)] = float(v)

def aggregate_relative_lob(coverage_percent=0.01, buckets=500):
    # CLAUDE FIX: Dynamic Percentage Buckets (e.g., 1% range)
    ltp = live_state["current_price"]
    if ltp == 0: return [], []
    
    target_range = ltp * coverage_percent
    step = target_range / buckets
    
    agg_bids, agg_asks = {}, {}
    for p, v in LOB["bids"].items():
        if v > 0 and p <= ltp:
            dist = int((ltp - p) / step)
            if dist < buckets:
                agg_bids[dist] = agg_bids.get(dist, 0) + v
    for p, v in LOB["asks"].items():
        if v > 0 and p >= ltp:
            dist = int((p - ltp) / step)
            if dist < buckets:
                agg_asks[dist] = agg_asks.get(dist, 0) + v
            
    # Fill empty buckets with 0 to ensure uniform 500-size shape for GPU
    final_bids = [agg_bids.get(i, 0.0) for i in range(buckets)]
    final_asks = [agg_asks.get(i, 0.0) for i in range(buckets)]
    return final_bids, final_asks

def get_l1_spread():
    bids = sorted([p for p,v in LOB["bids"].items() if v > 0], reverse=True)
    asks = sorted([p for p,v in LOB["asks"].items() if v > 0])
    return (asks[0] - bids[0]) if bids and asks else 0.0

# --- WEBSOCKETS ---
async def lob_stream():
    global is_synced, last_update_id
    url = f"wss://stream.binance.com:9443/ws/{SYMBOL_FUTURES}@depth" # True MS level stream (not 100ms)
    async with websockets.connect(url) as ws:
        print("🌊 LOB True Millisecond Stream Connected!")
        while True:
            msg = await ws.recv()
            event = json.loads(msg)
            if not is_synced: buffered_events.append(event)
            else: apply_lob_event(event)

def fetch_snapshot():
    global last_update_id, is_synced
    print("📸 Fetching Base Snapshot from REST (ONCE)...")
    res = requests.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={SYMBOL_SPOT}&limit=1000")
    data = res.json()
    last_update_id = data['lastUpdateId']
    
    for p, v in data['bids']: LOB["bids"][float(p)] = float(v)
    for p, v in data['asks']: LOB["asks"][float(p)] = float(v)
    
    for e in buffered_events:
        if e['u'] <= last_update_id: continue
        apply_lob_event(e)
    is_synced = True
    print("✅ Local Orderbook (LOB) Synced!")

async def trades_liqs_stream():
    streams = f"{SYMBOL_FUTURES}@aggTrade/{SYMBOL_FUTURES}@forceOrder/{SYMBOL_FUTURES}@kline_1m/{SYMBOL_FUTURES}@kline_3m/{SYMBOL_FUTURES}@kline_5m"
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    async with websockets.connect(url) as ws:
        print("🟢 Trades, Liqs, Klines Connected!")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            stream, d = data.get("stream", ""), data.get("data", {})
            now_ms = time.time() * 1000
            
            if "@aggTrade" in stream:
                is_buy = not d.get("m", True)
                trades_q.append((now_ms, float(d.get("q", 0)), is_buy))
                live_state["current_price"] = float(d.get("p", 0))
            elif "@forceOrder" in stream:
                o = d.get("o", {})
                liqs_q.append((now_ms, float(o.get("q", 0)), o.get("S") == "SELL"))
            elif "@kline" in stream:
                k = d.get("k", {})
                body = float(k.get("c")) - float(k.get("o"))
                if "1m" in stream: live_state["body_1m"] = body
                elif "3m" in stream: live_state["body_3m"] = body
                elif "5m" in stream: live_state["body_5m"] = body

async def fetch_oi_funding_loop():
    while True:
        try:
            oi = requests.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL_SPOT}", timeout=3).json()
            fr = requests.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={SYMBOL_SPOT}", timeout=3).json()
            live_state["open_interest"] = float(oi.get('openInterest', 0))
            live_state["funding_rate"] = float(fr.get('lastFundingRate', 0))
        except: pass
        await asyncio.sleep(10)

def clean_queues():
    cutoff = time.time() * 1000 - 1000 # Keep only last 1 second!
    while trades_q and trades_q[0][0] < cutoff: trades_q.popleft()
    while liqs_q and liqs_q[0][0] < cutoff: liqs_q.popleft()
    live_state["agg_buy_vol_1s"] = sum(v for t, v, is_buy in trades_q if is_buy)
    live_state["agg_sell_vol_1s"] = sum(v for t, v, is_buy in trades_q if not is_buy)
    live_state["liqs_long_vol_1s"] = sum(v for t, v, is_long in liqs_q if is_long)
    live_state["liqs_short_vol_1s"] = sum(v for t, v, is_long in liqs_q if not is_long)

# --- MS TICK RECORDING ---
snapshot_id = 0
async def snapshot_recording_loop():
    global snapshot_id
    print("⏳ Starting 1-Second Snapshot Loop (Optimal for 5M Prediction)...")
    while True:
        await asyncio.sleep(1) # 1-Second Snapshot!
        if not is_synced or live_state["current_price"] == 0: continue
        
        clean_queues()
        snapshot_id += 1
        ts = datetime.now()
        
        bids_shape, asks_shape = aggregate_relative_lob(coverage_percent=0.01, buckets=500)
        
        row = {
            "timestamp_ms": ts.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "current_price": round(live_state["current_price"], 2),
            "bid_ask_spread": round(get_l1_spread(), 2),
            "open_interest": live_state["open_interest"],
            "funding_rate": live_state["funding_rate"],
            "current_body_1m": round(live_state["body_1m"], 2),
            "current_body_3m": round(live_state["body_3m"], 2),
            "current_body_5m": round(live_state["body_5m"], 2),
            "agg_buy_vol_1s": round(live_state["agg_buy_vol_1s"], 2),
            "agg_sell_vol_1s": round(live_state["agg_sell_vol_1s"], 2),
            "liqs_long_vol_1s": round(live_state["liqs_long_vol_1s"], 2),
            "liqs_short_vol_1s": round(live_state["liqs_short_vol_1s"], 2),
            "bid_book_shape": bids_shape,
            "ask_book_shape": asks_shape,
        }
        
        parquet_buffer.append(row)
        if len(parquet_buffer) >= 60: # Save to disk every 1 minute
            flush_parquet_buffer()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Saved 60 Rows to Parquet! (Targets will be calculated in Pandas later)")



async def main():
    asyncio.create_task(lob_stream())
    await asyncio.sleep(2)
    await asyncio.to_thread(fetch_snapshot)
    
    await asyncio.gather(
        trades_liqs_stream(),
        fetch_oi_funding_loop(),
        snapshot_recording_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())
