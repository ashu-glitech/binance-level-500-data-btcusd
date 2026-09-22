import os
import json
import time
import asyncio
import websockets
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import http.server
import socketserver
import threading
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from collections import deque
from huggingface_hub import HfApi

total_rows_collected = 0
last_upload_time = "Not uploaded yet"

class HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        
        html = f"""
        <html>
        <head>
            <title>Binance LOB Collector Dashboard</title>
            <meta http-equiv="refresh" content="5">
            <style>
                body {{ background-color: #0f172a; color: #e2e8f0; font-family: 'Inter', sans-serif; margin: 0; padding: 40px; }}
                .container {{ max-width: 800px; margin: 0 auto; background: #1e293b; padding: 30px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }}
                h1 {{ color: #38bdf8; text-align: center; font-size: 32px; margin-bottom: 5px; }}
                h3 {{ color: #94a3b8; text-align: center; margin-top: 0; margin-bottom: 30px; font-weight: normal; }}
                .grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; }}
                .card {{ background: #0f172a; padding: 20px; border-radius: 10px; border-left: 5px solid #38bdf8; }}
                .card h2 {{ font-size: 14px; color: #94a3b8; margin: 0 0 10px 0; text-transform: uppercase; letter-spacing: 1px; }}
                .card p {{ font-size: 28px; font-weight: bold; margin: 0; color: #f8fafc; }}
                .status-badge {{ background: #22c55e; color: white; padding: 5px 15px; border-radius: 20px; font-size: 14px; font-weight: bold; }}
                .footer {{ text-align: center; margin-top: 30px; color: #64748b; font-size: 14px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Binance AI Collector</h1>
                <h3>HuggingFace Auto-Sync 🚀</h3>
                
                <div style="text-align: center; margin-bottom: 30px;">
                    <span class="status-badge">{"🟢 LOB SYNCED" if is_synced else "⏳ WAITING FOR SNAPSHOT"}</span>
                </div>
                
                <div class="grid">
                    <div class="card" style="border-color: #f59e0b;">
                        <h2>Current BTC Price</h2>
                        <p>${live_state.get('current_price', 0):,.2f}</p>
                    </div>
                    <div class="card" style="border-color: #3b82f6;">
                        <h2>Rows in Buffer</h2>
                        <p>{len(parquet_buffer)} / 60</p>
                    </div>
                    <div class="card" style="border-color: #10b981;">
                        <h2>Total Rows Collected</h2>
                        <p>{total_rows_collected}</p>
                    </div>
                    <div class="card" style="border-color: #8b5cf6;">
                        <h2>Last HF Upload</h2>
                        <p style="font-size: 18px; margin-top: 10px;">{last_upload_time}</p>
                    </div>
                    <div class="card" style="border-color: #ec4899;">
                        <h2>Open Interest</h2>
                        <p>{live_state.get('open_interest', 0):,.2f}</p>
                    </div>
                    <div class="card" style="border-color: #14b8a6;">
                        <h2>Funding Rate</h2>
                        <p>{live_state.get('funding_rate', 0):.6f}</p>
                    </div>
                </div>
                
                <div class="footer">
                    Auto-refreshing every 5 seconds. Data streaming directly to {HF_DATASET_REPO}
                </div>
            </div>
        </body>
        </html>
        """
        self.wfile.write(html.encode("utf-8"))

def start_health_server():
    PORT = int(os.environ.get("PORT", 10000))
    with socketserver.TCPServer(("", PORT), HealthCheckHandler) as httpd:
        print(f"🟢 [RENDER] Dummy Health Server listening on port {PORT}...", flush=True)
        httpd.serve_forever()

# HF API Setup
hf_api = HfApi()
HF_TOKEN = os.environ.get("HF_TOKEN")
HF_DATASET_REPO = os.environ.get("HF_DATASET_REPO") # e.g., "username/dataset-name"

if HF_TOKEN and HF_DATASET_REPO:
    try:
        hf_api.create_repo(repo_id=HF_DATASET_REPO, repo_type="dataset", token=HF_TOKEN, exist_ok=True)
        print(f"✅ Hugging Face Dataset {HF_DATASET_REPO} is ready for sync!")
    except Exception as e:
        print(f"⚠️ Could not verify HF Repo: {e}")

def get_working_proxy():
    print("🔍 Hunting for a free working proxy (Auto-Bypass Geo-Block)...", flush=True)
    try:
        res = requests.get("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", timeout=10)
        proxy_list = res.text.strip().splitlines()
        for p in proxy_list[:100]:
            proxy_dict = {"http": f"http://{p}", "https": f"http://{p}"}
            try:
                test = requests.get("https://fapi.binance.com/fapi/v1/time", proxies=proxy_dict, timeout=3)
                if test.status_code == 200:
                    print(f"✅ Found working proxy: {p}", flush=True)
                    return proxy_dict
            except:
                continue
    except:
        pass
    print("❌ Auto-Proxy Hunter failed to find a working proxy.", flush=True)
    return None

PROXIES = None

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
    global parquet_buffer, total_rows_collected, last_upload_time
    if not parquet_buffer: return
    df = pd.DataFrame(parquet_buffer)
    total_rows_collected += len(df)
    table = pa.Table.from_pandas(df)
    filename = get_daily_parquet_filename()
    if not os.path.exists(filename): pq.write_table(table, filename)
    else: pq.write_table(pa.concat_tables([pq.read_table(filename), table]), filename)
    parquet_buffer = []
    
    # Upload to Hugging Face after saving
    if HF_TOKEN and HF_DATASET_REPO:
        try:
            hf_api.upload_file(
                path_or_fileobj=filename,
                path_in_repo=filename,
                repo_id=HF_DATASET_REPO,
                repo_type="dataset",
                token=HF_TOKEN
            )
            last_upload_time = datetime.now().strftime('%H:%M:%S')
            print(f"🚀 Successfully Synced {filename} to Hugging Face Dataset: {HF_DATASET_REPO}")
        except Exception as e:
            print(f"⚠️ HF Upload Failed: {e}")

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
    try:
        res = requests.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={SYMBOL_FUTURES.upper()}&limit=1000", proxies=PROXIES, timeout=10)
        data = res.json()
        if 'lastUpdateId' not in data:
            print(f"❌ REST API Error: {data}")
            return
    except Exception as e:
        print(f"❌ Proxy/Connection Error: {e}")
        return
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
            oi = requests.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL_SPOT}", proxies=PROXIES, timeout=10).json()
            fr = requests.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={SYMBOL_SPOT}", proxies=PROXIES, timeout=10).json()
            live_state["open_interest"] = float(oi.get('openInterest', 0))
            live_state["funding_rate"] = float(fr.get('lastFundingRate', 0))
        except Exception as e:
            print(f"⚠️ Error fetching OI/Funding (Proxy issue?): {e}")
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
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Saved 60 Rows to Parquet! (Targets will be calculated in Pandas later)", flush=True)



async def main():
    global PROXIES
    threading.Thread(target=start_health_server, daemon=True).start()
    
    # Hunt for proxy AFTER health server is up so Render doesn't kill us for port timeout
    PROXIES = get_working_proxy()
    
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
