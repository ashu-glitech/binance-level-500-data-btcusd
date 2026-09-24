import os
import gc
import glob
import json
import time
import asyncio
import aiohttp
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import http.server
import socketserver
import threading
import zipfile
import shutil
from datetime import datetime, timedelta, timezone
from collections import deque
from huggingface_hub import HfApi, hf_hub_download, create_repo

# ==============================================================================
# 🚀 BINANCE LEVEL-500 ORDERBOOK COLLECTOR (Nifty-Proven Chunk+ZIP Formula)
# ==============================================================================

total_rows_collected = 0
last_upload_time = "Not uploaded yet"
DATA_DIR = "data"

class HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        html = f"""<html>
        <head><title>Binance LOB Collector Dashboard</title><meta http-equiv="refresh" content="5">
        <style>body {{ background-color: #0f172a; color: #e2e8f0; font-family: 'Inter', sans-serif; margin: 0; padding: 40px; }} .container {{ max-width: 800px; margin: 0 auto; background: #1e293b; padding: 30px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }} h1 {{ color: #38bdf8; text-align: center; font-size: 32px; margin-bottom: 5px; }} h3 {{ color: #94a3b8; text-align: center; margin-top: 0; margin-bottom: 30px; font-weight: normal; }} .grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; }} .card {{ background: #0f172a; padding: 20px; border-radius: 10px; border-left: 5px solid #38bdf8; }} .card h2 {{ font-size: 14px; color: #94a3b8; margin: 0 0 10px 0; text-transform: uppercase; letter-spacing: 1px; }} .card p {{ font-size: 28px; font-weight: bold; margin: 0; color: #f8fafc; }} .status-badge {{ background: #22c55e; color: white; padding: 5px 15px; border-radius: 20px; font-size: 14px; font-weight: bold; }} .footer {{ text-align: center; margin-top: 30px; color: #64748b; font-size: 14px; }}</style></head>
        <body><div class="container"><h1>Binance AI Collector</h1><h3>HuggingFace Auto-Sync 🚀 (Chunk+ZIP Formula)</h3>
        <div style="text-align: center; margin-bottom: 30px;"><span class="status-badge">{"🟢 LOB SYNCED" if is_synced else "⏳ WAITING FOR SNAPSHOT"}</span></div>
        <div class="grid">
        <div class="card" style="border-color: #f59e0b;"><h2>Current BTC Price</h2><p>${live_state.get('current_price', 0):,.2f}</p></div>
        <div class="card" style="border-color: #3b82f6;"><h2>Rows in Buffer</h2><p>{len(parquet_buffer)} / 60</p></div>
        <div class="card" style="border-color: #10b981;"><h2>Total Rows Collected</h2><p>{total_rows_collected}</p></div>
        <div class="card" style="border-color: #8b5cf6;"><h2>Last HF Upload</h2><p style="font-size: 18px; margin-top: 10px;">{last_upload_time}</p></div>
        <div class="card" style="border-color: #ec4899;"><h2>Open Interest</h2><p>{live_state.get('open_interest', 0):,.2f}</p></div>
        <div class="card" style="border-color: #14b8a6;"><h2>Funding Rate</h2><p>{live_state.get('funding_rate', 0):.6f}</p></div>
        </div>
        <div class="footer">Auto-refreshing every 5 seconds. Data streaming directly to {HF_DATASET_REPO}</div>
        </div></body></html>"""
        self.wfile.write(html.encode("utf-8"))

def start_health_server():
    PORT = int(os.environ.get("PORT", 10000))
    with socketserver.TCPServer(("", PORT), HealthCheckHandler) as httpd:
        print(f"🟢 [RENDER] Dummy Health Server listening on port {PORT}...", flush=True)
        httpd.serve_forever()

# HF API Setup
HF_TOKEN = os.environ.get("HF_TOKEN", "")
HF_DATASET_REPO = os.environ.get("HF_DATASET_REPO", "btcusddata/binance-ai-data")

if HF_TOKEN and HF_DATASET_REPO:
    try:
        api = HfApi(token=HF_TOKEN)
        create_repo(repo_id=HF_DATASET_REPO, repo_type="dataset", token=HF_TOKEN, private=True, exist_ok=True)
        print(f"✅ Hugging Face Dataset {HF_DATASET_REPO} is ready for sync!")
    except Exception as e:
        print(f"⚠️ Could not verify HF Repo: {e}")

def get_working_proxy():
    user_proxy = os.environ.get("BINANCE_PROXY")
    if user_proxy and user_proxy != "DIRECT":
        print(f"🔗 Using User-Provided Proxy: {user_proxy}", flush=True)
        return {"http": user_proxy, "https": user_proxy}
    print("🔗 DIRECT MODE (Default): No proxy used.", flush=True)
    return None

PROXIES = None
SYMBOL_FUTURES = "btcusdt"
SYMBOL_SPOT = "BTCUSDT"

live_state = {
    "agg_buy_vol_1s": 0.0, "agg_sell_vol_1s": 0.0,
    "liqs_long_vol_1s": 0.0, "liqs_short_vol_1s": 0.0,
    "body_1m": 0.0, "body_3m": 0.0, "body_5m": 0.0,
    "current_price": 0.0, "open_interest": 0.0, "funding_rate": 0.0
}

LOB = {"bids": {}, "asks": {}}
last_update_id = 0
buffered_events = []
is_synced = False
trades_q = deque()
liqs_q = deque()

parquet_buffer = []
last_hf_upload_time_seconds = time.time()

# ==============================================================================
# 🗂️ CHUNK + ZIP LOGIC (NIFTY PROVEN)
# ==============================================================================
def get_trading_date_str():
    # 5:30 AM IST Rollover (UTC 00:00) 
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_today_data_dir():
    date_str = get_trading_date_str()
    path = os.path.join(DATA_DIR, date_str)
    os.makedirs(path, exist_ok=True)
    return path, date_str

def save_parquet_chunk():
    global parquet_buffer, total_rows_collected
    if not parquet_buffer: return
    df = pd.DataFrame(parquet_buffer)
    parquet_buffer = []
    total_rows_collected += len(df)
    
    try:
        date_dir, _ = get_today_data_dir()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        filename = os.path.join(date_dir, f"binance_MS_chunk_{ts}.parquet")
        df.to_parquet(filename, engine='pyarrow', index=False)
        print(f"💾 Saved {len(df)} rows → {filename}", flush=True)
    except Exception as e:
        print(f"❌ Parquet save error: {e}", flush=True)
    finally:
        if 'df' in locals(): del df
        gc.collect()

def create_daily_zip(date_str=None):
    if date_str is None: date_str = get_trading_date_str()
    date_dir = os.path.join(DATA_DIR, date_str)
    if not os.path.exists(date_dir): return None
    
    chunk_files = sorted(glob.glob(os.path.join(date_dir, "*.parquet")))
    if not chunk_files: return None
    
    try:
        merged_parquet = os.path.join(date_dir, f"binance_ml_data_MS_{date_str}.parquet")
        tables = [pq.read_table(f) for f in chunk_files]
        merged = pa.concat_tables(tables)
        pq.write_table(merged, merged_parquet)
        
        zip_path = os.path.join(DATA_DIR, f"binance_ml_data_MS_{date_str}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(merged_parquet, arcname=os.path.basename(merged_parquet))
        print(f"📦 ZIP created: {zip_path} ({len(chunk_files)} chunks, {merged.num_rows} rows)", flush=True)
        return zip_path
    except Exception as e:
        print(f"❌ ZIP error: {e}", flush=True)
        return None

def upload_zip_to_huggingface():
    global last_upload_time
    if not HF_TOKEN or not HF_DATASET_REPO: return
    
    date_str = get_trading_date_str()
    zip_path = create_daily_zip(date_str)
    if not zip_path or not os.path.exists(zip_path): return
    
    try:
        api = HfApi(token=HF_TOKEN)
        zip_name = os.path.basename(zip_path)
        api.upload_file(
            path_or_fileobj=zip_path,
            path_in_repo=f"daily_vault/{zip_name}",
            repo_id=HF_DATASET_REPO,
            repo_type="dataset",
            token=HF_TOKEN
        )
        last_upload_time = datetime.now().strftime('%H:%M:%S')
        print(f"[{last_upload_time}] 🚀 [HF SYNC] ZIP uploaded: {zip_name}", flush=True)
    except Exception as e:
        print(f"⚠️ HF Upload Failed: {e}", flush=True)

def resume_from_hf():
    global total_rows_collected
    if not HF_TOKEN or not HF_DATASET_REPO: return
    date_str = get_trading_date_str()
    date_dir = os.path.join(DATA_DIR, date_str)
    os.makedirs(date_dir, exist_ok=True)

    existing_chunks = glob.glob(os.path.join(date_dir, "binance_MS_chunk_*.parquet"))
    if existing_chunks:
        print(f"✅ {len(existing_chunks)} chunk(s) already on disk for {date_str}. Resuming!", flush=True)
        for c in existing_chunks:
            try: total_rows_collected += pq.read_metadata(c).num_rows
            except: pass
        return

    zip_name = f"binance_ml_data_MS_{date_str}.zip"
    try:
        print(f"🔄 Checking HF for {zip_name}...", flush=True)
        zip_path_hf = hf_hub_download(
            repo_id=HF_DATASET_REPO,
            filename=f"daily_vault/{zip_name}",
            repo_type="dataset",
            token=HF_TOKEN
        )
        local_zip = os.path.join(DATA_DIR, zip_name)
        shutil.copy(zip_path_hf, local_zip)
        with zipfile.ZipFile(local_zip, 'r') as zf:
            zf.extractall(date_dir)
        extracted = glob.glob(os.path.join(date_dir, "*.parquet"))
        
        for c in extracted:
            try: total_rows_collected += pq.read_metadata(c).num_rows
            except: pass
            
        print(f"✅ Resumed from HF! Extracted {len(extracted)} file(s) for {date_str}. Starting with {total_rows_collected} rows.", flush=True)
    except Exception as e:
        print(f"ℹ️ No ZIP on HF for {date_str}. Starting fresh.", flush=True)

# ==============================================================================
# 📊 BINANCE LOB LOGIC
# ==============================================================================
def apply_lob_event(event):
    for p, v in event['b']: LOB["bids"][float(p)] = float(v)
    for p, v in event['a']: LOB["asks"][float(p)] = float(v)

def aggregate_relative_lob(coverage_percent=0.01, buckets=500):
    ltp = live_state["current_price"]
    if ltp == 0: return [], []
    target_range = ltp * coverage_percent
    step = target_range / buckets
    agg_bids, agg_asks = {}, {}
    for p, v in LOB["bids"].items():
        if v > 0 and p <= ltp:
            dist = int((ltp - p) / step)
            if dist < buckets: agg_bids[dist] = agg_bids.get(dist, 0) + v
    for p, v in LOB["asks"].items():
        if v > 0 and p >= ltp:
            dist = int((p - ltp) / step)
            if dist < buckets: agg_asks[dist] = agg_asks.get(dist, 0) + v
    final_bids = [agg_bids.get(i, 0.0) for i in range(buckets)]
    final_asks = [agg_asks.get(i, 0.0) for i in range(buckets)]
    return final_bids, final_asks

def get_l1_spread():
    bids = sorted([p for p,v in LOB["bids"].items() if v > 0], reverse=True)
    asks = sorted([p for p,v in LOB["asks"].items() if v > 0])
    return (asks[0] - bids[0]) if bids and asks else 0.0

async def lob_stream():
    global is_synced, last_update_id
    url = f"wss://fstream.binance.com/ws/{SYMBOL_FUTURES}@depth"
    proxy_str = None
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url, proxy=proxy_str) as ws:
            print("🌊 LOB True Millisecond Stream Connected!", flush=True)
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    event = json.loads(msg.data)
                    if not is_synced: buffered_events.append(event)
                    else: apply_lob_event(event)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break

def fetch_snapshot():
    global last_update_id, is_synced
    print("📸 Fetching Base Snapshot from REST (ONCE)...", flush=True)
    try:
        res = requests.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={SYMBOL_FUTURES.upper()}&limit=1000", proxies=PROXIES, timeout=10)
        data = res.json()
        if 'lastUpdateId' not in data:
            print(f"❌ REST API Error: {data}", flush=True); return
    except Exception as e:
        print(f"❌ Proxy/Connection Error: {e}", flush=True); return
    last_update_id = data['lastUpdateId']
    for p, v in data['bids']: LOB["bids"][float(p)] = float(v)
    for p, v in data['asks']: LOB["asks"][float(p)] = float(v)
    for e in buffered_events:
        if e['u'] <= last_update_id: continue
        apply_lob_event(e)
    is_synced = True
    print("✅ Local Orderbook (LOB) Synced!", flush=True)

async def trades_liqs_stream():
    streams = f"{SYMBOL_FUTURES}@aggTrade/{SYMBOL_FUTURES}@forceOrder/{SYMBOL_FUTURES}@kline_1m/{SYMBOL_FUTURES}@kline_3m/{SYMBOL_FUTURES}@kline_5m"
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    proxy_str = None
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url, proxy=proxy_str) as ws:
            print("🟢 Trades, Liqs, Klines Connected!", flush=True)
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
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
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break

async def fetch_oi_funding_loop():
    while True:
        try:
            oi = requests.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL_SPOT}", proxies=PROXIES, timeout=10).json()
            fr = requests.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={SYMBOL_SPOT}", proxies=PROXIES, timeout=10).json()
            live_state["open_interest"] = float(oi.get('openInterest', 0))
            live_state["funding_rate"] = float(fr.get('lastFundingRate', 0))
        except: pass
        await asyncio.sleep(10)

def clean_queues():
    cutoff = time.time() * 1000 - 1000
    while trades_q and trades_q[0][0] < cutoff: trades_q.popleft()
    while liqs_q and liqs_q[0][0] < cutoff: liqs_q.popleft()
    live_state["agg_buy_vol_1s"] = sum(v for t, v, is_buy in trades_q if is_buy)
    live_state["agg_sell_vol_1s"] = sum(v for t, v, is_buy in trades_q if not is_buy)
    live_state["liqs_long_vol_1s"] = sum(v for t, v, is_long in liqs_q if is_long)
    live_state["liqs_short_vol_1s"] = sum(v for t, v, is_long in liqs_q if not is_long)

async def snapshot_recording_loop():
    global last_hf_upload_time_seconds
    print("⏳ Starting 1-Second Snapshot Loop (Optimal for 5M Prediction)...", flush=True)
    while True:
        await asyncio.sleep(1)
        if live_state["current_price"] == 0 and len(LOB["bids"]) > 0:
            live_state["current_price"] = max(p for p, v in LOB["bids"].items() if v > 0)
        if not is_synced or live_state["current_price"] == 0: continue
        
        clean_queues()
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
        if len(parquet_buffer) >= 60:
            # 1. Save chunk
            await asyncio.to_thread(save_parquet_chunk)
            
            # 2. Check if it's time to upload (Every 30 mins)
            if time.time() - last_hf_upload_time_seconds > 1800:
                threading.Thread(target=upload_zip_to_huggingface, daemon=True).start()
                last_hf_upload_time_seconds = time.time()

async def main():
    resume_from_hf()
    global PROXIES
    threading.Thread(target=start_health_server, daemon=True).start()
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
