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
        if self.path == "/download_latest":
            date_str = get_trading_date_str()
            date_dir = os.path.join(DATA_DIR, date_str)
            files = sorted(glob.glob(os.path.join(date_dir, "binance_MS_chunk_*.parquet")))
            if files:
                latest_file = files[-1]
                with open(latest_file, "rb") as f: content = f.read()
                self.send_response(200)
                self.send_header("Content-type", "application/octet-stream")
                self.send_header("Content-Disposition", f"attachment; filename={os.path.basename(latest_file)}")
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"No data yet. Wait 1 minute.")
            return

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        html = f"""<html>
        <head><title>Binance LOB Collector Dashboard</title><meta http-equiv="refresh" content="5">
        <style>body {{ background-color: #0f172a; color: #e2e8f0; font-family: 'Inter', sans-serif; margin: 0; padding: 40px; }} .container {{ max-width: 800px; margin: 0 auto; background: #1e293b; padding: 30px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }} h1 {{ color: #38bdf8; text-align: center; font-size: 32px; margin-bottom: 5px; }} h3 {{ color: #94a3b8; text-align: center; margin-top: 0; margin-bottom: 30px; font-weight: normal; }} .grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; }} .card {{ background: #0f172a; padding: 20px; border-radius: 10px; border-left: 5px solid #38bdf8; }} .card h2 {{ font-size: 14px; color: #94a3b8; margin: 0 0 10px 0; text-transform: uppercase; letter-spacing: 1px; }} .card p {{ font-size: 28px; font-weight: bold; margin: 0; color: #f8fafc; }} .status-badge {{ background: #22c55e; color: white; padding: 5px 15px; border-radius: 20px; font-size: 14px; font-weight: bold; }} .footer {{ text-align: center; margin-top: 30px; color: #64748b; font-size: 14px; }} .btn {{ display: inline-block; background: #3b82f6; color: white; text-decoration: none; padding: 10px 20px; border-radius: 5px; margin-top: 20px; font-weight: bold; transition: background 0.3s; }} .btn:hover {{ background: #2563eb; }}</style></head>
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
        </div>
        <div style="text-align: center;"><a href="/download_latest" class="btn">📥 Download Latest 1m Chunk</a></div>
        <div class="footer">Auto-refreshing every 5 seconds. Data streaming directly to {HF_DATASET_REPO}</div>
        </div></body></html>"""
        self.wfile.write(html.encode("utf-8"))

def start_health_server():
    PORT = int(os.environ.get("PORT", 10000))
    with socketserver.TCPServer(("", PORT), HealthCheckHandler) as httpd:
        print(f"🟢 [RENDER] Dummy Health Server listening on port {PORT}...", flush=True)
        httpd.serve_forever()

# HF API Setup
HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()  # ✅ CRITICAL: Strip hidden spaces/newlines!
HF_DATASET_REPO = os.environ.get("HF_DATASET_REPO", "btcusddata/binance-ai-data").strip()

if HF_TOKEN and HF_DATASET_REPO:
    try:
        api = HfApi(token=HF_TOKEN)
        create_repo(repo_id=HF_DATASET_REPO, repo_type="dataset", token=HF_TOKEN, exist_ok=True)
        print(f"✅ Hugging Face Dataset {HF_DATASET_REPO} is ready for sync!", flush=True)
    except Exception as e:
        print(f"⚠️ Could not verify HF Repo: {e}", flush=True)

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

# --- LOB MEMORY CLEANUP (Run every 5 mins to prevent RAM leak) ---
lob_cleanup_last_time = time.time()
def cleanup_lob_memory():
    global lob_cleanup_last_time
    if time.time() - lob_cleanup_last_time < 300: return  # Every 5 mins
    before_bids = len(LOB["bids"])
    before_asks = len(LOB["asks"])
    LOB["bids"] = {p: v for p, v in LOB["bids"].items() if v > 0}
    LOB["asks"] = {p: v for p, v in LOB["asks"].items() if v > 0}
    freed = (before_bids - len(LOB["bids"])) + (before_asks - len(LOB["asks"]))
    if freed > 0:
        print(f"🧹 LOB Cleanup: Freed {freed} zero-volume entries. Bids={len(LOB['bids'])}, Asks={len(LOB['asks'])}", flush=True)
    lob_cleanup_last_time = time.time()
    gc.collect()

parquet_buffer = []
last_hf_upload_time_seconds = time.time()
uploaded_chunks = set()  # Track which chunks already uploaded to HF (avoid re-upload!)

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

def upload_new_chunks_to_hf():
    """200 IQ: Upload ONLY new chunks (not yet uploaded) to HF.
    Each chunk = ~100KB. 15 min interval = ~192 MB/month. Well under 5GB!"""
    global last_upload_time
    if not HF_TOKEN or not HF_DATASET_REPO: return

    date_str = get_trading_date_str()
    date_dir = os.path.join(DATA_DIR, date_str)
    if not os.path.exists(date_dir): return

    all_chunks = sorted(glob.glob(os.path.join(date_dir, "binance_MS_chunk_*.parquet")))
    new_chunks = [f for f in all_chunks if f not in uploaded_chunks]
    if not new_chunks:
        print(f"[HF SYNC] No new chunks to upload.", flush=True)
        return

    try:
        api = HfApi(token=HF_TOKEN)
        for chunk_file in new_chunks:
            chunk_name = os.path.basename(chunk_file)
            api.upload_file(
                path_or_fileobj=chunk_file,
                path_in_repo=f"chunks/{date_str}/{chunk_name}",
                repo_id=HF_DATASET_REPO,
                repo_type="dataset",
                token=HF_TOKEN
            )
            uploaded_chunks.add(chunk_file)
        last_upload_time = datetime.now().strftime('%H:%M:%S')
        print(f"[{last_upload_time}] 🚀 [HF SYNC] {len(new_chunks)} chunk(s) uploaded! (~{len(new_chunks)*100}KB bandwidth used)", flush=True)
    except Exception as e:
        print(f"⚠️ HF Chunk Upload Failed: {e}", flush=True)

def upload_final_daily_zip(prev_date_str):
    """Uploads the final merged ZIP for the entire day (Runs once at Midnight UTC)."""
    print(f"🌅 [DAY ROLLOVER] Generating final ZIP for {prev_date_str}...", flush=True)
    if not HF_TOKEN or not HF_DATASET_REPO: return
    zip_path = create_daily_zip(prev_date_str)
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
        print(f"✅ [DAY ROLLOVER] Successfully uploaded final ZIP {zip_name} to HF!", flush=True)
    except Exception as e:
        print(f"⚠️ Day Rollover ZIP Upload Failed: {e}", flush=True)

def resume_from_hf():
    """On restart, download today's individual chunks from HF to resume data collection."""
    global total_rows_collected
    if not HF_TOKEN or not HF_DATASET_REPO: return
    date_str = get_trading_date_str()
    date_dir = os.path.join(DATA_DIR, date_str)
    os.makedirs(date_dir, exist_ok=True)

    # If chunks already on disk, just count and resume
    existing_chunks = sorted(glob.glob(os.path.join(date_dir, "binance_MS_chunk_*.parquet")))
    if existing_chunks:
        for c in existing_chunks:
            try: total_rows_collected += pq.read_metadata(c).num_rows
            except: pass
        print(f"✅ {len(existing_chunks)} chunk(s) already on disk. Resuming with {total_rows_collected} rows!", flush=True)
        # Mark existing disk chunks as already uploaded so we don't re-upload them
        uploaded_chunks.update(existing_chunks)
        return

    # Try downloading individual chunks from HF
    try:
        print(f"🔄 Checking HF for today's chunks (chunks/{date_str}/)...", flush=True)
        api = HfApi(token=HF_TOKEN)
        all_files = api.list_repo_files(repo_id=HF_DATASET_REPO, repo_type="dataset", token=HF_TOKEN)
        today_chunks = sorted([f for f in all_files if f.startswith(f"chunks/{date_str}/binance_MS_chunk_")])

        if not today_chunks:
            print(f"ℹ️ No chunks on HF for {date_str}. Starting fresh.", flush=True)
            return

        for hf_path in today_chunks:
            chunk_name = os.path.basename(hf_path)
            local_path = os.path.join(date_dir, chunk_name)
            downloaded = hf_hub_download(
                repo_id=HF_DATASET_REPO,
                filename=hf_path,
                repo_type="dataset",
                token=HF_TOKEN
            )
            shutil.copy(downloaded, local_path)
            try: total_rows_collected += pq.read_metadata(local_path).num_rows
            except: pass
            uploaded_chunks.add(local_path)  # Mark as already uploaded!

        print(f"✅ Resumed {len(today_chunks)} chunk(s) from HF! Starting with {total_rows_collected} rows.", flush=True)
    except Exception as e:
        print(f"ℹ️ Could not resume from HF: {e}. Starting fresh.", flush=True)

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
    """LOB WebSocket with AUTO-RECONNECT. Never dies!"""
    global is_synced, last_update_id
    url = f"wss://fstream.binance.com/ws/{SYMBOL_FUTURES}@depth"
    while True:  # ✅ INFINITE RECONNECT LOOP
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, proxy=None, heartbeat=30, timeout=10, receive_timeout=30) as ws:
                    print("🌊 LOB True Millisecond Stream Connected!", flush=True)
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            event = json.loads(msg.data)
                            if not is_synced: buffered_events.append(event)
                            else: apply_lob_event(event)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
        except Exception as e:
            print(f"⚠️ LOB Stream dropped: {e}. Reconnecting in 5s...", flush=True)
        await asyncio.sleep(5)  # Wait 5 sec then reconnect

def fetch_snapshot():
    global last_update_id, is_synced
    print("📸 Fetching Base Snapshot from REST...", flush=True)
    while not is_synced:
        try:
            res = requests.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={SYMBOL_FUTURES.upper()}&limit=1000", proxies=PROXIES, timeout=10)
            data = res.json()
            if 'lastUpdateId' not in data:
                print(f"❌ REST API Error: {data}. Retrying in 5s...", flush=True)
                time.sleep(5)
                continue
            
            last_update_id = data['lastUpdateId']
            for p, v in data['bids']: LOB["bids"][float(p)] = float(v)
            for p, v in data['asks']: LOB["asks"][float(p)] = float(v)
            
            for e in buffered_events:
                if e['u'] <= last_update_id: continue
                apply_lob_event(e)
            buffered_events.clear()  # ✅ CRITICAL: Free memory after sync!
            is_synced = True
            print("✅ Local Orderbook (LOB) Synced!", flush=True)
            break  # Success, exit loop
        except Exception as e:
            print(f"❌ Proxy/Connection Error fetching snapshot: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)

async def trades_liqs_stream():
    """Trades/Liqs/Klines WebSocket with AUTO-RECONNECT.
    This is CRITICAL - without it, candle body and trade volume show 0 forever!"""
    streams = f"{SYMBOL_FUTURES}@aggTrade/{SYMBOL_FUTURES}@forceOrder/{SYMBOL_FUTURES}@kline_1m/{SYMBOL_FUTURES}@kline_3m/{SYMBOL_FUTURES}@kline_5m"
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:  # ✅ INFINITE RECONNECT LOOP
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, proxy=None, heartbeat=30, timeout=10, receive_timeout=30) as ws:
                    print("🟢 Trades, Liqs, Klines Connected!", flush=True)
                    messages_received = 0
                    async for msg in ws:
                        messages_received += 1
                        if messages_received % 1000 == 0:
                            print(f"📊 Trades stream flowing... ({messages_received} msgs)", flush=True)
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
                                try:
                                    body = float(k.get("c", 0)) - float(k.get("o", 0))
                                    if "kline_1m" in stream: live_state["body_1m"] = body
                                    elif "kline_3m" in stream: live_state["body_3m"] = body
                                    elif "kline_5m" in stream: live_state["body_5m"] = body
                                except: pass
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): 
                            print(f"⚠️ Trades stream closed or error: {msg}", flush=True)
                            break
        except Exception as e:
            print(f"⚠️ Trades/Klines Stream dropped: {e}. Reconnecting in 5s...", flush=True)
        print("🔄 Reconnecting Trades/Klines stream in 5s...", flush=True)
        await asyncio.sleep(5)  # Wait 5 sec then reconnect

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
    current_date_str = get_trading_date_str()
    
    while True:
        await asyncio.sleep(1)
        
        # --- DAY ROLLOVER CHECK ---
        new_date_str = get_trading_date_str()
        if new_date_str != current_date_str:
            print(f"🔄 Date changed from {current_date_str} to {new_date_str}. Triggering rollover!", flush=True)
            threading.Thread(target=upload_final_daily_zip, args=(current_date_str,), daemon=True).start()
            current_date_str = new_date_str
            global total_rows_collected
            total_rows_collected = 0  # Reset for new day
        # --------------------------

        if live_state["current_price"] == 0 and len(LOB["bids"]) > 0:
            live_state["current_price"] = max(p for p, v in LOB["bids"].items() if v > 0)
        if not is_synced or live_state["current_price"] == 0: continue
        
        clean_queues()
        cleanup_lob_memory()  # ✅ CRITICAL: Prevent LOB RAM leak!
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
            # 1. Save chunk to disk (every 1 min)
            await asyncio.to_thread(save_parquet_chunk)
            
            # 2. Upload NEW chunks to HF every 15 min (~100KB each = ~192MB/month)
            if time.time() - last_hf_upload_time_seconds > 900:  # 15 min
                threading.Thread(target=upload_new_chunks_to_hf, daemon=True).start()
                last_hf_upload_time_seconds = time.time()

async def main():
    global PROXIES
    # ✅ STEP 1: Start health server FIRST so Render sees the port immediately!
    threading.Thread(target=start_health_server, daemon=True).start()
    await asyncio.sleep(1)  # Give server 1 sec to bind the port

    # ✅ STEP 2: Resume from HF in background (non-blocking)
    await asyncio.to_thread(resume_from_hf)

    # ✅ STEP 3: Get proxy, connect streams, start collecting
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
