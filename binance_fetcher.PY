import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
from pathlib import Path
import logging

# ---------------- CONFIGURATION ----------------
SYMBOLS_FILE = "symbols.txt"
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d", "1w", "1M"]
YEARS_OF_DATA = 5
BASE_URL = "https://api.binance.com/api/v3/klines"
MAX_CONCURRENT_REQUESTS = 10  # Increased for better throughput
REQUEST_DELAY = 0.02  # Reduced delay
MAX_RETRIES = 3
ERROR_LOG = "errors.log"
DATA_DIR = "data"  # Organized data directory

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(ERROR_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------- UTILITIES ----------------
def load_symbols():
    """Load symbols from file or return defaults."""
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r") as f:
            symbols = [line.strip().upper() for line in f if line.strip()]
        return symbols
    return ["ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]

def get_start_timestamp():
    """Calculate start timestamp for historical data."""
    return int((datetime.now() - timedelta(days=YEARS_OF_DATA * 365)).timestamp() * 1000)

def timeframe_to_ms(tf):
    """Convert timeframe string to milliseconds."""
    conversions = {
        'm': 60 * 1000,
        'h': 60 * 60 * 1000,
        'd': 24 * 60 * 60 * 1000,
        'w': 7 * 24 * 60 * 60 * 1000,
        'M': 30 * 24 * 60 * 60 * 1000
    }
    unit = tf[-1]
    value = int(tf[:-1]) if len(tf) > 1 else 1
    return value * conversions.get(unit, 15 * 60 * 1000)

def klines_to_df(klines):
    """Convert klines data to pandas DataFrame."""
    if not klines:
        return pd.DataFrame()
    
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    
    # Optimize data types
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    # Keep only OHLCV data
    df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    
    return df.dropna()

def get_optimal_limit(tf):
    """Get optimal request limit based on timeframe."""
    if tf in ["1m", "5m"]:
        return 1000
    elif tf in ["15m", "30m", "1h"]:
        return 1000
    elif tf in ["4h", "8h", "12h"]:
        return 1000
    else:  # 1d, 1w, 1M
        return 500

# ---------------- ASYNC FETCH ----------------
async def fetch_klines(session, symbol, tf, start_ts, limit=1000, retries=0):
    """Fetch klines data with exponential backoff retry."""
    params = {
        "symbol": symbol,
        "interval": tf,
        "startTime": start_ts,
        "limit": limit
    }
    
    try:
        async with session.get(BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
            if response.status == 429:  # Rate limit
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited on {symbol} {tf}, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                return await fetch_klines(session, symbol, tf, start_ts, limit, retries)
            
            if response.status != 200:
                text = await response.text()
                raise Exception(f"HTTP {response.status}: {text}")
            
            return await response.json()
            
    except asyncio.TimeoutError:
        logger.warning(f"Timeout on {symbol} {tf}, retry {retries + 1}/{MAX_RETRIES}")
        if retries < MAX_RETRIES:
            await asyncio.sleep(2 ** retries)
            return await fetch_klines(session, symbol, tf, start_ts, limit, retries + 1)
        logger.error(f"Max retries exceeded for {symbol} {tf} at {start_ts}")
        return []
        
    except Exception as e:
        logger.error(f"Error fetching {symbol} {tf}: {e}")
        if retries < MAX_RETRIES:
            await asyncio.sleep(2 ** retries)
            return await fetch_klines(session, symbol, tf, start_ts, limit, retries + 1)
        return []

async def fetch_all_klines(symbol, tf, start_ts, end_ts=None):
    """Fetch all historical klines for a symbol and timeframe."""
    all_klines = []
    duration_ms = timeframe_to_ms(tf)
    limit = get_optimal_limit(tf)
    end_ts = end_ts or int(datetime.now().timestamp() * 1000)
    
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        request_count = 0
        
        while start_ts < end_ts:
            klines = await fetch_klines(session, symbol, tf, start_ts, limit)
            if not klines:
                break
                
            all_klines.extend(klines)
            request_count += 1
            
            # Update start timestamp
            last_timestamp = klines[-1][0]
            start_ts = last_timestamp + duration_ms
            
            # Check if we've reached current time
            if start_ts >= end_ts:
                break
            
            await asyncio.sleep(REQUEST_DELAY)
    
    return all_klines, request_count

# ---------------- UPDATE CSV ----------------
async def update_symbol_tf(symbol, tf, progress_callback=None):
    """Update CSV file for a symbol and timeframe."""
    Path(DATA_DIR).mkdir(exist_ok=True)
    csv_file = os.path.join(DATA_DIR, f"{symbol}_{tf}.csv")
    
    # Determine start timestamp
    if os.path.exists(csv_file):
        try:
            existing_df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
            if not existing_df.empty:
                last_time = existing_df.index[-1]
                start_ts = int(last_time.timestamp() * 1000) + timeframe_to_ms(tf)
            else:
                existing_df = pd.DataFrame()
                start_ts = get_start_timestamp()
        except Exception as e:
            logger.warning(f"Error reading {csv_file}: {e}. Starting fresh.")
            existing_df = pd.DataFrame()
            start_ts = get_start_timestamp()
    else:
        existing_df = pd.DataFrame()
        start_ts = get_start_timestamp()
    
    # Skip if already up to date
    current_ts = int(datetime.now().timestamp() * 1000)
    if start_ts >= current_ts:
        logger.info(f"[SKIP] {symbol} {tf} already up to date")
        return
    
    # Fetch new data
    all_klines, req_count = await fetch_all_klines(symbol, tf, start_ts)
    
    if not all_klines:
        logger.info(f"[NO DATA] {symbol} {tf}")
        return
    
    # Convert to DataFrame
    new_df = klines_to_df(all_klines)
    if new_df.empty:
        logger.info(f"[EMPTY] {symbol} {tf}")
        return
    
    # Combine and deduplicate
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df])
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    else:
        combined_df = new_df
    
    combined_df.sort_index(inplace=True)
    
    # Atomic write
    temp_file = csv_file + ".tmp"
    combined_df.to_csv(temp_file)
    os.replace(temp_file, csv_file)
    
    logger.info(f"[SUCCESS] {symbol} {tf} | Rows: {len(new_df)} | Requests: {req_count}")

# ---------------- MAIN RUNNER ----------------
async def main():
    """Main execution function."""
    symbols = load_symbols()
    logger.info(f"Starting data fetch for {len(symbols)} symbols across {len(TIMEFRAMES)} timeframes")
    
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async def sem_update(symbol, tf):
        async with sem:
            await update_symbol_tf(symbol, tf)
    
    tasks = [
        asyncio.create_task(sem_update(sym, tf))
        for sym in symbols
        for tf in TIMEFRAMES
    ]
    
    # Use tqdm for progress tracking
    for task in tqdm.as_completed(tasks, total=len(tasks), desc="Processing"):
        await task
    
    logger.info("Data fetch completed successfully!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)