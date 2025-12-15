from __future__ import annotations

import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import random
import time
import math
from typing import List, Tuple, Optional, Dict, Callable
from dataclasses import dataclass, field

# ---------------- CONFIG ----------------
SYMBOLS_FILE = "symbols.txt"
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d", "1w", "1M"]
YEARS_OF_DATA = 5
BASE_URL = "https://api.binance.com/api/v3/klines"
MAX_CONCURRENT_REQUESTS = 10
REQUEST_DELAY = 0.05
MAX_RETRIES = 4
ERROR_LOG = "errors.log"
DATA_DIR = "data"
TCP_CONNECTOR_LIMIT = 100
TCP_CONNECTOR_LIMIT_PER_HOST = 30

# Setup logging to console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(ERROR_LOG), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ---------------- HELPERS ----------------
def load_symbols() -> List[str]:
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r") as f:
            return [line.strip().upper() for line in f if line.strip()]
    # fallback list
    return ["ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]

def get_start_timestamp() -> int:
    return int((datetime.now() - timedelta(days=YEARS_OF_DATA * 365)).timestamp() * 1000)

def timeframe_to_ms(tf: str) -> int:
    conversions = {
        'm': 60 * 1000,
        'h': 60 * 60 * 1000,
        'd': 24 * 60 * 60 * 1000,
        'w': 7 * 24 * 60 * 60 * 1000,
        'M': 30 * 24 * 60 * 60 * 1000
    }
    # tf like "1m", "15m", "1h", "1M"
    unit = tf[-1]
    value = int(tf[:-1]) if len(tf) > 1 else 1
    return value * conversions.get(unit, 15 * 60 * 1000)

def get_optimal_limit(tf: str) -> int:
    if tf in ("1m", "5m"):
        return 500
    if tf in ("15m", "30m", "1h"):
        return 1000
    if tf in ("4h", "8h", "12h"):
        return 1000
    return 500

def klines_to_df(klines: List[List]) -> pd.DataFrame:
    if not klines:
        return pd.DataFrame()
    cols = [
        'timestamp','open','high','low','close','volume','close_time',
        'quote_asset_volume','number_of_trades','taker_buy_base_volume',
        'taker_buy_quote_volume','ignore'
    ]
    df = pd.DataFrame(klines, columns=cols)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df = df[['open','high','low','close','volume']].astype(float)
    return df.dropna()

# ---------------- ETA ESTIMATOR ----------------
class ETAEstimator:
    def __init__(self, alpha: float = 0.2, max_samples: Optional[int] = 200):
        if not (0 < alpha <= 1):
            raise ValueError("alpha must be in (0,1]")
        self.alpha = float(alpha)
        self.ema: Optional[float] = None
        self.sample_count = 0
        self.max_samples = max_samples
        self._recent: List[float] = []

    def sample(self, duration_seconds: float) -> None:
        if duration_seconds < 0:
            return
        self.sample_count += 1
        if self.ema is None:
            self.ema = float(duration_seconds)
        else:
            self.ema = (self.alpha * float(duration_seconds)) + ((1 - self.alpha) * self.ema)
        if self.max_samples is not None:
            self._recent.append(float(duration_seconds))
            if len(self._recent) > self.max_samples:
                self._recent.pop(0)

    def predict(self, remaining_tasks: int) -> float:
        if remaining_tasks <= 0:
            return 0.0
        if self.ema is None:
            return float(remaining_tasks)
        return float(self.ema) * float(remaining_tasks)

    def get_human_readable(self, remaining_tasks: int) -> str:
        return self._format_seconds(self.predict(remaining_tasks))

    @staticmethod
    def _format_seconds(sec: float) -> str:
        sec = max(0.0, float(sec))
        if sec < 1:
            return "<1s"
        m, s = divmod(int(round(sec)), 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h}h {m}m {s}s"
        if m > 0:
            return f"{m}m {s}s"
        return f"{s}s"

# ---------------- PROGRESS MODEL ----------------
@dataclass
class TimeframeProgress:
    timeframe: str
    total_tasks: int = 0
    completed_tasks: int = 0
    last_rows: int = 0
    last_requests: int = 0
    done: bool = False
    start_time: Optional[float] = None
    end_time: Optional[float] = None

@dataclass
class SymbolProgress:
    symbol: str
    total_tasks: int = 0
    completed_tasks: int = 0
    timeframes: Dict[str, TimeframeProgress] = field(default_factory=dict)
    start_time: Optional[float] = None
    end_time: Optional[float] = None

class ProgressModel:
    def __init__(self, symbols: List[str], timeframes: List[str], on_update: Optional[Callable[[str, str, int, int], None]] = None):
        self.symbols: Dict[str, SymbolProgress] = {}
        self.timeframe_summary: Dict[str, TimeframeProgress] = {}
        self.total_tasks = len(symbols) * len(timeframes)
        self.completed_tasks = 0
        self.on_update = on_update

        for tf in timeframes:
            self.timeframe_summary[tf] = TimeframeProgress(timeframe=tf, total_tasks=len(symbols))

        for s in symbols:
            sp = SymbolProgress(symbol=s, total_tasks=len(timeframes))
            for tf in timeframes:
                sp.timeframes[tf] = TimeframeProgress(timeframe=tf, total_tasks=1)
            self.symbols[s] = sp

    def mark_done(self, symbol: str, timeframe: str, rows: int, requests: int) -> None:
        if symbol not in self.symbols:
            return
        sp = self.symbols[symbol]
        tfp = sp.timeframes.get(timeframe)
        if tfp is None:
            return
        if tfp.done:
            return

        tfp.done = True
        tfp.completed_tasks = 1
        tfp.last_rows = rows
        tfp.last_requests = requests
        tfp.end_time = time.time()
        if tfp.start_time is None:
            tfp.start_time = tfp.end_time

        sp.completed_tasks += 1
        if sp.start_time is None:
            sp.start_time = tfp.start_time
        if sp.completed_tasks >= sp.total_tasks:
            sp.end_time = time.time()

        tfs = self.timeframe_summary.get(timeframe)
        if tfs:
            tfs.completed_tasks += 1
            tfs.last_rows = rows
            tfs.last_requests = requests
            if tfs.completed_tasks >= tfs.total_tasks:
                tfs.done = True
                tfs.end_time = time.time()

        self.completed_tasks += 1
        if self.on_update:
            try:
                self.on_update(symbol, timeframe, rows, requests)
            except Exception:
                pass

    def get_remaining(self) -> int:
        return max(0, self.total_tasks - self.completed_tasks)

# ---------------- HTTP FETCH ----------------
class BinanceFetcher:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._weight_info: Dict[str, int] = {}

    async def _sleep_backoff(self, attempt: int) -> None:
        base = 1.0
        max_sleep = base * (2 ** attempt)
        await asyncio.sleep(random.uniform(0, max_sleep))

    async def get_klines(self, symbol: str, tf: str, start_ts: int, limit: int) -> List:
        params = {'symbol': symbol, 'interval': tf, 'startTime': start_ts, 'limit': limit}
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with self.session.get(BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    headers = resp.headers
                    weight = headers.get('X-MBX-USED-WEIGHT')
                    if weight is not None:
                        try:
                            self._weight_info[symbol] = int(weight)
                        except Exception:
                            pass

                    if resp.status == 200:
                        return await resp.json()

                    if resp.status == 429:
                        retry_after = headers.get('Retry-After')
                        wait = int(retry_after) if retry_after and retry_after.isdigit() else 60
                        logger.warning(f"429 Rate limited for {symbol} {tf}. Retry after {wait}s (attempt {attempt}).")
                        await asyncio.sleep(wait + random.uniform(0, 1.0))
                        continue

                    if 500 <= resp.status < 600:
                        text = await resp.text()
                        logger.warning(f"Server error {resp.status} for {symbol} {tf}: {text[:200]} (attempt {attempt})")
                        if attempt < MAX_RETRIES:
                            await self._sleep_backoff(attempt)
                            continue
                        return []

                    text = await resp.text()
                    logger.error(f"HTTP {resp.status} for {symbol} {tf}: {text}")
                    return []
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {symbol} {tf} start={start_ts} (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    await self._sleep_backoff(attempt)
                    continue
                return []
            except aiohttp.ClientError as e:
                logger.warning(f"ClientError fetching {symbol} {tf}: {e} (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    await self._sleep_backoff(attempt)
                    continue
                return []
            except Exception as e:
                logger.exception(f"Unexpected error fetching {symbol} {tf}: {e}")
                return []
        return []

async def fetch_all_klines_for_symbol_tf(fetcher: BinanceFetcher, symbol: str, tf: str, start_ts: int, end_ts: Optional[int] = None, stop_event: Optional[asyncio.Event] = None) -> Tuple[List, int]:
    all_klines: List = []
    duration_ms = timeframe_to_ms(tf)
    limit = get_optimal_limit(tf)
    end_ts = end_ts or int(datetime.now().timestamp() * 1000)
    req_count = 0
    cur_start = start_ts

    while cur_start < end_ts:
        if stop_event is not None and stop_event.is_set():
            logger.info(f"Cancellation requested for {symbol} {tf}")
            break
        chunk = await fetcher.get_klines(symbol, tf, cur_start, limit)
        req_count += 1
        if not chunk:
            break
        all_klines.extend(chunk)
        last_ts = chunk[-1][0]
        next_start = last_ts + duration_ms
        if next_start <= cur_start:
            logger.warning(f"Stuck at same timestamp for {symbol} {tf} start={cur_start}; breaking to avoid infinite loop.")
            break
        cur_start = next_start
        await asyncio.sleep(REQUEST_DELAY + random.uniform(0, 0.05))
        if cur_start >= end_ts:
            break

    return all_klines, req_count

# ---------------- CSV UPDATE ----------------
async def update_symbol_tf(session: aiohttp.ClientSession, symbol: str, tf: str, stop_event: Optional[asyncio.Event] = None) -> Tuple[int, int]:
    Path(DATA_DIR).mkdir(exist_ok=True)
    csv_file = os.path.join(DATA_DIR, f"{symbol}_{tf}.csv")
    existing_df = pd.DataFrame()

    if os.path.exists(csv_file):
        try:
            existing_df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
            if not existing_df.empty:
                last_time = existing_df.index[-1]
                start_ts = int(last_time.timestamp() * 1000) + timeframe_to_ms(tf)
            else:
                start_ts = get_start_timestamp()
        except Exception as e:
            logger.warning(f"Error reading {csv_file}: {e}. Starting fresh.")
            existing_df = pd.DataFrame()
            start_ts = get_start_timestamp()
    else:
        start_ts = get_start_timestamp()

    current_ts = int(datetime.now().timestamp() * 1000)
    if start_ts >= current_ts:
        logger.info(f"[SKIP] {symbol} {tf} already up to date")
        return 0, 0

    fetcher = BinanceFetcher(session)
    all_klines, req_count = await fetch_all_klines_for_symbol_tf(fetcher, symbol, tf, start_ts, stop_event=stop_event)
    if not all_klines:
        logger.info(f"[NO DATA] {symbol} {tf}")
        return 0, req_count

    new_df = klines_to_df(all_klines)
    if new_df.empty:
        logger.info(f"[EMPTY] {symbol} {tf}")
        return 0, req_count

    if not existing_df.empty:
        try:
            existing_df.index = pd.to_datetime(existing_df.index)
        except Exception:
            existing_df = existing_df.copy()
            existing_df.index = pd.to_datetime(existing_df.index)
        combined_df = pd.concat([existing_df, new_df])
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    else:
        combined_df = new_df

    combined_df.sort_index(inplace=True)
    temp_file = csv_file + ".tmp"
    combined_df.to_csv(temp_file)
    os.replace(temp_file, csv_file)

    logger.info(f"[SUCCESS] {symbol} {tf} | New rows: {len(new_df)} | Requests: {req_count}")
    return len(new_df), req_count

# ---------------- ASYNC RUNNER ----------------
async def downloader_async_run(
    symbols: List[str],
    timeframes: List[str],
    stop_event: Optional[asyncio.Event],
    progress_model: ProgressModel,
    eta: ETAEstimator,
    on_update_callback: Optional[Callable[[str, str, str], None]] = None
) -> None:
    logger.info(f"Starting data fetch for {len(symbols)} symbols across {len(timeframes)} timeframes")

    connector = aiohttp.TCPConnector(limit=TCP_CONNECTOR_LIMIT, limit_per_host=TCP_CONNECTOR_LIMIT_PER_HOST)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def sem_task(symbol: str, tf: str):
            async with sem:
                if stop_event is not None and stop_event.is_set():
                    return
                start = time.time()
                rows, reqs = await update_symbol_tf(session, symbol, tf, stop_event=stop_event)
                duration = max(0.0, time.time() - start)
                eta.sample(duration)
                progress_model.mark_done(symbol, tf, rows, reqs)
                remaining = progress_model.get_remaining()
                eta_str = eta.get_human_readable(remaining)
                if on_update_callback:
                    on_update_callback(symbol, tf, eta_str)

        tasks = [asyncio.create_task(sem_task(sym, tf)) for sym in symbols for tf in timeframes]
        try:
            for coro in asyncio.as_completed(tasks):
                if stop_event is not None and stop_event.is_set():
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    break
                try:
                    await coro
                except asyncio.CancelledError:
                    logger.info("Task cancelled")
                except Exception as e:
                    logger.exception(f"Task failed: {e}")
        finally:
            # ensure any pending tasks are cancelled
            for t in tasks:
                if not t.done():
                    t.cancel()

    logger.info("Data fetch completed")

# ---------------- MAIN ----------------
def main():
    symbols = load_symbols()
    if not symbols:
        logger.error("No symbols found in symbols.txt! Exiting.")
        return
    
    logger.info(f"Loaded symbols: {symbols}")
    logger.info(f"Timeframes: {TIMEFRAMES}")
    
    stop_event = asyncio.Event()

    def on_update_callback(symbol: str, tf: str, eta_str: str):
        pass

    progress_model = ProgressModel(symbols, TIMEFRAMES)
    eta = ETAEstimator(alpha=0.18)

    try:
        asyncio.run(
            downloader_async_run(
                symbols,
                TIMEFRAMES,
                stop_event,
                progress_model,
                eta,
                on_update_callback
            )
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Stopping...")
        stop_event.set()

if __name__ == '__main__':
    main()
