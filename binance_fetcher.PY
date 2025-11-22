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

# PyQt6
from PyQt6 import QtWidgets, QtCore
from PyQt6.QtCore import pyqtSignal, QThread, QSettings

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

# ---------------- SETTINGS MANAGER ----------------
class SettingsManager:
    def __init__(self, organization: str = "MyOrg", application: str = "BinanceDownloader"):
        self.qsettings = QSettings(organization, application)

    def save_symbols(self, symbols: List[str]) -> None:
        self.qsettings.setValue("symbols", symbols)

    def load_symbols(self) -> List[str]:
        v = self.qsettings.value("symbols", [])
        if isinstance(v, list):
            return [str(x) for x in v]
        if isinstance(v, str):
            return [v]
        return []

    def save_timeframes(self, tfs: List[str]) -> None:
        self.qsettings.setValue("timeframes", tfs)

    def load_timeframes(self) -> List[str]:
        v = self.qsettings.value("timeframes", [])
        if isinstance(v, list):
            return [str(x) for x in v]
        if isinstance(v, str):
            return [v]
        return []

    def save_geometry(self, geometry: bytes) -> None:
        self.qsettings.setValue("geometry", geometry)

    def load_geometry(self) -> Optional[bytes]:
        v = self.qsettings.value("geometry", None)
        return v if v is not None else None

    def save_window_state(self, state: bytes) -> None:
        self.qsettings.setValue("window_state", state)

    def load_window_state(self) -> Optional[bytes]:
        v = self.qsettings.value("window_state", None)
        return v if v is not None else None

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

    def get_symbol_progress(self, symbol: str) -> Tuple[int, int]:
        sp = self.symbols.get(symbol)
        if not sp:
            return 0, 0
        return sp.completed_tasks, sp.total_tasks

    def get_timeframe_progress(self, tf: str) -> Tuple[int, int]:
        tfs = self.timeframe_summary.get(tf)
        if not tfs:
            return 0, 0
        return tfs.completed_tasks, tfs.total_tasks

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
    on_tree_update: Optional[Callable[[str, str, str], None]] = None
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
                if on_tree_update:
                    try:
                        on_tree_update(symbol, tf, eta_str)
                    except Exception:
                        pass

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

    logger.info("Data fetch completed (async runner)")

# ---------------- WORKER THREAD ----------------
class DownloaderThread(QThread):
    # log_signal: simple log text
    log_signal = pyqtSignal(str)
    # tree_update_signal: symbol, timeframe, eta_str
    tree_update_signal = pyqtSignal(str, str, str)
    # per-task progress: symbol, timeframe, new_rows, requests
    single_progress_signal = pyqtSignal(str, str, int, int)
    finished_signal = pyqtSignal()

    def __init__(self, symbols: List[str], timeframes: List[str]):
        super().__init__()
        self.symbols = symbols
        self.timeframes = timeframes
        self._stop_flag = False

    def run(self) -> None:
        self.log_signal.emit("Downloader thread started")

        async def _runner():
            stop_event = asyncio.Event()
            progress_model = ProgressModel(self.symbols, self.timeframes, on_update=self._on_update)
            eta = ETAEstimator(alpha=0.18)

            def on_tree_update(symbol: str, tf: str, eta_str: str):
                # emit signal for GUI update
                self.tree_update_signal.emit(symbol, tf, eta_str)

            task = asyncio.create_task(downloader_async_run(self.symbols, self.timeframes, stop_event, progress_model, eta, on_tree_update=on_tree_update))

            while not task.done():
                if self._stop_flag:
                    self.log_signal.emit("Stop requested — signalling cancellation")
                    stop_event.set()
                await asyncio.sleep(0.2)

            try:
                await task
            except Exception as e:
                self.log_signal.emit(f"Downloader async runner error: {e}")

        try:
            asyncio.run(_runner())
        except Exception as e:
            self.log_signal.emit(f"Downloader thread error: {e}")
        finally:
            self.log_signal.emit("Downloader thread finished")
            self.finished_signal.emit()

    def _on_update(self, symbol: str, timeframe: str, rows: int, requests: int) -> None:
        # forward progress to GUI
        self.single_progress_signal.emit(symbol, timeframe, rows, requests)

    def request_stop(self) -> None:
        self._stop_flag = True

# ---------------- GUI ----------------
class ProgressItemWidget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QtWidgets.QHBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.pb = QtWidgets.QProgressBar()
        self.pb.setRange(0, 100)
        self.label = QtWidgets.QLabel("")
        self.layout.addWidget(self.pb, 1)
        self.layout.addWidget(self.label, 0)

    def set_busy(self):
        self.pb.setRange(0, 0)
        self.label.setText("working")

    def set_done(self, rows: int):
        self.pb.setRange(0, 1)
        self.pb.setValue(1)
        self.label.setText(f"done ({rows} rows)")

    def set_percent(self, pct: int, txt: str = ''):
        self.pb.setRange(0, 100)
        self.pb.setValue(int(pct))
        self.label.setText(txt)

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Binance Klines Downloader")
        self.resize(1100, 700)
        self.settings = SettingsManager()
        self._build_ui()
        self.worker: Optional[DownloaderThread] = None
        self._load_settings()

    def _build_ui(self):
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        main_layout = QtWidgets.QVBoxLayout(central)
        top_layout = QtWidgets.QHBoxLayout()

        # left: symbols and timeframes
        left_v = QtWidgets.QVBoxLayout()
        self.symbol_list = QtWidgets.QListWidget()
        self.symbol_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.MultiSelection)
        for s in load_symbols():
            self.symbol_list.addItem(QtWidgets.QListWidgetItem(s))
        left_v.addWidget(QtWidgets.QLabel("Symbols"))
        left_v.addWidget(self.symbol_list)

        self.tf_list = QtWidgets.QListWidget()
        self.tf_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.MultiSelection)
        for tf in TIMEFRAMES:
            self.tf_list.addItem(QtWidgets.QListWidgetItem(tf))
        left_v.addWidget(QtWidgets.QLabel("Timeframes"))
        left_v.addWidget(self.tf_list)
        top_layout.addLayout(left_v, 2)

        # center: tree view
        center_v = QtWidgets.QVBoxLayout()
        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderLabels(["Symbol / Timeframe", "Progress", "ETA"])
        self.tree.setColumnWidth(0, 300)
        center_v.addWidget(self.tree)
        top_layout.addLayout(center_v, 5)

        # right: controls and logs
        right_v = QtWidgets.QVBoxLayout()
        self.start_btn = QtWidgets.QPushButton("Start")
        self.stop_btn = QtWidgets.QPushButton("Stop")
        self.stop_btn.setEnabled(False)
        self.download_all_btn = QtWidgets.QPushButton("Download All")
        right_v.addWidget(self.start_btn)
        right_v.addWidget(self.stop_btn)
        right_v.addWidget(self.download_all_btn)

        self.overall_pb = QtWidgets.QProgressBar()
        right_v.addWidget(QtWidgets.QLabel("Overall Progress"))
        right_v.addWidget(self.overall_pb)

        self.log_view = QtWidgets.QPlainTextEdit()
        self.log_view.setReadOnly(True)
        right_v.addWidget(QtWidgets.QLabel("Logs"))
        right_v.addWidget(self.log_view, 1)
        top_layout.addLayout(right_v, 2)

        main_layout.addLayout(top_layout)
        self.status_label = QtWidgets.QLabel("Idle")
        main_layout.addWidget(self.status_label)

        # connections
        self.start_btn.clicked.connect(self.start_download)
        self.stop_btn.clicked.connect(self.stop_download)
        self.download_all_btn.clicked.connect(self.download_all)

    def _load_settings(self):
        syms = self.settings.load_symbols()
        tfs = self.settings.load_timeframes()

        if syms:
            for i in range(self.symbol_list.count()):
                item = self.symbol_list.item(i)
                item.setSelected(item.text() in syms)
        else:
            for i in range(self.symbol_list.count()):
                self.symbol_list.item(i).setSelected(True)

        if tfs:
            for i in range(self.tf_list.count()):
                item = self.tf_list.item(i)
                item.setSelected(item.text() in tfs)
        else:
            for i in range(self.tf_list.count()):
                self.tf_list.item(i).setSelected(True)

        geom = self.settings.load_geometry()
        if geom:
            self.restoreGeometry(geom)

        state = self.settings.load_window_state()
        if state:
            self.restoreState(state)

    def _save_settings(self):
        syms = [self.symbol_list.item(i).text() for i in range(self.symbol_list.count()) if self.symbol_list.item(i).isSelected()]
        tfs = [self.tf_list.item(i).text() for i in range(self.tf_list.count()) if self.tf_list.item(i).isSelected()]
        self.settings.save_symbols(syms)
        self.settings.save_timeframes(tfs)
        self.settings.save_geometry(self.saveGeometry())
        self.settings.save_window_state(self.saveState())

    def append_log(self, text: str) -> None:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.log_view.appendPlainText(f"[{ts}] {text}")

    def start_download(self):
        symbols = [self.symbol_list.item(i).text() for i in range(self.symbol_list.count()) if self.symbol_list.item(i).isSelected()]
        timeframes = [self.tf_list.item(i).text() for i in range(self.tf_list.count()) if self.tf_list.item(i).isSelected()]
        if not symbols or not timeframes:
            QtWidgets.QMessageBox.warning(self, "Selection Required", "Select at least one symbol and timeframe")
            return

        self._save_settings()

        # prepare tree
        self.tree.clear()
        self._item_map: Dict[Tuple[str, Optional[str]], QtWidgets.QTreeWidgetItem] = {}
        for s in symbols:
            top = QtWidgets.QTreeWidgetItem([s, '', ''])
            self.tree.addTopLevelItem(top)
            self._item_map[(s, None)] = top
            for tf in timeframes:
                child = QtWidgets.QTreeWidgetItem([tf, '', ''])
                top.addChild(child)
                widget = ProgressItemWidget()
                widget.set_busy()
                self.tree.setItemWidget(child, 1, widget)
                self._item_map[(s, tf)] = child

        # reset overall pb
        self.overall_total = len(symbols) * len(timeframes)
        self.overall_completed = 0
        self.overall_pb.setRange(0, self.overall_total)
        self.overall_pb.setValue(0)

        self.status_label.setText('Running')
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        # start worker
        self.worker = DownloaderThread(symbols, timeframes)
        self.worker.log_signal.connect(self.append_log)
        self.worker.single_progress_signal.connect(self.on_single_progress)
        self.worker.tree_update_signal.connect(self.on_tree_update)
        self.worker.finished_signal.connect(self.on_finished)
        self.worker.start()

    def download_all(self):
        syms = load_symbols()
        tfs = TIMEFRAMES
        # ensure UI selection mirrors this
        for i in range(self.symbol_list.count()):
            item = self.symbol_list.item(i)
            item.setSelected(item.text() in syms)
        for i in range(self.tf_list.count()):
            item = self.tf_list.item(i)
            item.setSelected(item.text() in tfs)
        self.append_log(f"Download All requested: {len(syms)} symbols x {len(tfs)} timeframes")
        self.start_download()

    def stop_download(self):
        if self.worker:
            self.append_log('Stop requested by user')
            self.worker.request_stop()
            self.stop_btn.setEnabled(False)

    def on_single_progress(self, symbol: str, tf: str, new_rows: int, requests: int):
        item = self._item_map.get((symbol, tf))
        if item is not None:
            widget: ProgressItemWidget = self.tree.itemWidget(item, 1)
            if widget:
                widget.set_done(new_rows)
        # update overall
        self.overall_completed += 1
        self.overall_pb.setValue(self.overall_completed)
        self.append_log(f"{symbol} {tf} -> rows={new_rows} requests={requests}")

    def on_tree_update(self, symbol: str, tf: str, eta_str: str):
        item = self._item_map.get((symbol, tf))
        if item is not None:
            item.setText(2, eta_str)
        parent = self._item_map.get((symbol, None))
        if parent is not None:
            # keep parent's ETA as last child's ETA (simple approach)
            parent.setText(2, eta_str if eta_str else '')

    def on_finished(self):
        self.append_log('Worker finished')
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.status_label.setText('Idle')

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            self.worker.request_stop()
            self.worker.wait(1000)
        self._save_settings()
        super().closeEvent(event)

# ---------------- ENTRYPOINT ----------------
def main_gui():
    app = QtWidgets.QApplication([])
    win = MainWindow()
    win.show()
    app.exec()

if __name__ == '__main__':
    main_gui()
