# Binance Klines Downloader (PyQt6 GUI) 🚀

A desktop application for downloading historical candlestick (OHLCV) data from the Binance API across multiple symbols and timeframes — stored locally as CSV files.  

Features a modern PyQt6 interface, asynchronous data fetching with aiohttp, CSV merging, ETA estimation, and persistent app settings.

---

## 📌 Project Overview

This project provides a simple, user-friendly GUI for pulling historical market data from Binance.  
It handles:

- Asynchronous downloads for dozens of symbol/timeframe pairs  
- Intelligent recovery and incremental updates  
- CSV storage in a structured, merge-safe format  
- GUI progress tracking with individual and overall indicators  
- Built-in stopping, retry, backoff, and rate-limit handling  

The GUI ensures smooth usability while the backend performs efficient multi-task fetching without blocking.

---

## ⚙️ Features

### 🧩 Core Functionality
- 📥 Download OHLCV klines for any symbol & timeframe Binance provides  
- 🔁 Automatically resume from the last timestamp in existing CSVs  
- 💾 Saves CSV files under `data/<SYMBOL>_<TF>.csv`  
- 🔍 No duplicate rows, safe merges  

### 🧵 Concurrency & Reliability
- ⚡ Async downloader (aiohttp)  
- 🔀 Parallel tasks (configurable semaphore)  
- ⏱ Automatic exponential-backoff retry logic  
- 🚧 Handles HTTP 429 (rate limits) and Binance weight headers  

### 🖥 GUI Features (PyQt6)
- 📊 Tree-view progress for each (symbol, timeframe) pair  
- ⏳ Live ETA per task + overall progress bar  
- 📝 Log viewer  
- 🟢 Start, 🛑 Stop, and **Download All** options  
- 💾 Saves selected symbols/timeframes + window geometry via `QSettings`  

---

## 🧠 Tech Stack

- **Python 3.9+**  
- **PyQt6** — GUI  
- **aiohttp** — async HTTP client  
- **pandas / numpy** — data processing  
- **python-dateutil / pytz / tqdm** — utilities  

---

## 🛠 Installation & Setup

### 1️⃣ Clone the repository
```bash
git clone https://github.com/Arppittjaiin/Crypto-data-using-binance.git
cd Crypto-data-using-binance

```
2️⃣ Install Python dependencies
```
pip install -r requirements.txt
```
3️⃣ (Optional) Edit symbols

Modify symbols.txt to include one symbol per line:

BTCUSDT

ETHUSDT

SOLUSDT
...

🚀 Usage
Start the GUI
python binance_fetcher.py

During Runtime

Select one or more symbols

Select one or more timeframes

Click Start

Watch the progress bars + ETA update in real time

CSV outputs appear in the data/ folder:

data/BTCUSDT_1m.csv

data/ETHUSDT_1h.csv

If CSVs exist, the downloader continues from the last timestamp forward, ensuring efficient incremental updates.

🛠 How It Works (Internals)
📦 Main Components

MainWindow — PyQt6 interface

DownloaderThread (QThread) — runs async loop without freezing GUI

downloader_async_run — manages all tasks concurrently

BinanceFetcher — retries, handles rate limits, parses headers

update_symbol_tf — loads CSV, merges new rows, saves atomic temp file

🔗 Binance Endpoint Used
GET https://api.binance.com/api/v3/klines
  - symbol=<SYMBOL>
  - interval=<TF>
  - startTime=<ms>
  - limit=<n>

⏱ Timeframes (default)
1m, 5m, 15m, 30m, 1h, 4h, 8h, 12h, 1d, 1w, 1M

⚙️ Configuration

Tweak constants at the top of binance_fetcher.py:

YEARS_OF_DATA – how far back to fetch when no CSV exists

TIMEFRAMES – selectable timeframe list

MAX_CONCURRENT_REQUESTS – async concurrency

REQUEST_DELAY – delay between requests

TCP_CONNECTOR_LIMIT – aiohttp connections

BASE_URL – Binance endpoint

🧪 Troubleshooting
❗ Getting many 429 rate limits?

Reduce:

MAX_CONCURRENT_REQUESTS = 5

REQUEST_DELAY = 0.10

❗ CSV errors?

The app will automatically:

Log the error

Recreate CSV from fresh download.

❗ GUI freezing?

Ensure:

python binance_fetcher.py


is run with Python 3.9–3.12 + a clean virtual environment.

👤 Author

Arpit Jain (AJ)

🔒 License

Licensed under the MIT License.

Disclaimer:

This tool is for educational and research purposes. Always verify the accuracy of data before using it for trading decisions. The authors are not responsible for any financial losses.

Contributions are welcome! Feel free to:


Report bugs, suggest features, submit pull requests, improve documentation, and support


For issues or questions:

Check the errors.log for detailed error messages, review the troubleshooting section in this README, and open an issue in the repository.
