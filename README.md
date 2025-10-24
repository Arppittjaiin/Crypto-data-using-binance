# Binance Historical Data Fetcher

A high-performance, asynchronous Python tool for fetching and maintaining historical cryptocurrency data from Binance. Supports multiple symbols and timeframes with automatic updates and intelligent resume capabilities.

## Features

- **Asynchronous & Fast**: Concurrent API requests with configurable rate limiting
- **Multiple Timeframes**: 1m, 5m, 15m, 30m, 1h, 4h, 8h, 12h, 1d, 1w, 1M
- **Smart Resume**: Automatically continues from the last downloaded data point
- **Robust Error Handling**: Exponential backoff, retry logic, and rate limit detection
- **Progress Tracking**: Real-time progress bars for monitoring downloads
- **Data Integrity**: Atomic file writes and duplicate detection
- **Organized Storage**: Clean CSV files in a dedicated data directory

## Requirements

- Python 3.7+
- Dependencies (see `requirements.txt`)

## Installation

1. Clone or download this repository

2. Install dependencies:
```bash
pip install -r requirements.txt
```

Or manually:
```bash
pip install aiohttp pandas tqdm
```

## Configuration

### symbols.txt

Create a `symbols.txt` file with one trading pair per line:

```
BTCUSDT
ETHUSDT
BNBUSDT
ADAUSDT
SOLUSDT
XRPUSDT
```

If the file doesn't exist, the script will use default symbols: ADAUSDT, BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, BNBUSDT.

### Script Parameters

Edit these constants at the top of the script to customize behavior:

```python
YEARS_OF_DATA = 5                  # Years of historical data to fetch
MAX_CONCURRENT_REQUESTS = 10       # Concurrent API requests
REQUEST_DELAY = 0.02               # Delay between requests (seconds)
MAX_RETRIES = 3                    # Retry attempts for failed requests
DATA_DIR = "data"                  # Output directory for CSV files
```

## Usage

### First Run (Download Historical Data)

```bash
python binance_fetcher.py
```

This will:
- Create a `data/` directory
- Download 5 years of historical data for all symbols and timeframes
- Save data as CSV files: `data/BTCUSDT_1h.csv`, `data/ETHUSDT_1d.csv`, etc.

### Subsequent Runs (Update Data)

Simply run the same command:

```bash
python binance_fetcher.py
```

The script will:
- Detect existing CSV files
- Resume from the last timestamp
- Only download new/missing data
- Skip already up-to-date files

### Schedule Automatic Updates

**Linux/Mac (crontab):**
```bash
# Update every hour
0 * * * * cd /path/to/script && /usr/bin/python3 binance_fetcher.py

# Update every day at 2 AM
0 2 * * * cd /path/to/script && /usr/bin/python3 binance_fetcher.py
```

**Windows (Task Scheduler):**
Create a scheduled task to run `python binance_fetcher.py` at your desired interval.

## Output Format

CSV files are stored in the `data/` directory with the following structure:

```
data/
├── BTCUSDT_1m.csv
├── BTCUSDT_5m.csv
├── BTCUSDT_1h.csv
├── ETHUSDT_1h.csv
└── ...
```

### CSV Columns

Each CSV file contains:
- **timestamp** (index): DateTime of the candle
- **open**: Opening price
- **high**: Highest price
- **low**: Lowest price
- **close**: Closing price
- **volume**: Trading volume

Example:
```csv
timestamp, open, high, low, close, volume
2024-01-01 00:00:00,42150.50,42200.00,42100.25,42180.75,125.34
2024-01-01 01:00:00,42180.75,42250.00,42150.00,42220.50,98.76
```

## Logging

The script maintains two types of logs:

1. **Console Output**: Real-time progress and status messages
2. **errors.log**: Detailed error logs with timestamps

## Error Handling

The script handles common issues automatically:

- **Rate Limits**: Detects HTTP 429 and waits before retrying
- **Network Errors**: Exponential backoff with configurable retries
- **Timeouts**: 15-second timeout with automatic retry
- **Corrupted Files**: Falls back to fresh download if CSV is unreadable
- **Duplicate Data**: Automatically deduplicates by timestamp

## Performance Tips

1. **Adjust Concurrency**: Increase `MAX_CONCURRENT_REQUESTS` for faster downloads (max 20)
2. **Reduce Symbols**: Fewer symbols = faster completion
3. **Select Timeframes**: Remove unused timeframes from the `TIMEFRAMES` list
4. **Monitor Rate Limits**: Watch `errors.log` for rate limit warnings

## Troubleshooting

### "Rate limited" messages
- Reduce `MAX_CONCURRENT_REQUESTS`
- Increase `REQUEST_DELAY`

### Slow downloads
- Increase `MAX_CONCURRENT_REQUESTS` (up to 20)
- Check your internet connection
- Verify Binance API is accessible

### Missing data
- Check `errors.log` for specific errors
- Verify symbol names are correct (must be uppercase, e.g., BTCUSDT)
- Ensure Binance supports the symbol

### Script crashes
- Check Python version (3.7+ required)
- Verify all dependencies are installed
- Review `errors.log` for details

## API Limits

Binance API limits:
- **Weight**: 1200 requests per minute
- **Orders**: 10 orders per second
- **Raw Requests**: Varies by endpoint

This script is designed to stay well within limits, but aggressive settings may trigger rate limiting.

## Data Usage

Approximate data downloaded per symbol:
- 1m timeframe: ~2.6 million rows (5 years)
- 1h timeframe: ~43,800 rows (5 years)
- 1d timeframe: ~1,825 rows (5 years)

Total CSV size varies by symbol, but expect:
- 1m: 50-100 MB per symbol
- 1h: 2-5 MB per symbol
- 1d: 100-200 KB per symbol

## Advanced Usage

### Custom Date Range

Modify the `get_start_timestamp()` function:

```python
def get_start_timestamp():
    # Start from January 1, 2020
    return int(datetime(2020, 1, 1).timestamp() * 1000)
```

### Single Symbol/Timeframe

Modify the `main()` function:

```python
async def main():
    await update_symbol_tf("BTCUSDT", "1h")
```

### Export to Different Formats

After fetching, convert CSV to other formats:

```python
import pandas as pd

df = pd.read_csv("data/BTCUSDT_1h.csv", index_col=0, parse_dates=True)
df.to_parquet("data/BTCUSDT_1h.parquet")  # Parquet format
df.to_json("data/BTCUSDT_1h.json")        # JSON format
```

## License

This project is open source and available for any use.

## Disclaimer

This tool is for educational and research purposes. Always verify data accuracy before using it for trading decisions. The authors are not responsible for any financial losses.

## Contributing

Contributions are welcome! Feel free to:
- Report bugs
- Suggest features
- Submit pull requests
- Improve documentation

## Support

For issues or questions:
1. Check `errors.log` for detailed error messages
2. Review this README's troubleshooting section
3. Open an issue on the repository

---

**Happy Data Fetching! 📊**
