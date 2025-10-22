import os
TICKERS = ["BBVA.MC", "SAB.MC", "IBE.MC", "NTGY.MC", "TEF.MC", "CLNX.MC"]
START_DATE = "2020-01-01"
END_DATE = "2025-01-31"
STREAM_HOST = "localhost"
STREAM_PORT = 8080
BATCH_INTERVAL_SECS = 10
FX_TICKER = "EURUSD=X" 

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PARQUET_BATCH_PATH = os.path.join(_BASE_DIR, "../data/parquet/ibex/")
_PARQUET_STREAM_PATH = os.path.join(_BASE_DIR, "../data/parquet/ibex_stream/")