"""
OI Signal Generator
Creates datewise .db files with trading signals based on OI analysis.

Strategies:
- Strat-1 (Unwinding): call_oi_change - put_oi_change (every 1 min)
- Strat-2 (STC Opportunist): Spot movement + OI change conditions (every 1 min)
- Strat-3 (PCR Filter): PCR vs PCR_VWAP comparison (every 1 min)

Output: signals_{YYYYMMDD}.db with tables:
- meta: baseline info, ATM, config
- oi_data: timestamped OI/PCR raw data
- signal_unwinding: Strat-1 signals
- signal_stc_opportunist: Strat-2 signals
- signal_pcr_filter: Strat-3 signals
"""

import os
import sys
import sqlite3
import pandas as pd
import logging
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from io import StringIO
import threading
import signal as sig

# ==================== CONFIGURATION ====================

# ========== SELECT SYMBOL HERE ==========
SYMBOL = "NIFTY"  # Change to "SENSEX" for Sensex
# ========================================

# Symbol-specific configurations
SYMBOL_CONFIG = {
    "NIFTY": {
        "strike_interval": 50,      # Nifty has 50-point strikes
        "strikes_above_below": 7,   # Total 15 strikes (7 above + ATM + 7 below)
        "spot_movement_threshold": 40,  # Points for Strat-2
        "spot_movement_max": 100,
    },
    "SENSEX": {
        "strike_interval": 100,     # Sensex has 100-point strikes
        "strikes_above_below": 7,   # Total 15 strikes
        "spot_movement_threshold": 80,  # Sensex moves more, so higher threshold
        "spot_movement_max": 160,
    },
    "BANKNIFTY": {
        "strike_interval": 100,     # BankNifty has 100-point strikes
        "strikes_above_below": 7,
        "spot_movement_threshold": 100,
        "spot_movement_max": 200,
    },
}

# Validate symbol selection
if SYMBOL not in SYMBOL_CONFIG:
    raise ValueError(f"Invalid SYMBOL: {SYMBOL}. Choose from: {list(SYMBOL_CONFIG.keys())}")

# Get config for selected symbol
_config = SYMBOL_CONFIG[SYMBOL]
STRIKE_INTERVAL = _config["strike_interval"]
STRIKES_ABOVE_BELOW = _config["strikes_above_below"]
SPOT_MOVEMENT_THRESHOLD = _config["spot_movement_threshold"]
SPOT_MOVEMENT_MAX = _config["spot_movement_max"]

# Paths
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Backend directory - check local first, then fall back to Zerodha path
LOCAL_BACKEND_DIR = SCRIPT_DIR / "backend"
ZERODHA_BACKEND_DIR_ALT = Path(r"D:\omshree\ZerodhaCSVData\backend")

# Use local backend if it exists, otherwise use Zerodha path
if LOCAL_BACKEND_DIR.exists():
    ZERODHA_BACKEND_DIR = LOCAL_BACKEND_DIR
else:
    ZERODHA_BACKEND_DIR = ZERODHA_BACKEND_DIR_ALT

OUTPUT_DIR = SCRIPT_DIR / "signals_output_1.2"

# Strategy intervals (seconds)
SIGNAL_INTERVAL = 60  # 1 minute for all strategies

# Market hours
MARKET_OPEN_TIME = "09:15:00"
MARKET_CLOSE_TIME = "15:30:00"

# ATM Mode: "static" = fix ATM at morning open, "dynamic" = recalculate from current spot
ATM_MODE = "static"  # Options: "static", "dynamic"

# ==================== BATCH PROCESSING CONFIG ====================
# Set PROCESSING_MODE to "batch" to replay historical dates instead of running live.
# Add any YYYYMMDD dates you want to generate signals for to BATCH_DATES.
PROCESSING_MODE = "batch"   # "live" or "batch"

BATCH_DATES = [
    # Add each date you want to process (YYYYMMDD).
    # Required data for each date YYYYMMDD (DD=day, MM=month, YYYY=year):
    #   backend/DDMMYYYY/OPTIONS/SENSEX/  ← that day's intraday OI CSVs
    #   backend/DDMMYYYY/SPOT/SENSEX/     ← that day's spot CSV
    #   backend/PREV_DDMMYYYY/OPTIONS/SENSEX/  ← previous trading day's OI (for baseline)
    # "20260227",
    "20260226",
    "20260225",
    "20260224",
    "20260223",
    "20260220",
    "20260219",
    "20260218",
    "20260217",
    "20260216",
    "20260213",
    "20260212",
    "20260211",
    "20260210",
    "20260209",
    "20260206",
    "20260205",
    "20260204",
    "20260203",
    "20260202",
    "20260201",
    "20260130",
    "20260129",
    "20260128",
    "20260127",
    "20260123",
    "20260122",
    "20260121",
    "20260120",
    "20260119",
    "20260116",  
    "20260114",
    "20260113",
    "20260112",
    "20260109",
    "20260107",
    "20260106",
    "20260105",
    "20260102",
    "20260101",
    "20251231",
    "20251230"
]

# Where batch-mode DBs are written (should match SIGNALS_DIR in backtest_engine.py)
BATCH_OUTPUT_DIR = Path(
    r"D:\PrakulEditDaily\OI_SignalGenerator\historical_oi_signal_generator\signals_output_1.2"
)
# ================================================================

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# Global state
SHUTDOWN = threading.Event()
DB_LOCK = threading.Lock()

# ==================== UTILITY FUNCTIONS ====================

def get_previous_trading_day(from_date=None):
    """Get the previous trading day (skip weekends)"""
    if from_date is None:
        from_date = date.today()

    current = from_date - timedelta(days=1)
    max_attempts = 10

    for _ in range(max_attempts):
        # Skip weekends
        if current.weekday() >= 5:
            current = current - timedelta(days=1)
            continue

        # Check if data exists
        date_str = current.strftime("%d%m%Y")
        options_dir = ZERODHA_BACKEND_DIR / date_str / "OPTIONS" / SYMBOL
        if options_dir.exists():
            return current

        current = current - timedelta(days=1)

    raise FileNotFoundError(f"No trading data found in last {max_attempts} days")


def get_today_folder():
    """Get today's data folder path"""
    today_str = datetime.now().strftime("%d%m%Y")
    return ZERODHA_BACKEND_DIR / today_str


def get_expiry_from_folder(options_dir):
    """Extract expiry date from option file names in folder"""
    ce_files = list(options_dir.glob(f"*_{SYMBOL}_*_CE.csv"))
    if not ce_files:
        return None

    # Parse expiry from filename: YYYYMMDD_SYMBOL_EXPIRYDATE_STRIKE_CE.csv
    import re
    match = re.search(rf"_{SYMBOL}_(\d+)_", ce_files[0].name)
    if match:
        return match.group(1)
    return None


# ==================== DATA READING FUNCTIONS ====================

def read_last_row_efficient(csv_path, usecols=None):
    """Read only the last row of a CSV file efficiently (last 2KB)"""
    if not csv_path.exists():
        return None

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            header = f.readline().strip()

        with open(csv_path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 2048))
            tail = f.read().decode("utf-8", errors="ignore")

        lines = [l for l in tail.split("\n") if l.strip()]
        if len(lines) < 1:
            return None

        df = pd.read_csv(StringIO(header + "\n" + lines[-1]), usecols=usecols)
        return df.iloc[-1] if not df.empty else None
    except Exception as e:
        logger.debug(f"Error reading {csv_path}: {e}")
        return None


def get_spot_price(target_date=None):
    """Get the latest spot price from spot CSV"""
    if target_date is None:
        target_date = date.today()

    date_ddmmyyyy = target_date.strftime("%d%m%Y")
    date_yyyymmdd = target_date.strftime("%Y%m%d")

    spot_path = ZERODHA_BACKEND_DIR / date_ddmmyyyy / "SPOT" / SYMBOL / f"{date_yyyymmdd}_{SYMBOL}_SPOT.csv"

    if not spot_path.exists():
        return None, None

    row = read_last_row_efficient(spot_path, usecols=["recv_ist_iso", "spot_price"])
    if row is not None:
        return float(row["spot_price"]), row["recv_ist_iso"]
    return None, None


def calculate_atm(spot_price):
    """Calculate ATM strike from spot price"""
    return int(round(spot_price / STRIKE_INTERVAL) * STRIKE_INTERVAL)


def get_strikes_around_atm(atm_strike):
    """Get list of strikes around ATM"""
    return sorted([
        atm_strike + i * STRIKE_INTERVAL
        for i in range(-STRIKES_ABOVE_BELOW, STRIKES_ABOVE_BELOW + 1)
    ])


def get_oi_for_strikes(target_date, strikes, expiry):
    """Get current OI for given strikes"""
    date_ddmmyyyy = target_date.strftime("%d%m%Y")
    date_yyyymmdd = target_date.strftime("%Y%m%d")
    options_dir = ZERODHA_BACKEND_DIR / date_ddmmyyyy / "OPTIONS" / SYMBOL

    if not options_dir.exists():
        return []

    results = []
    for strike in strikes:
        ce_pattern = f"{date_yyyymmdd}_{SYMBOL}_{expiry}_{float(strike)}_CE.csv"
        pe_pattern = f"{date_yyyymmdd}_{SYMBOL}_{expiry}_{float(strike)}_PE.csv"

        ce_path = options_dir / ce_pattern
        pe_path = options_dir / pe_pattern

        ce_row = read_last_row_efficient(ce_path, usecols=["oi", "volume_traded"])
        pe_row = read_last_row_efficient(pe_path, usecols=["oi", "volume_traded"])

        if ce_row is not None and pe_row is not None:
            results.append({
                "strike": strike,
                "call_oi": int(float(ce_row.get("oi", 0) or 0)),
                "put_oi": int(float(pe_row.get("oi", 0) or 0)),
                "call_volume": int(float(ce_row.get("volume_traded", 0) or 0)),
                "put_volume": int(float(pe_row.get("volume_traded", 0) or 0)),
            })

    return results


def get_baseline_oi(yesterday_date, strikes, expiry):
    """Get baseline OI from previous day's closing (last row)"""
    date_ddmmyyyy = yesterday_date.strftime("%d%m%Y")
    date_yyyymmdd = yesterday_date.strftime("%Y%m%d")
    options_dir = ZERODHA_BACKEND_DIR / date_ddmmyyyy / "OPTIONS" / SYMBOL

    if not options_dir.exists():
        logger.warning(f"Yesterday's options dir not found: {options_dir}")
        return []

    # Get yesterday's expiry (may differ from today's)
    yesterday_expiry = get_expiry_from_folder(options_dir)
    if yesterday_expiry is None:
        yesterday_expiry = expiry

    results = []
    for strike in strikes:
        ce_pattern = f"{date_yyyymmdd}_{SYMBOL}_{yesterday_expiry}_{float(strike)}_CE.csv"
        pe_pattern = f"{date_yyyymmdd}_{SYMBOL}_{yesterday_expiry}_{float(strike)}_PE.csv"

        ce_path = options_dir / ce_pattern
        pe_path = options_dir / pe_pattern

        ce_row = read_last_row_efficient(ce_path, usecols=["oi"])
        pe_row = read_last_row_efficient(pe_path, usecols=["oi"])

        call_oi = int(float(ce_row.get("oi", 0) or 0)) if ce_row is not None else 0
        put_oi = int(float(pe_row.get("oi", 0) or 0)) if pe_row is not None else 0

        results.append({
            "strike": strike,
            "call_oi": call_oi,
            "put_oi": put_oi,
        })

    return results


# ==================== CALCULATION FUNCTIONS ====================

def calculate_oi_changes(current_oi, baseline_oi):
    """Calculate OI changes from baseline"""
    baseline_dict = {row["strike"]: row for row in baseline_oi}

    results = []
    for current in current_oi:
        strike = current["strike"]
        baseline = baseline_dict.get(strike, {"call_oi": 0, "put_oi": 0})

        results.append({
            **current,
            "call_oi_change": current["call_oi"] - baseline["call_oi"],
            "put_oi_change": current["put_oi"] - baseline["put_oi"],
        })

    return results


def calculate_totals(oi_data):
    """Calculate total OI and volume across all strikes"""
    return {
        "total_call_oi": sum(row.get("call_oi", 0) for row in oi_data),
        "total_put_oi": sum(row.get("put_oi", 0) for row in oi_data),
        "total_call_volume": sum(row.get("call_volume", 0) for row in oi_data),
        "total_put_volume": sum(row.get("put_volume", 0) for row in oi_data),
        "total_call_oi_change": sum(row.get("call_oi_change", 0) for row in oi_data),
        "total_put_oi_change": sum(row.get("put_oi_change", 0) for row in oi_data),
    }


def calculate_pcr(total_put_oi, total_call_oi):
    """Calculate Put-Call Ratio"""
    if total_call_oi == 0:
        return 0.0
    return round(total_put_oi / total_call_oi, 4)


# ==================== DATABASE FUNCTIONS ====================

def get_db_path(target_date=None):
    """Get path for the signal database for a given date (defaults to today for live mode)."""
    if target_date is None:
        target_date = date.today()
        out_dir = OUTPUT_DIR
    else:
        out_dir = BATCH_OUTPUT_DIR if PROCESSING_MODE == "batch" else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    date_str = target_date.strftime("%Y%m%d")
    return out_dir / f"signals_{SYMBOL}_{date_str}.db"


def init_database(db_path):
    """Initialize database with required tables"""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Meta table for baseline info and config
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT
            )
        """)

        # OI data table (shared intermediate data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS oi_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                spot_price REAL,
                atm_strike INTEGER,
                total_call_oi INTEGER,
                total_put_oi INTEGER,
                total_call_volume INTEGER,
                total_put_volume INTEGER,
                total_call_oi_change INTEGER,
                total_put_oi_change INTEGER,
                pcr REAL,
                pcr_vwap REAL,
                net_oi_change INTEGER
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_oi_data_timestamp ON oi_data(timestamp)")

        # Strat-1: Unwinding signal
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_unwinding (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                signal_value INTEGER,
                net_oi_change INTEGER,
                call_oi_change INTEGER,
                put_oi_change INTEGER
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_unwinding_timestamp ON signal_unwinding(timestamp)")

        # Strat-2: STC Opportunist signal
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_stc_opportunist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                signal_value INTEGER,
                net_oi_change INTEGER,
                spot_change REAL,
                call_oi_change INTEGER,
                put_oi_change INTEGER
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stc_timestamp ON signal_stc_opportunist(timestamp)")

        # Strat-3: PCR Filter signal
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_pcr_filter (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                signal_value INTEGER,
                pcr REAL,
                pcr_vwap REAL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pcr_filter_timestamp ON signal_pcr_filter(timestamp)")

        conn.commit()
        logger.info(f"Database initialized: {db_path}")


def save_meta(conn, key, value):
    """Save metadata to database"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO meta (key, value, updated_at)
        VALUES (?, ?, ?)
    """, (key, str(value), datetime.now().isoformat()))
    conn.commit()


def get_meta(conn, key):
    """Get metadata from database"""
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else None


def save_oi_data(conn, data):
    """Save OI data to database"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO oi_data (
            timestamp, spot_price, atm_strike,
            total_call_oi, total_put_oi,
            total_call_volume, total_put_volume,
            total_call_oi_change, total_put_oi_change,
            pcr, pcr_vwap, net_oi_change
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["timestamp"],
        data["spot_price"],
        data["atm_strike"],
        data["total_call_oi"],
        data["total_put_oi"],
        data["total_call_volume"],
        data["total_put_volume"],
        data["total_call_oi_change"],
        data["total_put_oi_change"],
        data["pcr"],
        data["pcr_vwap"],
        data["net_oi_change"],
    ))
    conn.commit()


def save_signal_unwinding(conn, timestamp, signal_value, net_oi_change, call_oi_change, put_oi_change):
    """Save Strat-1 signal"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO signal_unwinding (timestamp, signal_value, net_oi_change, call_oi_change, put_oi_change)
        VALUES (?, ?, ?, ?, ?)
    """, (timestamp, signal_value, net_oi_change, call_oi_change, put_oi_change))
    conn.commit()


def save_signal_stc(conn, timestamp, signal_value, net_oi_change, spot_change, call_oi_change, put_oi_change):
    """Save Strat-2 signal"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO signal_stc_opportunist (timestamp, signal_value, net_oi_change, spot_change, call_oi_change, put_oi_change)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (timestamp, signal_value, net_oi_change, spot_change, call_oi_change, put_oi_change))
    conn.commit()


def save_signal_pcr_filter(conn, timestamp, signal_value, pcr, pcr_vwap):
    """Save Strat-3 signal"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO signal_pcr_filter (timestamp, signal_value, pcr, pcr_vwap)
        VALUES (?, ?, ?, ?)
    """, (timestamp, signal_value, pcr, pcr_vwap))
    conn.commit()


# ==================== PCR VWAP CALCULATION ====================

class PCRVWAPCalculator:
    """
    Calculates PCR VWAP (Volume Weighted Average PCR)

    Formula:
        PCR_VWAP = Σ(PCR_i × ΔVolume_i) / Σ(ΔVolume_i)

    Where:
        - PCR_i = Put OI / Call OI at timestamp i
        - ΔVolume_i = Incremental volume at timestamp i (current cumulative - previous cumulative)
        - Σ = Sum from market open to current time

    This gives more weight to PCR values during high-volume periods,
    making it a better indicator of where the "real" PCR action is.

    Note: Exchange provides cumulative volume (total traded so far today).
    We must calculate incremental volume = current_cumulative - previous_cumulative.
    """

    def __init__(self):
        self.cumulative_pcr_x_volume = 0.0  # Σ(PCR × ΔVolume)
        self.cumulative_volume = 0          # Σ(ΔVolume)
        self.last_total_volume = 0          # Previous cumulative volume (to calculate increment)
        self.update_count = 0               # Number of updates for debugging

    def reset(self):
        """Reset for new day"""
        self.cumulative_pcr_x_volume = 0.0
        self.cumulative_volume = 0
        self.last_total_volume = 0
        self.update_count = 0

    def update(self, pcr, current_cumulative_volume):
        """
        Update VWAP with new data point

        Args:
            pcr: Current PCR value (Put OI / Call OI)
            current_cumulative_volume: Total volume traded so far today (from exchange)
        """
        if current_cumulative_volume <= 0 or pcr <= 0:
            return

        # Calculate INCREMENTAL volume (new trades since last update)
        incremental_volume = current_cumulative_volume - self.last_total_volume

        # Update the tracker for next iteration
        self.last_total_volume = current_cumulative_volume

        # Only add to VWAP if there was new volume
        if incremental_volume > 0:
            self.cumulative_pcr_x_volume += pcr * incremental_volume
            self.cumulative_volume += incremental_volume
            self.update_count += 1

    def get_vwap(self):
        """Get current PCR VWAP"""
        if self.cumulative_volume > 0:
            return round(self.cumulative_pcr_x_volume / self.cumulative_volume, 4)
        return 0.0

    def get_stats(self):
        """Get debug stats"""
        return {
            'updates': self.update_count,
            'total_incremental_volume': self.cumulative_volume,
            'last_cumulative_volume': self.last_total_volume,
            'vwap': self.get_vwap()
        }


# ==================== BATCH DATA STORE ====================

class BatchDataStore:
    """
    Preloads all Zerodha CSV data for a historical date into memory so that
    every timestamp lookup is O(log N) instead of re-reading files each tick.

    Usage:
        store = BatchDataStore(date(2026, 2, 25))
        spot  = store.get_spot_at(datetime(2026, 2, 25, 10, 30))
        oi    = store.get_oi_at(datetime(2026, 2, 25, 10, 30), strikes, expiry)
    """

    def __init__(self, target_date):
        self.target_date   = target_date
        self.date_ddmmyyyy = target_date.strftime("%d%m%Y")   # "25022026"
        self.date_yyyymmdd = target_date.strftime("%Y%m%d")   # "20260225"
        self._cache        = {}   # {csv_path_str: pd.DataFrame indexed by naive-IST datetime}

    def _load(self, csv_path):
        """Load a CSV into a DataFrame indexed by naive IST timestamps (cached)."""
        key = str(csv_path)
        if key in self._cache:
            return self._cache[key]
        try:
            df = pd.read_csv(csv_path)
            if "recv_ist_iso" not in df.columns or df.empty:
                self._cache[key] = pd.DataFrame()
                return self._cache[key]
            # Parse tz-aware IST timestamps, strip tz to get naive IST datetimes
            df["recv_ist_iso"] = pd.to_datetime(df["recv_ist_iso"]).apply(
                lambda x: x.replace(tzinfo=None) if pd.notna(x) else pd.NaT
            )
            df = df.dropna(subset=["recv_ist_iso"])
            df = df.sort_values("recv_ist_iso").set_index("recv_ist_iso")
            self._cache[key] = df
            logger.debug(f"[BatchDataStore] Loaded {len(df)} rows: {csv_path.name}")
        except Exception as e:
            logger.debug(f"[BatchDataStore] Failed to load {csv_path}: {e}")
            self._cache[key] = pd.DataFrame()
        return self._cache[key]

    def get_row_at(self, csv_path, ts, cols=None):
        """Return the last available row with recv_ist_iso <= ts, or None."""
        df = self._load(csv_path)
        if df.empty:
            return None
        if cols:
            available = [c for c in cols if c in df.columns]
            df = df[available] if available else df
        subset = df.loc[:ts]
        return subset.iloc[-1] if not subset.empty else None

    def get_spot_at(self, ts):
        """Return spot price at or just before ts, or None."""
        spot_path = (
            ZERODHA_BACKEND_DIR / self.date_ddmmyyyy / "SPOT" / SYMBOL
            / f"{self.date_yyyymmdd}_{SYMBOL}_SPOT.csv"
        )
        row = self.get_row_at(spot_path, ts, cols=["spot_price"])
        return float(row["spot_price"]) if row is not None else None

    def get_oi_at(self, ts, strikes, expiry):
        """Return OI snapshot for the given strikes at or just before ts."""
        options_dir = ZERODHA_BACKEND_DIR / self.date_ddmmyyyy / "OPTIONS" / SYMBOL
        if not options_dir.exists():
            return []
        results = []
        for strike in strikes:
            ce_path = options_dir / f"{self.date_yyyymmdd}_{SYMBOL}_{expiry}_{float(strike)}_CE.csv"
            pe_path = options_dir / f"{self.date_yyyymmdd}_{SYMBOL}_{expiry}_{float(strike)}_PE.csv"
            ce_row = self.get_row_at(ce_path, ts, cols=["oi", "volume_traded"])
            pe_row = self.get_row_at(pe_path, ts, cols=["oi", "volume_traded"])
            if ce_row is not None and pe_row is not None:
                results.append({
                    "strike":       strike,
                    "call_oi":      int(float(ce_row.get("oi", 0) or 0)),
                    "put_oi":       int(float(pe_row.get("oi", 0) or 0)),
                    "call_volume":  int(float(ce_row.get("volume_traded", 0) or 0)),
                    "put_volume":   int(float(pe_row.get("volume_traded", 0) or 0)),
                })
        return results


# ==================== SIGNAL GENERATOR CLASS ====================

class OISignalGenerator:
    """Main signal generator class"""

    def __init__(self):
        self.db_path = None
        self.baseline_oi = None
        self.baseline_atm = None
        self.baseline_strikes = None  # Strikes for baseline (yesterday's ATM)
        self.expiry = None
        self.yesterday_date = None
        self.reference_spot = None  # Spot at start for Strat-2
        self.pcr_vwap_calc = PCRVWAPCalculator()
        # Static ATM mode: fix ATM at morning and use throughout the day
        self.static_atm = None
        self.static_strikes = None

    def initialize(self):
        """Initialize the signal generator"""
        logger.info("=" * 60)
        logger.info("OI SIGNAL GENERATOR - Initializing")
        logger.info("=" * 60)

        # Get database
        self.db_path = get_db_path()
        init_database(self.db_path)

        # Get yesterday's date for baseline
        self.yesterday_date = get_previous_trading_day()
        logger.info(f"Baseline date: {self.yesterday_date.strftime('%Y-%m-%d')}")

        # Get yesterday's closing spot to determine ATM for BASELINE strikes
        yesterday_spot, _ = get_spot_price(self.yesterday_date)
        if yesterday_spot is None:
            raise RuntimeError("Could not get yesterday's spot price for baseline")

        self.baseline_atm = calculate_atm(yesterday_spot)
        self.baseline_strikes = get_strikes_around_atm(self.baseline_atm)
        logger.info(f"Yesterday's spot: {yesterday_spot}, Baseline ATM: {self.baseline_atm}")
        logger.info(f"Baseline strikes: {self.baseline_strikes[0]} to {self.baseline_strikes[-1]}")

        # Get today's expiry
        today_options_dir = get_today_folder() / "OPTIONS" / SYMBOL
        self.expiry = get_expiry_from_folder(today_options_dir)
        if self.expiry is None:
            raise RuntimeError("Could not determine today's expiry")
        logger.info(f"Today's expiry: {self.expiry}")

        # Get baseline OI from yesterday (using YESTERDAY's ATM strikes)
        self.baseline_oi = get_baseline_oi(self.yesterday_date, self.baseline_strikes, self.expiry)
        if not self.baseline_oi:
            raise RuntimeError("Could not get baseline OI")

        baseline_totals = calculate_totals(self.baseline_oi)
        logger.info(f"Baseline Call OI: {baseline_totals['total_call_oi']:,}")
        logger.info(f"Baseline Put OI: {baseline_totals['total_put_oi']:,}")

        # Get initial spot for Strat-2 reference and static ATM
        self.reference_spot, _ = get_spot_price()
        if self.reference_spot:
            current_atm = calculate_atm(self.reference_spot)
            current_strikes = get_strikes_around_atm(current_atm)
            logger.info(f"Today's spot: {self.reference_spot}, Today's ATM: {current_atm}")
            logger.info(f"Today's strikes: {current_strikes[0]} to {current_strikes[-1]}")

            # Set static ATM for static mode (fixed throughout the day)
            self.static_atm = current_atm
            self.static_strikes = current_strikes
            logger.info(f"ATM Mode: {ATM_MODE.upper()}")
            if ATM_MODE == "static":
                logger.info(f"Static ATM locked at: {self.static_atm}")

        # Save metadata
        with sqlite3.connect(self.db_path) as conn:
            save_meta(conn, "symbol", SYMBOL)
            save_meta(conn, "baseline_date", self.yesterday_date.isoformat())
            save_meta(conn, "baseline_atm", self.baseline_atm)
            save_meta(conn, "expiry", self.expiry)
            save_meta(conn, "baseline_strikes", ",".join(map(str, self.baseline_strikes)))
            save_meta(conn, "baseline_call_oi", baseline_totals["total_call_oi"])
            save_meta(conn, "baseline_put_oi", baseline_totals["total_put_oi"])
            save_meta(conn, "atm_mode", ATM_MODE)
            save_meta(conn, "static_atm", self.static_atm)
            save_meta(conn, "static_strikes", ",".join(map(str, self.static_strikes)) if self.static_strikes else "")

        logger.info("Initialization complete!")
        logger.info("=" * 60)

    def generate_signals(self):
        """Generate all signals for current timestamp"""
        timestamp = datetime.now().isoformat()

        # Get current spot
        spot_price, _ = get_spot_price()
        if spot_price is None:
            logger.warning("Could not get current spot price")
            return

        # Calculate ATM and strikes based on ATM_MODE config
        if ATM_MODE == "static":
            # Use fixed ATM from morning (stable, no jumps)
            current_atm = self.static_atm
            current_strikes = self.static_strikes
        else:
            # Dynamic: recalculate ATM from current spot (follows price but can jump)
            current_atm = calculate_atm(spot_price)
            current_strikes = get_strikes_around_atm(current_atm)

        # Get current OI using selected strikes
        current_oi = get_oi_for_strikes(date.today(), current_strikes, self.expiry)
        if not current_oi:
            logger.warning("Could not get current OI data")
            return

        # Calculate current totals (sum of OI for today's strikes)
        current_totals = calculate_totals(current_oi)

        # For NET OI CHANGE (summary metrics), dashboard uses:
        # - Current: today's strikes around today's ATM
        # - Baseline: YESTERDAY's strikes around YESTERDAY's ATM
        # This is stored in self.baseline_oi (calculated during init with baseline_strikes)
        baseline_totals = calculate_totals(self.baseline_oi)

        # Calculate total OI changes (for summary/net change)
        # Uses: current (today's ATM strikes) vs baseline (yesterday's ATM strikes)
        total_call_oi_change = current_totals["total_call_oi"] - baseline_totals["total_call_oi"]
        total_put_oi_change = current_totals["total_put_oi"] - baseline_totals["total_put_oi"]

        # Calculate PCR (uses current OI totals)
        pcr = calculate_pcr(current_totals["total_put_oi"], current_totals["total_call_oi"])

        # Update PCR VWAP
        total_volume = current_totals["total_call_volume"] + current_totals["total_put_volume"]
        self.pcr_vwap_calc.update(pcr, total_volume)
        pcr_vwap = self.pcr_vwap_calc.get_vwap()

        # Calculate net OI change (call change - put change)
        net_oi_change = total_call_oi_change - total_put_oi_change

        # Spot change for Strat-2
        spot_change = spot_price - self.reference_spot if self.reference_spot else 0

        with DB_LOCK:
            with sqlite3.connect(self.db_path) as conn:
                # Save OI data
                save_oi_data(conn, {
                    "timestamp": timestamp,
                    "spot_price": spot_price,
                    "atm_strike": current_atm,
                    "total_call_oi": current_totals["total_call_oi"],
                    "total_put_oi": current_totals["total_put_oi"],
                    "total_call_volume": current_totals["total_call_volume"],
                    "total_put_volume": current_totals["total_put_volume"],
                    "total_call_oi_change": total_call_oi_change,
                    "total_put_oi_change": total_put_oi_change,
                    "pcr": pcr,
                    "pcr_vwap": pcr_vwap,
                    "net_oi_change": net_oi_change,
                })

                # Strat-1: Unwinding signal
                # net_oi_change = call_oi_change - put_oi_change
                # Signal 1 = positive net change (bullish, trades below ATM)
                # Signal 0 = negative net change (bearish, trades above ATM)
                strat1_signal = 1 if net_oi_change > 0 else 0
                save_signal_unwinding(
                    conn, timestamp, strat1_signal, net_oi_change,
                    total_call_oi_change, total_put_oi_change
                )

                # Strat-2: STC Opportunist signal
                # 0 = no signal
                # 1 = spot down 40-50 pts AND both OI changes positive (trades above ATM)
                # 2 = spot up 40-50 pts AND both OI changes positive (trades below ATM)
                strat2_signal = 0
                if SPOT_MOVEMENT_THRESHOLD <= abs(spot_change) <= SPOT_MOVEMENT_MAX:
                    if total_call_oi_change > 0 and total_put_oi_change > 0:
                        if spot_change < 0:  # Spot went down
                            strat2_signal = 1
                        else:  # Spot went up
                            strat2_signal = 2

                save_signal_stc(
                    conn, timestamp, strat2_signal, net_oi_change, spot_change,
                    total_call_oi_change, total_put_oi_change
                )

                # Strat-3: PCR Filter signal
                # 0 = entry eligible (PCR <= PCR_VWAP)
                # 1 = entry NOT eligible (PCR > PCR_VWAP)
                strat3_signal = 1 if pcr > pcr_vwap else 0
                save_signal_pcr_filter(conn, timestamp, strat3_signal, pcr, pcr_vwap)

        # Log summary
        logger.info(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Spot: {spot_price:.0f} (ATM: {current_atm}) | "
            f"CE OI: {current_totals['total_call_oi']:,} | "
            f"PE OI: {current_totals['total_put_oi']:,} | "
            f"PCR: {pcr:.3f} | "
            f"PCR VWAP: {pcr_vwap:.3f} | "
            f"Signals: S1={strat1_signal:+,} S2={strat2_signal} S3={strat3_signal}"
        )

    def run(self):
        """Main run loop with market hours awareness"""

        # Wait for market open if started before 9:15 AM
        now = datetime.now()
        market_open = now.replace(
            hour=int(MARKET_OPEN_TIME[:2]),
            minute=int(MARKET_OPEN_TIME[3:5]),
            second=int(MARKET_OPEN_TIME[6:8]),
            microsecond=0
        )
        market_close = now.replace(
            hour=int(MARKET_CLOSE_TIME[:2]),
            minute=int(MARKET_CLOSE_TIME[3:5]),
            second=int(MARKET_CLOSE_TIME[6:8]),
            microsecond=0
        )

        # If already past market close, don't start
        if now >= market_close:
            logger.info(f"Market is already closed ({MARKET_CLOSE_TIME}). Exiting.")
            return

        # If before market open, wait
        if now < market_open:
            wait_seconds = (market_open - now).total_seconds()
            logger.info(f"Market not open yet. Waiting until {MARKET_OPEN_TIME} ({wait_seconds:.0f}s)...")
            # Wait in small intervals so we can still respond to Ctrl+C
            while not SHUTDOWN.is_set() and datetime.now() < market_open:
                SHUTDOWN.wait(min(5, (market_open - datetime.now()).total_seconds()))
            if SHUTDOWN.is_set():
                logger.info("Shutdown during pre-market wait.")
                return
            logger.info("Market open! Starting signal generation.")

        logger.info("Starting signal generation loop...")
        logger.info(f"Signal interval: {SIGNAL_INTERVAL} seconds")
        logger.info(f"Auto-shutdown at: {MARKET_CLOSE_TIME}")

        while not SHUTDOWN.is_set():
            # Check if market has closed
            if datetime.now() >= market_close:
                logger.info(f"Market closed ({MARKET_CLOSE_TIME}). Auto-shutting down.")
                break

            try:
                self.generate_signals()
            except Exception as e:
                logger.error(f"Error generating signals: {e}")
                import traceback
                traceback.print_exc()

            # Wait for next interval
            SHUTDOWN.wait(SIGNAL_INTERVAL)

        logger.info("Signal generator stopped.")

    # ==================== BATCH METHODS ====================

    def initialize_batch(self, target_date, store):
        """
        Initialise for a specific historical date using preloaded BatchDataStore.
        Mirrors initialize() but:
          - uses target_date instead of date.today()
          - gets the opening spot from store (first available at 9:15)
          - baseline OI still reads yesterday's last-row (same as live mode)
        """
        logger.info("=" * 60)
        logger.info(f"OI SIGNAL GENERATOR (BATCH) - {target_date.strftime('%Y-%m-%d')}")
        logger.info("=" * 60)

        self.db_path = get_db_path(target_date)
        init_database(self.db_path)

        # Previous trading day → baseline OI
        self.yesterday_date = get_previous_trading_day(target_date)
        logger.info(f"Baseline date: {self.yesterday_date.strftime('%Y-%m-%d')}")

        yesterday_spot, _ = get_spot_price(self.yesterday_date)
        if yesterday_spot is None:
            raise RuntimeError(f"Could not get baseline spot for {self.yesterday_date}")

        self.baseline_atm     = calculate_atm(yesterday_spot)
        self.baseline_strikes = get_strikes_around_atm(self.baseline_atm)
        logger.info(f"Yesterday spot: {yesterday_spot}, Baseline ATM: {self.baseline_atm}")
        logger.info(f"Baseline strikes: {self.baseline_strikes[0]} to {self.baseline_strikes[-1]}")

        # Today's expiry – filter by the target date's file prefix so mixed-date folders work
        import re as _re
        date_ddmmyyyy     = target_date.strftime("%d%m%Y")
        today_options_dir = ZERODHA_BACKEND_DIR / date_ddmmyyyy / "OPTIONS" / SYMBOL
        ce_files = sorted(today_options_dir.glob(f"{store.date_yyyymmdd}_{SYMBOL}_*_CE.csv"))
        self.expiry = None
        if ce_files:
            m = _re.search(rf"_{SYMBOL}_(\d+)_", ce_files[0].name)
            if m:
                self.expiry = m.group(1)
        if self.expiry is None:
            # Fall back to folder-wide scan
            self.expiry = get_expiry_from_folder(today_options_dir)
        if self.expiry is None:
            raise RuntimeError(f"Could not determine expiry for {target_date}")
        logger.info(f"Expiry: {self.expiry}")

        # Baseline OI (yesterday's closing OI, same as live mode)
        self.baseline_oi = get_baseline_oi(self.yesterday_date, self.baseline_strikes, self.expiry)
        if not self.baseline_oi:
            raise RuntimeError("Could not get baseline OI")

        baseline_totals = calculate_totals(self.baseline_oi)
        logger.info(f"Baseline Call OI: {baseline_totals['total_call_oi']:,}")
        logger.info(f"Baseline Put OI:  {baseline_totals['total_put_oi']:,}")

        # Opening spot (9:15 of target date) for reference_spot and static ATM.
        # Fall back to first available tick if no data at exactly 09:15.
        market_open_ts = datetime.combine(target_date, datetime.strptime(MARKET_OPEN_TIME, "%H:%M:%S").time())
        self.reference_spot = store.get_spot_at(market_open_ts)
        if self.reference_spot is None:
            # Use the earliest available spot tick in the SPOT file
            spot_path = (
                ZERODHA_BACKEND_DIR / store.date_ddmmyyyy / "SPOT" / SYMBOL
                / f"{store.date_yyyymmdd}_{SYMBOL}_SPOT.csv"
            )
            df_spot = store._load(spot_path)
            if not df_spot.empty and "spot_price" in df_spot.columns:
                self.reference_spot = float(df_spot["spot_price"].iloc[0])
                logger.warning(
                    f"No spot at 09:15 – using first available tick "
                    f"({df_spot.index[0].strftime('%H:%M:%S')}: {self.reference_spot:.2f})"
                )
        if self.reference_spot:
            current_atm     = calculate_atm(self.reference_spot)
            current_strikes = get_strikes_around_atm(current_atm)
            logger.info(f"Opening spot: {self.reference_spot}, ATM: {current_atm}")
            logger.info(f"Opening strikes: {current_strikes[0]} to {current_strikes[-1]}")
            self.static_atm     = current_atm
            self.static_strikes = current_strikes
            logger.info(f"ATM Mode: {ATM_MODE.upper()}")
            if ATM_MODE == "static":
                logger.info(f"Static ATM locked at: {self.static_atm}")

        # Reset PCR VWAP accumulator for this date
        self.pcr_vwap_calc.reset()

        # Save metadata (same schema as live mode)
        with sqlite3.connect(self.db_path) as conn:
            save_meta(conn, "symbol",           SYMBOL)
            save_meta(conn, "baseline_date",    self.yesterday_date.isoformat())
            save_meta(conn, "baseline_atm",     self.baseline_atm)
            save_meta(conn, "expiry",           self.expiry)
            save_meta(conn, "baseline_strikes", ",".join(map(str, self.baseline_strikes)))
            save_meta(conn, "baseline_call_oi", baseline_totals["total_call_oi"])
            save_meta(conn, "baseline_put_oi",  baseline_totals["total_put_oi"])
            save_meta(conn, "atm_mode",         ATM_MODE)
            save_meta(conn, "static_atm",       self.static_atm)
            save_meta(conn, "static_strikes",
                      ",".join(map(str, self.static_strikes)) if self.static_strikes else "")

        logger.info("Batch initialization complete!")
        logger.info("=" * 60)

    def generate_signals_batch(self, ts, store):
        """
        Generate all signals for a specific historical timestamp.
        Mirrors generate_signals() but reads OI/spot from BatchDataStore
        instead of live CSV last-row reads.
        """
        timestamp = ts.isoformat()

        spot_price = store.get_spot_at(ts)
        if spot_price is None:
            logger.debug(f"[{ts.strftime('%H:%M:%S')}] No spot data, skipping")
            return

        if ATM_MODE == "static":
            current_atm     = self.static_atm
            current_strikes = self.static_strikes
        else:
            current_atm     = calculate_atm(spot_price)
            current_strikes = get_strikes_around_atm(current_atm)

        current_oi = store.get_oi_at(ts, current_strikes, self.expiry)
        if not current_oi:
            logger.debug(f"[{ts.strftime('%H:%M:%S')}] No OI data, skipping")
            return

        current_totals  = calculate_totals(current_oi)
        baseline_totals = calculate_totals(self.baseline_oi)

        total_call_oi_change = current_totals["total_call_oi"] - baseline_totals["total_call_oi"]
        total_put_oi_change  = current_totals["total_put_oi"]  - baseline_totals["total_put_oi"]

        pcr = calculate_pcr(current_totals["total_put_oi"], current_totals["total_call_oi"])
        total_volume = current_totals["total_call_volume"] + current_totals["total_put_volume"]
        self.pcr_vwap_calc.update(pcr, total_volume)
        pcr_vwap = self.pcr_vwap_calc.get_vwap()

        net_oi_change = total_call_oi_change - total_put_oi_change
        spot_change   = spot_price - self.reference_spot if self.reference_spot else 0

        with DB_LOCK:
            with sqlite3.connect(self.db_path) as conn:
                save_oi_data(conn, {
                    "timestamp":              timestamp,
                    "spot_price":             spot_price,
                    "atm_strike":             current_atm,
                    "total_call_oi":          current_totals["total_call_oi"],
                    "total_put_oi":           current_totals["total_put_oi"],
                    "total_call_volume":      current_totals["total_call_volume"],
                    "total_put_volume":       current_totals["total_put_volume"],
                    "total_call_oi_change":   total_call_oi_change,
                    "total_put_oi_change":    total_put_oi_change,
                    "pcr":                    pcr,
                    "pcr_vwap":               pcr_vwap,
                    "net_oi_change":          net_oi_change,
                })

                strat1_signal = 1 if net_oi_change > 0 else 0
                save_signal_unwinding(
                    conn, timestamp, strat1_signal, net_oi_change,
                    total_call_oi_change, total_put_oi_change
                )

                strat2_signal = 0
                if SPOT_MOVEMENT_THRESHOLD <= abs(spot_change) <= SPOT_MOVEMENT_MAX:
                    if total_call_oi_change > 0 and total_put_oi_change > 0:
                        strat2_signal = 1 if spot_change < 0 else 2
                save_signal_stc(
                    conn, timestamp, strat2_signal, net_oi_change, spot_change,
                    total_call_oi_change, total_put_oi_change
                )

                strat3_signal = 1 if pcr > pcr_vwap else 0
                save_signal_pcr_filter(conn, timestamp, strat3_signal, pcr, pcr_vwap)

        logger.info(
            f"[{ts.strftime('%H:%M:%S')}] "
            f"Spot: {spot_price:.0f} (ATM: {current_atm}) | "
            f"CE OI: {current_totals['total_call_oi']:,} | "
            f"PE OI: {current_totals['total_put_oi']:,} | "
            f"PCR: {pcr:.3f} | "
            f"PCR VWAP: {pcr_vwap:.3f} | "
            f"Signals: S1={strat1_signal:+} S2={strat2_signal} S3={strat3_signal}"
        )

    def run_batch(self, target_date, store):
        """
        Replay a full trading day at SIGNAL_INTERVAL-second steps.
        Iterates from 09:15:00 to 15:30:00, calling generate_signals_batch()
        at every step. No sleep — runs as fast as the data allows.
        """
        from datetime import timedelta
        open_time  = datetime.strptime(MARKET_OPEN_TIME,  "%H:%M:%S").time()
        close_time = datetime.strptime(MARKET_CLOSE_TIME, "%H:%M:%S").time()
        ts    = datetime.combine(target_date, open_time)
        ts_end = datetime.combine(target_date, close_time)

        logger.info(f"Starting batch replay: {ts} → {ts_end} (interval={SIGNAL_INTERVAL}s)")
        rows_written = 0
        while ts <= ts_end:
            try:
                self.generate_signals_batch(ts, store)
                rows_written += 1
            except Exception as e:
                logger.error(f"Error at {ts}: {e}")
            ts += timedelta(seconds=SIGNAL_INTERVAL)

        logger.info(
            f"Batch complete for {target_date}. "
            f"{rows_written} timestamps processed -> {self.db_path}"
        )


# ==================== MAIN ====================

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Shutdown signal received...")
    SHUTDOWN.set()


if __name__ == "__main__":
    sig.signal(sig.SIGINT, signal_handler)
    sig.signal(sig.SIGTERM, signal_handler)

    print("=" * 60)
    print("OI SIGNAL GENERATOR")
    print("=" * 60)
    print(f"Symbol:  {SYMBOL}")
    print(f"Mode:    {PROCESSING_MODE.upper()}")
    print(f"Backend: {ZERODHA_BACKEND_DIR}")
    print(f"Interval: {SIGNAL_INTERVAL}s")
    print("=" * 60)

    if PROCESSING_MODE == "batch":
        print(f"Batch dates: {BATCH_DATES}")
        print(f"Output dir:  {BATCH_OUTPUT_DIR}")
        print()

        for date_str in BATCH_DATES:
            try:
                target_date = datetime.strptime(date_str, "%Y%m%d").date()
                print(f"\n{'='*60}")
                print(f"  Processing: {target_date}")
                print(f"{'='*60}")
                store     = BatchDataStore(target_date)
                generator = OISignalGenerator()
                generator.initialize_batch(target_date, store)
                generator.run_batch(target_date, store)
            except Exception as e:
                logger.error(f"Failed to process {date_str}: {e}")
                import traceback
                traceback.print_exc()

        print("\nAll batch dates processed.")

    else:
        # Live mode (unchanged behaviour)
        print(f"Output dir: {OUTPUT_DIR}")
        print("Press Ctrl+C to stop")
        print()
        try:
            generator = OISignalGenerator()
            generator.initialize()
            generator.run()
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
