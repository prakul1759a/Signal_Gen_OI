"""
OI Signal Generator
Creates datewise .db files with trading signals based on OI analysis.

Strategies:
- Strat-1 (Unwinding): call_oi_change - put_oi_change (every 1 min)
- Strat-2 (STC Opportunist): Spot movement + OI change conditions (every 1 min)
- Strat-3 (PCR Filter): PCR vs PCR_VWAP comparison (every 1 min)
- Strat-4 (Net OI Momentum): net_oi_change vs EMA(net_oi_change) (every 1 min)

Output: signals_{YYYYMMDD}.db with tables:
- meta: baseline info, ATM, config
- oi_data: timestamped OI/PCR raw data
- signal_unwinding: Strat-1 signals
- signal_stc_opportunist: Strat-2 signals
- signal_pcr_filter: Strat-3 signals
- signal_net_oi_momentum: Strat-4 signals
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
SYMBOL = "SENSEX"  # Change to "SENSEX" for Sensex
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

OUTPUT_DIR = SCRIPT_DIR / "signals_output_2.1"

# Strategy intervals (seconds)
SIGNAL_INTERVAL = 30  # 1 minute for all strategies

# Market hours
MARKET_OPEN_TIME = "09:15:00"
MARKET_CLOSE_TIME = "15:30:00"

# ATM Mode: "static" = fix ATM at morning open, "dynamic" = recalculate from current spot
ATM_MODE = "static"  # Options: "static", "dynamic"

# Morning Baseline: if True, use first OI reading in 9:15–9:17 AM window as baseline
# instead of yesterday's closing OI. Useful on gap-up/gap-down days so changes
# are measured from today's actual open, not yesterday's close.
USE_MORNING_BASELINE = True

# Net OI EMA period for Signal 4 (net OI momentum)
# EMA window = NET_OI_EMA_PERIOD × SIGNAL_INTERVAL seconds
# Default: 20 × 30s = 10 minutes of smoothing
NET_OI_EMA_PERIOD = 20

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

def get_db_path():
    """Get path for today's signal database (includes symbol name)"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")
    return OUTPUT_DIR / f"signals_{SYMBOL}_{today_str}.db"


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

        # Strat-4: Net OI Momentum signal
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_net_oi_momentum (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                signal_value INTEGER,
                net_oi_change INTEGER,
                net_oi_change_ema REAL,
                spot_price REAL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_net_oi_momentum_timestamp ON signal_net_oi_momentum(timestamp)")

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


def save_signal_net_oi_momentum(conn, timestamp, signal_value, net_oi_change, net_oi_change_ema, spot_price):
    """Save Strat-4 signal"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO signal_net_oi_momentum (timestamp, signal_value, net_oi_change, net_oi_change_ema, spot_price)
        VALUES (?, ?, ?, ?, ?)
    """, (timestamp, signal_value, net_oi_change, round(net_oi_change_ema, 2), spot_price))
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


# ==================== NET OI EMA CALCULATOR ====================

class EMACalculator:
    """
    Exponential Moving Average calculator for net OI change (Signal 4).

    Formula:
        EMA_t = alpha * value_t + (1 - alpha) * EMA_{t-1}
        alpha = 2 / (period + 1)

    EMA is chosen over SMA because it weights recent OI changes more heavily,
    making the signal responsive to momentum shifts without overreacting to noise.
    With period=20 at 30s intervals, this is a ~10-minute smoothing window.

    Signal fires (=1) when current net_oi_change exceeds its own EMA,
    meaning OI momentum is accelerating above its recent trend.
    """

    def __init__(self, period=20):
        self.period = period
        self.alpha = 2 / (period + 1)
        self.ema = None

    def reset(self):
        """Reset for new day"""
        self.ema = None

    def update(self, value):
        """Update EMA with new value, returns current EMA"""
        if self.ema is None:
            self.ema = float(value)
        else:
            self.ema = self.alpha * value + (1 - self.alpha) * self.ema
        return self.ema

    def get_ema(self):
        return self.ema if self.ema is not None else 0.0


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
        # Morning baseline (used when USE_MORNING_BASELINE=True)
        self.morning_baseline_oi = None
        self.morning_baseline_set = False
        # Signal 4: Net OI EMA
        self.net_oi_ema_calc = EMACalculator(period=NET_OI_EMA_PERIOD)

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
            save_meta(conn, "use_morning_baseline", str(USE_MORNING_BASELINE))
            save_meta(conn, "net_oi_ema_period", NET_OI_EMA_PERIOD)

        if USE_MORNING_BASELINE:
            logger.info("Morning baseline: ENABLED (captures first reading in 9:15–9:17 AM window)")
        else:
            logger.info("Morning baseline: DISABLED (using yesterday's closing OI)")
        logger.info(f"Net OI EMA period: {NET_OI_EMA_PERIOD} (~{NET_OI_EMA_PERIOD * SIGNAL_INTERVAL // 60}min window)")
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

        # Morning baseline capture: first reading in 9:15–9:17 window sets a fresh baseline
        if USE_MORNING_BASELINE and not self.morning_baseline_set:
            now_time = datetime.now().time()
            window_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0).time()
            window_end = datetime.now().replace(hour=9, minute=17, second=59, microsecond=0).time()
            if window_start <= now_time <= window_end:
                self.morning_baseline_oi = current_oi[:]
                self.morning_baseline_set = True
                mb_totals = calculate_totals(self.morning_baseline_oi)
                with DB_LOCK:
                    with sqlite3.connect(self.db_path) as _conn:
                        save_meta(_conn, "morning_baseline_set_at", timestamp)
                        save_meta(_conn, "morning_baseline_call_oi", mb_totals["total_call_oi"])
                        save_meta(_conn, "morning_baseline_put_oi", mb_totals["total_put_oi"])
                logger.info(
                    f"Morning baseline captured: "
                    f"CE={mb_totals['total_call_oi']:,} PE={mb_totals['total_put_oi']:,}"
                )

        # Choose reference baseline for OI change calculations:
        # - If morning baseline is enabled and captured: use it (changes from today's open)
        # - Otherwise: use yesterday's closing OI (default)
        if USE_MORNING_BASELINE and self.morning_baseline_set:
            reference_totals = calculate_totals(self.morning_baseline_oi)
        else:
            reference_totals = calculate_totals(self.baseline_oi)

        # Calculate total OI changes vs reference baseline
        total_call_oi_change = current_totals["total_call_oi"] - reference_totals["total_call_oi"]
        total_put_oi_change = current_totals["total_put_oi"] - reference_totals["total_put_oi"]

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

        # Signal 4: update EMA and determine momentum signal
        net_oi_ema = self.net_oi_ema_calc.update(net_oi_change)
        strat4_signal = 1 if net_oi_change > net_oi_ema else 0

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

                # Strat-4: Net OI Momentum signal
                # 1 = net_oi_change > EMA (momentum accelerating above recent trend)
                # 0 = net_oi_change <= EMA (momentum at or below recent trend)
                save_signal_net_oi_momentum(
                    conn, timestamp, strat4_signal, net_oi_change, net_oi_ema, spot_price
                )

        # Log summary
        logger.info(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Spot: {spot_price:.0f} (ATM: {current_atm}) | "
            f"CE OI: {current_totals['total_call_oi']:,} | "
            f"PE OI: {current_totals['total_put_oi']:,} | "
            f"PCR: {pcr:.3f} | "
            f"PCR VWAP: {pcr_vwap:.3f} | "
            f"Signals: S1={strat1_signal:+,} S2={strat2_signal} S3={strat3_signal} S4={strat4_signal}"
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


# ==================== MAIN ====================

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Shutdown signal received...")
    SHUTDOWN.set()


if __name__ == "__main__":
    # Register signal handlers
    sig.signal(sig.SIGINT, signal_handler)
    sig.signal(sig.SIGTERM, signal_handler)

    print("=" * 60)
    print("OI SIGNAL GENERATOR")
    print("=" * 60)
    print(f"Symbol: {SYMBOL}")
    print(f"Backend: {ZERODHA_BACKEND_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Interval: {SIGNAL_INTERVAL}s")
    print("=" * 60)
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
