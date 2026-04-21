# Signal Generator

Reads the live options chain OI data every minute and writes trading signals to a per-day SQLite database. The signals are consumed by the Paper Trading Engine and Backtest Engine for forced exits and entry filtering.

---

## Files

| File | Purpose |
|------|---------|
| `V2.1_Sensex_oi_signal_generator_final_dynamic_and_static_compatibility.py` | Live version, runs during market hours |
| `Historical_Signal_Generator/Historical_oi_signal_generator.py` | Processes historical Zerodha CSV data to generate signal DBs for the Backtest Engine |

---

## Running (Live)

```
python V2.1_Sensex_oi_signal_generator_final_dynamic_and_static_compatibility.py
```

Set `SYMBOL = "SENSEX"` or `"NIFTY"` at the top before running. The program reads today's Zerodha CSV files as they are written by `test_final_CSV_Token2.py`.

---

## Strategies Computed Each Minute

**Strat-1 — Unwinding** (`signal_unwinding` table):
`net_oi_change = call_oi_change - put_oi_change` across strikes around ATM. Positive = more put OI being added (bullish pressure), negative = more call OI being added (bearish pressure). This is the signal currently used by the trading engine.

**Strat-2 — STC Opportunist** (`signal_stc_opportunist` table):
Spot has moved by a threshold amount AND OI change exceeds a threshold value in the same direction. Flags short-term continuation setups.

**Strat-3 — PCR Filter** (`signal_pcr_filter` table):
PCR compared against anchored VWAP of the PCR series. Signals when PCR crosses above or below its own VWAP.

**Strat-4 — Net OI Momentum** (`signal_net_oi_momentum` table, V2.1 only):
`net_oi_change` compared against its own EMA. Acts as a trend filter on the Strat-1 signal.

Only `signal_unwinding` (Strat-1) is currently consumed by the trading engine. The others are computed and stored for future use.

---

## Output

Per-day DB: `signals_output_X.X/signals_SENSEX_YYYYMMDD.db`

Tables: `meta`, `oi_data`, `signal_unwinding`, `signal_stc_opportunist`, `signal_pcr_filter`, `signal_net_oi_momentum`

---

## Data Source

Reads Zerodha FULL-mode CSV files written by `test_final_CSV_Token2.py`.

Path resolution order:
1. Local `backend/` folder next to the script
2. `D:\omshree\ZerodhaCSVData\backend\` (network/alternate path)

---

## Symbol-Specific Configuration

Set automatically from `SYMBOL_CONFIG` when `SYMBOL` is changed:

| Parameter | NIFTY | SENSEX | BANKNIFTY |
|-----------|-------|--------|-----------|
| Strike interval | 50 | 100 | 100 |
| Strikes each side | 7 | 7 | 7 |
| Spot move threshold (Strat-2) | 40 pts | 80 pts | 100 pts |

---

## Historical Version

`Historical_Signal_Generator/Historical_oi_signal_generator.py` runs the same Strat-1/2/3 logic against already-saved Zerodha CSV files. Used to backfill signal DBs for dates where the live generator was not running. Output folder and date range are set at the top of the file.

---

## Dependencies

`pandas`, `sqlite3` (stdlib), `threading` (stdlib)
