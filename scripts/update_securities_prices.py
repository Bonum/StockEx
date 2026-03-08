#!/usr/bin/env python3
"""
Fetch real ATHEX stock prices from Yahoo Finance and update securities.txt.
Optionally exports OHLCV bars for the RL agent observation vector.

Usage:
    python scripts/update_securities_prices.py              # update current prices
    python scripts/update_securities_prices.py --history 10 # also print 10-day history
    python scripts/update_securities_prices.py --reset-start # reset start_price to oldest close
    python scripts/update_securities_prices.py --ohlcv 60   # export 60 days of OHLCV for RL agent
    python scripts/update_securities_prices.py --seed-chart # seed dashboard price chart DB

Requires: pip install yfinance
"""
import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

# Mapping: project symbol -> Yahoo Finance ticker
TICKER_MAP = {
    "ALPHA": "ALPHA.AT",    # Alpha Services and Holdings
    "PEIR":  "TPEIR.AT",    # Piraeus Financial Holdings
    "EXAE":  "EXAE.AT",     # Hellenic Exchanges (Athens Stock Exchange)
    "QUEST": "QUEST.AT",    # Quest Holdings
    "NBG":   "ETE.AT",      # National Bank of Greece
    "EUROB": "EUROB.AT",    # Eurobank Ergasias
    "AEG":   "AEGN.AT",     # Aegean Airlines
    "INTKA": "INTRK.AT",    # Intrakat
    "AAAK":  "AAAK.AT",     # E. Pairis S.A.
    "ATTIK": "ATTICA.AT",   # Attica Bank
    "AMZN":  "AMZN",        # Amazon
    "TSLA":  "TSLA",        # Tesla
    "NVDA":  "NVDA",        # NVIDIA
    "GOOGL": "GOOGL",       # Alphabet (Google)
    "AAPL":  "AAPL",        # Apple
    "ENX":   "ENX.PA",      # Euronext N.V.
}

SECURITIES_FILE = os.path.join(os.path.dirname(__file__), "..", "shared", "data", "securities.txt")
OHLCV_DIR = os.path.join(os.path.dirname(__file__), "..", "shared", "data", "ohlcv")
HISTORY_DB = os.path.join(os.path.dirname(__file__), "..", "shared", "data", "dashboard_history.db")

# Tickers that trade in USD and need EUR conversion
USD_TICKERS = {"AMZN", "TSLA", "NVDA", "GOOGL", "AAPL"}


def _get_usd_eur_rate() -> float:
    """Fetch current USD/EUR exchange rate from Yahoo Finance."""
    try:
        t = yf.Ticker("USDEUR=X")
        hist = t.history(period="5d")
        if not hist.empty:
            rate = float(hist.iloc[-1]["Close"])
            print(f"  USD/EUR rate: {rate:.4f}")
            return rate
    except Exception as e:
        print(f"  WARNING: Could not fetch USD/EUR rate ({e}), using fallback", file=sys.stderr)
    return 0.92  # fallback


def _get_usd_eur_series(days: int) -> dict[str, float]:
    """Fetch daily USD/EUR rates. Returns {date_str: rate}."""
    try:
        t = yf.Ticker("USDEUR=X")
        hist = t.history(period=f"{days + 10}d")
        if not hist.empty:
            return {d.strftime("%Y-%m-%d"): round(float(r["Close"]), 6) for d, r in hist.iterrows()}
    except Exception:
        pass
    return {}


def fetch_prices(days=15):
    """Fetch price history for all securities. USD prices are converted to EUR."""
    results = {}
    period = f"{days + 5}d"  # extra days to account for weekends/holidays

    # Pre-fetch USD/EUR rates for the period
    usd_eur_daily = {}
    needs_conversion = any(sym in USD_TICKERS for sym in TICKER_MAP)
    if needs_conversion:
        print("  Fetching USD/EUR exchange rates...")
        usd_eur_daily = _get_usd_eur_series(days + 5)
        fallback_rate = _get_usd_eur_rate()

    for sym, ticker in TICKER_MAP.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=period)
            if hist.empty:
                print(f"  WARNING: {sym} ({ticker}): no data returned", file=sys.stderr)
                results[sym] = None
                continue

            hist = hist.tail(days)

            # Determine if USD→EUR conversion is needed
            convert = sym in USD_TICKERS

            history = []
            ohlcv = []
            for date, row in hist.iterrows():
                date_str = date.strftime("%Y-%m-%d")
                rate = usd_eur_daily.get(date_str, fallback_rate) if convert else 1.0

                history.append({
                    "date": date_str,
                    "close": round(row["Close"] * rate, 4),
                })
                ohlcv.append({
                    "date": date_str,
                    "open": round(row["Open"] * rate, 4),
                    "high": round(row["High"] * rate, 4),
                    "low": round(row["Low"] * rate, 4),
                    "close": round(row["Close"] * rate, 4),
                    "volume": int(row["Volume"]),
                })

            rate_last = 1.0
            if convert:
                last_date = hist.index[-1].strftime("%Y-%m-%d")
                rate_last = usd_eur_daily.get(last_date, fallback_rate)

            results[sym] = {
                "current": round(hist.iloc[-1]["Close"] * rate_last, 4),
                "start": round(hist.iloc[0]["Close"] * (usd_eur_daily.get(hist.index[0].strftime("%Y-%m-%d"), fallback_rate) if convert else 1.0), 4),
                "history": history,
                "ohlcv": ohlcv,
                "converted": convert,
            }
            if convert:
                print(f"  {sym}: converted USD -> EUR (rate ~{rate_last:.4f})")
        except Exception as e:
            print(f"  ERROR: {sym} ({ticker}): {e}", file=sys.stderr)
            results[sym] = None

    return results


def update_securities_file(results, reset_start=False):
    """Update securities.txt with real prices."""
    # Read existing to preserve symbols without data
    existing = {}
    if os.path.exists(SECURITIES_FILE):
        with open(SECURITIES_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) >= 3:
                    existing[parts[0]] = {"start": float(parts[1]), "current": float(parts[2])}

    # Merge real data
    for sym, data in results.items():
        if data is None:
            continue  # keep existing values
        if reset_start or sym not in existing:
            existing[sym] = {"start": data["start"], "current": data["current"]}
        else:
            existing[sym]["current"] = data["current"]
            if reset_start:
                existing[sym]["start"] = data["start"]

    # Write back
    with open(SECURITIES_FILE, "w") as f:
        f.write("#SYMBOL\t<start_price>\t<current_price>\n")
        for sym in TICKER_MAP:
            if sym in existing:
                f.write(f"{sym}\t{existing[sym]['start']:.2f}\t{existing[sym]['current']:.2f}\n")

    print(f"Updated {SECURITIES_FILE}")


def print_history(results):
    """Print price history table."""
    print("\n=== ATHEX Price History ===\n")
    for sym, data in results.items():
        if data is None:
            print(f"{sym}: no data available")
            continue
        print(f"{sym} (Yahoo: {TICKER_MAP[sym]})  Current: {data['current']:.2f} EUR")
        for day in data["history"]:
            print(f"  {day['date']}  {day['close']:.4f}")
        print()


def export_ohlcv(results):
    """Export OHLCV bars as JSON files for the RL agent to seed real price history."""
    os.makedirs(OHLCV_DIR, exist_ok=True)
    exported = 0
    for sym, data in results.items():
        if data is None or "ohlcv" not in data:
            continue
        ohlcv_file = os.path.join(OHLCV_DIR, f"{sym}.json")
        with open(ohlcv_file, "w") as f:
            json.dump(data["ohlcv"], f, indent=2)
        exported += 1
        print(f"  {sym}: {len(data['ohlcv'])} bars -> {ohlcv_file}")
    print(f"Exported OHLCV for {exported} symbols to {OHLCV_DIR}")


def seed_dashboard_db(results):
    """Insert OHLCV data into dashboard_history.db for the price chart.

    Each daily bar is inserted as a single candle at 10:00 market open
    of that trading day (bucket = Unix timestamp rounded to 60s).
    """
    conn = sqlite3.connect(HISTORY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            symbol  TEXT,
            bucket  INTEGER,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL,
            volume  INTEGER,
            PRIMARY KEY (symbol, bucket)
        )
    """)

    inserted = 0
    for sym, data in results.items():
        if data is None or "ohlcv" not in data:
            continue
        for bar in data["ohlcv"]:
            # Convert date string to Unix timestamp at 10:00 local time
            dt = datetime.strptime(bar["date"], "%Y-%m-%d").replace(hour=10, minute=0)
            bucket = int(dt.timestamp() // 60) * 60
            conn.execute(
                "INSERT OR REPLACE INTO ohlcv VALUES (?,?,?,?,?,?,?)",
                (sym, bucket, bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]),
            )
            inserted += 1

    conn.commit()
    conn.close()
    print(f"Seeded {inserted} candles into {HISTORY_DB}")


def main():
    parser = argparse.ArgumentParser(description="Update securities.txt with real ATHEX prices")
    parser.add_argument("--history", type=int, default=0,
                        help="Number of historical trading days to display (default: 0)")
    parser.add_argument("--reset-start", action="store_true",
                        help="Reset start_price to oldest fetched closing price")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and display prices without writing to file")
    parser.add_argument("--json", action="store_true",
                        help="Output results as JSON")
    parser.add_argument("--ohlcv", type=int, default=0,
                        help="Export N days of OHLCV data for RL agent (default: 0, disabled)")
    parser.add_argument("--seed-chart", action="store_true",
                        help="Seed dashboard_history.db with fetched OHLCV for price chart display")
    args = parser.parse_args()

    days = max(args.history, args.ohlcv, 10)
    print(f"Fetching {days}-day history from Yahoo Finance...")
    results = fetch_prices(days=days)

    if args.json:
        print(json.dumps(results, indent=2))
        return

    if args.history > 0:
        print_history(results)

    if not args.dry_run:
        update_securities_file(results, reset_start=args.reset_start)
        if args.ohlcv > 0:
            print(f"\n=== Exporting OHLCV for RL Agent ({days} bars) ===")
            export_ohlcv(results)
        if args.seed_chart:
            print(f"\n=== Seeding Dashboard Price Chart ===")
            seed_dashboard_db(results)
    else:
        print("(dry run — no files updated)")

    # Summary
    print("\n--- Current Prices ---")
    for sym, data in results.items():
        status = f"{data['current']:.2f} EUR" if data else "N/A"
        print(f"  {sym:6s}  {status}")


if __name__ == "__main__":
    main()
