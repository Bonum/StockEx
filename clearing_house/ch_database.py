"""SQLite persistence for the Clearing House simulation.

DB path: /app/data/clearing_house.db (shared volume with dashboard).
All public functions are thread-safe via thread-local connections.
"""

import sqlite3
import threading
import time
import datetime
import os

CH_DB_PATH = os.getenv("CH_DB_PATH", "/app/data/clearing_house.db")
CH_MEMBERS = [f"USR{i:02d}" for i in range(1, 11)]
CH_STARTING_CAPITAL = 100_000.0
CH_DAILY_OBLIGATION = 10  # minimum securities (qty sum) per trading day

_local = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS ch_members (
    member_id   TEXT PRIMARY KEY,
    capital     REAL NOT NULL DEFAULT 100000.0,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ch_holdings (
    member_id   TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    quantity    INTEGER NOT NULL DEFAULT 0,
    avg_cost    REAL NOT NULL DEFAULT 0.0,
    PRIMARY KEY (member_id, symbol)
);

CREATE TABLE IF NOT EXISTS ch_daily_trades (
    member_id        TEXT NOT NULL,
    trading_date     TEXT NOT NULL,
    buy_count        INTEGER NOT NULL DEFAULT 0,
    sell_count       INTEGER NOT NULL DEFAULT 0,
    total_securities INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (member_id, trading_date)
);

CREATE TABLE IF NOT EXISTS ch_trade_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id    TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    side         TEXT NOT NULL,
    quantity     INTEGER NOT NULL,
    price        REAL NOT NULL,
    cl_ord_id    TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    timestamp    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ch_settlements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id       TEXT NOT NULL,
    trading_date    TEXT NOT NULL,
    opening_capital REAL NOT NULL,
    closing_capital REAL NOT NULL,
    realized_pnl    REAL NOT NULL DEFAULT 0.0,
    unrealized_pnl  REAL NOT NULL DEFAULT 0.0,
    obligation_met  INTEGER NOT NULL DEFAULT 0,
    settled_at      REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ch_trade_log_member
    ON ch_trade_log(member_id, trading_date);
CREATE INDEX IF NOT EXISTS idx_ch_settlements_member
    ON ch_settlements(member_id, trading_date);
"""


def _conn() -> sqlite3.Connection:
    """Thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(CH_DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db() -> None:
    """Create schema and seed the 10 members if missing."""
    conn = _conn()
    conn.executescript(SCHEMA)
    conn.commit()
    now = time.time()
    for mid in CH_MEMBERS:
        conn.execute(
            "INSERT OR IGNORE INTO ch_members (member_id, capital, created_at) VALUES (?,?,?)",
            (mid, CH_STARTING_CAPITAL, now),
        )
    conn.commit()
    print(f"[CH-DB] Initialized at {CH_DB_PATH}")


# ── Members ────────────────────────────────────────────────────────────────────

def get_member(member_id: str) -> dict | None:
    row = _conn().execute(
        "SELECT member_id, capital FROM ch_members WHERE member_id=?", (member_id,)
    ).fetchone()
    return dict(row) if row else None


def get_all_members() -> list[dict]:
    rows = _conn().execute(
        "SELECT member_id, capital FROM ch_members ORDER BY member_id"
    ).fetchall()
    return [dict(r) for r in rows]


# ── Holdings ───────────────────────────────────────────────────────────────────

def get_holdings(member_id: str) -> list[dict]:
    rows = _conn().execute(
        "SELECT symbol, quantity, avg_cost FROM ch_holdings WHERE member_id=? AND quantity>0",
        (member_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_holding(member_id: str, symbol: str) -> dict:
    row = _conn().execute(
        "SELECT quantity, avg_cost FROM ch_holdings WHERE member_id=? AND symbol=?",
        (member_id, symbol),
    ).fetchone()
    return dict(row) if row else {"quantity": 0, "avg_cost": 0.0}


# ── Trade recording (atomic) ───────────────────────────────────────────────────

def today_str() -> str:
    return datetime.date.today().isoformat()


def record_trade(
    member_id: str,
    symbol: str,
    side: str,
    quantity: int,
    price: float,
    cl_ord_id: str,
) -> None:
    """Atomically update holdings, capital, daily counter, and trade log."""
    conn = _conn()
    date = today_str()
    value = quantity * price

    with conn:  # auto-commit / rollback
        # 1. Update holdings
        holding = get_holding(member_id, symbol)
        old_qty = holding["quantity"]
        old_avg = holding["avg_cost"]

        if side == "BUY":
            new_qty = old_qty + quantity
            new_avg = (old_qty * old_avg + quantity * price) / new_qty if new_qty else price
            conn.execute(
                """INSERT INTO ch_holdings (member_id, symbol, quantity, avg_cost)
                   VALUES (?,?,?,?)
                   ON CONFLICT(member_id, symbol) DO UPDATE
                   SET quantity=excluded.quantity, avg_cost=excluded.avg_cost""",
                (member_id, symbol, new_qty, round(new_avg, 4)),
            )
            # 2. Deduct capital
            conn.execute(
                "UPDATE ch_members SET capital = capital - ? WHERE member_id=?",
                (value, member_id),
            )
        else:  # SELL
            new_qty = max(0, old_qty - quantity)
            if new_qty == 0:
                conn.execute(
                    "DELETE FROM ch_holdings WHERE member_id=? AND symbol=?",
                    (member_id, symbol),
                )
            else:
                conn.execute(
                    "UPDATE ch_holdings SET quantity=? WHERE member_id=? AND symbol=?",
                    (new_qty, member_id, symbol),
                )
            # 2. Add capital
            conn.execute(
                "UPDATE ch_members SET capital = capital + ? WHERE member_id=?",
                (value, member_id),
            )

        # 3. Update daily trade counter
        buy_inc = 1 if side == "BUY" else 0
        sell_inc = 1 if side == "SELL" else 0
        conn.execute(
            """INSERT INTO ch_daily_trades (member_id, trading_date, buy_count, sell_count, total_securities)
               VALUES (?,?,?,?,?)
               ON CONFLICT(member_id, trading_date) DO UPDATE
               SET buy_count   = buy_count + excluded.buy_count,
                   sell_count  = sell_count + excluded.sell_count,
                   total_securities = total_securities + excluded.total_securities""",
            (member_id, date, buy_inc, sell_inc, quantity),
        )

        # 4. Log the trade
        conn.execute(
            """INSERT INTO ch_trade_log
               (member_id, symbol, side, quantity, price, cl_ord_id, trading_date, timestamp)
               VALUES (?,?,?,?,?,?,?,?)""",
            (member_id, symbol, side, quantity, price, cl_ord_id, date, time.time()),
        )


def get_daily_trades(member_id: str, date: str | None = None) -> dict:
    date = date or today_str()
    row = _conn().execute(
        "SELECT buy_count, sell_count, total_securities FROM ch_daily_trades WHERE member_id=? AND trading_date=?",
        (member_id, date),
    ).fetchone()
    return dict(row) if row else {"buy_count": 0, "sell_count": 0, "total_securities": 0}


def get_trade_log(member_id: str, date: str | None = None, limit: int = 50) -> list[dict]:
    date = date or today_str()
    rows = _conn().execute(
        """SELECT symbol, side, quantity, price, cl_ord_id, timestamp
           FROM ch_trade_log WHERE member_id=? AND trading_date=?
           ORDER BY timestamp DESC LIMIT ?""",
        (member_id, date, limit),
    ).fetchall()
    return [dict(r) for r in rows]


# ── EOD Settlement ─────────────────────────────────────────────────────────────

def record_settlement(
    member_id: str,
    trading_date: str,
    opening_capital: float,
    closing_capital: float,
    realized_pnl: float,
    unrealized_pnl: float,
    obligation_met: bool,
) -> None:
    _conn().execute(
        """INSERT INTO ch_settlements
           (member_id, trading_date, opening_capital, closing_capital,
            realized_pnl, unrealized_pnl, obligation_met, settled_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            member_id, trading_date, opening_capital, closing_capital,
            realized_pnl, unrealized_pnl, int(obligation_met), time.time(),
        ),
    )
    _conn().commit()


def get_settlements(member_id: str, limit: int = 30) -> list[dict]:
    rows = _conn().execute(
        """SELECT trading_date, opening_capital, closing_capital,
                  realized_pnl, unrealized_pnl, obligation_met, settled_at
           FROM ch_settlements WHERE member_id=?
           ORDER BY settled_at DESC LIMIT ?""",
        (member_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


# ── Leaderboard ────────────────────────────────────────────────────────────────

def get_leaderboard(date: str | None = None) -> list[dict]:
    """Returns member stats. Caller adds holdings_value using live prices."""
    date = date or today_str()
    members = get_all_members()
    result = []
    for m in members:
        mid = m["member_id"]
        dt = get_daily_trades(mid, date)
        holdings = get_holdings(mid)
        result.append({
            "member_id": mid,
            "capital": round(m["capital"], 2),
            "holdings": holdings,
            "buy_count": dt["buy_count"],
            "sell_count": dt["sell_count"],
            "total_securities": dt["total_securities"],
            "obligation_met": dt["total_securities"] >= CH_DAILY_OBLIGATION,
        })
    return result


# ── Auth ───────────────────────────────────────────────────────────────────────

def verify_password(member_id: str, password: str) -> bool:
    """Password equals the member ID (e.g. USR01 / USR01)."""
    return member_id in CH_MEMBERS and password == member_id
