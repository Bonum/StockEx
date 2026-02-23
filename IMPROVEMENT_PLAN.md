# KafkaTradingSystem Improvement Plan

> **Created:** 2026-02-03
> **Status:** ✅ Core implementation complete (Phases 1-5 done)
> **Last Updated:** 2026-02-03
> **Resume:** Share this file with Claude to continue from where we left off

---

## Overview

This plan outlines improvements to transform the trading system from a demo/prototype into a more robust, production-like system. Improvements are organized by priority and complexity.

---

## Phase 1: Real-Time Dashboard (WebSocket/SSE)

**Problem:** Dashboard polls `/data` every 2 seconds (`setInterval(refresh, 2000)`), causing unnecessary load and delayed updates.

**Solution:** Implement Server-Sent Events (SSE) for real-time push updates.

### Tasks

- [x] **1.1** Add SSE endpoint to `dashboard/dashboard.py` ✅
  - Create `/stream` endpoint using Flask's `Response` with `text/event-stream`
  - Push events when new orders/trades/snapshots arrive from Kafka
  - File: `dashboard/dashboard.py`

- [x] **1.2** Update dashboard frontend to use SSE ✅
  - Replace `setInterval(refresh, 2000)` with `EventSource('/stream')`
  - Handle reconnection on disconnect
  - File: `dashboard/templates/index.html`

- [x] **1.3** Add connection status indicator ✅
  - Show connected/disconnected state in UI
  - File: `dashboard/templates/index.html`

### Why SSE over WebSocket?
- Simpler implementation (HTTP-based, no special protocol)
- One-way server-to-client is sufficient for this use case
- Better browser support and automatic reconnection

---

## Phase 2: SQLite Persistence

**Problem:** All data is in-memory; order books and trades lost on restart.

**Solution:** Add SQLite database for persistence with in-memory caching.

### Tasks

- [x] **2.1** Create database schema and initialization ✅
  - Tables: `orders`, `trades`, `order_book_entries`
  - Add migration/init script
  - New file: `matcher/database.py`

- [x] **2.2** Modify matcher to persist trades ✅
  - Write trades to SQLite when matched
  - Load recent trades on startup
  - File: `matcher/matcher.py`

- [x] **2.3** Persist order book state ✅
  - Save resting orders to database
  - Restore order books on matcher restart
  - File: `matcher/matcher.py`

- [x] **2.4** Add trade history endpoint ✅
  - `GET /trades?symbol=X&limit=N&offset=M`
  - Support filtering and pagination
  - File: `matcher/matcher.py`

- [x] **2.5** Add Docker volume for database persistence ✅
  - Mount SQLite file to host
  - File: `docker-compose.yml`

### Schema Design

```sql
CREATE TABLE trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    buy_order_id TEXT,
    sell_order_id TEXT,
    timestamp REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_book (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cl_ord_id TEXT UNIQUE,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,  -- 'BUY' or 'SELL'
    price REAL,
    quantity INTEGER NOT NULL,
    remaining_qty INTEGER NOT NULL,
    status TEXT DEFAULT 'OPEN',  -- OPEN, FILLED, CANCELLED
    timestamp REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trades_symbol ON trades(symbol);
CREATE INDEX idx_trades_timestamp ON trades(timestamp);
CREATE INDEX idx_orderbook_symbol_side ON order_book(symbol, side);
```

---

## Phase 3: FIX Protocol Improvements

**Problem:** Basic FIX handling with no validation, no execution reports, no order management.

### Tasks

- [x] **3.1** Add FIX message validation ✅
  - Validate required fields (ClOrdID, Symbol, Side, OrderQty)
  - Validate field formats and ranges
  - Return Reject (35=3) for invalid messages
  - File: `fix_oeg/fix_oeg_server.py`

- [x] **3.2** Send Execution Reports back to clients ✅
  - Send 35=8 (ExecutionReport) for order acknowledgment
  - Send fill reports when trades occur
  - Requires Kafka consumer for trades topic in fix_oeg
  - File: `fix_oeg/fix_oeg_server.py`

- [x] **3.3** Support Order Cancel Request (35=F) ✅
  - Parse cancel requests
  - Publish to orders Kafka topic (with type=cancel)
  - Files: `fix_oeg/fix_oeg_server.py`

- [x] **3.4** Support Order Cancel/Replace (35=G) ✅
  - Parse modify requests
  - Implement price/quantity amendments
  - Files: `fix_oeg/fix_oeg_server.py`, `matcher/matcher.py`

- [ ] **3.5** Add session-level validation (DEFERRED)
  - Sequence number checking handled by QuickFIX engine
  - Heartbeat/TestRequest handled automatically
  - File: `fix_oeg/fix_oeg_server.py`

### Execution Report Fields (35=8)

```
Tag 35 = 8 (ExecutionReport)
Tag 37 = OrderID (exchange-assigned)
Tag 11 = ClOrdID (client order ID)
Tag 17 = ExecID
Tag 20 = ExecTransType (0=New)
Tag 39 = OrdStatus (0=New, 1=PartialFill, 2=Filled)
Tag 150 = ExecType (0=New, F=Trade)
Tag 55 = Symbol
Tag 54 = Side
Tag 38 = OrderQty
Tag 44 = Price
Tag 32 = LastShares (fill qty)
Tag 31 = LastPx (fill price)
Tag 14 = CumQty
Tag 151 = LeavesQty
Tag 6 = AvgPx
```

---

## Phase 4: Enhanced Order Types

**Problem:** Only limit orders supported.

### Tasks

- [x] **4.1** Support Market Orders ✅
  - Match immediately at best available price
  - No price field required (OrdType=1)
  - File: `matcher/matcher.py`

- [x] **4.2** Support IOC (Immediate-or-Cancel) ✅
  - TimeInForce=3: Fill what's available, cancel rest
  - File: `matcher/matcher.py`

- [x] **4.3** Support FOK (Fill-or-Kill) ✅
  - TimeInForce=4: Fill entire order or reject
  - File: `matcher/matcher.py`

- [x] **4.4** Support GTC (Good-Till-Cancel) ✅
  - TimeInForce=1: Persist until explicitly cancelled
  - Implemented via SQLite persistence (Phase 2)
  - File: `matcher/matcher.py`

---

## Phase 5: Monitoring & Observability

### Tasks

- [x] **5.1** Add health check endpoints ✅
  - `/health` endpoint for matcher, dashboard, frontend
  - Check Kafka/DB connectivity and service stats
  - Files: `matcher/matcher.py`, `dashboard/dashboard.py`, `frontend/frontend.py`

- [x] **5.2** Add structured logging ✅
  - JSON log format for parsing
  - Include correlation IDs
  - New file: `shared/logging_utils.py`

- [x] **5.3** Add metrics endpoint ✅
  - Order count, trade count, latency stats
  - Prometheus-compatible format (`/metrics` endpoint)
  - File: `matcher/matcher.py`

- [ ] **5.4** Add Kafka lag monitoring (OPTIONAL)
  - Track consumer group lag
  - Alert on high lag
  - New file: `shared/monitoring.py`

---

## Phase 6: Testing & Quality

### Tasks

- [x] **6.1** Add unit tests for matcher ✅
  - Test matching logic (full/partial fills, price-time priority)
  - Test order types (market, limit, IOC, FOK)
  - Test cancel and amend operations
  - New file: `matcher/test_matcher.py`

- [ ] **6.2** Add integration tests (OPTIONAL)
  - End-to-end order flow tests
  - FIX client to trade execution
  - New directory: `tests/`

- [ ] **6.3** Add load testing script (OPTIONAL)
  - Generate high-volume order flow
  - Measure latency and throughput
  - New file: `tests/load_test.py`

---

## Implementation Order (Recommended)

1. **Phase 1** (Real-Time Dashboard) - Quick win, improves UX
2. **Phase 2** (SQLite Persistence) - Foundation for reliability
3. **Phase 3.1-3.2** (FIX Validation + Execution Reports) - Core trading functionality
4. **Phase 5.1** (Health Checks) - Operational necessity
5. **Phase 4.1** (Market Orders) - Common order type
6. **Phase 3.3** (Order Cancellation) - Essential for trading
7. Remaining phases as needed

---

## Quick Reference: Key Files

| Component | Main File | Port |
|-----------|-----------|------|
| FIX Gateway | `fix_oeg/fix_oeg_server.py` | 5001 |
| Matcher | `matcher/matcher.py` | 6000 |
| Dashboard | `dashboard/dashboard.py` | 5005 |
| Frontend | `frontend/frontend.py` | 5000 |
| Config | `shared/config.py` | - |

---

## How to Continue

When resuming work, tell Claude:
1. Which phase/task to work on (e.g., "Let's implement Phase 1.1")
2. Or ask Claude to pick the next logical task

Each task is designed to be implementable in a single session.
