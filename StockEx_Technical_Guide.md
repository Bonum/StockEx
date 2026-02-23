# StockEx Trading Platform
## Complete Technical & Developer Guide v1.0

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [System Architecture](#2-system-architecture)
3. [Module Descriptions](#3-module-descriptions)
4. [Dashboard User Interface](#4-dashboard-user-interface)
5. [Data Flow & Messaging](#5-data-flow--messaging)
6. [Configuration](#6-configuration)
7. [Quick Reference](#7-quick-reference)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Introduction

**StockEx** is a comprehensive real-time trading simulation platform inspired by Euronext OPTIQ. It provides a complete electronic trading ecosystem including order entry via FIX protocol, order matching engine, market data distribution, and live visualization through a web dashboard.

### Key Features

- **FIX 4.4 Protocol Support** — Institutional-grade order entry via QuickFIX
- **Real-time Order Matching** — Price-time priority matching engine with SQLite persistence
- **Live Market Data Streaming** — Kafka-based event distribution
- **Web Dashboard with SSE** — Real-time updates without page refresh
- **Order Management** — Edit and Cancel capabilities from the UI
- **Trading Analytics** — Volume, Value, VWAP statistics with visualizations

### Access URLs

| Service | URL | Purpose |
|---------|-----|---------|
| Dashboard | http://localhost:5005 | Main trading view |
| Frontend | http://localhost:5000 | Manual order entry |
| FIX Client 1 | http://localhost:5002 | FIX order submission |
| FIX Client 2 | http://localhost:5003 | FIX order submission |
| Matcher API | http://localhost:6000 | REST API for order book/trades |

---

## 2. System Architecture

### High-Level Architecture Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  FIX UI Client  │     │  FIX UI Client  │     │    Frontend     │
│    (Port 5002)  │     │    (Port 5003)  │     │   (Port 5000)   │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │ FIX 4.4               │ FIX 4.4               │ HTTP
         ▼                       ▼                       ▼
┌────────────────────────────────────────────────────────────────┐
│                      FIX OEG (Port 5001)                       │
│                   QuickFIX Order Entry Gateway                 │
└────────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ Kafka [orders]
┌────────────────────────────────────────────────────────────────┐
│                    Apache Kafka (Port 9092)                    │
│              Topics: orders, trades, snapshots                 │
└───────┬────────────────────┬───────────────────────┬───────────┘
        │                    │                       │
        ▼                    ▼                       ▼
┌───────────────┐    ┌───────────────┐      ┌───────────────┐
│    Matcher    │    │  MD Feeder    │      │   Dashboard   │
│  (Port 6000)  │    │   (MDF)       │      │  (Port 5005)  │
│               │    │               │      │               │
│ Order Book    │    │ Price Sim     │      │ Web UI + SSE  │
│ Trade Match   │    │ BBO Publish   │      │ Real-time     │
└───────────────┘    └───────────────┘      └───────────────┘
```

### Component Summary

| Component | Technology | Port | Function |
|-----------|------------|------|----------|
| Zookeeper | Confluent 7.5.0 | 2181 | Kafka coordination |
| Kafka | Confluent 7.5.0 | 9092, 29092 | Message streaming |
| FIX OEG | QuickFIX/Python | 5001 | FIX protocol gateway |
| Matcher | Python/Flask/SQLite | 6000 | Order matching engine |
| MDF | Python | - | Market data simulation |
| Dashboard | Flask/SSE | 5005 | Real-time web UI |
| Frontend | Flask | 5000 | Manual order entry |
| FIX Clients | QuickFIX/Flask | 5002, 5003 | FIX order UI |

---

## 3. Module Descriptions

### 3.1 Zookeeper

**Port:** 2181

Apache Zookeeper provides distributed coordination for the Kafka cluster. It manages broker metadata, topic configurations, and cluster membership.

- **Image:** confluentinc/cp-zookeeper:7.5.0
- **Dependencies:** None
- **Environment:** `ZOOKEEPER_CLIENT_PORT=2181`

---

### 3.2 Apache Kafka

**Ports:** 9092 (internal), 29092 (host access)

Apache Kafka serves as the central message backbone for the entire trading system. All order flow, trade executions, and market data are distributed through Kafka topics.

- **Image:** confluentinc/cp-kafka:7.5.0
- **Dependencies:** Zookeeper

**Topics:**

| Topic | Purpose | Producers | Consumers |
|-------|---------|-----------|-----------|
| `orders` | Order flow | FIX OEG, MDF, Frontend | Matcher, Dashboard |
| `trades` | Executed trades | Matcher | Dashboard, Consumer |
| `snapshots` | BBO updates | MDF | Dashboard, Snapshot Viewer |

---

### 3.3 FIX Order Entry Gateway (FIX OEG)

**Port:** 5001

The FIX OEG is a QuickFIX/Python acceptor that receives orders via FIX 4.4 protocol from institutional clients. It validates incoming FIX messages, normalizes them to JSON format, and publishes to the Kafka `orders` topic.

**Supported FIX Messages:**

| Message Type | Tag 35 | Description |
|--------------|--------|-------------|
| NewOrderSingle | D | New order submission |
| OrderCancelRequest | F | Cancel existing order |
| OrderCancelReplaceRequest | G | Modify existing order |

**Key Functions:**
- FIX session management (logon, heartbeat, logout)
- Message validation and normalization
- Order ID generation
- Kafka publishing

---

### 3.4 FIX UI Clients

**Ports:** 5002 (Client 1), 5003 (Client 2)

Web-based FIX initiator clients that connect to the FIX OEG. They provide a user interface for submitting orders via FIX protocol, simulating institutional trading terminals.

**Features:**
- New Order Single submission
- Order cancellation
- Order amendment
- Real-time execution reports

**Configuration Files:**
- `client1.cfg` — FIX session config for Client 1
- `client2.cfg` — FIX session config for Client 2

---

### 3.5 Matcher (Order Matching Engine)

**Port:** 6000

The core matching engine that maintains order books for all securities. It consumes orders from Kafka, attempts to match them using price-time priority, and publishes resulting trades.

**Matching Algorithm:** Price-Time Priority (FIFO)
- Buy orders sorted by price descending (highest first)
- Sell orders sorted by price ascending (lowest first)
- Within same price level, earlier orders have priority

**Persistence:** SQLite database (`/app/data/matcher.db`)
- Order book survives container restarts
- Volume-mapped for data durability

**REST API Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/orderbook/<symbol>` | GET | Get order book depth for symbol |
| `/trades` | GET | Get recent trades |
| `/health` | GET | Health check with statistics |

**Order Book Response Example:**
```json
{
  "symbol": "ALPHA",
  "bids": [
    {"price": 25.45, "quantity": 100, "cl_ord_id": "MDF-123"},
    {"price": 25.40, "quantity": 200, "cl_ord_id": "MDF-124"}
  ],
  "asks": [
    {"price": 25.55, "quantity": 150, "cl_ord_id": "MDF-125"},
    {"price": 25.60, "quantity": 100, "cl_ord_id": "MDF-126"}
  ]
}
```

---

### 3.6 Market Data Feeder (MDF)

**Port:** Internal only

Simulates market data by generating random orders and publishing Best Bid/Offer (BBO) snapshots. Creates realistic market activity with configurable order rates and price movements.

**Order Generation:**
- **90% Passive Orders** — Placed away from mid price to build order book
- **10% Aggressive Orders** — Cross the spread to generate trades

**Price Simulation:**
- Small random drift (±2 ticks) with 10% probability
- Prices bounded by minimum (1.00)
- Half spread: 0.10 (10 cents)

**Output:**
- Orders → Kafka `orders` topic
- Snapshots → Kafka `snapshots` topic

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `ORDERS_PER_MIN` | 8 | Order generation rate |
| `TICK_SIZE` | 0.05 | Minimum price increment |

---

### 3.7 Dashboard

**Port:** 5005

Real-time web dashboard providing comprehensive market visualization. Uses Server-Sent Events (SSE) for live streaming updates without page refresh.

**Technology Stack:**
- Backend: Python/Flask
- Frontend: HTML5/CSS3/JavaScript
- Streaming: Server-Sent Events (SSE)
- Data: Kafka consumer + REST API calls to Matcher

**Features:**

| Panel | Description |
|-------|-------------|
| Orders | Live order feed with Edit/Cancel actions |
| Market Snapshot | BBO table with scrolling ticker tape |
| Trades | Executed trades with value calculation |
| Order Book | Depth of book per symbol |
| Trade Chart | Price line + volume bars visualization |
| Trading Statistics | Aggregated metrics with bar charts |

**SSE Events:**

| Event | Data | Trigger |
|-------|------|---------|
| `connected` | Status | Initial connection |
| `init` | Full state | On connect |
| `order` | Order JSON | New order received |
| `trade` | Trade JSON | Trade executed |
| `snapshot` | BBO JSON | Price update |

---

### 3.8 Frontend (Manual Order Entry)

**Port:** 5000

Simple web interface for manual order submission. Allows users to enter orders directly without FIX protocol.

**Features:**
- Symbol selection dropdown
- Side selection (BUY/SELL)
- Price and quantity input
- Submit order to Kafka

---

### 3.9 Consumer (Debug Utility)

**Port:** Internal only

Debug utility that consumes and logs messages from Kafka `trades` topic. Outputs trade information to console for monitoring.

**Output Format:**
```
TRADE: ALPHA - 100 @ 25.50
TRADE: EXAE - 50 @ 42.10
```

---

### 3.10 Snapshot Viewer

**Port:** Internal only

Utility service that subscribes to the `snapshots` topic and logs BBO updates. Writes to log files in `/app/logs` for analysis.

---

## 4. Dashboard User Interface

### 4.1 Orders Panel

Displays real-time incoming orders with full management capabilities.

| Column | Description |
|--------|-------------|
| Symbol | Stock ticker (ALPHA, EXAE, PEIR, QUEST) |
| Side | BUY (green) or SELL (red) |
| Qty | Order quantity in shares |
| Price | Limit price |
| Source | Order origin (MDF, FIX, Manual) |
| Time | Order timestamp |
| Actions | Edit / Cancel buttons |

**Order Actions:**
- **Edit** — Opens modal to modify quantity and/or price (sends amend to Kafka)
- **Cancel** — Sends cancel request to Kafka
- **Row Selection** — Click row to select, use header buttons for actions

---

### 4.2 Market Snapshot

Shows Best Bid/Offer (BBO) for all securities with real-time updates.

| Column | Description |
|--------|-------------|
| Symbol | Security identifier |
| Best Bid | Highest buy price (green) |
| Best Ask | Lowest sell price (red) |
| Spread | Ask - Bid difference |
| Mid | Midpoint: (Bid + Ask) / 2 |
| Updated | Last update timestamp |

**Ticker Tape:**
- Scrolling bar at bottom shows recent trades
- ▲ Green: Price up from previous
- ▼ Red: Price down from previous
- ● Yellow: No change
- Hover to pause scrolling

---

### 4.3 Trades Panel

Lists all executed trades with calculated values.

| Column | Description |
|--------|-------------|
| Symbol | Traded security |
| Qty | Executed quantity |
| Price | Execution price |
| Value | Trade value (Qty × Price) |
| Time | Execution timestamp |

---

### 4.4 Order Book

Displays full market depth for selected symbol.

**Controls:**
- **Symbol Dropdown** — Select security to view
- **Refresh Button** — Manual refresh (auto-refreshes every 3 seconds)

**Display:**
- **Left Side:** Bid Qty | Bid Price (green, sorted price descending)
- **Right Side:** Ask Price | Ask Qty (red, sorted price ascending)
- **Header:** Shows count of bids and asks

---

### 4.5 Trade Chart

Visual representation of trade activity over time.

**Elements:**
- **Green Line** — Price trend connecting trade execution prices
- **Blue Bars** — Volume per trade
- **Y-Axis** — Price scale
- **X-Axis** — Trade sequence (last 100 trades)

**Controls:**
- Symbol dropdown to filter by security or view all

---

### 4.6 Trading Statistics

Aggregated metrics calculated from all trades.

| Metric | Description | Formula |
|--------|-------------|---------|
| Trades | Count of executed trades | COUNT(*) |
| Volume | Total shares traded | Σ Quantity |
| Value | Total monetary value | Σ (Qty × Price) |
| Start | First trade price | First price in session |
| Last | Most recent price | Latest price |
| VWAP | Volume-Weighted Average Price | Σ(Qty × Price) / Σ Qty |

**Bar Charts:**
- Grouped by symbol showing Volume (green) and Value (blue) side by side
- Normalized to maximum value in dataset

---

## 5. Data Flow & Messaging

### 5.1 Order Entry Flow

```
   ┌──────────────┐
   │  FIX Client  │
   └──────┬───────┘
          │ FIX 4.4 NewOrderSingle (35=D)
          ▼
   ┌──────────────┐
   │   FIX OEG    │  Validate → Normalize → Generate cl_ord_id
   └──────┬───────┘
          │ JSON
          ▼
   ┌──────────────┐
   │    Kafka     │  Topic: orders
   │   [orders]   │
   └──────┬───────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌───────────┐
│Matcher │  │ Dashboard │
└────────┘  └───────────┘
```

### 5.2 Order Matching Flow

```
                    ┌─────────────────┐
                    │  Incoming Order │
                    │   (from Kafka)  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Parse & Validate│
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
       ┌────────────┐                ┌────────────┐
       │  BUY Order │                │ SELL Order │
       └──────┬─────┘                └──────┬─────┘
              │                             │
              ▼                             ▼
    ┌──────────────────┐         ┌──────────────────┐
    │ Check SELL book  │         │ Check BUY book   │
    │ for price ≤ bid  │         │ for price ≥ ask  │
    └────────┬─────────┘         └────────┬─────────┘
             │                            │
      ┌──────┴──────┐              ┌──────┴──────┐
      ▼             ▼              ▼             ▼
  ┌───────┐    ┌────────┐     ┌───────┐    ┌────────┐
  │ Match │    │No Match│     │ Match │    │No Match│
  │ Found │    │        │     │ Found │    │        │
  └───┬───┘    └───┬────┘     └───┬───┘    └───┬────┘
      │            │              │            │
      ▼            ▼              ▼            ▼
  ┌───────┐    ┌────────┐     ┌───────┐    ┌────────┐
  │Execute│    │Add to  │     │Execute│    │Add to  │
  │ Trade │    │BUY Book│     │ Trade │    │SELLBook│
  └───┬───┘    └────────┘     └───┬───┘    └────────┘
      │                           │
      └───────────┬───────────────┘
                  ▼
           ┌────────────┐
           │Kafka:trades│
           └────────────┘
```

### 5.3 Real-time Dashboard Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         BROWSER                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Dashboard UI                          │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────────┐│   │
│  │  │ Orders  │ │ Trades  │ │  Book   │ │    Statistics   ││   │
│  │  └────▲────┘ └────▲────┘ └────▲────┘ └────────▲────────┘│   │
│  └───────┼───────────┼───────────┼───────────────┼──────────┘   │
│          └───────────┴─────┬─────┴───────────────┘              │
│                    ┌───────▼────────┐                          │
│                    │ EventSource    │  SSE Connection          │
│                    │ /stream        │                          │
│                    └───────┬────────┘                          │
└────────────────────────────┼────────────────────────────────────┘
                             │ HTTP (SSE)
                             ▼
┌────────────────────────────────────────────────────────────────┐
│                      DASHBOARD SERVER                          │
│   ┌──────────────┐      ┌──────────────┐     ┌────────────┐   │
│   │Kafka Consumer│─────▶│ SSE Broadcast│────▶│  Clients   │   │
│   │ (orders,     │      │   Queue      │     │  Queue[]   │   │
│   │  trades,     │      └──────────────┘     └────────────┘   │
│   │  snapshots)  │                                            │
│   └──────────────┘                                            │
└────────────────────────────────────────────────────────────────┘
```

### 5.4 Complete System Interaction

```
          ┌─────────┐ ┌─────────┐ ┌─────────┐
          │FIX Cli 1│ │FIX Cli 2│ │Frontend │
          └────┬────┘ └────┬────┘ └────┬────┘
               └─────┬─────┴───────────┘
                     ▼
              ┌─────────────┐
              │   FIX OEG   │◄──── FIX 4.4 Protocol
              └──────┬──────┘
                     ▼
    ┌────────────────────────────────────┐
    │            KAFKA CLUSTER           │
    │  ┌────────┐┌────────┐┌──────────┐ │
    │  │orders  ││trades  ││snapshots │ │
    │  └───┬────┘└───▲────┘└────▲─────┘ │
    └──────┼─────────┼──────────┼───────┘
           │         │          │
     ┌─────┼─────────┼──────────┼─────┐
     │     ▼         │          │     │
     │ ┌────────┐    │          │     │
     │ │MATCHER │────┘          │     │
     │ │ Book   │               │     │
     │ │ Match  │               │     │
     │ └────────┘               │     │
     │     ▲                    │     │
     │     │ REST               │     │
     │ ┌───┴────────────────────┴──┐  │
     │ │        DASHBOARD          │  │
     │ │   (SSE + Kafka Consumer)  │  │
     │ └───────────────────────────┘  │
     │ ┌───────────────────────────┐  │
     │ │      MD FEEDER (MDF)      │──┘
     │ │   Orders + Snapshots      │
     │ └───────────────────────────┘
     └────────────────────────────────┘
```

### 5.5 Message Sequence: New Order to Trade

```
FIX Client    FIX OEG      Kafka       Matcher     Dashboard
    │            │           │            │            │
    │──35=D────▶│            │            │            │
    │NewOrder   │            │            │            │
    │           │──JSON────▶│            │            │
    │           │  [orders] │            │            │
    │           │           │──consume──▶│            │
    │           │           │            │──match()   │
    │           │           │◀──trade────│            │
    │           │           │  [trades]  │            │
    │           │           │──────────────consume───▶│
    │           │           │            │     render()
```

---

### 5.6 Message Formats

**Order Message:**
```json
{
  "symbol": "ALPHA",
  "side": "BUY",
  "price": 25.50,
  "quantity": 100,
  "cl_ord_id": "MDF-1234567890-1",
  "timestamp": 1234567890.123,
  "source": "MDF"
}
```

**Cancel Message:**
```json
{
  "type": "cancel",
  "orig_cl_ord_id": "MDF-1234567890-1",
  "symbol": "ALPHA",
  "timestamp": 1234567890.456
}
```

**Amend Message:**
```json
{
  "type": "amend",
  "orig_cl_ord_id": "MDF-1234567890-1",
  "cl_ord_id": "amend-1234567890",
  "symbol": "ALPHA",
  "quantity": 150,
  "price": 25.45,
  "timestamp": 1234567890.789
}
```

**Trade Message:**
```json
{
  "symbol": "ALPHA",
  "price": 25.50,
  "quantity": 100,
  "buy_order_id": "order-123",
  "sell_order_id": "order-456",
  "timestamp": 1234567890.123
}
```

**Snapshot Message:**
```json
{
  "symbol": "ALPHA",
  "best_bid": 25.45,
  "best_ask": 25.55,
  "bid_size": 500,
  "ask_size": 300,
  "timestamp": 1234567890.123,
  "source": "MDF"
}
```

---

## 6. Configuration

### 6.1 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | kafka:9092 | Kafka broker address |
| `MATCHER_URL` | http://matcher:6000 | Matcher API endpoint |
| `TICK_SIZE` | 0.05 | Minimum price increment |
| `ORDERS_PER_MIN` | 8 | MDF order generation rate |
| `SECURITIES_FILE` | /app/data/securities.txt | Securities configuration |
| `KAFKA_RETRIES` | 30 | Connection retry attempts |
| `KAFKA_RETRY_DELAY` | 2 | Seconds between retries |

### 6.2 Securities Configuration

File: `shared_data/securities.txt`

```
#SYMBOL	<start_price>	<current_price>
ALPHA	25.00	25.00
PEIR	18.50	18.50
EXAE	42.00	42.00
QUEST	12.75	12.75
```

| Symbol | Start Price | Description |
|--------|-------------|-------------|
| ALPHA | 25.00 | Test Security A |
| EXAE | 42.00 | Test Security B |
| PEIR | 18.50 | Test Security C |
| QUEST | 12.75 | Test Security D |

### 6.3 Docker Volumes

| Volume | Path | Purpose |
|--------|------|---------|
| `matcher_data` | /app/data | SQLite database persistence |
| `shared_data` | /app/data | Securities and order ID files |
| `logs` | /app/logs | Snapshot viewer logs |

---

## 7. Quick Reference

### 7.1 Starting the System

```bash
# Start all services
docker compose up --build

# Start in background
docker compose up -d --build

# View logs
docker compose logs -f dashboard
docker compose logs -f matcher

# Stop all services
docker compose down

# Reset matcher database
docker volume rm stockex_matcher_data
```

### 7.2 Dashboard Actions

| Action | How To |
|--------|--------|
| View order book depth | Select symbol from Order Book dropdown |
| Edit an order | Click Edit button on order row |
| Cancel an order | Click Cancel button on order row |
| Filter trade chart | Select symbol from Trade Chart dropdown |
| Pause ticker tape | Hover mouse over the ticker |
| Select order row | Click on the row |
| Bulk actions | Select row, use header Edit/Cancel buttons |

### 7.3 Connection Status

| Status | Indicator | Meaning |
|--------|-----------|---------|
| Live | ● Green | Connected to SSE stream |
| Connecting | ● Yellow | Establishing connection |
| Disconnected | ● Red | Connection lost, auto-reconnecting |

### 7.4 API Endpoints

**Matcher API (Port 6000):**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/orderbook/<symbol>` | GET | Order book for symbol |
| `/trades` | GET | Recent trades list |
| `/health` | GET | Service health status |

**Dashboard API (Port 5005):**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard page |
| `/data` | GET | Current state (polling fallback) |
| `/stream` | GET | SSE event stream |
| `/orderbook/<symbol>` | GET | Proxy to matcher |
| `/order/cancel` | POST | Cancel order request |
| `/order/amend` | POST | Amend order request |

---

## 8. Troubleshooting

### Orders/Trades Panel Empty

1. Check Kafka connection:
   ```bash
   docker logs dashboard | grep "Kafka"
   ```
2. Verify MDF is producing:
   ```bash
   docker logs stockex-md_feeder-1 --tail 10
   ```
3. Restart dashboard:
   ```bash
   docker restart dashboard
   ```

### Order Book Empty

1. Check matcher is receiving orders:
   ```bash
   docker logs matcher | grep "received"
   ```
2. Verify matcher Kafka consumer connected:
   ```bash
   docker logs matcher | grep "consumer connected"
   ```
3. Reset matcher database if corrupted:
   ```bash
   docker compose down
   docker volume rm stockex_matcher_data
   docker compose up -d
   ```

### Connection Status Shows Disconnected

1. SSE stream timeout — will auto-reconnect after 3 seconds
2. Check dashboard container is running:
   ```bash
   docker ps | grep dashboard
   ```
3. Check browser console for errors (F12)

### Prices Going Negative or Extreme

1. Old bug in MDF — update to latest version
2. Reset securities file:
   ```
   #SYMBOL	<start_price>	<current_price>
   ALPHA	25.00	25.00
   PEIR	18.50	18.50
   EXAE	42.00	42.00
   QUEST	12.75	12.75
   ```
3. Restart MDF and matcher:
   ```bash
   docker restart stockex-md_feeder-1 matcher
   ```

---

## Document Information

- **Version:** 1.0
- **Platform:** StockEx Trading Simulation
- **Inspired by:** Euronext OPTIQ

---

*StockEx v1.0 — Trading Simulation Platform*
