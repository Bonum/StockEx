---
title: StockEx Trading Demo
emoji: 📈
colorFrom: green
colorTo: blue
sdk: docker
pinned: false
app_port: 7860
---

# StockEx – Kafka-based Stock Exchange Simulator

A real-time stock exchange simulation built with **Apache Kafka**, **Python**, and **Flask**.

## Architecture

| Service | Description |
|---|---|
| **Matcher** | Order matching engine (price-time priority, limit/market orders) |
| **MD Feeder** | Synthetic market data generator, drives order flow |
| **Dashboard** | Real-time trading dashboard with SSE streaming |

## Features

- **Live Order Book** – best bid/ask with depth
- **Trade Feed** – real-time executions with SSE push
- **Market Snapshot** – BBO per symbol with ticker tape
- **Trade Chart** – live price/volume chart
- **Price History** – OHLCV candlestick chart (1H / 8H / 1D / 1W / 1M)
- **Start / End of Day** – reset prices, pause/resume simulation
- **Order Management** – cancel and amend resting orders

## Securities

`ALPHA` · `PEIR` · `EXAE` · `QUEST` · `NBG`

## Stack

- Apache Kafka (KRaft mode, no ZooKeeper)
- Python 3.11 · Flask · kafka-python
- SQLite (matcher persistence + OHLCV history)
- Canvas API candlestick charts
- Server-Sent Events for real-time UI updates

## Source

GitHub: [github.com/Bonum/StockEx](https://github.com/Bonum/StockEx)
