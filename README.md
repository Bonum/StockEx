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
Includes a FIX 4.4 order gateway, live matching engine, SSE-streamed dashboard, AI-powered clearing house members, and candlestick charts.

🔗 **Live demo:** [huggingface.co/spaces/RayMelius/StockEx](https://huggingface.co/spaces/RayMelius/StockEx)
📦 **Source:** [github.com/Bonum/StockEx](https://github.com/Bonum/StockEx)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                   Single Docker Container                            │
│                                                                      │
│   nginx :7860 (reverse proxy)                                        │
│       /       → Dashboard        :5000                               │
│       /ch/    → Clearing House   :5004                               │
│       /fix/   → FIX UI           :5002                               │
│       /frontend/ → Order Entry   :5003                               │
│                                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐   │
│  │  MD Feeder  │  │  FIX OEG     │  │  FIX UI    │  │ AI Analyst│   │
│  │ (simulator) │  │  :5001       │  │  :5002     │  │           │   │
│  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘  └─────┬─────┘   │
│         │                │                 │ FIX 4.4       │         │
│         │    ┌───────────┘                 │               │         │
│         ▼    ▼                             │               │         │
│  ┌──────────────────────────────────────────────────────┐  │         │
│  │              Apache Kafka (KRaft)                    │  │         │
│  │  orders │ trades │ snapshots │ control │ ai_insights │  │         │
│  └───────────────────┬──────────────────────────────────┘  │         │
│                      │                                      │         │
│         ┌────────────┘                                      │         │
│         ▼                                                   │         │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────┐ │         │
│  │   Matcher   │  │  Dashboard :5000 │  │ Clearing House │ │         │
│  │  Flask:6000 │  │  SSE + REST API  │  │ :5004          │◄┘         │
│  │  SQLite DB  │  │  SQLite OHLCV    │  │ AI Members     │           │
│  └─────────────┘  └──────────────────┘  └────────────────┘           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Services

| Service | Port | Description |
|---|---|---|
| **nginx** | 7860 | Reverse proxy — single public port |
| **Dashboard** | 5000 | Flask app, SSE streaming, session control, OHLCV history, AI insights |
| **Matcher** | 6000 | Price-time priority matching engine, REST API, SQLite persistence |
| **MD Feeder** | — | Regime-based synthetic market data generator with technical indicators |
| **Clearing House** | 5004 | 10 AI-driven trading members with RL/LLM strategies |
| **AI Analyst** | — | LLM market commentary (Groq → HF → Ollama fallback) |
| **FIX OEG** | 5001 | FIX 4.4 acceptor — translates FIX messages to Kafka orders |
| **FIX UI Client** | 5002 | Browser UI for sending FIX orders and viewing execution reports |
| **Frontend** | 5003 | Simple order entry form |

---

## Clearing House

The Clearing House simulates **10 trading members** (USR01–USR10) that autonomously trade securities using AI strategies.

### Member Details
- Starting capital: **€100,000** each
- Daily obligation: **20 securities** (quantity sum) per trading day
- Password = username (e.g. USR01 logs in with password "USR01")
- End-of-day settlement triggered by Dashboard session end

### AI Trading Strategies

Members use one of three strategy modes, switchable dynamically via `POST /ch/api/strategy`:

| Strategy | `CH_AI_STRATEGY` | Description |
|---|---|---|
| **Hybrid** (default) | `hybrid` | USR01–05 use RL, USR06–10 use LLM |
| **RL only** | `rl` | All members use the RL neural network |
| **LLM only** | `llm` | All members use LLM (Groq → HF → Ollama) |

Every strategy falls back to rule-based trading if the primary method fails.

### RL Agent — Neural Network Details

The RL strategy uses [Adilbai/stock-trading-rl-agent](https://huggingface.co/Adilbai/stock-trading-rl-agent), a **PPO (Proximal Policy Optimization)** model trained with Stable-Baselines3.

| Property | Value |
|---|---|
| **Algorithm** | PPO (Proximal Policy Optimization) |
| **Policy** | MlpPolicy (Multi-Layer Perceptron) |
| **Model size** | ~2.5 MB (`final_model.zip`) |
| **Scaler** | ~50 KB (`scaler.pkl`, StandardScaler) |
| **Observation space** | 3,008 dimensions (60 bars × 50 features + 8 portfolio) |
| **Action space** | 2D: action type (Hold/Buy/Sell) + position size (0–1) |
| **Training steps** | 500,000 |
| **Learning rate** | 0.0003 |
| **Gamma** | 0.99 |
| **License** | MIT (free, runs locally, no API keys needed) |
| **Stored at** | Downloaded to `/app/data/rl_model/` on first use via HuggingFace Hub |

**How the RL agent decides:**

```
Kafka trades → Per-symbol OHLCV bars (60-bar rolling window)
                        ↓
          Technical indicators (50 features per bar):
          SMA(5,10,20,50), EMA(12,26), MACD, Signal, Histogram,
          RSI(14), Bollinger Bands (upper/lower/width/position),
          Volatility(20), Price changes, H/L ratio, Volume ratio,
          + lagged features at 1,2,3,5,10 bars
                        ↓
          Scaler normalizes → 3,000 market features
          + 8 portfolio features (capital, position, net worth, etc.)
                        ↓
          PPO neural network forward pass → [action_type, position_size]
                        ↓
          action_type: 0=Hold, 1=Buy, 2=Sell
          position_size: fraction of capital (Buy) or holdings (Sell)
```

The RL model is a pre-trained neural network, **not** an LLM — it runs a single forward pass through the policy network (~milliseconds) rather than generating text. No API keys or internet required at inference time.

### LLM Strategy

Uses the same fallback chain as the AI Analyst:
- **Groq** (preferred on HF Spaces) → `GROQ_API_KEY` required
- **HuggingFace Inference** → `HF_TOKEN` required
- **Ollama** (preferred locally) → `OLLAMA_HOST` required

Sends a text prompt with member state + market BBOs, expects JSON response with symbol/side/quantity/price.

### Clearing House UI

- **Leaderboard** at `/ch/` — rankings, P&L, obligation status
- **Portfolio** at `/ch/portfolio` — holdings, trades, AI decisions (login required)
- **API**: `/ch/api/leaderboard`, `/ch/api/config`, `/ch/api/strategy`

---

## Market Data Feeder — Regime-Based Simulation

The MD Feeder generates synthetic orders using a **regime-driven price dynamics engine** that produces meaningful technical indicator signals.

### Price Regimes

| Regime | Behavior | Effect on Indicators |
|---|---|---|
| **trending_up** | Positive drift + momentum | MACD > 0, RSI rising, price above SMA |
| **trending_down** | Negative drift + momentum | MACD < 0, RSI falling, price below SMA |
| **mean_revert** | Pulls toward start price | RSI oscillates around 50, BB position ~0.5 |
| **volatile** | Wide swings, expanded spread | BB width expands, RSI spikes, high volatility |
| **calm** | Tight range, narrow spread | BB width contracts, RSI stable, low volatility |

Regimes auto-rotate every 15–50 ticks. When price deviates significantly from start, the engine biases toward mean reversion to prevent runaway prices.

### Snapshots with Embedded Indicators

Each market data snapshot includes computed indicators:

```json
{
  "symbol": "ALPHA",
  "best_bid": 24.85,
  "best_ask": 25.05,
  "indicators": {
    "sma_5": 24.92,
    "sma_20": 24.78,
    "ema_12": 24.88,
    "ema_26": 24.75,
    "macd": 0.13,
    "rsi_14": 62.5,
    "bb_pos": 0.72,
    "regime": "trending_up"
  }
}
```

---

## Features

- **Live Order Book** — best bid/ask with full depth per symbol
- **Trade Feed** — real-time executions pushed via Server-Sent Events
- **Market Snapshot** — BBO table + scrolling ticker tape
- **Price Chart** — candlestick + close-price line + volume bars; Live / 1H / 8H / 1D / 1W / 1M periods
- **All-Symbols View** — normalised % change chart comparing all securities on one axis
- **Trading Statistics** — per-symbol trade count, volume, value, VWAP, bar chart
- **Start / End of Day** — resets opening prices, starts/stops MD simulation
- **Suspend / Resume** — pauses order generation without ending the session
- **Order Management** — cancel and amend resting orders from the dashboard
- **Clearing House** — 10 AI members trading with RL/LLM strategies, leaderboard, portfolio UI
- **AI Analyst** — LLM-generated market commentary with provider switching
- **FIX UI Client** — send NewOrderSingle via FIX 4.4, view execution reports at `/fix/`
- **Mobile Responsive** — single-column layout on phones and tablets

---

## Kafka Topics

| Topic | Description |
|---|---|
| `orders` | Limit orders from all sources (MDF, FIX, Clearing House, Frontend) |
| `trades` | Executed trades from the Matcher |
| `snapshots` | BBO snapshots with technical indicators from MD Feeder |
| `control` | Session control signals (start/stop/suspend/resume) |
| `ai_insights` | AI Analyst market commentary |

---

## Securities

| Symbol | Name | Start Price |
|---|---|---|
| ALPHA | Alpha Bank | €24.95 |
| PEIR | Piraeus Bank | €18.05 |
| EXAE | Athens Exchange Group | €42.05 |
| QUEST | Quest Holdings | €12.60 |
| NBG | National Bank of Greece | €18.05 |
| ATTIK | Attika Bank | €3.95 |
| INTKA | Intertech | €3.95 |
| AEG | AEG | €3.95 |
| AAAK | AAAK | €3.95 |
| EUROB | Eurobank | €3.95 |

---

## Stack

- **Apache Kafka 3.7** (KRaft mode — no ZooKeeper)
- **Python 3.11** · Flask 2.2 · kafka-python 2.0
- **Stable-Baselines3** + PyTorch — RL trading agent (PPO)
- **QuickFIX** (FIX 4.4 protocol, compiled from C++)
- **SQLite** — matcher order/trade persistence + OHLCV history + clearing house DB
- **Canvas 2D API** — candlestick charts rendered client-side
- **Server-Sent Events** — real-time push to browser (no WebSocket)
- **nginx** — reverse proxy with `sub_filter` URL rewriting for `/fix/`
- **LLM integrations** — Groq, HuggingFace Inference, Ollama (local)

---

## Environment Variables

### Core
| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `MATCHER_URL` | `http://matcher:6000` | Matcher service URL |
| `SECURITIES_FILE` | `/app/data/securities.txt` | Securities definition file |

### Clearing House AI
| Variable | Default | Description |
|---|---|---|
| `CH_AI_STRATEGY` | `hybrid` | Trading strategy: `llm`, `rl`, or `hybrid` |
| `CH_AI_INTERVAL` | `45` | Seconds between AI trading cycles |
| `CH_RL_MODEL_REPO` | `Adilbai/stock-trading-rl-agent` | HuggingFace model repo for RL |
| `CH_RL_MIN_BARS` | `30` | Minimum price bars before RL starts |

### LLM Providers
| Variable | Default | Description |
|---|---|---|
| `GROQ_API_KEY` | — | Groq API key (free tier available) |
| `GROQ_MODEL` | `llama-3.1-8b-instant` | Groq model name |
| `HF_TOKEN` | — | HuggingFace API token |
| `HF_MODEL` | `RayMelius/stockex-ch-trader` | HuggingFace model for CH trader |
| `OLLAMA_HOST` | — | Ollama server URL (e.g. `http://localhost:11434`) |
| `OLLAMA_MODEL` | `llama3.1:8b` | Ollama model name |

---

## Deployment

### Option 1 — Use the live HuggingFace Space (zero setup)

Open: **https://huggingface.co/spaces/RayMelius/StockEx**

No account or installation required.

---

### Option 2 — Deploy your own HuggingFace Space

> Pushes to your GitHub `main` branch auto-deploy to HuggingFace via GitHub Actions.

**Step 1 — Fork the repository**

```bash
# On GitHub: click Fork on https://github.com/Bonum/StockEx
```

**Step 2 — Create a HuggingFace Space**

1. Go to [huggingface.co/new-space](https://huggingface.co/new-space)
2. Choose **Docker** SDK
3. Set `App port` to `7860`
4. Note your Space URL: `https://huggingface.co/spaces/<your-username>/StockEx`

**Step 3 — Get a HuggingFace write token**

1. Go to [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)
2. Create a token with **Write** permission
3. Copy the token (starts with `hf_…`)

**Step 4 — Add the token as a GitHub Secret**

1. In your fork: **Settings → Secrets and variables → Actions**
2. Click **New repository secret**
3. Name: `HF_TOKEN` · Value: your token from Step 3

**Step 5 — Update the Space URL in the workflow** *(if your username differs)*

Edit `.github/workflows/deploy-hf.yml`:
```yaml
git remote add huggingface https://<your-hf-username>:${HF_TOKEN}@huggingface.co/spaces/<your-hf-username>/StockEx.git
```

**Step 6 — Push**

```bash
git push origin main
```

GitHub Actions runs the CI tests, then pushes to HuggingFace. The Space builds (~5–15 min on first run due to QuickFIX compilation) and goes live automatically on every subsequent push.

---

### Option 3 — Local with Docker Compose

**Prerequisites:** Docker Desktop

```bash
git clone https://github.com/Bonum/StockEx.git
cd StockEx
docker-compose up
```

| URL | Service |
|---|---|
| http://localhost:5005 | Trading Dashboard |
| http://localhost:5004 | Clearing House |
| http://localhost:5002 | FIX UI Client |
| http://localhost:6000 | Matcher REST API |

Set `CH_AI_STRATEGY=rl` in `.env` or `docker-compose.yml` to use the RL agent for all members.

---

### Option 4 — Local without Docker

**Prerequisites:** Python 3.11+, Apache Kafka running on `localhost:9092`

```bash
git clone https://github.com/Bonum/StockEx.git
cd StockEx
pip install kafka-python Flask requests quickfix stable-baselines3 huggingface_hub pandas scikit-learn

# In separate terminals:
export PYTHONPATH=$(pwd)
export KAFKA_BOOTSTRAP=localhost:9092
export MATCHER_URL=http://localhost:6000

python matcher/matcher.py          # terminal 1
python md_feeder/mdf_simulator.py  # terminal 2
python dashboard/dashboard.py      # terminal 3
cd clearing_house && python app.py # terminal 4
```

Then open http://localhost:5000.

---

## CI / CD

| Trigger | Action |
|---|---|
| Push to `main` | Run matcher unit tests + Docker build check |
| Push to `main` (tests pass) | Auto-deploy to HuggingFace Spaces |
| Pull request to `main` | Run tests only |

GitHub Actions workflows: `.github/workflows/ci.yml` · `.github/workflows/deploy-hf.yml`

---

## Project Structure

```
StockEx/
├── matcher/            # Matching engine + SQLite persistence
├── md_feeder/          # Regime-based synthetic market data generator
├── dashboard/          # Flask dashboard + SSE + OHLCV history
│   └── templates/      # Single-page trading UI
├── clearing_house/     # AI trading members (RL + LLM strategies)
│   ├── app.py          # Flask app, REST API, SSE
│   ├── ch_ai_trader.py # Strategy dispatcher + LLM integration
│   ├── ch_rl_trader.py # RL agent (PPO neural network)
│   ├── ch_database.py  # SQLite: members, trades, settlements
│   └── templates/      # Leaderboard + portfolio UI
├── ai_analyst/         # LLM market commentary service
├── fix_oeg/            # FIX 4.4 Order Entry Gateway
├── fix-ui-client/      # FIX browser UI
├── frontend/           # Simple order entry form
├── shared/             # Shared config + Kafka utils
├── shared_data/        # securities.txt (symbol list + prices)
├── notebooks/          # Fine-tuning notebooks
├── Dockerfile          # HuggingFace / single-container build
├── docker-compose.yml  # Local multi-container dev setup
├── entrypoint.sh       # Container startup (Kafka → services → nginx)
├── kafka-kraft.properties
└── nginx.conf          # Reverse proxy config
```
